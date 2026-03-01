#!/usr/bin/env python3
"""
Run Air Quality backfill for 2023+ to data/silver/openmeteo_airquality.
Uses fixed Open-Meteo API params: nitrogen_dioxide, ozone, sulphur_dioxide, carbon_monoxide.

Usage:
  cd /path/to/project  # e.g. Desktop/bkk-pm25-data-ingestion
  python run_aq_backfill_2023.py

Output: data/silver/openmeteo_airquality/year=YYYY/month=MM/
"""
from __future__ import annotations

import gc
import gzip
import hashlib
import json
import logging
import random
import time
import uuid
from pathlib import Path
from typing import Any

import pandas as pd
import requests

# Config
PROJECT_ROOT = Path.cwd()
DATA_ROOT = PROJECT_ROOT / "data"
BRONZE_AQ = DATA_ROOT / "bronze" / "openmeteo_airquality"
SILVER_AQ = DATA_ROOT / "silver" / "openmeteo_airquality"
STATIONS_PATH = DATA_ROOT / "stations" / "bangkok_stations.parquet"
CHECKPOINT_ROOT = PROJECT_ROOT / "checkpoints"
LOGS_DIR = PROJECT_ROOT / "logs"

OPENMETEO_AQ_BASE = "https://air-quality-api.open-meteo.com/v1/air-quality"
AQ_HISTORICAL_START = "2013-01-01"
BACKFILL_START = "2023-01-01T00:00:00Z"
CHUNK_MONTHS = 3
STATION_BATCH_SIZE = 20
REQUEST_DELAY_SEC = 1.0
MAX_RETRIES = 5
BASE_BACKOFF_SEC = 2
JITTER_MIN, JITTER_MAX = 1, 3
PIPELINE_VERSION = "1.0.0"

AQ_SILVER_COLUMNS = [
    "stationID", "lat", "lon", "timestamp_utc", "timestamp_unix_ms",
    "pm2_5_ugm3", "pm10_ugm3", "no2_ugm3", "o3_ugm3", "so2_ugm3", "co_ugm3",
    "data_source", "ingestion_timestamp_utc", "load_id", "pipeline_version", "record_hash",
]
AQ_SILVER_DTYPES = {
    "stationID": "string", "lat": "float64", "lon": "float64",
    "timestamp_utc": "datetime64[ns, UTC]", "timestamp_unix_ms": "int64",
    "pm2_5_ugm3": "float32", "pm10_ugm3": "float32", "no2_ugm3": "float32",
    "o3_ugm3": "float32", "so2_ugm3": "float32", "co_ugm3": "float32",
    "data_source": "string", "ingestion_timestamp_utc": "datetime64[ns, UTC]",
    "load_id": "string", "pipeline_version": "string", "record_hash": "string",
}


def setup_logging(load_id: str) -> logging.Logger:
    logger = logging.getLogger("aq_backfill")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    fh = logging.FileHandler(LOGS_DIR / f"aq_backfill_{load_id}.log", encoding="utf-8")
    fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
    logger.addHandler(fh)
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
    logger.addHandler(ch)
    return logger


def parse_timestamp_to_utc(value: Any) -> pd.Timestamp | None:
    if value is None:
        return None
    try:
        ts = pd.to_datetime(value, utc=True)
        return ts.tz_localize("UTC") if ts.tz is None else ts
    except Exception:
        return None


def record_hash(row: pd.Series, columns: list[str]) -> str:
    key = "|".join(str(row.get(c, "")) for c in columns)
    return hashlib.sha256(key.encode()).hexdigest()[:16]


def now_utc() -> pd.Timestamp:
    return pd.Timestamp.now(tz="UTC")


def get_backfill_chunks() -> list[tuple[str, str]]:
    start = pd.Timestamp(BACKFILL_START, tz="UTC")
    end = now_utc()
    chunks = []
    s = start
    while s < end:
        e = s + pd.DateOffset(months=CHUNK_MONTHS) - pd.Timedelta(seconds=1)
        if e > end:
            e = end
        chunks.append((s.strftime("%Y-%m-%dT%H:%M:%SZ"), e.strftime("%Y-%m-%dT%H:%M:%SZ")))
        s = s + pd.DateOffset(months=CHUNK_MONTHS)
    return chunks


def fetch_aq_station(lat: float, lon: float, start: str, end: str, log: logging.Logger, chunk_label: str | None = None) -> dict | None:
    if end[:10] < AQ_HISTORICAL_START:
        log.info("Historical limit: end %s < %s — skip", end[:10], AQ_HISTORICAL_START)
        return None
    params = {
        "latitude": lat, "longitude": lon,
        "start_date": start[:10], "end_date": end[:10],
        "hourly": "pm2_5,pm10,nitrogen_dioxide,ozone,sulphur_dioxide,carbon_monoxide",
        "timezone": "UTC",
    }
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(OPENMETEO_AQ_BASE, params=params, timeout=60)
            if r.status_code == 200:
                return r.json()
            if 400 <= r.status_code < 500:
                log.warning("[%s] HTTP %s AQ — skip. %s", chunk_label or "?", r.status_code, (r.text or "")[:200])
                return None
            if r.status_code == 429:
                backoff = 60 + random.uniform(JITTER_MIN, JITTER_MAX)
                log.warning("Rate limit 429; sleeping %.1fs", backoff)
                time.sleep(backoff)
                continue
            backoff = BASE_BACKOFF_SEC ** (attempt + 1) + random.uniform(JITTER_MIN, JITTER_MAX)
            log.warning("HTTP %s, retry in %.1fs", r.status_code, backoff)
            time.sleep(backoff)
        except Exception as e:
            log.warning("Request failed: %s", e)
            time.sleep(BASE_BACKOFF_SEC ** (attempt + 1) + random.uniform(JITTER_MIN, JITTER_MAX))
    return None


def raw_aq_to_silver(raw: dict, station_id: str, lat: float, lon: float, load_id: str, ingestion_ts: pd.Timestamp) -> pd.DataFrame | None:
    try:
        hourly = raw.get("hourly") or {}
        if "time" not in hourly:
            return None
        times = hourly["time"]
        n = len(times)
        df = pd.DataFrame({
            "stationID": [station_id] * n,
            "lat": [lat] * n, "lon": [lon] * n,
            "timestamp_utc": [parse_timestamp_to_utc(t) for t in times],
            "pm2_5_ugm3": hourly.get("pm2_5", [None] * n),
            "pm10_ugm3": hourly.get("pm10", [None] * n),
            "no2_ugm3": hourly.get("nitrogen_dioxide", hourly.get("no2", [None] * n)),
            "o3_ugm3": hourly.get("ozone", hourly.get("o3", [None] * n)),
            "so2_ugm3": hourly.get("sulphur_dioxide", hourly.get("so2", [None] * n)),
            "co_ugm3": hourly.get("carbon_monoxide", hourly.get("co", [None] * n)),
        })
    except Exception as e:
        return None
    df = df.dropna(subset=["timestamp_utc"])
    if df.empty:
        return None
    df["timestamp_unix_ms"] = df["timestamp_utc"].astype("int64") // 10**6
    df["data_source"] = "openmeteo_airquality"
    df["ingestion_timestamp_utc"] = ingestion_ts
    df["load_id"] = load_id
    df["pipeline_version"] = PIPELINE_VERSION
    df["record_hash"] = df.apply(lambda r: record_hash(r, ["stationID", "timestamp_utc"]), axis=1)
    for col in AQ_SILVER_COLUMNS:
        if col not in df.columns:
            return None
    df = df[AQ_SILVER_COLUMNS].copy()
    for col, dtype in AQ_SILVER_DTYPES.items():
        if col in ("timestamp_utc", "ingestion_timestamp_utc"):
            df[col] = pd.to_datetime(df[col], utc=True)
        else:
            df[col] = df[col].astype(dtype)
    return df


def write_bronze(payload: dict, year: int, month: int, chunk_id: str, batch_id: str) -> Path:
    dir_path = BRONZE_AQ / str(year) / f"{month:02d}"
    dir_path.mkdir(parents=True, exist_ok=True)
    path = dir_path / f"batch_{chunk_id}_{batch_id}.json.gz"
    with gzip.open(path, "wt", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)
    return path


def write_silver_partition(df: pd.DataFrame, partition_year: int, partition_month: int) -> Path | None:
    part_dir = SILVER_AQ / f"year={partition_year}" / f"month={partition_month:02d}"
    part_dir.mkdir(parents=True, exist_ok=True)
    tmp_dir = SILVER_AQ / "tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    safe_id = f"{partition_year}{partition_month:02d}_{id(df)}_{int(pd.Timestamp.now().value)}"
    tmp_path = tmp_dir / f"part_{safe_id}.parquet"
    try:
        df.to_parquet(tmp_path, index=False)
        final_path = part_dir / f"part_{safe_id}.parquet"
        tmp_path.rename(final_path)
        return final_path
    except Exception:
        if tmp_path.exists():
            tmp_path.unlink()
        return None


def checkpoint_path(chunk_id: str, batch_id: str, year: int) -> Path:
    return CHECKPOINT_ROOT / f"year={year}" / f"chunk={chunk_id}" / f"batch={batch_id}.done"


def checkpoint_exists(chunk_id: str, batch_id: str, year: int) -> bool:
    return checkpoint_path(chunk_id, batch_id, year).exists()


def write_checkpoint(chunk_id: str, batch_id: str, year: int) -> None:
    p = checkpoint_path(chunk_id, batch_id, year)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.touch()


def run_backfill() -> None:
    for d in (BRONZE_AQ, SILVER_AQ, CHECKPOINT_ROOT, LOGS_DIR, STATIONS_PATH.parent):
        d.mkdir(parents=True, exist_ok=True)

    load_id = str(uuid.uuid4())
    log = setup_logging(load_id)
    log.info("AQ Backfill 2023+ load_id=%s", load_id)

    stations = pd.read_parquet(STATIONS_PATH)
    stations["stationID"] = stations["stationID"].astype("string")
    log.info("Stations: %d", len(stations))

    chunks = get_backfill_chunks()
    log.info("Chunks: %d (from %s)", len(chunks), BACKFILL_START[:10])

    ingestion_ts = now_utc()

    for chunk_idx, (start, end) in enumerate(chunks):
        chunk_id = start[:10].replace("-", "")
        chunk_label = pd.to_datetime(start).strftime("%b %Y")
        year_start = int(start[:4])

        log.info("--- [%s] Chunk %s ---", chunk_label, chunk_id)

        for batch_start in range(0, len(stations), STATION_BATCH_SIZE):
            batch = stations.iloc[batch_start : batch_start + STATION_BATCH_SIZE]
            batch_id = f"{batch_start:04d}"

            if checkpoint_exists(chunk_id, batch_id, year_start):
                log.info("Skip chunk=%s batch=%s (checkpoint)", chunk_id, batch_id)
                continue

            batch_had_data = False
            for _, row in batch.iterrows():
                sid, lat, lon = str(row["stationID"]), float(row["lat"]), float(row["lon"])
                raw_aq = fetch_aq_station(lat, lon, start, end, log, chunk_label=chunk_label)
                if REQUEST_DELAY_SEC > 0:
                    time.sleep(REQUEST_DELAY_SEC)
                if raw_aq:
                    batch_had_data = True
                    write_bronze(raw_aq, year_start, int(start[5:7]), chunk_id, batch_id)
                    df_aq = raw_aq_to_silver(raw_aq, sid, lat, lon, load_id, ingestion_ts)
                    if df_aq is not None and not df_aq.empty:
                        for (py, pm), g in df_aq.groupby([df_aq["timestamp_utc"].dt.year, df_aq["timestamp_utc"].dt.month]):
                            write_silver_partition(g, int(py), int(pm))
                    del df_aq
                del raw_aq
                gc.collect()

            write_checkpoint(chunk_id, batch_id, year_start)

    log.info("AQ Backfill complete. Silver: %s", SILVER_AQ)


if __name__ == "__main__":
    run_backfill()
