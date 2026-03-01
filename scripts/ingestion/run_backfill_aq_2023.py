#!/usr/bin/env python3
"""
Backfill Air Quality Data for Year 2023

Usage:
    python run_backfill_aq_2023.py
    
This script will:
1. Load 79 Bangkok stations
2. Fetch hourly AQ data from Open-Meteo API (2023-01-01 → 2023-12-31)
3. Save to Bronze (JSON.gz) and Silver (Parquet)
4. Match the format of existing 2024-2026 data
"""

from __future__ import annotations

import sys

sys.stdout.reconfigure(line_buffering=True)

import gzip
import hashlib
import json
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import polars as pl
import requests

PIPELINE_VERSION = "2.0.0"
PROJECT_ROOT = Path.cwd()

BRONZE_AQ = PROJECT_ROOT / "data" / "bronze" / "openmeteo_airquality"
SILVER_AQ = PROJECT_ROOT / "data" / "silver" / "openmeteo_airquality"
STATIONS_PATH = PROJECT_ROOT / "data" / "stations" / "bangkok_stations.parquet"
CHECKPOINT_DIR = PROJECT_ROOT / "checkpoints"
LOGS_DIR = PROJECT_ROOT / "logs"

OPENMETEO_AQ_BASE = "https://air-quality-api.open-meteo.com/v1/air-quality"

TARGET_YEAR = 2023
START_DATE = f"{TARGET_YEAR}-01-01"
END_DATE = f"{TARGET_YEAR}-12-31"

CHUNK_DAYS = 7
REQUEST_DELAY_SEC = 1.0
MAX_RETRIES = 5
BASE_BACKOFF_SEC = 2

BRONZE_AQ.mkdir(parents=True, exist_ok=True)
SILVER_AQ.mkdir(parents=True, exist_ok=True)
CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)


def fetch_airquality_data(
    lat: float,
    lon: float,
    start_date: str,
    end_date: str,
    retries: int = MAX_RETRIES,
) -> dict[str, Any] | None:
    """Fetch air quality data from Open-Meteo API"""
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": "pm2_5,pm10,nitrogen_dioxide,ozone,sulphur_dioxide,carbon_monoxide",
        "timezone": "GMT",
    }

    for attempt in range(retries):
        try:
            response = requests.get(OPENMETEO_AQ_BASE, params=params, timeout=30)

            if response.status_code == 200:
                return response.json()
            elif response.status_code == 400:
                print(f"⚠️  API error 400: {response.text[:200]}")
                return None
            elif response.status_code in [429, 503]:
                wait_time = BASE_BACKOFF_SEC * (2**attempt)
                print(f"⏳ Rate limit, waiting {wait_time}s...")
                time.sleep(wait_time)
            else:
                print(f"❌ HTTP {response.status_code}")
                return None

        except requests.exceptions.RequestException as e:
            print(f"❌ Request failed (attempt {attempt + 1}/{retries}): {e}")
            if attempt < retries - 1:
                time.sleep(BASE_BACKOFF_SEC * (2**attempt))

    return None


def save_bronze(data: dict, station_id: str, date_str: str, batch_id: str) -> Path:
    """Save raw JSON response to Bronze layer"""
    year, month = date_str[:4], date_str[5:7]
    bronze_dir = BRONZE_AQ / year / month
    bronze_dir.mkdir(parents=True, exist_ok=True)

    filename = f"batch_{date_str.replace('-', '')}_{batch_id}.json.gz"
    filepath = bronze_dir / filename

    with gzip.open(filepath, "wt", encoding="utf-8") as f:
        json.dump(data, f)

    return filepath


def transform_to_silver(bronze_data: dict, station_id: str, load_id: str) -> pl.DataFrame:
    """Transform Bronze JSON to Silver Parquet schema"""
    hourly = bronze_data.get("hourly", {})
    times = hourly.get("time", [])

    if not times:
        return pl.DataFrame()

    records = []
    for i, time_str in enumerate(times):
        record = {
            "stationID": station_id,
            "lat": bronze_data["latitude"],
            "lon": bronze_data["longitude"],
            "timestamp_utc": time_str,
            "timestamp_unix_ms": int(
                pd.Timestamp(time_str, tz="UTC").timestamp() * 1000
            ),
            "pm2_5_ugm3": hourly.get("pm2_5", [])[i]
            if i < len(hourly.get("pm2_5", []))
            else None,
            "pm10_ugm3": hourly.get("pm10", [])[i]
            if i < len(hourly.get("pm10", []))
            else None,
            "nitrogen_dioxide_ugm3": hourly.get("nitrogen_dioxide", [])[i]
            if i < len(hourly.get("nitrogen_dioxide", []))
            else None,
            "ozone_ugm3": hourly.get("ozone", [])[i]
            if i < len(hourly.get("ozone", []))
            else None,
            "sulphur_dioxide_ugm3": hourly.get("sulphur_dioxide", [])[i]
            if i < len(hourly.get("sulphur_dioxide", []))
            else None,
            "carbon_monoxide_ugm3": hourly.get("carbon_monoxide", [])[i]
            if i < len(hourly.get("carbon_monoxide", []))
            else None,
            "data_source": "open-meteo-airquality",
            "ingestion_timestamp_utc": datetime.utcnow().isoformat() + "Z",
            "load_id": load_id,
            "pipeline_version": PIPELINE_VERSION,
        }

        record_str = f"{station_id}_{time_str}_{record['pm2_5_ugm3']}"
        record["record_hash"] = hashlib.md5(record_str.encode()).hexdigest()

        records.append(record)

    df = pl.DataFrame(records)
    df = df.with_columns(
        pl.col("timestamp_utc")
        .str.to_datetime("%Y-%m-%dT%H:%M")
        .dt.replace_time_zone("UTC")
    )

    return df


def save_silver(df: pl.DataFrame, year: int, month: int) -> Path | None:
    """Save to Silver layer with Hive partitioning"""
    if df.is_empty():
        return None

    silver_dir = SILVER_AQ / f"year={year}" / f"month={month:02d}"
    silver_dir.mkdir(parents=True, exist_ok=True)

    timestamp_ns = int(time.time() * 1_000_000_000)
    filename = f"part_{year}{month:02d}_{id(df)}_{timestamp_ns}.parquet"
    filepath = silver_dir / filename

    df.write_parquet(filepath, compression="snappy")

    md5_hash = hashlib.md5(filepath.read_bytes()).hexdigest()
    md5_path = filepath.with_suffix(".parquet.md5")
    md5_path.write_text(f"{md5_hash}  {filepath.name}\n")

    return filepath


def generate_date_chunks(
    start_date: str, end_date: str, chunk_days: int
) -> list[tuple[str, str]]:
    """Generate date range chunks"""
    start = pd.Timestamp(start_date)
    end = pd.Timestamp(end_date)

    chunks = []
    current = start

    while current <= end:
        chunk_end = min(current + timedelta(days=chunk_days - 1), end)
        chunks.append((current.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d")))
        current = chunk_end + timedelta(days=1)

    return chunks


def main() -> None:
    """Main execution"""
    print("\n" + "=" * 100, flush=True)
    print(" " * 30 + "AIR QUALITY BACKFILL 2023", flush=True)
    print("=" * 100, flush=True)

    stations_df = pl.read_parquet(STATIONS_PATH)
    print(f"✅ Loaded {len(stations_df)} stations", flush=True)

    date_chunks = generate_date_chunks(START_DATE, END_DATE, CHUNK_DAYS)
    print(f"✅ Generated {len(date_chunks)} date chunks (7-day windows)", flush=True)

    load_id = str(uuid.uuid4())
    total_requests = len(date_chunks) * len(stations_df)

    print(f"\n📊 Execution Plan:", flush=True)
    print(f"   Load ID: {load_id}", flush=True)
    print(f"   Year: {TARGET_YEAR}", flush=True)
    print(f"   Total API requests: {total_requests:,}", flush=True)
    print(f"   Estimated time: ~{total_requests * REQUEST_DELAY_SEC / 60:.1f} minutes", flush=True)
    print("=" * 100 + "\n", flush=True)

    start_time = time.time()
    success_count = 0
    error_count = 0
    bronze_files = []

    for chunk_idx, (chunk_start, chunk_end) in enumerate(date_chunks):
        print(f"\n📅 Chunk {chunk_idx + 1}/{len(date_chunks)}: {chunk_start} → {chunk_end}", flush=True)

        batch_id = f"{chunk_idx:04d}"
        chunk_dfs = []

        for station_idx, station_row in enumerate(stations_df.iter_rows(named=True)):
            station_id = station_row["stationID"]
            lat = station_row["lat"]
            lon = station_row["lon"]

            if (station_idx + 1) % 10 == 0:
                print(f"  📍 Progress: {station_idx + 1}/{len(stations_df)} stations", flush=True)

            data = fetch_airquality_data(lat, lon, chunk_start, chunk_end)

            if data:
                bronze_path = save_bronze(data, station_id, chunk_start, batch_id)
                bronze_files.append(bronze_path)

                silver_df = transform_to_silver(data, station_id, load_id)
                if not silver_df.is_empty():
                    chunk_dfs.append(silver_df)

                success_count += 1
            else:
                error_count += 1

            time.sleep(REQUEST_DELAY_SEC)

        if chunk_dfs:
            combined_df = pl.concat(chunk_dfs)

            year = int(chunk_start[:4])
            month = int(chunk_start[5:7])
            silver_path = save_silver(combined_df, year, month)

            if silver_path:
                print(f"  ✅ Silver: {silver_path.name} ({len(combined_df):,} rows)", flush=True)

        elapsed = time.time() - start_time
        progress_pct = (chunk_idx + 1) / len(date_chunks) * 100
        print(f"  ⏱️  {progress_pct:.1f}% | Elapsed: {elapsed / 60:.1f}m", flush=True)

    elapsed_total = time.time() - start_time

    print("\n" + "=" * 100)
    print(" " * 35 + "BACKFILL COMPLETE")
    print("=" * 100)
    print(f"✅ Successful: {success_count:,} / {total_requests:,}")
    print(f"❌ Failed: {error_count:,}")
    print(f"📁 Bronze files: {len(bronze_files):,}")
    print(f"⏱️  Total time: {elapsed_total / 60:.1f} minutes")
    print("=" * 100)

    log_entry = {
        "load_id": load_id,
        "pipeline_version": PIPELINE_VERSION,
        "target_year": TARGET_YEAR,
        "success_count": success_count,
        "error_count": error_count,
        "elapsed_seconds": elapsed_total,
        "completed_at": datetime.utcnow().isoformat() + "Z",
    }

    log_path = LOGS_DIR / f"backfill_aq_{TARGET_YEAR}_{load_id[:8]}.json"
    with open(log_path, "w") as f:
        json.dump(log_entry, f, indent=2)

    print(f"\n📝 Log: {log_path}")

    print("\n" + "=" * 100)
    print("VERIFICATION")
    print("=" * 100)

    silver_2023 = SILVER_AQ / "year=2023"
    if silver_2023.exists():
        parquet_files = [
            f for f in silver_2023.rglob("*.parquet") if not f.name.endswith(".md5")
        ]
        print(f"✅ Created {len(parquet_files)} Parquet files in year=2023")

        if parquet_files:
            df_all = pl.read_parquet(list(silver_2023.rglob("*.parquet")))
            print(f"   Total rows: {len(df_all):,}")
            print(
                f"   Date range: {df_all['timestamp_utc'].min()} → {df_all['timestamp_utc'].max()}"
            )
            print(f"   Stations: {df_all['stationID'].n_unique()}")
            print(
                f"   PM2.5 available: {(1 - df_all['pm2_5_ugm3'].null_count() / len(df_all)) * 100:.1f}%"
            )
    else:
        print("❌ No data in year=2023")

    print("\n✅ Backfill complete! Next: Run Silver → Gold pipeline\n")


if __name__ == "__main__":
    main()
