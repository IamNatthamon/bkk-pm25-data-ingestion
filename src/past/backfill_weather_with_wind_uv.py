#!/usr/bin/env python3
"""
Re-fetch ALL weather data (2010 → present) with wind U/V components.

Re-fetches bronze files using the EXACT same structure as the existing ingestion:
  data/bronze/openmeteo_weather/{YEAR}/{MONTH:02d}/batch_{YYYYMMDD}_{station_idx:04d}.json.gz

Chunk size: quarterly (3 months per file, same as existing data)
Stations:   ALL 79 stations from bangkok_stations.parquet
Resume:     Skips files that already contain wind_u_component_10m

Usage:
    python3 backfill_weather_with_wind_uv.py                          # 2010 → last year
    python3 backfill_weather_with_wind_uv.py --start-year 2019        # from 2019
    python3 backfill_weather_with_wind_uv.py --start-year 2020 --end-year 2023
    python3 backfill_weather_with_wind_uv.py --dry-run                # preview without fetching
"""

from __future__ import annotations

import argparse
import gzip
import json
import logging
import random
import sys
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import requests

# ── logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)
sys.stdout.reconfigure(line_buffering=True)

# ── paths ─────────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).parent
DATA_ROOT = PROJECT_ROOT / "data"
BRONZE_WEATHER = DATA_ROOT / "bronze" / "openmeteo_weather"
STATIONS_PATH = DATA_ROOT / "stations" / "bangkok_stations.parquet"

# ── API ───────────────────────────────────────────────────────────────────────
OPENMETEO_ARCHIVE_BASE = "https://archive-api.open-meteo.com/v1/archive"
HOURLY_PARAMS = (
    "temperature_2m,relative_humidity_2m,surface_pressure,precipitation,"
    "wind_speed_10m,wind_direction_10m,shortwave_radiation,cloud_cover,"
    "wind_u_component_10m,wind_v_component_10m"
)

# ── retry / throttle ──────────────────────────────────────────────────────────
# Open-Meteo Archive: ~600 calls/min; quarterly requests count as 3+ calls each.
# Use 4–5s between requests to stay under limit.
MAX_RETRIES = 5
BASE_BACKOFF_SEC = 2.0
REQUEST_DELAY_SEC = 4.0       # sleep between every request to avoid 429
COOLDOWN_AFTER_429_SEC = 60   # wait 1 min after 429 before retry (let limit reset)

LAST_REQUEST_TIME = 0.0
MIN_INTERVAL = 4.0

# ── quarterly chunk boundaries (month_start, month_end) ──────────────────────
QUARTERS: list[tuple[int, int]] = [(1, 3), (4, 6), (7, 9), (10, 12)]

session = requests.Session()


# ─────────────────────────────────────────────────────────────────────────────
# helpers
# ─────────────────────────────────────────────────────────────────────────────

def last_day_of_month(year: int, month: int) -> int:
    """Return the last calendar day for a given year/month."""
    import calendar
    return calendar.monthrange(year, month)[1]


def build_quarters(start_year: int, end_year: int) -> list[tuple[str, str]]:
    """
    Return list of (start_date, end_date) quarterly chunks from start_year to end_year.
    Each chunk covers exactly one calendar quarter.
    """
    chunks: list[tuple[str, str]] = []
    for year in range(start_year, end_year + 1):
        for q_start, q_end in QUARTERS:
            start = date(year, q_start, 1)
            end = date(year, q_end, last_day_of_month(year, q_end))
            if end > date.today():
                end = date.today() - __import__("datetime").timedelta(days=1)
            if start > end:
                continue
            chunks.append((start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")))
    return chunks


def bronze_path(year: int, quarter_month: int, station_idx: int) -> Path:
    """Return the bronze file path for a given quarter-start and station index."""
    return (
        BRONZE_WEATHER
        / str(year)
        / f"{quarter_month:02d}"
        / f"batch_{year}{quarter_month:02d}01_{station_idx:04d}.json.gz"
    )


def has_wind_uv(filepath: Path) -> bool:
    """Return True if the file already contains wind_u_component_10m (was re-fetched with UV)."""
    try:
        with gzip.open(filepath, "rt", encoding="utf-8") as f:
            data = json.load(f)
        return "wind_u_component_10m" in data.get("hourly", {})
    except Exception:
        return False


def fetch_quarter(
    lat: float,
    lon: float,
    start_date: str,
    end_date: str,
    station_id: str,
) -> dict[str, Any] | None:
    """
    Fetch one quarterly chunk from Open-Meteo Archive API.
    Retries up to MAX_RETRIES with exponential backoff on 429 / server errors.
    """
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": HOURLY_PARAMS,
        "timezone": "UTC",
    }

    for attempt in range(MAX_RETRIES):
        try:
            global LAST_REQUEST_TIME
            now = time.time()
            elapsed = now - LAST_REQUEST_TIME
            if elapsed < MIN_INTERVAL:
                time.sleep(MIN_INTERVAL - elapsed)
            LAST_REQUEST_TIME = time.time()

            resp = session.get(OPENMETEO_ARCHIVE_BASE, params=params, timeout=60)

            if resp.status_code == 200:
                return resp.json()

            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                if retry_after:
                    wait = float(retry_after)
                else:
                    wait = BASE_BACKOFF_SEC * (2 ** attempt)
                wait += random.uniform(0, 1.0)
                wait = max(wait, COOLDOWN_AFTER_429_SEC)  # min 60s to let limit reset
                log.warning(
                    "429 rate-limit  station=%s  attempt=%d/%d  waiting=%.0fs (cooldown)",
                    station_id, attempt + 1, MAX_RETRIES, wait,
                )
                time.sleep(wait)
                LAST_REQUEST_TIME = time.time()  # so next request respects MIN_INTERVAL
                continue

            if resp.status_code >= 500:
                wait = BASE_BACKOFF_SEC * (2 ** attempt)
                log.warning(
                    "HTTP %d server-error  station=%s  attempt=%d/%d  waiting=%.0fs",
                    resp.status_code, station_id, attempt + 1, MAX_RETRIES, wait,
                )
                time.sleep(wait)
                continue

            # 4xx (not 429) — no point retrying
            log.error("HTTP %d  station=%s  %s→%s  skipping", resp.status_code, station_id, start_date, end_date)
            return None

        except requests.exceptions.Timeout:
            wait = BASE_BACKOFF_SEC * (2 ** attempt)
            log.warning("Timeout  station=%s  attempt=%d/%d  waiting=%.0fs", station_id, attempt + 1, MAX_RETRIES, wait)
            time.sleep(wait)
        except Exception as exc:
            wait = BASE_BACKOFF_SEC * (2 ** attempt)
            log.warning("Error  station=%s  %s  attempt=%d/%d  waiting=%.0fs", station_id, exc, attempt + 1, MAX_RETRIES, wait)
            time.sleep(wait)

    log.error("FAILED after %d retries  station=%s  %s→%s", MAX_RETRIES, station_id, start_date, end_date)
    return None


def save_bronze(data: dict[str, Any], out_path: Path) -> None:
    """Write JSON data to a gzipped file, creating parent directories as needed."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(out_path, "wt", encoding="utf-8") as f:
        json.dump(data, f)


# ─────────────────────────────────────────────────────────────────────────────
# main
# ─────────────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Re-fetch weather data for ALL years with wind U/V components",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--start-year", type=int, default=2010, help="First year to fetch (default: 2010)")
    parser.add_argument("--end-year", type=int, default=date.today().year - 1, help="Last year to fetch (default: last year)")
    parser.add_argument("--stations", help="Comma-separated stationIDs to process (default: all)")
    parser.add_argument("--dry-run", action="store_true", help="Print what would be done without fetching")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    log.info("=" * 70)
    log.info("Weather backfill: wind U/V components  (%d → %d)", args.start_year, args.end_year)
    log.info("=" * 70)

    # ── load stations ─────────────────────────────────────────────────────────
    if not STATIONS_PATH.exists():
        log.error("Stations file not found: %s", STATIONS_PATH)
        sys.exit(1)

    stations_df = pd.read_parquet(STATIONS_PATH)
    stations_df["stationID"] = stations_df["stationID"].astype(str)

    if args.stations:
        filter_ids = {s.strip() for s in args.stations.split(",")}
        stations_df = stations_df[stations_df["stationID"].isin(filter_ids)].reset_index(drop=True)

    total_stations = len(stations_df)
    log.info("Stations: %d", total_stations)

    # ── build work list ───────────────────────────────────────────────────────
    quarters = build_quarters(args.start_year, args.end_year)
    total_tasks = total_stations * len(quarters)
    log.info("Quarters: %d (%d → %d Q4)", len(quarters), args.start_year, args.end_year)
    log.info("Total tasks (station × quarter): %d", total_tasks)

    # ── scan for already-done tasks ───────────────────────────────────────────
    done = skipped_uv = skipped_no_uv_will_refetch = 0
    for q_start, _ in quarters:
        year = int(q_start[:4])
        month = int(q_start[5:7])
        for idx, row in stations_df.iterrows():
            fp = bronze_path(year, month, int(idx))
            if fp.exists() and has_wind_uv(fp):
                done += 1

    log.info("Already fetched (have wind U/V): %d  |  Remaining: %d", done, total_tasks - done)

    if args.dry_run:
        log.info("[DRY-RUN] No files will be written.")
        return

    # ── main loop ─────────────────────────────────────────────────────────────
    completed = 0
    skipped = 0
    failed = 0
    task_num = 0

    for q_idx, (q_start, q_end) in enumerate(quarters, 1):
        year = int(q_start[:4])
        month = int(q_start[5:7])
        q_number = (month - 1) // 3 + 1

        quarter_fetched = 0
        quarter_skipped = 0
        quarter_failed = 0

        log.info("=== START QUARTER %d-Q%d (%s → %s) ===", year, q_number, q_start, q_end)
        log.info("Stations: %d", total_stations)

        for s_idx, (_, row) in enumerate(stations_df.iterrows(), 1):
            station_id = str(row["stationID"])
            lat = float(row["lat"])
            lon = float(row["lon"])
            station_idx = int(stations_df[stations_df["stationID"] == station_id].index[0])

            task_num += 1
            fp = bronze_path(year, month, station_idx)

            # ── resume: skip if already has wind U/V ──────────────────────────
            if fp.exists() and has_wind_uv(fp):
                skipped += 1
                quarter_skipped += 1
                log.debug(
                    "[%d/%d] SKIP  %s  %s→%s  (u10/v10 present)",
                    task_num, total_tasks, station_id, q_start, q_end,
                )
                continue

            # ── fetch ─────────────────────────────────────────────────────────
            log.info(
                "[%d/%d] FETCH  %-10s  %s→%s  %s",
                task_num, total_tasks, station_id, q_start, q_end, fp.name,
            )

            data = fetch_quarter(lat, lon, q_start, q_end, station_id)

            if data is None:
                failed += 1
                quarter_failed += 1
                log.warning("[%d/%d] FAILED  %s  %s→%s", task_num, total_tasks, station_id, q_start, q_end)
                continue

            # verify response has hourly data + wind U/V (required for this backfill)
            hourly = data.get("hourly") or {}
            hourly_time = hourly.get("time") or []
            has_uv = "wind_u_component_10m" in hourly
            if not hourly_time or not has_uv:
                failed += 1
                quarter_failed += 1
                log.warning(
                    "[%d/%d] INCOMPLETE  %s  %s→%s  (no hourly or missing wind_u) — will retry",
                    task_num, total_tasks, station_id, q_start, q_end,
                )
                continue

            n_records = len(hourly_time)
            save_bronze(data, fp)
            completed += 1
            quarter_fetched += 1

            log.info(
                "[%d/%d] SAVED  %-10s  %s→%s  n=%d  u10v10=%s  → %s",
                task_num, total_tasks, station_id, q_start, q_end,
                n_records, "YES" if has_uv else "MISSING",
                fp.relative_to(PROJECT_ROOT),
            )

            # ── throttle ──────────────────────────────────────────────────────
            time.sleep(REQUEST_DELAY_SEC)

        log.info(
            "Quarter summary:\nfetched=%d\nskipped=%d\nfailed=%d",
            quarter_fetched, quarter_skipped, quarter_failed,
        )
        log.info("=== END QUARTER %d-Q%d ===", year, q_number)

        # ── quarter progress summary ──────────────────────────────────────────
        log.info(
            "── Quarter %d/%d done (%s→%s)  fetched=%d  skipped=%d  failed=%d ──",
            q_idx, len(quarters), q_start, q_end, completed, skipped, failed,
        )

    # ── final summary ─────────────────────────────────────────────────────────
    log.info("=" * 70)
    log.info("DONE  fetched=%d  skipped=%d  failed=%d  total=%d", completed, skipped, failed, total_tasks)
    log.info("Bronze dir: %s", BRONZE_WEATHER)
    log.info("=" * 70)


if __name__ == "__main__":
    main()
