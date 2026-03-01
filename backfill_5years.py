#!/usr/bin/env python3
"""
Backfill Air Quality Data for 5 Years (2019-2023)

This script fetches historical air quality data from Open-Meteo API
for Bangkok stations covering years 2019, 2020, 2021, 2022.
Year 2023 is already complete.
"""

from __future__ import annotations

import asyncio
import gzip
import hashlib
import json
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path

import pandas as pd
import polars as pl
import httpx

sys.stdout.reconfigure(line_buffering=True)

PIPELINE_VERSION = "2.0.0"
PROJECT_ROOT = Path.cwd()

BRONZE_AQ = PROJECT_ROOT / "data" / "bronze" / "openmeteo_airquality"
SILVER_AQ = PROJECT_ROOT / "data" / "silver" / "openmeteo_airquality"
STATIONS_PATH = PROJECT_ROOT / "data" / "stations" / "bangkok_stations.parquet"
LOGS_DIR = PROJECT_ROOT / "logs"

OPENMETEO_AQ_BASE = "https://air-quality-api.open-meteo.com/v1/air-quality"

# Years to backfill (2019-2022, since 2023 is already done)
TARGET_YEARS = [2019, 2020, 2021, 2022]

CHUNK_MONTHS = 3  # Process 3 months at a time
MAX_CONCURRENT = 10
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3

BRONZE_AQ.mkdir(parents=True, exist_ok=True)
SILVER_AQ.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)


async def fetch_data_async(
    client: httpx.AsyncClient,
    lat: float,
    lon: float,
    start_date: str,
    end_date: str,
    station_id: str,
) -> tuple[str, dict | None]:
    """Fetch air quality data asynchronously"""
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": "pm2_5,pm10,nitrogen_dioxide,ozone,sulphur_dioxide,carbon_monoxide",
        "timezone": "GMT",
    }

    for attempt in range(MAX_RETRIES):
        try:
            response = await client.get(OPENMETEO_AQ_BASE, params=params)

            if response.status_code == 200:
                return (station_id, response.json())
            elif response.status_code == 429:
                await asyncio.sleep(2**attempt)
            else:
                if attempt == MAX_RETRIES - 1:
                    print(
                        f"⚠️  {station_id} ({start_date}): HTTP {response.status_code}",
                        flush=True,
                    )
                return (station_id, None)

        except Exception as e:
            if attempt == MAX_RETRIES - 1:
                print(f"❌ {station_id} ({start_date}): {e}", flush=True)
            await asyncio.sleep(2**attempt)

    return (station_id, None)


def save_bronze(data: dict, station_id: str, date_str: str, batch_id: str) -> Path:
    """Save raw JSON response to Bronze layer"""
    year, month = date_str[:4], date_str[5:7]
    bronze_dir = BRONZE_AQ / year / month
    bronze_dir.mkdir(parents=True, exist_ok=True)

    filename = f"batch_{date_str.replace('-', '')}_{batch_id}_{station_id}.json.gz"
    filepath = bronze_dir / filename

    with gzip.open(filepath, "wt", encoding="utf-8") as f:
        json.dump(data, f)

    return filepath


def transform_to_silver(
    bronze_data: dict, station_id: str, load_id: str
) -> pl.DataFrame:
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
            "ingestion_timestamp_utc": datetime.utcnow(),
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

    # Cast to Float64 for consistency
    df = df.with_columns(
        [
            pl.col("pm2_5_ugm3").cast(pl.Float64),
            pl.col("pm10_ugm3").cast(pl.Float64),
            pl.col("nitrogen_dioxide_ugm3").cast(pl.Float64),
            pl.col("ozone_ugm3").cast(pl.Float64),
            pl.col("sulphur_dioxide_ugm3").cast(pl.Float64),
            pl.col("carbon_monoxide_ugm3").cast(pl.Float64),
        ]
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


def generate_month_ranges(year: int) -> list[tuple[str, str]]:
    """Generate (start_date, end_date) tuples for each month in a year"""
    ranges = []
    for month in range(1, 13):
        start = f"{year}-{month:02d}-01"

        # Calculate last day of month
        if month == 12:
            end = f"{year}-{month:02d}-31"
        else:
            next_month = pd.Timestamp(f"{year}-{month+1:02d}-01")
            last_day = (next_month - pd.Timedelta(days=1)).day
            end = f"{year}-{month:02d}-{last_day:02d}"

        ranges.append((start, end))

    return ranges


async def process_year_month(
    stations_df: pl.DataFrame,
    year: int,
    month: int,
    start_date: str,
    end_date: str,
    load_id: str,
) -> tuple[int, int]:
    """Process one month of data"""
    print(
        f"\n📅 Processing {year}-{month:02d}: {start_date} → {end_date}", flush=True
    )

    batch_id = f"{year}{month:02d}"
    success = 0
    errors = 0
    chunk_dfs = []

    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
        tasks = []
        for station_row in stations_df.iter_rows(named=True):
            task = fetch_data_async(
                client,
                station_row["lat"],
                station_row["lon"],
                start_date,
                end_date,
                station_row["stationID"],
            )
            tasks.append(task)

        results = await asyncio.gather(*tasks)

        for station_id, data in results:
            if data:
                save_bronze(data, station_id, start_date, batch_id)

                silver_df = transform_to_silver(data, station_id, load_id)
                if not silver_df.is_empty():
                    chunk_dfs.append(silver_df)

                success += 1
            else:
                errors += 1

    if chunk_dfs:
        combined_df = pl.concat(chunk_dfs)
        silver_path = save_silver(combined_df, year, month)

        if silver_path:
            print(
                f"  ✅ Silver: {len(combined_df):,} rows | {combined_df['stationID'].n_unique()} stations",
                flush=True,
            )

    print(f"  📊 Success: {success}/{len(stations_df)} | Errors: {errors}", flush=True)

    return (success, errors)


async def main_async() -> None:
    """Main async execution"""
    print("\n" + "=" * 100, flush=True)
    print(" " * 25 + "🌫️ AIR QUALITY 5-YEAR BACKFILL (2019-2022)", flush=True)
    print("=" * 100, flush=True)

    stations_df = pl.read_parquet(STATIONS_PATH)
    print(f"✅ Loaded {len(stations_df)} stations", flush=True)

    load_id = str(uuid.uuid4())
    total_requests = len(TARGET_YEARS) * 12 * len(stations_df)

    print(f"\n📊 Execution Plan:", flush=True)
    print(f"   Load ID: {load_id}", flush=True)
    print(f"   Years: {', '.join(map(str, TARGET_YEARS))}", flush=True)
    print(f"   Total months: {len(TARGET_YEARS) * 12}", flush=True)
    print(f"   Total API requests: {total_requests:,}", flush=True)
    print(
        f"   Estimated time: ~{total_requests / MAX_CONCURRENT / 60:.1f} minutes",
        flush=True,
    )
    print("=" * 100 + "\n", flush=True)

    start_time = time.time()
    total_success = 0
    total_errors = 0

    for year in TARGET_YEARS:
        print(f"\n{'='*100}", flush=True)
        print(f"  🗓️  YEAR {year}", flush=True)
        print(f"{'='*100}", flush=True)

        month_ranges = generate_month_ranges(year)

        for month, (start_date, end_date) in enumerate(month_ranges, 1):
            success, errors = await process_year_month(
                stations_df, year, month, start_date, end_date, load_id
            )

            total_success += success
            total_errors += errors

            # Small delay between months to avoid rate limiting
            await asyncio.sleep(0.5)

        elapsed = time.time() - start_time
        print(
            f"\n  ⏱️  Year {year} complete | Elapsed: {elapsed / 60:.1f}m", flush=True
        )

    elapsed_total = time.time() - start_time

    print("\n" + "=" * 100, flush=True)
    print(" " * 35 + "BACKFILL COMPLETE", flush=True)
    print("=" * 100, flush=True)
    print(f"✅ Successful: {total_success:,} / {total_requests:,}", flush=True)
    print(f"❌ Failed: {total_errors:,}", flush=True)
    print(f"⏱️  Total time: {elapsed_total / 60:.1f} minutes", flush=True)
    print("=" * 100, flush=True)

    log_entry = {
        "load_id": load_id,
        "pipeline_version": PIPELINE_VERSION,
        "target_years": TARGET_YEARS,
        "success_count": total_success,
        "error_count": total_errors,
        "elapsed_seconds": elapsed_total,
        "completed_at": datetime.utcnow().isoformat() + "Z",
    }

    log_path = LOGS_DIR / f"backfill_5years_{load_id[:8]}.json"
    with open(log_path, "w") as f:
        json.dump(log_entry, f, indent=2)

    print(f"\n📝 Log: {log_path}", flush=True)
    print("\n✅ 5-year backfill complete! Data ready for 2019-2023.\n", flush=True)


def main() -> None:
    """Entry point"""
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
