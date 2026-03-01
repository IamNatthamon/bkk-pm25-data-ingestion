#!/usr/bin/env python3
"""
Fast Backfill Air Quality Data for Year 2023 (with async requests)

Usage:
    python run_backfill_aq_2023_fast.py
"""

from __future__ import annotations

import asyncio
import gzip
import hashlib
import json
import sys
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

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

TARGET_YEAR = 2023
START_DATE = f"{TARGET_YEAR}-01-01"
END_DATE = f"{TARGET_YEAR}-12-31"

CHUNK_DAYS = 30
MAX_CONCURRENT = 10
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3

BRONZE_AQ.mkdir(parents=True, exist_ok=True)
SILVER_AQ.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)


async def fetch_airquality_data_async(
    client: httpx.AsyncClient,
    lat: float,
    lon: float,
    start_date: str,
    end_date: str,
    station_id: str,
) -> tuple[str, dict[str, Any] | None]:
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
                await asyncio.sleep(2 ** attempt)
            else:
                print(f"⚠️  {station_id}: HTTP {response.status_code}", flush=True)
                return (station_id, None)

        except Exception as e:
            if attempt == MAX_RETRIES - 1:
                print(f"❌ {station_id}: {e}", flush=True)
                return (station_id, None)
            await asyncio.sleep(2 ** attempt)

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


async def process_chunk(
    stations_df: pl.DataFrame,
    chunk_start: str,
    chunk_end: str,
    chunk_idx: int,
    load_id: str,
) -> tuple[int, int, list[Path]]:
    """Process one date chunk with async requests"""
    print(f"\n📅 Chunk {chunk_idx}: {chunk_start} → {chunk_end}", flush=True)

    batch_id = f"{chunk_idx:04d}"
    success = 0
    errors = 0
    bronze_files = []
    chunk_dfs = []

    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
        tasks = []
        for station_row in stations_df.iter_rows(named=True):
            task = fetch_airquality_data_async(
                client,
                station_row["lat"],
                station_row["lon"],
                chunk_start,
                chunk_end,
                station_row["stationID"],
            )
            tasks.append(task)

        results = await asyncio.gather(*tasks)

        for station_id, data in results:
            if data:
                bronze_path = save_bronze(data, station_id, chunk_start, batch_id)
                bronze_files.append(bronze_path)

                silver_df = transform_to_silver(data, station_id, load_id)
                if not silver_df.is_empty():
                    chunk_dfs.append(silver_df)

                success += 1
            else:
                errors += 1

    if chunk_dfs:
        combined_df = pl.concat(chunk_dfs)

        year = int(chunk_start[:4])
        month = int(chunk_start[5:7])
        silver_path = save_silver(combined_df, year, month)

        if silver_path:
            print(
                f"  ✅ Silver: {silver_path.name} ({len(combined_df):,} rows)",
                flush=True,
            )

    print(f"  📊 Success: {success}/{len(stations_df)} | Errors: {errors}", flush=True)

    return (success, errors, bronze_files)


async def main_async() -> None:
    """Main async execution"""
    print("\n" + "=" * 100, flush=True)
    print(" " * 25 + "FAST AIR QUALITY BACKFILL 2023 (ASYNC)", flush=True)
    print("=" * 100, flush=True)

    stations_df = pl.read_parquet(STATIONS_PATH)
    print(f"✅ Loaded {len(stations_df)} stations", flush=True)

    date_chunks = generate_date_chunks(START_DATE, END_DATE, CHUNK_DAYS)
    print(f"✅ Generated {len(date_chunks)} date chunks ({CHUNK_DAYS}-day windows)", flush=True)

    load_id = str(uuid.uuid4())
    total_requests = len(date_chunks) * len(stations_df)

    print(f"\n📊 Execution Plan:", flush=True)
    print(f"   Load ID: {load_id}", flush=True)
    print(f"   Year: {TARGET_YEAR}", flush=True)
    print(f"   Total API requests: {total_requests:,}", flush=True)
    print(f"   Max concurrent: {MAX_CONCURRENT}", flush=True)
    print(f"   Estimated time: ~{total_requests / MAX_CONCURRENT / 60:.1f} minutes", flush=True)
    print("=" * 100 + "\n", flush=True)

    start_time = time.time()
    total_success = 0
    total_errors = 0
    all_bronze_files = []

    for chunk_idx, (chunk_start, chunk_end) in enumerate(date_chunks, 1):
        success, errors, bronze_files = await process_chunk(
            stations_df, chunk_start, chunk_end, chunk_idx, load_id
        )

        total_success += success
        total_errors += errors
        all_bronze_files.extend(bronze_files)

        elapsed = time.time() - start_time
        progress_pct = chunk_idx / len(date_chunks) * 100
        print(f"  ⏱️  {progress_pct:.1f}% | Elapsed: {elapsed / 60:.1f}m", flush=True)

    elapsed_total = time.time() - start_time

    print("\n" + "=" * 100, flush=True)
    print(" " * 35 + "BACKFILL COMPLETE", flush=True)
    print("=" * 100, flush=True)
    print(f"✅ Successful: {total_success:,} / {total_requests:,}", flush=True)
    print(f"❌ Failed: {total_errors:,}", flush=True)
    print(f"📁 Bronze files: {len(all_bronze_files):,}", flush=True)
    print(f"⏱️  Total time: {elapsed_total / 60:.1f} minutes", flush=True)
    print("=" * 100, flush=True)

    log_entry = {
        "load_id": load_id,
        "pipeline_version": PIPELINE_VERSION,
        "target_year": TARGET_YEAR,
        "success_count": total_success,
        "error_count": total_errors,
        "elapsed_seconds": elapsed_total,
        "completed_at": datetime.utcnow().isoformat() + "Z",
    }

    log_path = LOGS_DIR / f"backfill_aq_{TARGET_YEAR}_{load_id[:8]}.json"
    with open(log_path, "w") as f:
        json.dump(log_entry, f, indent=2)

    print(f"\n📝 Log: {log_path}", flush=True)

    print("\n" + "=" * 100, flush=True)
    print("VERIFICATION", flush=True)
    print("=" * 100, flush=True)

    silver_2023 = SILVER_AQ / "year=2023"
    if silver_2023.exists():
        parquet_files = [
            f for f in silver_2023.rglob("*.parquet") if not f.name.endswith(".md5")
        ]
        print(f"✅ Created {len(parquet_files)} Parquet files in year=2023", flush=True)

        if parquet_files:
            df_all = pl.read_parquet(list(silver_2023.rglob("*.parquet")))
            print(f"   Total rows: {len(df_all):,}", flush=True)
            print(
                f"   Date range: {df_all['timestamp_utc'].min()} → {df_all['timestamp_utc'].max()}",
                flush=True,
            )
            print(f"   Stations: {df_all['stationID'].n_unique()}", flush=True)
            print(
                f"   PM2.5 available: {(1 - df_all['pm2_5_ugm3'].null_count() / len(df_all)) * 100:.1f}%",
                flush=True,
            )
    else:
        print("❌ No data in year=2023", flush=True)

    print("\n✅ Backfill complete! Next: Run Silver → Gold pipeline\n", flush=True)


def main() -> None:
    """Entry point"""
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
