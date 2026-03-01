#!/usr/bin/env python3
"""Backfill missing months (Jan & Mar 2023)"""

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

OPENMETEO_AQ_BASE = "https://air-quality-api.open-meteo.com/v1/air-quality"

MISSING_MONTHS = [
    ("2023-01-01", "2023-01-31"),
    ("2023-03-01", "2023-03-31"),
]

async def fetch_data(client, lat, lon, start, end, station_id):
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start,
        "end_date": end,
        "hourly": "pm2_5,pm10,nitrogen_dioxide,ozone,sulphur_dioxide,carbon_monoxide",
        "timezone": "GMT",
    }
    
    try:
        response = await client.get(OPENMETEO_AQ_BASE, params=params, timeout=30)
        if response.status_code == 200:
            return (station_id, response.json())
    except:
        pass
    return (station_id, None)


def transform(data, station_id, load_id):
    hourly = data.get("hourly", {})
    times = hourly.get("time", [])
    
    if not times:
        return pl.DataFrame()
    
    records = []
    for i, time_str in enumerate(times):
        record = {
            "stationID": station_id,
            "lat": data["latitude"],
            "lon": data["longitude"],
            "timestamp_utc": time_str,
            "timestamp_unix_ms": int(pd.Timestamp(time_str, tz="UTC").timestamp() * 1000),
            "pm2_5_ugm3": hourly.get("pm2_5", [])[i] if i < len(hourly.get("pm2_5", [])) else None,
            "pm10_ugm3": hourly.get("pm10", [])[i] if i < len(hourly.get("pm10", [])) else None,
            "nitrogen_dioxide_ugm3": hourly.get("nitrogen_dioxide", [])[i] if i < len(hourly.get("nitrogen_dioxide", [])) else None,
            "ozone_ugm3": hourly.get("ozone", [])[i] if i < len(hourly.get("ozone", [])) else None,
            "sulphur_dioxide_ugm3": hourly.get("sulphur_dioxide", [])[i] if i < len(hourly.get("sulphur_dioxide", [])) else None,
            "carbon_monoxide_ugm3": hourly.get("carbon_monoxide", [])[i] if i < len(hourly.get("carbon_monoxide", [])) else None,
            "data_source": "open-meteo-airquality",
            "ingestion_timestamp_utc": datetime.utcnow(),  # Use datetime object
            "load_id": load_id,
            "pipeline_version": PIPELINE_VERSION,
        }
        
        record_str = f"{station_id}_{time_str}_{record['pm2_5_ugm3']}"
        record["record_hash"] = hashlib.md5(record_str.encode()).hexdigest()
        records.append(record)
    
    df = pl.DataFrame(records)
    df = df.with_columns(
        pl.col("timestamp_utc").str.to_datetime("%Y-%m-%dT%H:%M").dt.replace_time_zone("UTC")
    )
    
    # Cast float columns to Float64
    df = df.with_columns([
        pl.col("pm2_5_ugm3").cast(pl.Float64),
        pl.col("pm10_ugm3").cast(pl.Float64),
        pl.col("nitrogen_dioxide_ugm3").cast(pl.Float64),
        pl.col("ozone_ugm3").cast(pl.Float64),
        pl.col("sulphur_dioxide_ugm3").cast(pl.Float64),
        pl.col("carbon_monoxide_ugm3").cast(pl.Float64),
    ])
    
    return df


async def main():
    print("\n🔧 Backfilling missing months (Jan & Mar 2023)...\n", flush=True)
    
    stations = pl.read_parquet(STATIONS_PATH)
    load_id = str(uuid.uuid4())
    
    for start_date, end_date in MISSING_MONTHS:
        print(f"📅 Processing: {start_date} → {end_date}", flush=True)
        
        async with httpx.AsyncClient(timeout=30) as client:
            tasks = [
                fetch_data(client, row["lat"], row["lon"], start_date, end_date, row["stationID"])
                for row in stations.iter_rows(named=True)
            ]
            results = await asyncio.gather(*tasks)
        
        dfs = []
        for station_id, data in results:
            if data:
                df = transform(data, station_id, load_id)
                if not df.is_empty():
                    dfs.append(df)
        
        if dfs:
            combined = pl.concat(dfs)
            year = int(start_date[:4])
            month = int(start_date[5:7])
            
            silver_dir = SILVER_AQ / f"year={year}" / f"month={month:02d}"
            silver_dir.mkdir(parents=True, exist_ok=True)
            
            filepath = silver_dir / f"part_{year}{month:02d}_{int(time.time() * 1e9)}.parquet"
            combined.write_parquet(filepath, compression="snappy")
            
            print(f"  ✅ Saved: {len(combined):,} rows\n", flush=True)
    
    print("✅ Missing months backfilled!\n", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
