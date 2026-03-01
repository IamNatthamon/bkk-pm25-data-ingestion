#!/usr/bin/env python3
"""
Test script to verify wind U/V component ingestion
Fetches a small sample (1 day) and validates the new columns
"""

import sys
import time
import json
import gzip
import hashlib
from pathlib import Path
from datetime import datetime, timedelta
import requests
import pandas as pd

# Config
PROJECT_ROOT = Path(__file__).parent
DATA_ROOT = PROJECT_ROOT / "data"
BRONZE_WEATHER = DATA_ROOT / "bronze" / "openmeteo_weather"
SILVER_WEATHER = DATA_ROOT / "silver" / "openmeteo_weather"
OPENMETEO_ARCHIVE_BASE = "https://archive-api.open-meteo.com/v1/archive"

# Test station (Bangkok)
TEST_STATION = {
    "stationID": "test_bkk",
    "lat": 13.7563,
    "lon": 100.5018
}

# Date range (1 day for quick test)
TEST_DATE = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")

PIPELINE_VERSION = "1.0.1"  # Updated version with wind U/V

print("=" * 60)
print("Testing Wind U/V Component Ingestion")
print("=" * 60)
print(f"Station: {TEST_STATION['stationID']} ({TEST_STATION['lat']}, {TEST_STATION['lon']})")
print(f"Date: {TEST_DATE}")
print()

# Step 1: Fetch data from API
print("Step 1: Fetching data from Open-Meteo API...")
params = {
    "latitude": TEST_STATION["lat"],
    "longitude": TEST_STATION["lon"],
    "start_date": TEST_DATE,
    "end_date": TEST_DATE,
    "hourly": "temperature_2m,relative_humidity_2m,surface_pressure,precipitation,wind_speed_10m,wind_direction_10m,shortwave_radiation,cloud_cover,wind_u_component_10m,wind_v_component_10m",
    "timezone": "UTC"
}

try:
    response = requests.get(OPENMETEO_ARCHIVE_BASE, params=params, timeout=30)
    response.raise_for_status()
    raw_data = response.json()
    print(f"✓ API request successful (status {response.status_code})")
except Exception as e:
    print(f"✗ API request failed: {e}")
    sys.exit(1)

# Step 2: Save to Bronze
print("\nStep 2: Saving to Bronze layer...")
load_id = f"test_{int(time.time())}"
bronze_dir = BRONZE_WEATHER / TEST_STATION["stationID"]
bronze_dir.mkdir(parents=True, exist_ok=True)
bronze_path = bronze_dir / f"{TEST_DATE}_{load_id}.json.gz"

try:
    with gzip.open(bronze_path, "wt", encoding="utf-8") as f:
        json.dump(raw_data, f)
    print(f"✓ Bronze saved: {bronze_path}")
except Exception as e:
    print(f"✗ Bronze save failed: {e}")
    sys.exit(1)

# Step 3: Transform to Silver
print("\nStep 3: Transforming to Silver layer...")

def parse_timestamp_to_utc(value):
    """Convert timestamp to UTC"""
    try:
        if isinstance(value, str):
            return pd.to_datetime(value, utc=True)
        return pd.to_datetime(value, unit='s', utc=True)
    except:
        return None

def record_hash(row, columns):
    key = "|".join(str(row.get(c, "")) for c in columns)
    return hashlib.sha256(key.encode()).hexdigest()[:16]

try:
    hourly = raw_data.get("hourly", {})
    times = hourly.get("time", [])
    n = len(times)
    
    if n == 0:
        print("✗ No hourly data returned from API")
        sys.exit(1)
    
    print(f"  Processing {n} hourly records...")
    
    # Create DataFrame
    df = pd.DataFrame({
        "stationID": [TEST_STATION["stationID"]] * n,
        "lat": [TEST_STATION["lat"]] * n,
        "lon": [TEST_STATION["lon"]] * n,
        "timestamp_utc": [parse_timestamp_to_utc(t) for t in times],
        "temp_c": hourly.get("temperature_2m", [None] * n),
        "humidity_pct": hourly.get("relative_humidity_2m", [None] * n),
        "pressure_hpa": hourly.get("surface_pressure", [None] * n),
        "precipitation_mm": hourly.get("precipitation", [None] * n),
        "wind_ms": hourly.get("wind_speed_10m", [None] * n),
        "wind_dir_deg": hourly.get("wind_direction_10m", [None] * n),
        "shortwave_radiation_wm2": hourly.get("shortwave_radiation", [None] * n),
        "cloud_cover_pct": hourly.get("cloud_cover", [None] * n),
    })
    
    # Convert wind U and V components from km/h to m/s
    wind_u_kmh = hourly.get("wind_u_component_10m", [None] * n)
    wind_v_kmh = hourly.get("wind_v_component_10m", [None] * n)
    df["u10_ms"] = [None if v is None else v / 3.6 for v in wind_u_kmh]
    df["v10_ms"] = [None if v is None else v / 3.6 for v in wind_v_kmh]
    
    # Add metadata
    df["timestamp_unix_ms"] = df["timestamp_utc"].astype("int64") // 10**6
    df["data_source"] = "openmeteo_weather"
    df["ingestion_timestamp_utc"] = pd.Timestamp.now(tz="UTC")
    df["load_id"] = load_id
    df["pipeline_version"] = PIPELINE_VERSION
    df["record_hash"] = df.apply(lambda r: record_hash(r, ["stationID", "timestamp_utc"]), axis=1)
    
    # Enforce schema
    WEATHER_SILVER_COLUMNS = [
        "stationID", "lat", "lon", "timestamp_utc", "timestamp_unix_ms",
        "temp_c", "humidity_pct", "pressure_hpa", "precipitation_mm", "wind_ms", "wind_dir_deg",
        "shortwave_radiation_wm2", "cloud_cover_pct", "u10_ms", "v10_ms",
        "data_source", "ingestion_timestamp_utc", "load_id", "pipeline_version", "record_hash"
    ]
    
    WEATHER_SILVER_DTYPES = {
        "stationID": "string", "lat": "float64", "lon": "float64",
        "timestamp_utc": "datetime64[ns, UTC]", "timestamp_unix_ms": "int64",
        "temp_c": "float32", "humidity_pct": "float32", "pressure_hpa": "float32",
        "precipitation_mm": "float32", "wind_ms": "float32", "wind_dir_deg": "float32",
        "shortwave_radiation_wm2": "float32", "cloud_cover_pct": "float32",
        "u10_ms": "float32", "v10_ms": "float32",
        "data_source": "string", "ingestion_timestamp_utc": "datetime64[ns, UTC]",
        "load_id": "string", "pipeline_version": "string", "record_hash": "string"
    }
    
    df = df[WEATHER_SILVER_COLUMNS].copy()
    for col, dtype in WEATHER_SILVER_DTYPES.items():
        if col in ("timestamp_utc", "ingestion_timestamp_utc"):
            df[col] = pd.to_datetime(df[col], utc=True)
        else:
            df[col] = df[col].astype(dtype)
    
    print(f"✓ Transformation successful")
    
except Exception as e:
    print(f"✗ Transformation failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Step 4: Save to Silver
print("\nStep 4: Saving to Silver layer...")
year = pd.to_datetime(TEST_DATE).year
month = pd.to_datetime(TEST_DATE).month

silver_dir = SILVER_WEATHER / f"year={year}" / f"month={month:02d}"
silver_dir.mkdir(parents=True, exist_ok=True)
silver_path = silver_dir / f"test_{TEST_DATE}_{load_id}.parquet"

try:
    df.to_parquet(silver_path, index=False)
    print(f"✓ Silver saved: {silver_path}")
except Exception as e:
    print(f"✗ Silver save failed: {e}")
    sys.exit(1)

# Step 5: Validation
print("\nStep 5: Validating results...")
print("-" * 60)

# Check schema
print(f"Schema validation:")
print(f"  Total columns: {len(df.columns)}")
print(f"  Expected columns: {len(WEATHER_SILVER_COLUMNS)}")
if set(df.columns) == set(WEATHER_SILVER_COLUMNS):
    print(f"  ✓ All columns present")
else:
    print(f"  ✗ Column mismatch!")
    print(f"    Missing: {set(WEATHER_SILVER_COLUMNS) - set(df.columns)}")
    print(f"    Extra: {set(df.columns) - set(WEATHER_SILVER_COLUMNS)}")

# Check new columns
print(f"\nWind U/V component validation:")
print(f"  u10_ms dtype: {df['u10_ms'].dtype}")
print(f"  v10_ms dtype: {df['v10_ms'].dtype}")
print(f"  u10_ms null count: {df['u10_ms'].isna().sum()}/{len(df)}")
print(f"  v10_ms null count: {df['v10_ms'].isna().sum()}/{len(df)}")

if df['u10_ms'].dtype == 'float32' and df['v10_ms'].dtype == 'float32':
    print(f"  ✓ Correct data types")
else:
    print(f"  ✗ Incorrect data types!")

# Show sample data
print(f"\nSample data (first 3 rows):")
print(df[["timestamp_utc", "wind_ms", "wind_dir_deg", "u10_ms", "v10_ms"]].head(3).to_string(index=False))

# Check conversion
print(f"\nConversion validation (km/h → m/s):")
if len(wind_u_kmh) > 0 and wind_u_kmh[0] is not None:
    original_u = wind_u_kmh[0]
    converted_u = df["u10_ms"].iloc[0]
    expected_u = original_u / 3.6
    print(f"  U component: {original_u:.2f} km/h → {converted_u:.4f} m/s (expected: {expected_u:.4f})")
    if abs(converted_u - expected_u) < 0.001:
        print(f"  ✓ Conversion correct")
    else:
        print(f"  ✗ Conversion incorrect!")

if len(wind_v_kmh) > 0 and wind_v_kmh[0] is not None:
    original_v = wind_v_kmh[0]
    converted_v = df["v10_ms"].iloc[0]
    expected_v = original_v / 3.6
    print(f"  V component: {original_v:.2f} km/h → {converted_v:.4f} m/s (expected: {expected_v:.4f})")
    if abs(converted_v - expected_v) < 0.001:
        print(f"  ✓ Conversion correct")
    else:
        print(f"  ✗ Conversion incorrect!")

print("\n" + "=" * 60)
print("✓ Test completed successfully!")
print("=" * 60)
print(f"\nFiles created:")
print(f"  Bronze: {bronze_path}")
print(f"  Silver: {silver_path}")
print(f"\nYou can now use this updated schema in your production pipeline.")
