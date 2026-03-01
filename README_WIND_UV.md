# Wind U/V Components - Quick Start Guide

## Overview

The weather ingestion pipeline has been updated to include wind U and V components (u10_ms, v10_ms). This guide shows you how to use the updated pipeline.

## What Changed?

### New Columns in Weather Data

| Column | Type | Unit | Description |
|--------|------|------|-------------|
| `u10_ms` | float32 | m/s | Wind U component at 10m (eastward) |
| `v10_ms` | float32 | m/s | Wind V component at 10m (northward) |

### Why Wind U/V?

Wind U and V components provide:
- **Vector wind analysis**: Better for interpolation and spatial analysis
- **Atmospheric modeling**: Required for many meteorological models
- **Accurate calculations**: Can reconstruct wind speed and direction without ambiguity

**Relationship:**
```python
wind_speed = sqrt(u10_ms² + v10_ms²)
wind_direction = atan2(-u10_ms, -v10_ms) * 180/π
```

## Quick Start

### 1. Test the New Schema

Run the test script to verify everything works:

```bash
cd /Users/ahcint1n/Desktop/bkk-pm25-data-ingestion
python3 test_wind_uv_ingestion.py
```

Expected output:
```
✓ API request successful
✓ Bronze layer saved
✓ Transformation successful
✓ Silver layer saved
✓ All columns present
✓ Correct data types
✓ Conversion correct
```

### 2. Backfill Historical Data

To re-ingest weather data with wind U/V components:

```bash
# Backfill 2024 data
python3 backfill_weather_with_wind_uv.py \
  --start-date 2024-01-01 \
  --end-date 2024-12-31

# Backfill specific stations only
python3 backfill_weather_with_wind_uv.py \
  --start-date 2024-01-01 \
  --end-date 2024-12-31 \
  --stations "bkp103t,bkp104t"
```

### 3. Use the Updated Notebook

The main ingestion notebook has been updated:

```bash
jupyter notebook notebooks/exploration/bangkok_environmental_ingestion.ipynb
```

**Updated cells:**
- Cell 7: Schema definition (includes u10_ms, v10_ms)
- Cell 12: API parameters (includes wind_u_component_10m, wind_v_component_10m)
- Cell 15: Transformation logic (converts km/h → m/s)

## Reading the Data

### Using Polars

```python
import polars as pl

# Read weather data
df = pl.read_parquet(
    "data/silver/openmeteo_weather/year=2024/month=01/*.parquet"
)

# Check new columns
print(df.select([
    "timestamp_utc", 
    "stationID",
    "wind_ms",      # Original wind speed
    "wind_dir_deg", # Original wind direction
    "u10_ms",       # NEW: U component
    "v10_ms"        # NEW: V component
]).head())

# Verify wind speed calculation
df = df.with_columns([
    (pl.col("u10_ms")**2 + pl.col("v10_ms")**2).sqrt().alias("wind_ms_calc")
])

print(df.select(["wind_ms", "wind_ms_calc"]).head())
# Should match (within rounding)
```

### Using Pandas

```python
import pandas as pd

# Read weather data
df = pd.read_parquet(
    "data/silver/openmeteo_weather/year=2024/month=01/"
)

# Check new columns
print(df[["timestamp_utc", "stationID", "wind_ms", "u10_ms", "v10_ms"]].head())

# Calculate wind direction from components
import numpy as np
df["wind_dir_calc"] = np.arctan2(-df["u10_ms"], -df["v10_ms"]) * 180 / np.pi
df["wind_dir_calc"] = (df["wind_dir_calc"] + 360) % 360  # Normalize to 0-360

print(df[["wind_dir_deg", "wind_dir_calc"]].head())
```

## Schema Reference

### Complete Weather Schema (v1.0.1)

```python
WEATHER_SILVER_COLUMNS = [
    # Location
    "stationID",           # string
    "lat",                 # float64
    "lon",                 # float64
    
    # Time
    "timestamp_utc",       # datetime64[ns, UTC]
    "timestamp_unix_ms",   # int64
    
    # Meteorological variables
    "temp_c",              # float32 - Temperature (°C)
    "humidity_pct",        # float32 - Relative humidity (%)
    "pressure_hpa",        # float32 - Surface pressure (hPa)
    "precipitation_mm",    # float32 - Precipitation (mm)
    "wind_ms",             # float32 - Wind speed (m/s)
    "wind_dir_deg",        # float32 - Wind direction (degrees)
    "shortwave_radiation_wm2",  # float32 - Solar radiation (W/m²)
    "cloud_cover_pct",     # float32 - Cloud cover (%)
    
    # NEW: Wind components
    "u10_ms",              # float32 - Wind U component (m/s)
    "v10_ms",              # float32 - Wind V component (m/s)
    
    # Metadata
    "data_source",         # string
    "ingestion_timestamp_utc",  # datetime64[ns, UTC]
    "load_id",             # string
    "pipeline_version",    # string
    "record_hash"          # string
]
```

## Migration Guide

### Option A: Backfill (Recommended)

Re-ingest all historical data with the new schema:

```bash
# Backfill 2023-2025
for year in 2023 2024 2025; do
  python3 backfill_weather_with_wind_uv.py \
    --start-date ${year}-01-01 \
    --end-date ${year}-12-31
done
```

**Pros:**
- Complete data with all columns
- Consistent schema across all years

**Cons:**
- Takes time to re-fetch data
- API rate limits apply

### Option B: Add Null Columns

Add u10_ms and v10_ms as null columns to existing data:

```python
import polars as pl
from pathlib import Path

# Read old data
old_data = pl.read_parquet("data/silver/openmeteo_weather/year=2023/**/*.parquet")

# Add null columns
new_data = old_data.with_columns([
    pl.lit(None).cast(pl.Float32).alias("u10_ms"),
    pl.lit(None).cast(pl.Float32).alias("v10_ms")
])

# Reorder columns to match new schema
WEATHER_SILVER_COLUMNS = [...]  # Full list from above
new_data = new_data.select(WEATHER_SILVER_COLUMNS)

# Save back
# (Implement partitioned write logic)
```

**Pros:**
- Fast, no API calls needed
- Preserves existing data

**Cons:**
- Missing wind component data for old records
- Cannot reconstruct from wind_ms/wind_dir_deg (lossy)

## API Reference

### Open-Meteo Archive API

**Endpoint:** `https://archive-api.open-meteo.com/v1/archive`

**New Parameters:**
```
hourly=...,wind_u_component_10m,wind_v_component_10m
```

**Response Format:**
```json
{
  "hourly": {
    "time": ["2024-01-01T00:00", ...],
    "wind_u_component_10m": [0.7, -1.2, ...],  // km/h
    "wind_v_component_10m": [-2.9, 3.1, ...]   // km/h
  }
}
```

**Unit Conversion:**
```python
u10_ms = wind_u_component_10m / 3.6  # km/h → m/s
v10_ms = wind_v_component_10m / 3.6  # km/h → m/s
```

## Troubleshooting

### Issue: Schema mismatch when reading old + new data

**Error:**
```
polars.exceptions.SchemaError: data type mismatch for column u10_ms
```

**Solution:**
Either backfill old data OR add null columns to old data before combining.

### Issue: API rate limit (429)

**Solution:**
The backfill script already handles this with exponential backoff. If you still hit limits:
- Increase `REQUEST_DELAY_SEC` in the script
- Process fewer stations at a time using `--stations` flag
- Run backfill in smaller date ranges

### Issue: Conversion seems incorrect

**Verify:**
```python
# Check that wind speed from components matches original
import numpy as np
wind_speed_calc = np.sqrt(df["u10_ms"]**2 + df["v10_ms"]**2)
diff = np.abs(df["wind_ms"] - wind_speed_calc)
print(f"Max difference: {diff.max():.4f} m/s")
# Should be < 0.1 m/s (rounding error)
```

## Performance Notes

- **API calls:** Same rate as before (1 request/second)
- **Data size:** ~10% larger due to 2 additional float32 columns
- **Processing time:** Negligible difference in transformation

## Next Steps

1. ✅ Test the new schema with `test_wind_uv_ingestion.py`
2. ✅ Backfill historical data with `backfill_weather_with_wind_uv.py`
3. ⬜ Update Gold layer pipeline to include u10_ms and v10_ms
4. ⬜ Update ML models to use wind components as features
5. ⬜ Update documentation for downstream consumers

## Support

For issues or questions:
1. Check `WIND_UV_UPDATE.md` for detailed technical documentation
2. Review test script output: `test_wind_uv_ingestion.py`
3. Check the updated notebook: `notebooks/exploration/bangkok_environmental_ingestion.ipynb`

## Version History

- **v1.0.1** (2026-02-28): Added wind U/V components
- **v1.0.0** (2026-02-23): Initial weather ingestion pipeline
