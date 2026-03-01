# Wind U/V Component Update

## Overview

Extended the weather ingestion pipeline to include wind U and V components (u10_ms, v10_ms) from Open-Meteo Archive API.

## Changes Made

### 1. API Parameters Updated

**File:** `notebooks/exploration/bangkok_environmental_ingestion.ipynb` (Cell 12)

**Before:**
```python
hourly = "temperature_2m,relative_humidity_2m,surface_pressure,precipitation,wind_speed_10m,wind_direction_10m,shortwave_radiation,cloud_cover"
```

**After:**
```python
hourly = "temperature_2m,relative_humidity_2m,surface_pressure,precipitation,wind_speed_10m,wind_direction_10m,shortwave_radiation,cloud_cover,wind_u_component_10m,wind_v_component_10m"
```

### 2. Schema Extended

**File:** `notebooks/exploration/bangkok_environmental_ingestion.ipynb` (Cell 7)

**Added columns:**
- `u10_ms` (FLOAT32) - Wind U component in m/s
- `v10_ms` (FLOAT32) - Wind V component in m/s

**Updated WEATHER_SILVER_COLUMNS:**
```python
WEATHER_SILVER_COLUMNS = [
    "stationID", "lat", "lon", "timestamp_utc", "timestamp_unix_ms",
    "temp_c", "humidity_pct", "pressure_hpa", "precipitation_mm", "wind_ms", "wind_dir_deg",
    "shortwave_radiation_wm2", "cloud_cover_pct", "u10_ms", "v10_ms",  # NEW
    "data_source", "ingestion_timestamp_utc", "load_id", "pipeline_version", "record_hash"
]
```

**Updated WEATHER_SILVER_DTYPES:**
```python
WEATHER_SILVER_DTYPES = {
    # ... existing fields ...
    "u10_ms": "float32",  # NEW
    "v10_ms": "float32",  # NEW
    # ... metadata fields ...
}
```

### 3. Transformation Logic Updated

**File:** `notebooks/exploration/bangkok_environmental_ingestion.ipynb` (Cell 15)

**Added conversion from km/h to m/s:**
```python
# Convert wind U and V components from km/h to m/s
wind_u_kmh = hourly.get("wind_u_component_10m", [None] * n)
wind_v_kmh = hourly.get("wind_v_component_10m", [None] * n)
df["u10_ms"] = [None if v is None else v / 3.6 for v in wind_u_kmh]
df["v10_ms"] = [None if v is None else v / 3.6 for v in wind_v_kmh]
```

**Conversion formula:** `value_ms = value_kmh / 3.6`

### 4. Pipeline Version

Updated to `1.0.1` to reflect schema change.

## Data Source

- **API:** Open-Meteo Archive API
- **Endpoint:** `https://archive-api.open-meteo.com/v1/archive`
- **Documentation:** https://open-meteo.com/en/docs
- **New Parameters:**
  - `wind_u_component_10m` (km/h)
  - `wind_v_component_10m` (km/h)

## Unit Conversion

Open-Meteo returns wind components in **km/h**, but we store them in **m/s** for consistency with other meteorological data.

**Conversion:**
- Input: km/h
- Output: m/s
- Formula: `m/s = km/h / 3.6`

**Example:**
- U component: 0.70 km/h → 0.1944 m/s
- V component: -2.90 km/h → -0.8056 m/s

## Testing

A test script was created and successfully validated the changes:

**Test file:** `test_wind_uv_ingestion.py`

**Test results:**
- ✓ API request successful (status 200)
- ✓ Bronze layer saved
- ✓ Transformation successful
- ✓ Silver layer saved
- ✓ Schema validation passed (20 columns)
- ✓ Data types correct (float32)
- ✓ Unit conversion accurate
- ✓ No null values in test data

**Test data:**
- Station: Bangkok (13.7563, 100.5018)
- Date: 2026-02-21
- Records: 24 hourly observations

## Backward Compatibility

### Existing Data

Old weather data (without u10_ms/v10_ms) will need to be handled carefully:

1. **Reading old partitions:** Will fail schema validation
2. **Recommended approach:**
   - Keep old data as-is
   - Only apply new schema to new ingestions
   - OR re-ingest historical data with new schema

### Migration Strategy

If you need to unify old and new data:

1. **Option A: Backfill** (Recommended)
   - Re-fetch historical data with new parameters
   - Replace old partitions

2. **Option B: Add null columns**
   - Read old data
   - Add `u10_ms` and `v10_ms` columns with null values
   - Re-save with new schema

## Usage Example

```python
import polars as pl

# Read weather data with new columns
df = pl.read_parquet(
    "/Users/ahcint1n/Desktop/bkk-pm25-data-ingestion/data/silver/openmeteo_weather/year=2026/month=02/*.parquet"
)

# Check wind components
print(df.select(["timestamp_utc", "wind_ms", "wind_dir_deg", "u10_ms", "v10_ms"]).head())

# Calculate wind speed from components (verification)
df = df.with_columns([
    (pl.col("u10_ms")**2 + pl.col("v10_ms")**2).sqrt().alias("wind_ms_calculated")
])

# Should match wind_ms (within rounding)
print(df.select(["wind_ms", "wind_ms_calculated"]).head())
```

## Files Modified

1. `notebooks/exploration/bangkok_environmental_ingestion.ipynb`
   - Cell 7: Schema definition
   - Cell 12: API parameters
   - Cell 15: Transformation logic

## Files Created

1. `test_wind_uv_ingestion.py` - Test script
2. `WIND_UV_UPDATE.md` - This documentation

## Next Steps

1. **Update production scripts** if you have standalone Python scripts (not just notebooks)
2. **Update Gold layer pipeline** to include u10_ms and v10_ms
3. **Consider backfilling** historical data if wind components are needed for past dates
4. **Update documentation** for downstream consumers

## Notes

- Wind U/V components are useful for:
  - Vector wind analysis
  - Wind field interpolation
  - Atmospheric modeling
  - More accurate wind speed/direction calculations
  
- The conversion maintains the same retry/throttling strategy as existing weather ingestion
- No changes to rate limiting or request patterns
- Maintains existing partitioning (year=YYYY/month=MM)
- Maintains existing primary key (stationID + timestamp_utc)

## Validation Checklist

- [x] API parameters include wind_u_component_10m and wind_v_component_10m
- [x] Schema includes u10_ms and v10_ms columns
- [x] Data types are float32
- [x] Unit conversion (km/h → m/s) is correct
- [x] Transformation logic handles null values
- [x] Test script validates end-to-end pipeline
- [x] Documentation updated
