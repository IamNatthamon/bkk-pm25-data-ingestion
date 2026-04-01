# Bangkok PM2.5 Data Pipeline — Architecture Documentation

## Overview

Production-grade **Bronze → Silver → Gold** data lakehouse architecture for Bangkok PM2.5 forecasting.

```
┌──────────────────┐      ┌──────────────────┐      ┌──────────────────┐
│  BRONZE          │      │  SILVER          │      │  GOLD            │
│  Raw immutable   │─────▶│  Cleaned         │─────▶│  Model-ready     │
│  JSON.gz         │      │  Parquet         │      │  Features        │
└──────────────────┘      └──────────────────┘      └──────────────────┘
```

---

## Data Layers

### 🥉 Bronze Layer — Raw Immutable Data

**Purpose**: Store raw API responses exactly as received, immutable audit trail.

**Location**: `data/bronze/`

**Format**: Compressed JSON (`.json.gz`)

**Partitioning**: `{source}/{year}/{month}/batch_{YYYYMMDD}_{batch_id}.json.gz`

**Sources**:

| Source | API | Variables | Status |
|--------|-----|-----------|--------|
| `openmeteo_weather` | Open-Meteo Archive | temp, humidity, pressure, precip, wind, radiation, cloud | ✅ **122 files** (2010-2017) |
| `openmeteo_airquality` | Open-Meteo AQ | PM2.5, PM10, NO2, O3, SO2, CO | ❌ **Missing** (API bug) |

**Current Volume**:
- Files: **122 JSON.gz**
- Size: **3.1 MB** (compressed)
- Records: ~5.3M hourly observations
- Stations: 79 Bangkok monitoring stations

**Schema Example** (`openmeteo_weather`):

```json
{
  "latitude": 13.673111,
  "longitude": 100.65138,
  "elevation": 5.0,
  "hourly": {
    "time": ["2013-01-01T00:00", ...],
    "temperature_2m": [25.3, 25.1, ...],
    "relative_humidity_2m": [78, 79, ...],
    "surface_pressure": [1009.2, ...],
    "precipitation": [0.0, ...],
    "wind_speed_10m": [8.5, ...],
    "wind_direction_10m": [135, ...],
    "shortwave_radiation": [0, ...],
    "cloud_cover": [75, ...]
  }
}
```

**Retention**: Permanent (immutable)

---

### 🥈 Silver Layer — Cleaned & Validated

**Purpose**: Schema-enforced, validated, deduplicated data ready for analytics.

**Location**: `data/silver/`

**Format**: Parquet (columnar, compressed)

**Partitioning**: Hive-style `{source}/year={YYYY}/month={MM}/part_{YYYYMM}_{batch_id}.parquet`

**Schema** (`openmeteo_weather`):

| Column | Type | Description | Constraints |
|--------|------|-------------|-------------|
| `stationID` | String | Station identifier | Non-null |
| `lat` | Float64 | Latitude | 13.64–13.93 |
| `lon` | Float64 | Longitude | 100.34–100.86 |
| `timestamp_utc` | Datetime(UTC) | Observation timestamp | Non-null |
| `timestamp_unix_ms` | Int64 | Unix milliseconds | Non-null |
| `temp_c` | Float32 | Temperature (°C) | 12.7–40.0 |
| `humidity_pct` | Float32 | Relative humidity (%) | 19–100 |
| `pressure_hpa` | Float32 | Surface pressure (hPa) | 997.5–1022.8 |
| `precipitation_mm` | Float32 | Precipitation (mm) | 0–30.8 |
| `wind_ms` | Float32 | Wind speed (m/s) | 0–30.8 |
| `wind_dir_deg` | Float32 | Wind direction (°) | 1–360 |
| `shortwave_radiation_wm2` | Float32 | Solar radiation (W/m²) | 0–1038 |
| `cloud_cover_pct` | Float32 | Cloud cover (%) | 0–100 |
| `data_source` | String | Source identifier | Non-null |
| `ingestion_timestamp_utc` | Datetime(UTC) | When ingested | Non-null |
| `load_id` | String | Batch UUID | Non-null |
| `pipeline_version` | String | Pipeline version | Non-null |
| `record_hash` | String | MD5 hash for dedup | Non-null |

**Data Quality**:
- **Completeness**: 100% (0% null in all weather columns)
- **Temporal Coverage**: 100% (2010-01-01 → 2017-09-30, 2,830 days, 0 gaps)
- **Spatial Coverage**: 79 stations, ~67,932 records per station
- **Total Records**: **5,366,640 hourly observations**

**Current Volume**:
- Files: **7,350 Parquet**
- Size: **328.4 MB**
- Partitions: 93 year-month combinations

**Sidecar Files**: Each `.parquet` has a `.parquet.md5` checksum file for integrity verification.

---

### 🥇 Gold Layer — Model-Ready Features

**Purpose**: Aggregated, feature-engineered, normalized data ready for ML training.

**Location**: `data/gold/model_ready/`

**Format**: Parquet (one file per split)

**Files**:
- `train.parquet` — Training set (70%)
- `val.parquet` — Validation set (15%)
- `test.parquet` — Test set (15%)
- `normalization_stats.json` — Z-score statistics (computed from train only)
- `pipeline_manifest.json` — Metadata and feature list

**Temporal Resolution**: **Daily** (aggregated from hourly)

**Split Strategy**: **Chronological** (no random shuffle)

| Split | Rows | Date Range | Purpose |
|-------|------|------------|---------|
| **Train** | 153,279 | 2010-01-01 → 2015-04-25 | Model training |
| **Val** | 32,846 | 2015-04-25 → 2016-06-15 | Hyperparameter tuning |
| **Test** | 32,846 | 2016-06-15 → 2017-09-30 | Final evaluation |

**Total**: 218,971 daily station-observations (79 stations × ~2,772 days)

---

## Feature Engineering Pipeline

### Stage 1: Temporal Aggregation (Hourly → Daily)

Aggregates 24 hourly observations per day using solar-noon alignment (12:00 UTC+7 = 05:00 UTC).

**Weather Features**:
- `temp_2m_mean`, `temp_2m_min`, `temp_2m_max` — Daily temperature statistics
- `rh_2m_mean` — Mean relative humidity
- `pressure_mean` — Mean surface pressure
- `precip_sum` — Total daily precipitation
- `wind_speed_mean`, `wind_direction_mean` — Mean wind
- `radiation_mean` — Mean solar radiation
- `cloud_cover_mean` — Mean cloud cover

**Air Quality Features** (currently all null):
- `pm2_5_mean`, `pm10_mean` — Mean PM concentrations
- `no2_mean`, `o3_mean` — Mean gas concentrations

### Stage 2: Wind Vector Decomposition

Converts polar wind (speed + direction) to Cartesian components:

```
wind_u10_mean = -speed × sin(direction)  # Eastward component
wind_v10_mean = -speed × cos(direction)  # Northward component
```

### Stage 3: Lag Features

Adds temporal lag features (computed **before** train-test split):

**PM2.5 Lags** (target variable history):
- `pm2_5_mean_lag1`, `pm2_5_mean_lag2`, `pm2_5_mean_lag3`

**Temperature Lags**:
- `temp_2m_mean_lag1`, `temp_2m_mean_lag2`, `temp_2m_mean_lag3`

### Stage 4: Rolling Window Statistics

Computes rolling mean and standard deviation over multiple windows:

**Windows**: 3-day, 7-day, 14-day

**Features** (per window):
- `pm2_5_mean_rolling_mean_{N}d`
- `pm2_5_mean_rolling_std_{N}d`
- `temp_2m_mean_rolling_mean_{N}d`
- `temp_2m_mean_rolling_std_{N}d`

### Stage 5: Temporal Encoding

Cyclical encoding to capture seasonality:

```
day_of_year_sin = sin(2π × day_of_year / 365.25)
day_of_year_cos = cos(2π × day_of_year / 365.25)
month_sin = sin(2π × month / 12)
month_cos = cos(2π × month / 12)
```

### Stage 6: Hotspot Features (Placeholders)

Transboundary fire impact features (awaiting NASA FIRMS VIIRS integration):

- `hotspot_count_th` — Thailand fire count within 800km
- `hotspot_count_mm` — Myanmar fire count
- `hotspot_count_la` — Laos fire count
- `hotspot_frp_sum` — Total Fire Radiative Power
- `transboundary_index` — TBI = Σ(FRP × wind_alignment × exp(-distance/decay))

### Stage 7: Data Cleaning

**Missing Data Handling**:
- Linear interpolation for weather variables (max 3-day gap)
- No interpolation for PM2.5 (target variable)

**Outlier Clipping** (physical bounds):
- Temperature: -10°C to 55°C
- Humidity: 0% to 100%
- Pressure: 950 to 1050 hPa
- Precipitation: 0 to 500 mm
- PM2.5: 0 to 1000 µg/m³

### Stage 8: Normalization

**Z-score normalization** using **training set statistics only**:

```
X_norm = (X - μ_train) / σ_train
```

All normalized features have `_norm` suffix.

**Excluded from normalization**:
- `lat`, `lon` (spatial coordinates)
- `stationID`, `date` (identifiers)
- Categorical features

---

## Feature Summary

**Total Features**: 74 columns

**Feature Groups**:

| Group | Count | Examples |
|-------|-------|----------|
| **Identifiers** | 3 | `date`, `stationID`, `split` |
| **Spatial** | 2 | `lat`, `lon` |
| **Base Weather** | 11 | `temp_2m_mean`, `rh_2m_mean`, `wind_u10_mean` |
| **Base Air Quality** | 4 | `pm2_5_mean`, `pm10_mean`, `no2_mean`, `o3_mean` |
| **Lag Features** | 6 | `pm2_5_mean_lag1`, `temp_2m_mean_lag1` |
| **Rolling Features** | 12 | `pm2_5_mean_rolling_mean_7d`, `temp_2m_mean_rolling_std_14d` |
| **Temporal Encoding** | 4 | `day_of_year_sin`, `month_sin` |
| **Hotspot Features** | 5 | `hotspot_count_th`, `transboundary_index` |
| **Normalized** | 25 | All numeric features with `_norm` suffix |
| **Metadata** | 2 | `load_id` |

---

## Data Quality Status

### ✅ Available Data (100%)

**Weather Variables** (2010-01-01 → 2017-09-30):
- Temperature, humidity, pressure, precipitation
- Wind speed & direction (+ U/V components)
- Solar radiation, cloud cover
- All lag and rolling features derived from weather

**Metadata**:
- 79 Bangkok station locations
- Complete temporal coverage (no gaps)

### ❌ Missing Data (0%)

**Air Quality** (Critical):
- PM2.5, PM10, NO2, O3, SO2, CO — **ALL NULL**
- Root cause: Open-Meteo Air Quality API parameter error
- Impact: **Cannot train PM2.5 forecasting model**

**Hotspot Data** (High Priority):
- Fire counts, FRP, TBI — **ALL NULL**
- Root cause: NASA FIRMS VIIRS not integrated
- Impact: Missing transboundary pollution features

---

## ETL Scripts

### Core Pipeline Scripts

**1. `scripts/scan_data_layers.py`**
- Scans all data layers (Bronze/Silver/Gold)
- Reports file counts, sizes, and directory structure
- Usage: `python scripts/scan_data_layers.py`

**2. `scripts/inspect_data_quality.py`**
- Detailed data quality assessment
- Per-column statistics (null %, min/max/mean)
- Temporal and spatial coverage analysis
- Usage: `python scripts/inspect_data_quality.py --layer silver|gold`

**3. `scripts/run_silver_to_gold.py`**
- Executes complete Silver → Gold transformation
- Applies all feature engineering stages
- Creates train/val/test splits with normalization
- Usage: `python scripts/run_silver_to_gold.py`

### Source Modules

**`src/utils/`**:
- `config.py` — Pydantic-based configuration management
- `logger.py` — Structured logging with structlog
- `schema.py` — Data schemas and type definitions

**`src/silver_to_gold/`**:
- `loader.py` — Load Silver Parquet with schema validation
- `transforms.py` — Feature engineering transformations
- `quality.py` — Data quality assessment functions
- `pipeline.py` — Main orchestration logic

---

## Configuration

All pipeline parameters are centralized in `PipelineConfig` (Pydantic settings).

**Environment Variables** (optional):
- `BKK_PM25_PROJECT_ROOT` — Override project root
- `BKK_PM25_RANDOM_SEED` — Set random seed (default: 42)
- `BKK_PM25_TRAIN_RATIO` — Training split ratio (default: 0.70)
- `BKK_PM25_VAL_RATIO` — Validation split ratio (default: 0.15)
- `BKK_PM25_TEST_RATIO` — Test split ratio (default: 0.15)

**Key Parameters**:

```python
target_resolution = "daily"           # Temporal aggregation
lag_days = [1, 2, 3]                 # Lag feature windows
rolling_windows = [3, 7, 14]         # Rolling statistics windows
max_interpolation_gap_days = 3       # Max gap for linear interpolation
outlier_clip_enabled = True          # Apply physical bounds clipping
random_seed = 42                     # Reproducibility
```

---

## Usage Examples

### 1. Scan Data Layers

```bash
python scripts/scan_data_layers.py
```

**Output**:
```
📁 BRONZE - Weather
   Files: 122
   Size: 3.1 MB

📁 SILVER - Weather
   Files: 7,350
   Size: 328.4 MB

📁 GOLD - Model Ready
   Files: 3
   Size: 12.0 MB

SUMMARY:
  Bronze → Silver: 122 JSON.gz → 7350 Parquet
  Silver → Gold: 7350 Parquet → 3 splits
```

### 2. Inspect Silver Data Quality

```bash
python scripts/inspect_data_quality.py --layer silver
```

**Output**:
```
SILVER LAYER - WEATHER DATA
Total rows: 5,366,640
Total columns: 18
Date range: 2010-01-01 → 2017-09-30

TEMPORAL COVERAGE:
  min_date: 2010-01-01
  max_date: 2017-09-30
  unique_dates: 2830
  coverage_pct: 100.0

STATION COVERAGE:
  unique_stations: 79
  total_records: 5366640
```

### 3. Generate Gold Layer

```bash
python scripts/run_silver_to_gold.py
```

**Output**:
```
GOLD LAYER CREATED SUCCESSFULLY
       train: data/gold/model_ready/train.parquet
         val: data/gold/model_ready/val.parquet
        test: data/gold/model_ready/test.parquet
       stats: data/gold/model_ready/normalization_stats.json
    manifest: data/gold/model_ready/pipeline_manifest.json
```

### 4. Inspect Gold Layer

```bash
python scripts/inspect_data_quality.py --layer gold
```

**Output**:
```
GOLD LAYER - TRAIN SPLIT
Total rows: 153,279
Date range: 2010-01-01 → 2015-04-25
Unique stations: 79
PM2.5 available: ❌ NO (all null)
Normalized features: 25

PIPELINE MANIFEST:
  Total features: 74
  Train rows: 153,279
  Val rows: 32,846
  Test rows: 32,846
```

---

## Data Quality Guarantees

### Silver Layer

✅ **Schema Validation**: All records validated against Pydantic schemas  
✅ **Deduplication**: MD5 hash-based duplicate detection  
✅ **Temporal Integrity**: Chronological ordering preserved  
✅ **Spatial Bounds**: Coordinates within Bangkok bbox  
✅ **Physical Constraints**: All values within sensor specifications  
✅ **Metadata Tracking**: load_id, ingestion_timestamp, record_hash  

### Gold Layer

✅ **No Data Leakage**: Lag/rolling features computed before split  
✅ **Normalization Integrity**: Only training set statistics used  
✅ **Temporal Order**: Chronological splits, no random shuffle  
✅ **Reproducibility**: Fixed random seed (42)  
✅ **Feature Completeness**: All 74 features present in all splits  
✅ **Split Validation**: Ratios sum to 1.0, no overlap between splits  

---

## Known Issues & Action Items

### 🔴 Critical: PM2.5 Data Missing

**Problem**: Air quality API returns HTTP 400 — all PM2.5 values are null.

**Impact**: Cannot train PM2.5 forecasting model (target variable missing).

**Fix Required**:
1. Debug Open-Meteo Air Quality API parameters in ingestion notebook
2. Re-run ingestion: `bangkok_environmental_ingestion.ipynb`
3. Re-run Gold pipeline: `python scripts/run_silver_to_gold.py`

### 🟡 High Priority: Hotspot Data Integration

**Problem**: NASA FIRMS VIIRS data not integrated.

**Impact**: Missing transboundary pollution features from regional fires.

**Fix Required**:
1. Download VIIRS archive (2014–present) for SE Asia bbox
2. Implement daily aggregation + upwind filtering
3. Compute TBI (Transboundary Index)
4. Re-run Gold pipeline

### 🟢 Medium Priority: Extend Weather Backfill

**Problem**: Weather data ends at 2017-09-30 (missing 2017-10 → 2026-02).

**Impact**: Limited training data (7.75 years instead of 16 years).

**Fix Required**:
1. Resume weather ingestion from 2017-10-01
2. Consider switching to CDS API for native ERA5 variables (BLH, dewpoint)

---

## Design Principles

### 1. Immutability
- Bronze layer is **never modified** after write
- Silver transformations are **idempotent** (same input → same output)
- Gold features are **deterministic** (fixed seed, no randomness)

### 2. Schema Evolution
- All schemas defined in `src/utils/schema.py`
- Pydantic validation at layer boundaries
- Breaking changes require new pipeline version

### 3. Temporal Integrity
- **Never shuffle** time series data
- **Never use future data** in training window
- Compute lag features **before** train-test split
- Normalize using **only training set** statistics

### 4. Separation of Concerns
- **Bronze**: Ingestion only (no business logic)
- **Silver**: Validation + cleaning (no feature engineering)
- **Gold**: Feature engineering + ML prep (no raw I/O)

### 5. Observability
- Structured logging (structlog) at every stage
- Data quality metrics logged and saved
- Pipeline manifest tracks all transformations
- MD5 checksums for data integrity

---

## Storage Layout

```
data/
├── bronze/
│   └── openmeteo_weather/
│       └── {year}/{month}/
│           └── batch_{YYYYMMDD}_{batch_id}.json.gz
│
├── silver/
│   └── openmeteo_weather/
│       └── year={YYYY}/month={MM}/
│           ├── part_{YYYYMM}_{batch_id}.parquet
│           └── part_{YYYYMM}_{batch_id}.parquet.md5
│
├── gold/
│   └── model_ready/
│       ├── train.parquet
│       ├── val.parquet
│       ├── test.parquet
│       ├── normalization_stats.json
│       └── pipeline_manifest.json
│
└── stations/
    └── bangkok_stations.parquet
```

---

## Dependencies

**Core**:
- `polars>=1.0.0` — Fast DataFrame library (preferred over pandas)
- `pyarrow>=23.0.0` — Parquet I/O
- `pydantic>=2.0.0` — Schema validation
- `structlog>=24.0.0` — Structured logging

**Geospatial**:
- `geopandas>=1.0.0` — Spatial operations
- `shapely>=2.0.0` — Geometric operations

**ML** (optional):
- `torch>=2.0.0` — Deep learning framework
- `scikit-learn>=1.8.0` — Classical ML

**Install**:

```bash
# Using uv (recommended)
uv pip install -e .

# Or using pip
pip install -e .

# With ML dependencies
pip install -e ".[ml]"

# With dev dependencies
pip install -e ".[dev]"
```

---

## Performance Notes

### Silver Layer Loading
- **7,350 Parquet files** loaded lazily using `pl.scan_parquet()`
- Hive partitioning enables efficient date-range filtering
- Typical load time: <1 second (lazy), ~2 seconds (eager collect)

### Gold Pipeline Execution
- **Input**: 5.3M hourly records
- **Output**: 219K daily records
- **Processing time**: ~1 second (Polars LazyFrame optimization)
- **Memory usage**: <500 MB peak

### Optimization Strategies
- Use `LazyFrame` for all transformations (query optimization)
- Partition by year/month for incremental updates
- Compute expensive features (rolling, lag) only once
- Store normalized features alongside raw for flexibility

---

## Next Steps

1. **Fix Air Quality API** → Re-ingest PM2.5 data
2. **Integrate VIIRS Hotspots** → Add fire features
3. **Extend Weather Backfill** → 2017-10 → 2026-02
4. **Add Data Validation Tests** → pytest + hypothesis
5. **Implement Incremental Updates** → Only process new data
6. **Add DuckDB Analytics Layer** → Fast SQL queries on Gold

---

## References

- Open-Meteo Archive API: https://open-meteo.com/en/docs/historical-weather-api
- Open-Meteo Air Quality API: https://open-meteo.com/en/docs/air-quality-api
- NASA FIRMS VIIRS: https://firms.modaps.eosdis.nasa.gov/
- Polars Documentation: https://docs.pola.rs/
- Pydantic Settings: https://docs.pydantic.dev/latest/concepts/pydantic_settings/

---

**Last Updated**: 2026-02-28  
**Pipeline Version**: 1.0.0  
**Author**: Senior AI & Data Developer
