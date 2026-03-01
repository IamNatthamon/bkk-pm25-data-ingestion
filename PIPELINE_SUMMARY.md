# Bangkok PM2.5 Data Pipeline — Summary Report

**Generated**: 2026-02-28  
**Pipeline Version**: 1.0.0  
**Status**: ✅ Gold Layer Created | ⚠️ PM2.5 Data Missing

---

## Quick Stats

| Metric | Value |
|--------|-------|
| **Bronze Files** | 122 JSON.gz (3.1 MB) |
| **Silver Files** | 7,350 Parquet (328.4 MB) |
| **Gold Files** | 3 splits (12.0 MB) |
| **Total Records** | 5.3M hourly → 219K daily |
| **Stations** | 79 Bangkok monitoring stations |
| **Date Range** | 2010-01-01 → 2017-09-30 (7.75 years) |
| **Features** | 74 (49 raw + 25 normalized) |
| **Train/Val/Test** | 153K / 33K / 33K rows |

---

## ✅ What's Working

### Bronze Layer
- ✅ 122 raw JSON.gz files ingested from Open-Meteo Archive API
- ✅ Immutable storage with compression
- ✅ Organized by year/month for efficient access

### Silver Layer
- ✅ 7,350 Parquet files with Hive partitioning (`year=YYYY/month=MM`)
- ✅ Schema-validated with MD5 checksums
- ✅ 100% temporal coverage (no gaps)
- ✅ 5.3M hourly weather observations
- ✅ 79 stations with complete metadata

### Gold Layer
- ✅ Daily aggregation (hourly → daily)
- ✅ 74 engineered features:
  - 11 base weather features
  - 6 lag features (PM2.5 & temp)
  - 12 rolling statistics (3d/7d/14d windows)
  - 4 cyclical temporal encodings
  - 2 wind vector components (U/V)
  - 5 hotspot placeholders
  - 25 normalized features
- ✅ Chronological train/val/test splits (70/15/15)
- ✅ Z-score normalization (train stats only)
- ✅ Complete pipeline manifest and metadata

---

## ❌ Known Issues

### 🔴 Critical: PM2.5 Data Missing

**Problem**: All PM2.5 and air quality columns are null.

**Root Cause**: Open-Meteo Air Quality API returns HTTP 400 error (incorrect parameters in ingestion notebook).

**Impact**: 
- Cannot train PM2.5 forecasting model (target variable missing)
- All PM2.5-derived features (lag, rolling) are null

**Fix**:
1. Open `bangkok_environmental_ingestion.ipynb`
2. Debug and fix `fetch_aq_station()` API parameters
3. Re-run ingestion cells
4. Re-run: `python scripts/run_silver_to_gold.py`

### 🟡 High Priority: Hotspot Data Not Integrated

**Problem**: Fire hotspot features are placeholders (all null).

**Root Cause**: NASA FIRMS VIIRS data not downloaded or integrated.

**Impact**: Missing transboundary pollution features from regional biomass burning.

**Fix**:
1. Download VIIRS archive for SE Asia (Thailand, Myanmar, Laos)
2. Implement daily aggregation + upwind filtering
3. Compute Transboundary Index (TBI)
4. Join with Gold layer

---

## Data Flow

```
Open-Meteo API
      ↓
┌─────────────────────────────────────────────────────────────┐
│ BRONZE: Raw JSON.gz (immutable)                             │
│ - 122 files (2010-2017)                                     │
│ - Compressed hourly weather data                            │
│ - Organized: {source}/{year}/{month}/batch_*.json.gz        │
└─────────────────────────────────────────────────────────────┘
      ↓ (Schema validation, deduplication)
┌─────────────────────────────────────────────────────────────┐
│ SILVER: Cleaned Parquet (Hive-partitioned)                 │
│ - 7,350 files (year=YYYY/month=MM)                         │
│ - 5.3M hourly records, 79 stations                         │
│ - 100% temporal coverage, 0% nulls                         │
│ - MD5 checksums for integrity                              │
└─────────────────────────────────────────────────────────────┘
      ↓ (Aggregation, feature engineering, normalization)
┌─────────────────────────────────────────────────────────────┐
│ GOLD: Model-Ready Features                                  │
│ - 3 files (train/val/test)                                 │
│ - 219K daily records, 74 features                          │
│ - Chronological splits (70/15/15)                          │
│ - Z-score normalized                                        │
└─────────────────────────────────────────────────────────────┘
      ↓
   ML Models (ST-UNN, LSTM, GRU)
```

---

## Feature Engineering Pipeline

### 1. Temporal Aggregation
- **Input**: 5.3M hourly records
- **Output**: 219K daily records
- **Method**: Solar-noon aligned (12:00 UTC+7)
- **Aggregations**: mean, min, max, sum (depending on variable)

### 2. Wind Decomposition
- Converts polar (speed, direction) → Cartesian (U, V)
- Enables ML models to learn directional patterns

### 3. Lag Features
- PM2.5: lag 1/2/3 days
- Temperature: lag 1/2/3 days
- Computed **before** train-test split (no leakage)

### 4. Rolling Statistics
- Windows: 3-day, 7-day, 14-day
- Metrics: mean, std
- Captures short/medium-term trends

### 5. Temporal Encoding
- Cyclical sin/cos for day-of-year and month
- Captures seasonal patterns

### 6. Normalization
- Z-score: `(X - μ_train) / σ_train`
- Statistics computed from **training set only**
- Applied to all numeric features

---

## Scripts & Tools

### Data Scanning
```bash
python scripts/scan_data_layers.py
```
Shows file counts, sizes, and directory structure for all layers.

### Quality Inspection
```bash
python scripts/inspect_data_quality.py --layer silver
python scripts/inspect_data_quality.py --layer gold
```
Detailed per-column statistics, null percentages, temporal coverage.

### Pipeline Execution
```bash
python scripts/run_silver_to_gold.py
```
Transforms Silver → Gold with full feature engineering.

---

## File Structure

```
bkk-pm25-pipeline/
│
├── data/
│   ├── bronze/openmeteo_weather/          # 122 JSON.gz (3.1 MB)
│   ├── silver/openmeteo_weather/          # 7,350 Parquet (328 MB)
│   ├── gold/model_ready/                  # 3 splits (12 MB)
│   │   ├── train.parquet                  # 153K rows
│   │   ├── val.parquet                    # 33K rows
│   │   ├── test.parquet                   # 33K rows
│   │   ├── normalization_stats.json       # Z-score params
│   │   └── pipeline_manifest.json         # Metadata
│   └── stations/bangkok_stations.parquet  # 79 stations
│
├── src/
│   ├── utils/                             # Config, logging, schemas
│   └── silver_to_gold/                    # ETL pipeline modules
│
├── scripts/
│   ├── scan_data_layers.py                # Data inventory
│   ├── inspect_data_quality.py            # Quality reports
│   └── run_silver_to_gold.py              # Pipeline runner
│
├── pyproject.toml                         # Dependencies
├── DATA_ARCHITECTURE.md                   # Full architecture docs
├── QUICKSTART.md                          # Getting started guide
└── PIPELINE_SUMMARY.md                    # This file
```

---

## Next Actions

### Priority 0: Unblock Model Training
1. Fix Air Quality API in `bangkok_environmental_ingestion.ipynb`
2. Re-ingest PM2.5 data (2010-2017)
3. Re-run Gold pipeline
4. **Expected**: PM2.5 features populated, model training unblocked

### Priority 1: Add Hotspot Features
1. Download NASA FIRMS VIIRS archive
2. Filter SE Asia bbox (13-21°N, 97-106°E)
3. Compute daily fire counts + FRP
4. Calculate Transboundary Index
5. Re-run Gold pipeline

### Priority 2: Extend Weather Data
1. Resume weather backfill (2017-10 → 2026-02)
2. Add 8.4 years of additional training data
3. Consider ERA5 native variables (BLH, dewpoint)

---

## Technical Highlights

### Performance
- **Polars LazyFrame**: Query optimization + out-of-core processing
- **Hive Partitioning**: Efficient date-range filtering
- **Columnar Storage**: Fast column-wise operations
- **Processing Time**: Silver → Gold in ~1 second

### Data Quality
- **0% null** in weather variables
- **100% temporal coverage** (no gaps)
- **Schema validation** at every layer boundary
- **MD5 checksums** for data integrity

### ML Best Practices
- **No data leakage**: Lag features computed before split
- **No future information**: Chronological splits only
- **Normalization integrity**: Train statistics only
- **Reproducibility**: Fixed seed (42)

---

## Dependencies

**Core** (required):
- `polars>=1.0.0` — Fast DataFrames
- `pydantic>=2.0.0` — Schema validation
- `structlog>=24.0.0` — Structured logging

**ML** (optional):
- `torch>=2.0.0` — Deep learning
- `scikit-learn>=1.8.0` — Classical ML

**Install**:
```bash
pip install -e .              # Core only
pip install -e ".[ml]"        # With PyTorch
pip install -e ".[dev]"       # With testing tools
```

---

## Contact & Support

**Documentation**:
- `DATA_ARCHITECTURE.md` — Full technical architecture
- `QUICKSTART.md` — Getting started guide
- `README.md` — Project overview

**Logs**: `logs/silver_to_gold.log`

**Manifest**: `data/gold/model_ready/pipeline_manifest.json`

---

**Pipeline Status**: ✅ Infrastructure Complete | ⚠️ Awaiting PM2.5 Data
