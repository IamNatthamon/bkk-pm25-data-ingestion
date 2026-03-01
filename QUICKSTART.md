# Bangkok PM2.5 Pipeline — Quick Start Guide

## Installation

### 1. Install Dependencies

```bash
# Using uv (recommended - fast)
pip install uv
uv pip install -e .

# Or using pip
pip install -e .

# With ML dependencies (PyTorch)
pip install -e ".[ml]"
```

### 2. Verify Installation

```bash
python -c "import polars, pydantic, structlog; print('✅ All dependencies installed')"
```

---

## Data Pipeline Workflow

### Step 1: Scan Current Data

```bash
python scripts/scan_data_layers.py
```

**Expected Output**:
```
📁 BRONZE - Weather: 122 files (3.1 MB)
📁 SILVER - Weather: 7,350 files (328.4 MB)
📁 GOLD - Model Ready: 3 files (12.0 MB)
```

### Step 2: Inspect Data Quality

```bash
# Check Silver layer
python scripts/inspect_data_quality.py --layer silver

# Check Gold layer
python scripts/inspect_data_quality.py --layer gold
```

### Step 3: Generate Gold Layer

```bash
python scripts/run_silver_to_gold.py
```

**What it does**:
1. Loads 5.3M hourly weather records from Silver
2. Aggregates to daily resolution
3. Engineers 74 features (lag, rolling, temporal encoding)
4. Creates chronological train/val/test splits (70/15/15)
5. Normalizes using training set statistics
6. Writes 3 Parquet files + manifest

**Output**:
- `data/gold/model_ready/train.parquet` (153K rows)
- `data/gold/model_ready/val.parquet` (33K rows)
- `data/gold/model_ready/test.parquet` (33K rows)
- `data/gold/model_ready/normalization_stats.json`
- `data/gold/model_ready/pipeline_manifest.json`

---

## Current Data Status

### ✅ Available
- **Weather data**: 2010-01-01 → 2017-09-30 (7.75 years)
- **79 Bangkok stations**: Complete metadata
- **5.3M hourly observations**: 100% temporal coverage

### ❌ Missing (Blocks Model Training)
- **PM2.5 data**: All null (API bug)
- **Hotspot data**: Not integrated (NASA FIRMS VIIRS)

---

## Next Actions

### Priority 0: Fix PM2.5 Data

**Problem**: Open-Meteo Air Quality API returns 400 error.

**Solution**:
1. Open `bangkok_environmental_ingestion.ipynb`
2. Fix API parameters in `fetch_aq_station()` function
3. Re-run ingestion cells
4. Re-run: `python scripts/run_silver_to_gold.py`

### Priority 1: Integrate Hotspot Data

**Problem**: Fire data not available.

**Solution**:
1. Download VIIRS CSV from NASA FIRMS
2. Filter SE Asia bbox (Thailand, Myanmar, Laos)
3. Compute daily aggregates per country
4. Calculate Transboundary Index (TBI)
5. Join with Gold layer

---

## Configuration

All parameters are in `src/utils/config.py` (Pydantic settings).

**Override via environment variables**:

```bash
export BKK_PM25_TRAIN_RATIO=0.75
export BKK_PM25_VAL_RATIO=0.15
export BKK_PM25_TEST_RATIO=0.10
export BKK_PM25_RANDOM_SEED=42

python scripts/run_silver_to_gold.py
```

---

## Project Structure

```
bkk-pm25-pipeline/
├── src/
│   ├── utils/
│   │   ├── config.py          # Pydantic configuration
│   │   ├── logger.py          # Structured logging
│   │   └── schema.py          # Data schemas
│   └── silver_to_gold/
│       ├── loader.py          # Load Silver Parquet
│       ├── transforms.py      # Feature engineering
│       ├── quality.py         # Data quality checks
│       └── pipeline.py        # Main orchestration
│
├── scripts/
│   ├── scan_data_layers.py           # Scan Bronze/Silver/Gold
│   ├── inspect_data_quality.py       # Quality assessment
│   └── run_silver_to_gold.py         # Execute pipeline
│
├── data/
│   ├── bronze/                       # Raw JSON.gz (immutable)
│   ├── silver/                       # Cleaned Parquet (Hive-partitioned)
│   ├── gold/model_ready/             # ML-ready features
│   └── stations/                     # Station metadata
│
├── pyproject.toml                    # Dependencies + config
├── DATA_ARCHITECTURE.md              # Full architecture docs
├── QUICKSTART.md                     # This file
└── README.md                         # Project overview
```

---

## Troubleshooting

### Error: "No module named 'polars'"

```bash
pip install polars pydantic pydantic-settings structlog
```

### Error: "Silver directory not found"

Check that Silver layer exists:
```bash
ls -la data/silver/openmeteo_weather/
```

If missing, run ingestion notebook first.

### Error: "PM2.5 all null"

This is **expected** — Air Quality API is broken. See Priority 0 action above.

### Performance: Pipeline is slow

- Ensure you're using `polars` (not `pandas`)
- Check memory: Gold pipeline needs ~500 MB RAM
- Use SSD for data storage (not HDD)

---

## Support

For questions or issues:
1. Check `DATA_ARCHITECTURE.md` for detailed documentation
2. Review logs in `logs/silver_to_gold.log`
3. Inspect manifest: `cat data/gold/model_ready/pipeline_manifest.json`

---

**Last Updated**: 2026-02-28  
**Pipeline Version**: 1.0.0
