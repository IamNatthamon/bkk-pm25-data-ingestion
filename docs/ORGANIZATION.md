# Project Organization

This document describes the layout of the Bangkok PM2.5 Data Ingestion project after the April 2026 layout cleanup.

## Directory Structure

```
bkk-pm25-data-ingestion/
├── config/                     # Configuration modules
│   ├── __init__.py
│   └── gold.py                 # Gold pipeline config (GoldPipelineConfig)
│
├── docs/                       # Documentation
│   ├── ORGANIZATION.md         # This file
│   ├── DATA_ARCHITECTURE.md, QUICKSTART.md, PIPELINE_SUMMARY.md, …
│   └── …
│
├── notebooks/                  # Jupyter notebooks by purpose
│   ├── exploration/            # EDA, ingestion, visualization, explore_gold_airquality.ipynb
│   ├── pipelines/              # preprocessing_pipeline.ipynb (+ optional backup copy)
│   └── modeling/               # Model training and backfill
│
├── scripts/                    # Executable scripts
│   ├── pipeline/               # Orchestrators (implementations)
│   │   ├── run_ingestion.py
│   │   ├── run_bronze_to_silver.py
│   │   └── run_silver_to_gold.py
│   ├── ingestion/              # Air quality backfills
│   │   ├── backfill_5years.py, backfill_missing_months.py, run_backfill_aq_2023*.py, …
│   ├── merge_wind_uv_to_silver.py
│   ├── silver_firms_daily_aggregate.py
│   ├── create_gold_airquality.py
│   ├── validate_bronze_weather.py
│   ├── test_wind_uv_ingestion.py
│   ├── run_silver_to_gold.py   # Silver → Gold daily runner
│   ├── inspect_data_quality.py
│   ├── scan_data_layers.py
│   └── visualize_pipeline.py
│
├── src/                        # Importable packages
│   ├── gold/, silver_to_gold/, validation/, training/, …
│   ├── bronze_to_silver_firms_hotspot.py
│   └── past/backfill_weather_with_wind_uv.py
│
├── run_ingestion.py            # Thin wrappers → scripts/pipeline/* (optional; same for bronze→silver, silver→gold)
├── run_bronze_to_silver.py
├── run_silver_to_gold.py
├── data/                       # Bronze / silver / gold (gitignored where large)
├── models/
├── checkpoints/
├── logs/
├── README.md
├── pyproject.toml
└── requirements.txt
```

## Notebooks

| Location | Notebook | Purpose |
|----------|----------|---------|
| notebooks/exploration/ | bangkok_environmental_ingestion.ipynb | API ingestion (Bronze → Silver) |
| notebooks/exploration/ | explore_gold_airquality.ipynb | Explore hourly Gold AQ exports |
| notebooks/exploration/ | visualization.ipynb | EDA, data quality, spatial/temporal analysis |
| notebooks/pipelines/ | preprocessing_pipeline.ipynb | Feature engineering (Silver → Gold) |
| notebooks/modeling/ | backfill_airquality_2023.ipynb | Air quality backfill for 2023 |
| notebooks/modeling/ | model_training.ipynb | ST-UNN + baselines training & forecasting |

**Note:** Run Jupyter from the project root so Path.cwd() resolves correctly for data paths.

## Scripts

### Ingestion Scripts (scripts/ingestion/)

Run from project root:

```bash
# Backfill 5 years (2019–2022)
python scripts/ingestion/backfill_5years.py

# Backfill missing months (Jan & Mar 2023)
python scripts/ingestion/backfill_missing_months.py

# Backfill 2023 (standard)
python scripts/ingestion/run_backfill_aq_2023.py

# Backfill 2023 (async, faster)
python scripts/ingestion/run_backfill_aq_2023_fast.py

# Run AQ backfill 2023+ to silver
python scripts/ingestion/run_aq_backfill_2023.py
```

### Pipeline orchestrators (`scripts/pipeline/`)

```bash
python scripts/pipeline/run_ingestion.py --weather --aq
python scripts/pipeline/run_bronze_to_silver.py --all
python scripts/pipeline/run_silver_to_gold.py
```

Or use the thin wrappers at the repo root: `run_ingestion.py`, `run_bronze_to_silver.py`, `run_silver_to_gold.py`.

### Other scripts

```bash
# Silver → Gold transformation (daily pipeline)
python scripts/run_silver_to_gold.py

# Gold pipeline (hourly features, ML-ready)
python -m src.gold.pipeline
```

## Import Paths

After reorganization, use these import paths:

| Old | New |
|-----|-----|
| from config_gold import config | from config.gold import config |
| from gold_features import ... | from src.gold.features import ... |
| from gold_loader import ... | from src.gold.loader import ... |
| from gold_pipeline import run_gold_pipeline | from src.gold.pipeline import run_gold_pipeline |

## Gold vs Silver-to-Gold

- **src/gold/** — Hourly PM2.5 forecasting pipeline: loads Silver AQ, adds lag/rolling/temporal features, creates train/val/test splits, outputs to data/gold/.
- **src/silver_to_gold/** — Daily aggregation pipeline: merges weather + AQ, adds lag/rolling features, outputs to data/gold/model_ready/ for ST-UNN.

Both pipelines produce Gold-layer data; they serve different resolutions and use cases.
