# Recommended Execution Order

Production flow for BKK-PM25 data pipeline (bronze → silver → gold → preprocessing).

## 1. run_ingestion (Bronze)

```bash
# Weather (wind U/V)
python run_ingestion.py --weather

# Air quality (2019–2023)
python run_ingestion.py --aq

# Both
python run_ingestion.py --weather --aq
```

- **Weather:** `src/past/backfill_weather_with_wind_uv.py` → `data/bronze/openmeteo_weather`
- **AQ:** `scripts/ingestion/run_backfill_aq_2023_fast.py` (or `backfill_5years.py`, `backfill_missing_months.py`) → `data/bronze/openmeteo_airquality`
- **FIRMS:** Download CSVs from https://firms.modaps.eosdis.nasa.gov/country/ into `data/bronze/raw_hotspot`

All ingestion is idempotent (skips existing files where applicable).

---

## 2. run_bronze_to_silver (Silver)

```bash
# Merge wind U/V into Silver weather + FIRMS → Silver (raw + daily aggregate)
python run_bronze_to_silver.py --all
```

- **Weather:** `scripts/merge_wind_uv_to_silver.py` → adds `u10_ms`, `v10_ms` to `data/silver/openmeteo_weather`
- **AQ:** Produced by backfill scripts (no separate bronze→silver AQ script)
- **FIRMS:** `src/bronze_to_silver_firms_hotspot.py` → `data/silver/firms_hotspot` (raw with type, frp)
- **FIRMS daily:** `scripts/silver_firms_daily_aggregate.py` → `data/silver/firms_hotspot_daily` (filter type==0, confidence, 500km Bangkok; hotspot_count, frp_sum, frp_mean, weighted_frp_sum)

---

## 3. run_silver_to_gold (Gold)

```bash
# Daily pipeline → model_ready (train/val/test)
python run_silver_to_gold.py

# Optionally: hourly AQ 2023–2025 → airquality_combined
python run_silver_to_gold.py --aq-hourly
```

- **Daily:** `scripts/run_silver_to_gold.py` → `data/gold/model_ready` (weather + AQ daily, lags, rolling, splits; FIRMS can be wired in via config)
- **Hourly AQ:** `scripts/create_gold_airquality.py` → `data/gold/airquality_combined/`

---

## 4. run_preprocessing (Modeling-ready)

- Run **notebooks/pipelines/preprocessing_pipeline.ipynb** after Gold is ready.
- Configure it to **read only from Gold** (`data/gold/model_ready` or `airquality_combined`) and not recompute features already in Gold.
- Use chronological train/val/test split; no time leakage.

---

## One-shot (full pipeline)

```bash
python run_ingestion.py --weather --aq
# Download FIRMS to data/bronze/raw_hotspot manually if needed

python run_bronze_to_silver.py --all

python run_silver_to_gold.py
# Then run preprocessing notebook
```

---

## Config

- Paths and options: `src/utils/config.py` (`PipelineConfig`), env prefix `BKK_PM25_`.
- Gold-specific: `config/gold.py` (env prefix `GOLD_`).
