# BKK-PM25 Data Ingestion — Technical Audit Summary

**Date:** 2026-03  
**Scope:** Full repository audit and productionization (medallion: bronze → silver → gold)

---

## 1. Executive Summary

- **Architecture:** Bronze (raw JSON/CSV) → Silver (cleaned Parquet) → Gold (model-ready features). Two Gold paths exist: (A) `create_gold_airquality.py` → `airquality_combined/` (hourly AQ 2023–2025); (B) `run_silver_to_gold.py` + preprocessing notebook → `model_ready/` (daily, train/val/test).
- **Critical gaps:** FIRMS hotspot data is not merged into Gold; Silver AQ column names vary (long vs short); PM2.5 target often null when AQ ingestion is missing; preprocessing notebook reads Silver directly instead of Gold.
- **Recommendation:** Unify config, standardize Silver naming/timezone, add FIRMS to Silver (filters + daily aggregates) and to Gold merge, rebuild Gold with weather + AQ + FIRMS and strict no-NaN target, then point preprocessing to Gold only.

---

## 2. Repository Structure (Current)

```
bkk-pm25-data-ingestion/
├── config/gold.py                 # Gold pipeline config (paths, years)
├── data/
│   ├── bronze/openmeteo_weather, openmeteo_airquality, raw_hotspot
│   ├── silver/openmeteo_weather, openmeteo_airquality, firms_hotspot
│   ├── gold/airquality_combined, model_ready
│   └── stations/bangkok_stations.parquet
├── notebooks/ (exploration, pipelines, modeling)
├── scripts/ingestion/, run_silver_to_gold.py, scan_data_layers.py, ...
├── src/gold/, silver_to_gold/, utils/
├── Root: create_gold_airquality.py, merge_wind_uv_to_silver.py, bronze_to_silver_firms_hotspot.py,
│        backfill_*.py, run_backfill_aq_*.py, validate_bronze_weather.py, test_wind_uv_ingestion.py
├── pyproject.toml, README.md, PIPELINE_SUMMARY.md, DATA_ARCHITECTURE.md, ...
```

---

## 3. Issues Identified

| Category | Issue |
|----------|--------|
| **Data flows** | Silver AQ missing for many periods → Gold has null PM2.5. FIRMS Silver exists but is not joined into Gold (only placeholders). |
| **Schema** | `AIRQUALITY_SILVER_SCHEMA` uses `timestamp`, `station_id`, `latitude`, `longitude`; actual Silver uses `timestamp_utc`, `stationID`, `lat`, `lon`. Silver AQ files mix long names (`nitrogen_dioxide_ugm3`) and short (`no2_ugm3`). |
| **Paths** | Root scripts use `Path.cwd()` or `Path(__file__).parent` + hardcoded `data/bronze/...`. `config/gold.py` uses class-level `project_root = Path.cwd()`. |
| **Target** | PM2.5 (target) null when AQ not ingested. create_gold_airquality drops null `pm2_5_ugm3`; silver_to_gold uses weather-only with null AQ. |
| **Hotspot / U10/V10** | Weather u10_ms, v10_ms added by `merge_wind_uv_to_silver.py` (correct). FIRMS not filtered (type, confidence), not aggregated (hotspot_count, frp_sum, 500 km Bangkok), not merged into Gold. |
| **Preprocessing** | Notebook reads Silver and writes Gold model_ready; does not read from Gold. Risk of duplicated feature logic and inconsistency. |
| **Idempotency** | Bronze backfills skip existing files (resume); merge_wind_uv overwrites partition files. FIRMS batch writes then merge dedup. |

---

## 4. Proposed Folder Structure (Production)

```
src/
├── config.py           # Single PipelineConfig (paths, years, splits, FIRMS radius, etc.)
├── ingestion/          # Bronze ingestion helpers (validate, log, schema snapshot)
│   ├── __init__.py
│   ├── weather.py
│   ├── airquality.py
│   └── firms.py
├── bronze_to_silver/   # Bronze → Silver transforms
│   ├── __init__.py
│   ├── weather.py      # (optional) raw weather → silver; or keep merge_wind_uv at root
│   ├── airquality.py   # (optional) bronze AQ → silver
│   └── firms.py        # FIRMS: filter type==0, confidence, 500km Bangkok, daily agg
├── silver_to_gold/     # Existing + FIRMS merge, lags, rolling, no leakage
│   ├── __init__.py
│   ├── loader.py
│   ├── pipeline.py
│   ├── transforms.py
│   └── quality.py
├── validation/         # Schema checks, row counts, critical-null checks
│   ├── __init__.py
│   ├── schema.py
│   └── bronze_silver.py
├── gold/               # create_gold_airquality logic (merge AQ + weather + FIRMS)
│   ├── __init__.py
│   ├── loader.py
│   ├── pipeline.py
│   └── features.py
└── utils/
    ├── logger.py
    └── schema.py       # Canonical WEATHER_SILVER, AQ_SILVER, FIRMS_SILVER, GOLD

scripts/ (or root entrypoints)
├── run_ingestion.py         # Bronze: weather, AQ, FIRMS (or call backfill_* + FIRMS download)
├── run_bronze_to_silver.py  # Silver: merge_wind_uv, AQ already in backfills, bronze_to_silver_firms
├── run_silver_to_gold.py    # Gold: silver_to_gold pipeline (weather + AQ + FIRMS)
└── run_preprocessing.py     # Optional: notebook export or small script that reads Gold only
```

**Notebooks:** Unchanged in `notebooks/`. Preprocessing notebook should be updated to read only from `data/gold/model_ready` (or `airquality_combined` where applicable) and not recompute features already in Gold.

---

## 5. Silver Layer Standardization (Target)

- **Naming:** Keep `stationID`, `lat`, `lon`, `timestamp_utc` for compatibility; add optional `station_id` alias and `date` (date-only) where needed. Document canonical names in `src/utils/schema.py`.
- **Timezone:** Store `timestamp_utc` in UTC; add `timestamp_bangkok` (Asia/Bangkok) for display/analytics if required.
- **Weather:** Already has u10_ms, v10_ms. No duplicate rows; null critical keys dropped.
- **AQ:** Accept both long and short column names in loaders; normalize to one set (e.g. pm2_5_ugm3, no2_ugm3, o3_ugm3) in Silver→Gold.
- **FIRMS:** Filter `type == 0`; filter `confidence` in (`nominal`, `high`); spatial filter within 500 km of Bangkok center; aggregate daily: `hotspot_count`, `frp_sum`, `frp_mean`, `weighted_frp_sum`; save Parquet partitioned by year/month.

---

## 6. Gold Layer Rebuild (Target)

- **Merge:** Silver weather (daily) + Silver AQ (daily) + Silver FIRMS (daily aggregates by region or grid).
- **Target:** Drop or impute rows with null `pm2_5_ugm3` (or configurable); validate no NaN target before save.
- **Features:** lag_1, lag_2, lag_3 (PM2.5); 7-day rolling mean (and optionally 3/14); temporal encoding; wind u10/v10 from weather.
- **No leakage:** Chronological split; lags/rolling computed per station/series; normalization from train only.
- **Validation:** Row count, date range, null counts for key columns, split counts.

---

## 7. Recommended Execution Order

1. **run_ingestion** (Bronze)  
   - Weather: `backfill_weather_with_wind_uv.py` (or existing weather ingestion).  
   - AQ: `backfill_5years.py` / `run_backfill_aq_2023_fast.py` / `backfill_missing_months.py` as needed.  
   - FIRMS: Download to `data/bronze/raw_hotspot`, then (optionally) validate.

2. **run_bronze_to_silver** (Silver)  
   - Weather: Ensure Silver weather exists (from ingestion or separate bronze→silver); then `merge_wind_uv_to_silver.py`.  
   - AQ: Produced by backfill scripts.  
   - FIRMS: `bronze_to_silver_firms_hotspot.py` (then extend with type/confidence filter, 500 km Bangkok, daily agg in next step).

3. **run_silver_to_gold** (Gold)  
   - Run `scripts/run_silver_to_gold.py` (or refactored pipeline that loads Silver weather + AQ + FIRMS, merges, adds lags/rolling, splits, normalizes).  
   - Optionally run `create_gold_airquality.py` for hourly AQ 2023–2025 to `airquality_combined/`.

4. **run_preprocessing** (Modeling-ready)  
   - Run `notebooks/pipelines/preprocessing_pipeline.ipynb` **reading only from Gold** (model_ready or airquality_combined as designed), no recomputation of features already in Gold; clear train/val/test split and no time leakage.

---

## 8. Dependencies

- **pyproject.toml:** No critical missing runtime deps. Optional `ml` (torch, shap) for modeling.  
- **Config:** Use `pydantic-settings` with env prefix; resolve all paths from `project_root` (set via env or `Path(__file__).resolve().parent` for repo root).

---

## 9. Next Steps (Implementation)

- Add `src/config.py` (unified config with bronze/silver/gold/FIRMS paths and options).  
- Add `src/validation/` (schema + bronze/silver validation helpers).  
- Harden Bronze: validate schema after write, log row counts, handle empty API responses, idempotent writes.  
- Standardize Silver: FIRMS script extension (type, confidence, 500 km Bangkok, daily agg); optional timezone column.  
- Rebuild Gold: merge weather + AQ + FIRMS; lags 1/2/3 and 7-day rolling; no NaN target; validate.  
- Add `run_ingestion.py`, `run_bronze_to_silver.py`, `run_silver_to_gold.py` at repo root.  
- Update preprocessing notebook to read from Gold only and document split logic.
