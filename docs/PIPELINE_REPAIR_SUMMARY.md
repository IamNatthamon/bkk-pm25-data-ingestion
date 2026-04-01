# PM2.5 Pipeline Repair — Summary Report

**Date:** 2026-03-04  
**Goal:** Fix the pipeline so the model uses **real hotspot features** (32 columns) instead of placeholder columns that were all NaN.

---

## What Was Done

### 1. Data directories and paths
- **Project root** is resolved via `_resolve_project_root()` so that running the notebook from `notebooks/pipelines/` still uses `project_root / "data" / ...` (repo root).
- **Silver AQ:** config uses `openmeteo_airquality` with fallback to `air_quality` if the first does not exist.
- Checked: `data/bronze/raw_hotspot`, `data/silver/openmeteo_weather`, `data/silver/openmeteo_airquality` (or `air_quality`) are the intended locations.

### 2. Hotspot feature pipeline
- **Source:** `src/hotspot_features.py` — unchanged; already produces 32 columns.
- **Output of `build_hotspot_feature_table()`:**
  - **8 region columns:** `hotspot_thailand`, `hotspot_myanmar`, `hotspot_laos`, `hotspot_cambodia`, `hotspot_vietnam`, `hotspot_malaysia`, `hotspot_indonesia`, `hotspot_china_yunnan`
  - **24 lag columns:** `hotspot_<region>_lag1`, `_lag2`, `_lag3` for each region.
  - **Total:** 32 hotspot columns.

### 3. Preprocessing notebook — hotspot merge
- **Notebook:** `notebooks/pipelines/preprocessing_pipeline.ipynb`
- **Cell 31** now:
  1. Builds the hotspot feature table from `data/bronze/raw_hotspot` via `build_hotspot_feature_table()`.
  2. Merges into `merged_daily` with `merge_hotspot_features_into_daily(merged_daily, hotspot_df, date_col="date")`.
  3. Drops old placeholder columns: `hotspot_count`, `hotspot_count_lag1`, `hotspot_count_lag2`, `hotspot_count_lag3`, `hotspot_frp_sum`, `transboundary_index`, `hotspot_count_th`, `hotspot_count_mm`, `hotspot_count_la`, `frp_sum`, `frp_sum_lag1/2/3`, `frp_mean`, `weighted_frp_sum`.
- **Validation cell** (inserted after): checks that `len([c for c in merged_daily.columns if c.startswith("hotspot")]) == 32`.

### 4. Missing data handling
- **`handle_missing_data()`** (cell 34):
  - **Not interpolated:** `pm2_5_ugm3`, `lat`, `lon`, and any column whose name starts with `hotspot`.
  - **Interpolated:** only weather (and other numeric) columns, for small gaps (≤ 3 days) per station.

### 5. Merge helper fix
- **`src/hotspot_features.merge_hotspot_features_into_daily()`:** after merge, a single `date` column is kept (drop duplicate from right, keep left as `date`).

### 6. Gold dataset and manifest
- **Regeneration:** Re-run **all cells** of `notebooks/pipelines/preprocessing_pipeline.ipynb` (from project root or from `notebooks/pipelines/`) to regenerate:
  - `data/gold/model_ready/train.parquet`
  - `data/gold/model_ready/val.parquet`
  - `data/gold/model_ready/test.parquet`
  - `data/gold/model_ready/pipeline_manifest.json`
- **Manifest** written by `save_pipeline_manifest()` already includes:
  - `feature_cols` (includes all 32 hotspot columns)
  - `target_col` (e.g. `pm2_5_ugm3`)
  - `sequence_length`
  - `num_features`

### 7. Model training notebook
- **Notebook:** `notebooks/modeling/model_training.ipynb`
- **Manifest loading** (cell 6) now supports:
  - New format: `feature_cols`, `target_col`, `sequence_length`, `forecast_horizons`, `num_features`.
  - Legacy format: `features.columns`; infers `target_col`, `sequence_length`, `forecast_horizons` if missing.
- Drops ID-like names from the feature list (`date`, `stationID`, `lat`, `lon`, `split`) when building `FEATURE_COLS`.
- Prints how many **hotspot features** are in `FEATURE_COLS`.

---

## What You Need To Do

1. **Regenerate gold**
   - Open and run **all cells** of `notebooks/pipelines/preprocessing_pipeline.ipynb`.
   - Ensure the config cell runs first so `CFG.project_root` points to the repo root (where `data/` lives).
   - After the run you should have:
     - `data/gold/model_ready/train.parquet` (and val/test) with **32 hotspot columns** containing numeric values (not all NaN).
     - `data/gold/model_ready/pipeline_manifest.json` with `feature_cols` containing those 32 hotspot names.

2. **Validate**
   - In the preprocessing notebook, after the hotspot merge cell:
     - `[c for c in merged_daily.columns if c.startswith("hotspot")]` should have length **32**.
   - After saving:
     - Open `pipeline_manifest.json` and confirm `feature_cols` lists the hotspot columns.
     - Load `train.parquet` and check that `train[[c for c in train.columns if 'hotspot' in c]].describe()` shows non-null stats.

3. **Train the model**
   - Run `notebooks/modeling/model_training.ipynb`.
   - It will load `feature_cols` (including 32 hotspot features) and use them in the input tensor.
   - Check the printed “Hotspot features” count and, if you add correlation/SHAP, that hotspot features correlate with PM2.5 and appear in importance.

---

## Expected After Repair

| Item | Expected |
|------|----------|
| Hotspot columns in merged_daily | 32 |
| Hotspot columns in train/val/test.parquet | 32 |
| Hotspot columns in pipeline_manifest.json `feature_cols` | 32 |
| Hotspot values | Numeric, not all NaN |
| Interpolation | Excludes pm2_5_ugm3, lat, lon, and all `hotspot_*` columns |
| Model input | Includes all 32 hotspot features in the feature tensor |

---

## File Changes Summary

| File | Change |
|------|--------|
| `notebooks/pipelines/preprocessing_pipeline.ipynb` | Hotspot cell (31) uses bronze + merge; validation cell (32); handle_missing_data (34) exclusions; config AQ fallback and project root resolution |
| `src/hotspot_features.py` | Merge result: single `date` column (drop duplicate from merge) |
| `notebooks/modeling/model_training.ipynb` | Manifest loading supports both formats and prints hotspot feature count |
| `docs/PIPELINE_REPAIR_SUMMARY.md` | This report |
