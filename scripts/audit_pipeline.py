#!/usr/bin/env python3
"""
End-to-end audit of PM2.5 prediction pipeline with focus on hotspot feature usage.
Run from project root: python scripts/audit_pipeline.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

def step1_raw_hotspot():
    print("\n" + "=" * 70)
    print("STEP 1 — Raw hotspot data (data/bronze/raw_hotspot/)")
    print("=" * 70)
    bronze = PROJECT_ROOT / "data" / "bronze" / "raw_hotspot"
    if not bronze.exists():
        print("ERROR: data/bronze/raw_hotspot/ not found")
        return None
    files = list(bronze.rglob("*.csv"))
    print(f"  CSV file count: {len(files)}")
    # Sample region-relevant files for schema and stats (faster)
    region_substrings = ["Thailand", "Myanmar", "Laos", "Cambodia", "Vietnam", "Malaysia", "Indonesia", "China"]
    region_files = [f for f in files if any(s in str(f) for s in region_substrings)]
    print(f"  Region-relevant CSVs (8 countries): {len(region_files)}")
    if not region_files:
        print("  WARNING: No region CSVs found")
        return None
    # Load first few files to get schema and sample
    dfs = []
    for fp in region_files[:16]:  # limit for speed
        try:
            df = pd.read_csv(fp, nrows=5000)
            df["_source"] = fp.name
            dfs.append(df)
        except Exception as e:
            print(f"  Skip {fp.name}: {e}")
    if not dfs:
        print("  Could not load any CSV")
        return None
    raw = pd.concat(dfs, ignore_index=True)
    print(f"  Schema (columns): {list(raw.columns)}")
    print(f"  Sample total rows (from subset of files): {len(raw)}")
    print(f"  Missing: latitude={raw['latitude'].isna().sum()}, longitude={raw['longitude'].isna().sum()}, acq_date={raw['acq_date'].isna().sum()}")
    if "acq_date" in raw.columns:
        raw["acq_date"] = pd.to_datetime(raw["acq_date"], errors="coerce")
        raw = raw.dropna(subset=["acq_date"])
        print(f"  Date range (sample): {raw['acq_date'].min()} to {raw['acq_date'].max()}")
    return raw

def step2_hotspot_pipeline():
    print("\n" + "=" * 70)
    print("STEP 2 — Hotspot feature pipeline (src/hotspot_features.py)")
    print("=" * 70)
    from src.hotspot_features import (
        load_hotspot_data,
        assign_region,
        aggregate_hotspots,
        create_feature_table,
        create_lag_features,
        REGION_COUNTRY_FILTER,
    )
    bronze = PROJECT_ROOT / "data" / "bronze" / "raw_hotspot"
    # Load (with filter for speed)
    raw = load_hotspot_data(bronze, file_substrings=REGION_COUNTRY_FILTER)
    print(f"\n  load_hotspot_data(): shape={raw.shape}, columns={list(raw.columns)}")
    print(f"  date range: {raw['date'].min() if not raw.empty else 'N/A'} to {raw['date'].max() if not raw.empty else 'N/A'}")
    if raw.empty:
        print("  WARNING: No raw data")
        return None, None, None, None, None
    assigned = assign_region(raw)
    print(f"\n  assign_region(): shape={assigned.shape}")
    print(f"  unique source_region: {sorted(assigned['source_region'].unique().tolist())}")
    agg = aggregate_hotspots(assigned)
    print(f"\n  aggregate_hotspots(): shape={agg.shape}, columns={list(agg.columns)}")
    print(f"  date range: {agg['date'].min()} to {agg['date'].max()}")
    wide = create_feature_table(agg)
    print(f"\n  create_feature_table(): shape={wide.shape}, columns={list(wide.columns)}")
    with_lags = create_lag_features(wide)
    print(f"\n  create_lag_features(): shape={with_lags.shape}")
    hotspot_cols = [c for c in with_lags.columns if "hotspot" in c]
    print(f"  Hotspot-related columns count: {len(hotspot_cols)}")
    return raw, assigned, agg, wide, with_lags

def step3_aggregation_correctness(agg):
    print("\n" + "=" * 70)
    print("STEP 3 — Aggregation correctness")
    print("=" * 70)
    if agg is None or agg.empty:
        print("  No aggregated data")
        return
    dup = agg.duplicated(subset=["date", "source_region"]).sum()
    print(f"  Duplicate (date, source_region): {dup}")
    print(f"  One row per date per region: {dup == 0}")
    print(f"  Hotspot count stats: min={agg['hotspot_count'].min()}, max={agg['hotspot_count'].max()}, mean={agg['hotspot_count'].mean():.1f}")
    dates = pd.to_datetime(agg["date"]).dt.normalize()
    uniq_dates = dates.unique()
    print(f"  Unique dates: {len(uniq_dates)}")

def step4_feature_table():
    print("\n" + "=" * 70)
    print("STEP 4 — Hotspot feature table (build_hotspot_feature_table)")
    print("=" * 70)
    from src.hotspot_features import build_hotspot_feature_table, HOTSPOT_REGION_COLS
    bronze = PROJECT_ROOT / "data" / "bronze" / "raw_hotspot"
    hf = build_hotspot_feature_table(bronze)
    print(f"  Rows: {len(hf)}, Columns: {len(hf.columns)}")
    region_cols = [c for c in hf.columns if c in HOTSPOT_REGION_COLS or c.startswith("hotspot_") and "_lag" in c]
    print(f"  Hotspot-related columns: {len(region_cols)} (expected 32)")
    for c in HOTSPOT_REGION_COLS:
        present = c in hf.columns
        print(f"    {c}: {'OK' if present else 'MISSING'}")
    lag_examples = [c for c in hf.columns if "_lag1" in c][:3]
    print(f"  Lag column examples: {lag_examples}")

def step5_6_7_gold_and_manifest():
    print("\n" + "=" * 70)
    print("STEP 5–7 — Gold dataset and pipeline manifest")
    print("=" * 70)
    gold = PROJECT_ROOT / "data" / "gold" / "model_ready"
    for name in ["train", "val", "test"]:
        path = gold / f"{name}.parquet"
        if not path.exists():
            print(f"  {name}.parquet: NOT FOUND")
            continue
        df = pd.read_parquet(path)
        hotspot_cols = [c for c in df.columns if "hotspot" in c]
        print(f"\n  {name}.parquet: rows={len(df)}, cols={len(df.columns)}, hotspot_cols={len(hotspot_cols)}")
        if hotspot_cols:
            sub = df[hotspot_cols]
            print(f"    Hotspot columns: {hotspot_cols[:5]}... ({len(hotspot_cols)} total)")
            print(f"    Missing: {sub.isna().sum().sum()} (total)")
            print(f"    Mean: {sub.mean().mean():.4f}, Std: {sub.std().mean():.4f}")
            print(f"    Min: {sub.min().min():.2f}, Max: {sub.max().max():.2f}")
    manifest_path = gold / "pipeline_manifest.json"
    if not manifest_path.exists():
        print(f"\n  pipeline_manifest.json: NOT FOUND")
        return
    with open(manifest_path) as f:
        manifest = json.load(f)
    # Support both "feature_cols" (preprocessing_pipeline.ipynb) and "features"."columns" (gold layer)
    feature_cols = manifest.get("feature_cols") or (manifest.get("features") or {}).get("columns") or []
    print(f"\n  pipeline_manifest.json:")
    print(f"    Has 'feature_cols' (expected by model_training): {'feature_cols' in manifest}")
    print(f"    Total features in manifest: {len(feature_cols)}")
    hotspot_in_manifest = [c for c in feature_cols if "hotspot" in c]
    print(f"    Hotspot features in feature_cols: {len(hotspot_in_manifest)}")
    if hotspot_in_manifest:
        print(f"    List: {hotspot_in_manifest}")

def step8_9_model_input():
    print("\n" + "=" * 70)
    print("STEP 8–9 — Model input features and tensor")
    print("=" * 70)
    gold = PROJECT_ROOT / "data" / "gold" / "model_ready"
    manifest_path = gold / "pipeline_manifest.json"
    if not manifest_path.exists():
        print("  No manifest; cannot verify model input")
        return
    with open(manifest_path) as f:
        manifest = json.load(f)
    feature_cols = manifest.get("feature_cols") or (manifest.get("features") or {}).get("columns") or []
    train_path = gold / "train.parquet"
    if not train_path.exists():
        print("  train.parquet not found")
        return
    df = pd.read_parquet(train_path)
    # Model uses feature_cols from manifest
    missing = [c for c in feature_cols if c not in df.columns]
    present = [c for c in feature_cols if c in df.columns]
    hotspot_used = [c for c in present if "hotspot" in c]
    print(f"  feature_cols in manifest: {len(feature_cols)}")
    print(f"  Present in train.parquet: {len(present)}")
    print(f"  Missing in train.parquet: {len(missing)}")
    if missing:
        print(f"  Missing columns: {missing[:15]}")
    print(f"  Hotspot features present (used by model): {len(hotspot_used)}")
    if hotspot_used:
        print(f"  Example: {hotspot_used[:5]}")

def step10_correlation():
    print("\n" + "=" * 70)
    print("STEP 10 — Correlation: hotspot vs PM2.5")
    print("=" * 70)
    train_path = PROJECT_ROOT / "data" / "gold" / "model_ready" / "train.parquet"
    if not train_path.exists():
        print("  train.parquet not found")
        return
    df = pd.read_parquet(train_path)
    target = "pm2_5_ugm3" if "pm2_5_ugm3" in df.columns else "pm2_5_mean" if "pm2_5_mean" in df.columns else None
    hotspot_cols = [c for c in df.columns if "hotspot" in c and df[c].dtype in ["float64", "float32"]]
    if not target or not hotspot_cols:
        print("  No target or hotspot columns")
        return
    valid = df[[target] + hotspot_cols].dropna()
    if len(valid) < 10:
        print("  Too few non-NaN rows for correlation (hotspot columns are likely all NaN in current gold data)")
        print("  Non-null count for hotspot columns:", df[hotspot_cols].notna().sum().to_dict())
        return
    corr = valid[hotspot_cols].corrwith(valid[target])
    print("  Correlation of hotspot features with target:")
    for c in corr.index[:12]:
        print(f"    {c}: {corr[c]:.4f}")
    print(f"  (showing first 12 of {len(corr)})")

def step11_unused_features():
    print("\n" + "=" * 70)
    print("STEP 11 — Unused / misaligned features")
    print("=" * 70)
    gold = PROJECT_ROOT / "data" / "gold" / "model_ready"
    manifest_path = gold / "pipeline_manifest.json"
    train_path = gold / "train.parquet"
    if not manifest_path.exists() or not train_path.exists():
        print("  Missing manifest or train.parquet")
        return
    with open(manifest_path) as f:
        manifest = json.load(f)
    feature_cols = set(manifest.get("feature_cols") or (manifest.get("features") or {}).get("columns") or [])
    df = pd.read_parquet(train_path)
    dataset_cols = set(df.columns)
    in_manifest_not_in_data = feature_cols - dataset_cols
    in_data_not_in_manifest = dataset_cols - feature_cols
    # ID/target often in data but not in feature_cols
    id_like = {"stationID", "date", "lat", "lon", "split"}
    in_data_not_in_manifest = in_data_not_in_manifest - id_like
    if "pm2_5_ugm3" in feature_cols or "pm2_5_mean" in feature_cols:
        in_data_not_in_manifest.discard("pm2_5_ugm3")
        in_data_not_in_manifest.discard("pm2_5_mean")
    print(f"  In manifest but not in dataset: {len(in_manifest_not_in_data)}")
    if in_manifest_not_in_data:
        print(f"    {sorted(in_manifest_not_in_data)[:20]}")
    print(f"  In dataset but not in manifest (potential unused): {len(in_data_not_in_manifest)}")
    if in_data_not_in_manifest:
        print(f"    {sorted(in_data_not_in_manifest)[:20]}")

def main():
    print("PM2.5 Pipeline Audit — Full end-to-end verification")
    step1_raw_hotspot()
    raw, assigned, agg, wide, with_lags = step2_hotspot_pipeline()
    step3_aggregation_correctness(agg)
    step4_feature_table()
    step5_6_7_gold_and_manifest()
    step8_9_model_input()
    step10_correlation()
    step11_unused_features()
    print("\n" + "=" * 70)
    print("STEP 12 — See final audit report below")
    print("=" * 70)

if __name__ == "__main__":
    main()
