#!/usr/bin/env python3
"""
Generate visual summary of data pipeline

Usage:
    python scripts/visualize_pipeline.py
"""

from __future__ import annotations

import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import polars as pl

from src.utils.config import PipelineConfig


def print_pipeline_visual() -> None:
    """Print ASCII art visualization of pipeline"""

    print("\n" + "=" * 100)
    print(" " * 30 + "BANGKOK PM2.5 DATA PIPELINE")
    print("=" * 100)

    print("""
    ┌─────────────────────────────────────────────────────────────────────────────────┐
    │                           DATA LAKEHOUSE ARCHITECTURE                            │
    └─────────────────────────────────────────────────────────────────────────────────┘

    ┌──────────────────────┐       ┌──────────────────────┐       ┌──────────────────────┐
    │   🥉 BRONZE LAYER    │       │   🥈 SILVER LAYER    │       │   🥇 GOLD LAYER      │
    │                      │       │                      │       │                      │
    │  Raw Immutable Data  │──────▶│  Cleaned & Validated │──────▶│  Model-Ready         │
    │                      │       │                      │       │                      │
    │  Format: JSON.gz     │       │  Format: Parquet     │       │  Format: Parquet     │
    │  Size: 3.1 MB        │       │  Size: 328.4 MB      │       │  Size: 12.0 MB       │
    │  Files: 122          │       │  Files: 7,350        │       │  Files: 3 splits     │
    │                      │       │                      │       │                      │
    │  • Open-Meteo API    │       │  • Schema enforced   │       │  • 74 features       │
    │  • Hourly data       │       │  • Deduplicated      │       │  • Daily aggregation │
    │  • 2010-2017         │       │  • Hive-partitioned  │       │  • Lag & rolling     │
    │  • 79 stations       │       │  • MD5 checksums     │       │  • Normalized        │
    └──────────────────────┘       └──────────────────────┘       └──────────────────────┘
                                                                              │
                                                                              ▼
                                                                   ┌──────────────────────┐
                                                                   │  ML Models           │
                                                                   │  • ST-UNN            │
                                                                   │  • LSTM / GRU        │
                                                                   │  • Baseline MLP      │
                                                                   └──────────────────────┘
    """)

    print("=" * 100)


def print_feature_breakdown() -> None:
    """Print feature breakdown"""

    config = PipelineConfig()
    manifest_path = config.gold_dir / "pipeline_manifest.json"

    if not manifest_path.exists():
        print("\n⚠️  Gold layer not found. Run: python scripts/run_silver_to_gold.py\n")
        return

    import json

    with open(manifest_path) as f:
        manifest = json.load(f)

    print("\n" + "=" * 100)
    print(" " * 35 + "FEATURE BREAKDOWN")
    print("=" * 100)

    features = manifest["features"]["columns"]

    groups = {
        "Identifiers": ["date", "stationID", "split", "load_id"],
        "Spatial": ["lat", "lon"],
        "Base Weather": [
            "temp_2m_mean",
            "temp_2m_min",
            "temp_2m_max",
            "rh_2m_mean",
            "pressure_mean",
            "precip_sum",
            "wind_speed_mean",
            "wind_direction_mean",
            "wind_u10_mean",
            "wind_v10_mean",
            "radiation_mean",
            "cloud_cover_mean",
        ],
        "Air Quality": ["pm2_5_mean", "pm10_mean", "no2_mean", "o3_mean"],
        "Lag Features": [f for f in features if "_lag" in f],
        "Rolling Features": [f for f in features if "_rolling_" in f],
        "Temporal Encoding": [f for f in features if "_sin" in f or "_cos" in f],
        "Hotspot Features": [f for f in features if "hotspot" in f or "transboundary" in f],
    }

    for group_name, group_features in groups.items():
        actual_features = [f for f in group_features if f in features]
        status = "✅" if actual_features else "⚠️"
        print(f"\n{status} {group_name} ({len(actual_features)} features)")
        for feat in actual_features[:5]:
            print(f"   • {feat}")
        if len(actual_features) > 5:
            print(f"   ... and {len(actual_features) - 5} more")

    print("\n" + "=" * 100 + "\n")


def print_split_summary() -> None:
    """Print train/val/test split summary"""

    config = PipelineConfig()

    train_path = config.gold_dir / "train.parquet"
    val_path = config.gold_dir / "val.parquet"
    test_path = config.gold_dir / "test.parquet"

    if not train_path.exists():
        return

    train = pl.read_parquet(train_path)
    val = pl.read_parquet(val_path)
    test = pl.read_parquet(test_path)

    print("\n" + "=" * 100)
    print(" " * 35 + "TRAIN/VAL/TEST SPLITS")
    print("=" * 100)

    train_rows = f"{len(train):,}"
    val_rows = f"{len(val):,}"
    test_rows = f"{len(test):,}"
    
    print(f"""
    ┌────────────────────────────────────────────────────────────────────────────┐
    │  TRAIN (70%)                                                               │
    │  • Rows: {train_rows:<20}                                              │
    │  • Date Range: {str(train['date'].min())} → {str(train['date'].max())}                      │
    │  • Stations: {train['stationID'].n_unique()}                                                           │
    └────────────────────────────────────────────────────────────────────────────┘

    ┌────────────────────────────────────────────────────────────────────────────┐
    │  VALIDATION (15%)                                                          │
    │  • Rows: {val_rows:<20}                                               │
    │  • Date Range: {str(val['date'].min())} → {str(val['date'].max())}                       │
    │  • Stations: {val['stationID'].n_unique()}                                                            │
    └────────────────────────────────────────────────────────────────────────────┘

    ┌────────────────────────────────────────────────────────────────────────────┐
    │  TEST (15%)                                                                │
    │  • Rows: {test_rows:<20}                                               │
    │  • Date Range: {str(test['date'].min())} → {str(test['date'].max())}                       │
    │  • Stations: {test['stationID'].n_unique()}                                                           │
    └────────────────────────────────────────────────────────────────────────────┘
    """)

    print("=" * 100 + "\n")


def main() -> None:
    """Main execution"""
    print_pipeline_visual()
    print_feature_breakdown()
    print_split_summary()


if __name__ == "__main__":
    main()
