#!/usr/bin/env python3
"""
Inspect data quality for Bronze/Silver/Gold layers

Usage:
    python scripts/inspect_data_quality.py --layer silver
    python scripts/inspect_data_quality.py --layer gold
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import polars as pl

from src.silver_to_gold.loader import load_silver_weather
from src.silver_to_gold.quality import (
    assess_data_quality,
    check_station_coverage,
    check_temporal_coverage,
    print_quality_report,
)
from src.utils.config import PipelineConfig
from src.utils.logger import get_logger, setup_logging


def inspect_silver_layer(config: PipelineConfig) -> None:
    """Inspect Silver layer data quality"""
    log = get_logger(__name__)

    log.info("inspect.silver.start")

    weather_lazy = load_silver_weather(config.silver_weather_dir)
    weather_df = weather_lazy.collect()

    print(f"\n{'='*100}")
    print(f"SILVER LAYER - WEATHER DATA")
    print(f"{'='*100}")
    print(f"Total rows: {len(weather_df):,}")
    print(f"Total columns: {len(weather_df.columns)}")
    
    timestamp_col = "timestamp_utc" if "timestamp_utc" in weather_df.columns else "timestamp"
    if timestamp_col in weather_df.columns:
        print(f"Date range: {weather_df[timestamp_col].min()} → {weather_df[timestamp_col].max()}")
    
    print(f"Columns: {weather_df.columns}")
    print(f"{'='*100}\n")

    quality_report = assess_data_quality(weather_lazy)
    print_quality_report(quality_report)

    temporal_cov = check_temporal_coverage(weather_df)
    print("\nTEMPORAL COVERAGE:")
    for key, val in temporal_cov.items():
        print(f"  {key}: {val}")

    station_cov = check_station_coverage(weather_df)
    print("\nSTATION COVERAGE:")
    for key, val in station_cov.items():
        print(f"  {key}: {val}")

    print("\n")


def inspect_gold_layer(config: PipelineConfig) -> None:
    """Inspect Gold layer data quality"""
    log = get_logger(__name__)

    log.info("inspect.gold.start")

    train_path = config.gold_dir / "train.parquet"
    val_path = config.gold_dir / "val.parquet"
    test_path = config.gold_dir / "test.parquet"

    if not train_path.exists():
        print(f"\nGold layer not found at {config.gold_dir}")
        print("Run: python scripts/run_silver_to_gold.py\n")
        return

    for split_name, path in [("TRAIN", train_path), ("VAL", val_path), ("TEST", test_path)]:
        df = pl.read_parquet(path)

        print(f"\n{'='*100}")
        print(f"GOLD LAYER - {split_name} SPLIT")
        print(f"{'='*100}")
        print(f"Total rows: {len(df):,}")
        print(f"Total columns: {len(df.columns)}")
        print(f"Date range: {df['date'].min()} → {df['date'].max()}")
        
        station_col = "stationID" if "stationID" in df.columns else "station_id"
        print(f"Unique stations: {df[station_col].n_unique()}")

        pm25_available = df["pm2_5_mean"].null_count() < len(df)
        print(f"PM2.5 available: {'✅ YES' if pm25_available else '❌ NO (all null)'}")

        normalized_cols = [c for c in df.columns if c.endswith("_norm")]
        print(f"Normalized features: {len(normalized_cols)}")
        print(f"{'='*100}\n")

    manifest_path = config.gold_dir / "pipeline_manifest.json"
    if manifest_path.exists():
        import json

        with open(manifest_path) as f:
            manifest = json.load(f)

        print("\nPIPELINE MANIFEST:")
        print(f"  Created: {manifest['created_at']}")
        print(f"  Total features: {manifest['features']['total_count']}")
        print(f"  Normalized features: {manifest['features']['normalized_count']}")
        print(f"  Train rows: {manifest['splits']['train']['rows']:,}")
        print(f"  Val rows: {manifest['splits']['val']['rows']:,}")
        print(f"  Test rows: {manifest['splits']['test']['rows']:,}")
        print("\n")


def main() -> None:
    """Main execution"""
    parser = argparse.ArgumentParser(description="Inspect data quality")
    parser.add_argument(
        "--layer",
        choices=["silver", "gold"],
        default="silver",
        help="Which layer to inspect",
    )

    args = parser.parse_args()

    config = PipelineConfig()
    setup_logging(level="INFO")

    if args.layer == "silver":
        inspect_silver_layer(config)
    elif args.layer == "gold":
        inspect_gold_layer(config)


if __name__ == "__main__":
    main()
