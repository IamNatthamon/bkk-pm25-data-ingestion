"""
Gold Layer Pipeline - Silver → Gold Transformation

Transforms Silver air quality data into ML-ready Gold layer with:
- Feature engineering (lags, rolling stats, temporal features)
- Train/Val/Test chronological splits
- Normalization
- Data quality checks
"""

from __future__ import annotations

import json
import sys
import time
from datetime import datetime
from pathlib import Path

import polars as pl

from config.gold import config
from src.gold.features import (
    add_lag_features,
    add_rate_of_change,
    add_rolling_features,
    add_target_variable,
    add_temporal_features,
    interpolate_missing,
)
from src.gold.loader import load_silver_airquality, load_stations

sys.stdout.reconfigure(line_buffering=True)


def create_chronological_splits(
    df: pl.DataFrame,
    train_ratio: float,
    val_ratio: float,
    test_ratio: float,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """
    Create chronological train/val/test splits.
    
    No shuffling - maintains temporal order to prevent data leakage.
    """
    print(f"\n📊 Creating chronological splits:", flush=True)
    print(f"   Train: {train_ratio:.1%} | Val: {val_ratio:.1%} | Test: {test_ratio:.1%}", flush=True)
    
    # Sort by time
    df = df.sort("timestamp_utc")
    
    n = len(df)
    train_end = int(n * train_ratio)
    val_end = int(n * (train_ratio + val_ratio))
    
    train_df = df[:train_end]
    val_df = df[train_end:val_end]
    test_df = df[val_end:]
    
    print(f"\n   Train: {len(train_df):,} rows | {train_df['timestamp_utc'].min()} → {train_df['timestamp_utc'].max()}", flush=True)
    print(f"   Val:   {len(val_df):,} rows | {val_df['timestamp_utc'].min()} → {val_df['timestamp_utc'].max()}", flush=True)
    print(f"   Test:  {len(test_df):,} rows | {test_df['timestamp_utc'].min()} → {test_df['timestamp_utc'].max()}", flush=True)
    
    return train_df, val_df, test_df


def normalize_features(
    train_df: pl.DataFrame,
    val_df: pl.DataFrame,
    test_df: pl.DataFrame,
    feature_cols: list[str],
    method: str = "standard",
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, dict]:
    """
    Normalize features using training set statistics.
    
    Critical: Only fit on training data to prevent data leakage!
    """
    print(f"\n🔧 Normalizing features ({method}):", flush=True)
    
    stats = {}
    
    for col in feature_cols:
        if col not in train_df.columns:
            continue
        
        if method == "standard":
            # Z-score normalization
            mean = train_df[col].mean()
            std = train_df[col].std()
            
            if std == 0 or std is None:
                print(f"  ⚠️  {col}: std=0, skipping", flush=True)
                continue
            
            stats[col] = {"mean": mean, "std": std, "method": "standard"}
            
            # Apply to all splits
            train_df = train_df.with_columns(
                ((pl.col(col) - mean) / std).alias(col)
            )
            val_df = val_df.with_columns(
                ((pl.col(col) - mean) / std).alias(col)
            )
            test_df = test_df.with_columns(
                ((pl.col(col) - mean) / std).alias(col)
            )
            
        elif method == "minmax":
            # Min-max normalization to [0, 1]
            min_val = train_df[col].min()
            max_val = train_df[col].max()
            
            if max_val == min_val:
                print(f"  ⚠️  {col}: max=min, skipping", flush=True)
                continue
            
            stats[col] = {"min": min_val, "max": max_val, "method": "minmax"}
            
            train_df = train_df.with_columns(
                ((pl.col(col) - min_val) / (max_val - min_val)).alias(col)
            )
            val_df = val_df.with_columns(
                ((pl.col(col) - min_val) / (max_val - min_val)).alias(col)
            )
            test_df = test_df.with_columns(
                ((pl.col(col) - min_val) / (max_val - min_val)).alias(col)
            )
    
    print(f"  ✅ Normalized {len(stats)} features", flush=True)
    
    return train_df, val_df, test_df, stats


def run_gold_pipeline() -> None:
    """Main Gold pipeline execution"""
    
    print("\n" + "=" * 100, flush=True)
    print(" " * 25 + "🌟 GOLD LAYER PIPELINE - PM2.5 FORECASTING", flush=True)
    print("=" * 100, flush=True)
    
    start_time = time.time()
    
    # 1. Load Silver data
    print("\n📂 Step 1: Loading Silver data", flush=True)
    df = load_silver_airquality(config.silver_aq_path, config.target_years)
    
    # 2. Basic data quality
    print("\n🔍 Step 2: Data quality checks", flush=True)
    initial_rows = len(df)
    
    # Remove rows with missing target
    df = df.filter(pl.col(config.target_column).is_not_null())
    print(f"  Removed {initial_rows - len(df):,} rows with null {config.target_column}", flush=True)
    
    # 3. Feature engineering
    print("\n🔧 Step 3: Feature engineering", flush=True)
    
    # Interpolate missing values
    if config.interpolate_missing:
        numeric_cols = [
            "pm2_5_ugm3", "pm10_ugm3", "nitrogen_dioxide_ugm3",
            "ozone_ugm3", "sulphur_dioxide_ugm3", "carbon_monoxide_ugm3"
        ]
        df = interpolate_missing(df, numeric_cols, config.interpolation_method)
    
    # Add temporal features
    df = add_temporal_features(df)
    
    # Add lag features
    df = add_lag_features(df, config.target_column, config.lag_hours)
    
    # Add rolling features
    df = add_rolling_features(df, config.target_column, config.rolling_windows)
    
    # Add rate of change
    df = add_rate_of_change(df, config.target_column)
    
    # Add target variable (future value to predict)
    df = add_target_variable(
        df, config.target_column, config.forecast_horizon
    )
    
    # Remove rows with null features/target (due to lag/rolling/target shift)
    df = df.drop_nulls()
    print(f"\n  ✅ Final dataset: {len(df):,} rows | {len(df.columns)} features", flush=True)
    
    # 4. Train/Val/Test split
    print("\n📊 Step 4: Creating splits", flush=True)
    train_df, val_df, test_df = create_chronological_splits(
        df, config.train_ratio, config.val_ratio, config.test_ratio
    )
    
    # 5. Normalization
    if config.normalize_features:
        print("\n🔧 Step 5: Normalization", flush=True)
        
        # Identify feature columns (exclude metadata and target)
        exclude_cols = [
            "stationID", "timestamp_utc", "timestamp_unix_ms",
            "data_source", "ingestion_timestamp_utc", "load_id",
            "pipeline_version", "record_hash", "lat", "lon",
            f"target_{config.target_column}_{config.forecast_horizon}h"
        ]
        
        feature_cols = [c for c in df.columns if c not in exclude_cols]
        
        train_df, val_df, test_df, norm_stats = normalize_features(
            train_df, val_df, test_df, feature_cols, config.normalization_method
        )
    else:
        norm_stats = {}
    
    # 6. Save Gold layer
    print("\n💾 Step 6: Saving Gold layer", flush=True)
    
    gold_dir = config.gold_output_path
    gold_dir.mkdir(parents=True, exist_ok=True)
    
    # Save splits
    train_path = gold_dir / "train.parquet"
    val_path = gold_dir / "val.parquet"
    test_path = gold_dir / "test.parquet"
    
    train_df.write_parquet(train_path, compression="snappy")
    val_df.write_parquet(val_path, compression="snappy")
    test_df.write_parquet(test_path, compression="snappy")
    
    print(f"  ✅ Train: {train_path}", flush=True)
    print(f"  ✅ Val:   {val_path}", flush=True)
    print(f"  ✅ Test:  {test_path}", flush=True)
    
    # Save normalization stats
    if norm_stats:
        stats_path = gold_dir / "normalization_stats.json"
        with open(stats_path, "w") as f:
            json.dump(norm_stats, f, indent=2, default=str)
        print(f"  ✅ Stats: {stats_path}", flush=True)
    
    # Save pipeline metadata
    metadata = {
        "pipeline_version": config.pipeline_version,
        "created_at": datetime.utcnow().isoformat() + "Z",
        "source_years": config.target_years,
        "total_rows": len(df),
        "train_rows": len(train_df),
        "val_rows": len(val_df),
        "test_rows": len(test_df),
        "num_features": len(feature_cols) if config.normalize_features else len(df.columns),
        "forecast_horizon": config.forecast_horizon,
        "target_column": f"target_{config.target_column}_{config.forecast_horizon}h",
        "lag_features": config.lag_hours,
        "rolling_windows": config.rolling_windows,
        "normalization_method": config.normalization_method if config.normalize_features else None,
    }
    
    metadata_path = gold_dir / "pipeline_metadata.json"
    with open(metadata_path, "w") as f:
        json.dump(metadata, f, indent=2)
    print(f"  ✅ Metadata: {metadata_path}", flush=True)
    
    elapsed = time.time() - start_time
    
    print("\n" + "=" * 100, flush=True)
    print(" " * 35 + "✅ PIPELINE COMPLETE", flush=True)
    print("=" * 100, flush=True)
    print(f"⏱️  Total time: {elapsed / 60:.1f} minutes", flush=True)
    print(f"📊 Gold layer ready for ML training!", flush=True)
    print("=" * 100 + "\n", flush=True)


if __name__ == "__main__":
    run_gold_pipeline()
