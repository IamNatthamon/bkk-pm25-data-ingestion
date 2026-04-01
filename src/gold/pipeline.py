"""
Gold Layer Pipeline — DEPRECATED

Use src.silver_to_gold.pipeline.run_silver_to_gold_pipeline() instead.
This module is kept for backward compatibility only.
"""

from __future__ import annotations

import json
import time
import warnings
from datetime import datetime
from pathlib import Path

import polars as pl

from src.utils.logger import get_logger

log = get_logger(__name__)

warnings.warn(
    "src.gold.pipeline is deprecated. Use src.silver_to_gold.pipeline instead.",
    DeprecationWarning,
    stacklevel=2,
)

try:
    from config.gold import config as _config  # type: ignore[import]
    from src.gold.features import (
        add_lag_features,
        add_rate_of_change,
        add_rolling_features,
        add_target_variable,
        add_temporal_features,
        interpolate_missing,
    )
    from src.gold.loader import load_silver_airquality, load_stations
except ImportError:
    _config = None  # type: ignore[assignment]


def create_chronological_splits(
    df: pl.DataFrame,
    train_ratio: float,
    val_ratio: float,
    test_ratio: float,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Create chronological train/val/test splits. No shuffling — prevents data leakage."""
    log.info("splits.creating", train=train_ratio, val=val_ratio, test=test_ratio)

    df = df.sort("timestamp_utc")
    n = len(df)
    train_end = int(n * train_ratio)
    val_end = int(n * (train_ratio + val_ratio))

    train_df = df[:train_end]
    val_df = df[train_end:val_end]
    test_df = df[val_end:]

    log.info(
        "splits.created",
        train_rows=len(train_df),
        val_rows=len(val_df),
        test_rows=len(test_df),
    )
    return train_df, val_df, test_df


def normalize_features(
    train_df: pl.DataFrame,
    val_df: pl.DataFrame,
    test_df: pl.DataFrame,
    feature_cols: list[str],
    method: str = "standard",
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, dict]:
    """Normalize features using training set statistics only — prevents data leakage."""
    log.info("normalize.start", method=method, num_cols=len(feature_cols))

    stats: dict = {}

    for col in feature_cols:
        if col not in train_df.columns:
            continue

        if method == "standard":
            mean = train_df[col].mean()
            std = train_df[col].std()

            if std == 0 or std is None:
                log.warning("normalize.skip_zero_std", column=col)
                continue

            stats[col] = {"mean": mean, "std": std, "method": "standard"}
            train_df = train_df.with_columns(((pl.col(col) - mean) / std).alias(col))
            val_df = val_df.with_columns(((pl.col(col) - mean) / std).alias(col))
            test_df = test_df.with_columns(((pl.col(col) - mean) / std).alias(col))

        elif method == "minmax":
            min_val = train_df[col].min()
            max_val = train_df[col].max()

            if max_val == min_val:
                log.warning("normalize.skip_constant", column=col)
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

    log.info("normalize.complete", num_normalized=len(stats))
    return train_df, val_df, test_df, stats


def run_gold_pipeline() -> None:
    """Main Gold pipeline execution. DEPRECATED — use src.silver_to_gold.pipeline instead."""
    warnings.warn(
        "run_gold_pipeline() is deprecated. Use src.silver_to_gold.pipeline.run_silver_to_gold_pipeline() instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    if _config is None:
        raise RuntimeError("config.gold module not available.")
    config = _config

    log.info("pipeline.start", version="gold-legacy")
    start_time = time.time()

    log.info("pipeline.step", step=1, name="load_silver")
    df = load_silver_airquality(config.silver_aq_path, config.target_years)

    log.info("pipeline.step", step=2, name="quality_check")
    initial_rows = len(df)
    df = df.filter(pl.col(config.target_column).is_not_null())
    log.info("pipeline.null_dropped", column=config.target_column, dropped=initial_rows - len(df))

    log.info("pipeline.step", step=3, name="feature_engineering")

    if config.interpolate_missing:
        numeric_cols = [
            "pm2_5_ugm3", "pm10_ugm3", "nitrogen_dioxide_ugm3",
            "ozone_ugm3", "sulphur_dioxide_ugm3", "carbon_monoxide_ugm3",
        ]
        df = interpolate_missing(df, numeric_cols, config.interpolation_method)

    df = add_temporal_features(df)
    df = add_lag_features(df, config.target_column, config.lag_hours)
    df = add_rolling_features(df, config.target_column, config.rolling_windows)
    df = add_rate_of_change(df, config.target_column)
    df = add_target_variable(df, config.target_column, config.forecast_horizon)
    df = df.drop_nulls()
    log.info("pipeline.features_ready", rows=len(df), columns=len(df.columns))

    log.info("pipeline.step", step=4, name="split")
    train_df, val_df, test_df = create_chronological_splits(
        df, config.train_ratio, config.val_ratio, config.test_ratio
    )

    if config.normalize_features:
        log.info("pipeline.step", step=5, name="normalize")
        exclude_cols = [
            "stationID", "timestamp_utc", "timestamp_unix_ms",
            "data_source", "ingestion_timestamp_utc", "load_id",
            "pipeline_version", "record_hash", "lat", "lon",
            f"target_{config.target_column}_{config.forecast_horizon}h",
        ]
        feature_cols = [c for c in df.columns if c not in exclude_cols]
        train_df, val_df, test_df, norm_stats = normalize_features(
            train_df, val_df, test_df, feature_cols, config.normalization_method
        )
    else:
        norm_stats = {}
        feature_cols = list(df.columns)

    log.info("pipeline.step", step=6, name="save")
    gold_dir = config.gold_output_path
    gold_dir.mkdir(parents=True, exist_ok=True)

    train_path = gold_dir / "train.parquet"
    val_path = gold_dir / "val.parquet"
    test_path = gold_dir / "test.parquet"

    train_df.write_parquet(train_path, compression="snappy")
    val_df.write_parquet(val_path, compression="snappy")
    test_df.write_parquet(test_path, compression="snappy")
    log.info("pipeline.saved", train=str(train_path), val=str(val_path), test=str(test_path))

    if norm_stats:
        stats_path = gold_dir / "normalization_stats.json"
        with open(stats_path, "w") as f:
            json.dump(norm_stats, f, indent=2, default=str)
        log.info("pipeline.stats_saved", path=str(stats_path))

    metadata = {
        "pipeline_version": config.pipeline_version,
        "created_at": datetime.utcnow().isoformat() + "Z",
        "source_years": config.target_years,
        "total_rows": len(df),
        "train_rows": len(train_df),
        "val_rows": len(val_df),
        "test_rows": len(test_df),
        "num_features": len(feature_cols),
        "forecast_horizon": config.forecast_horizon,
        "target_column": f"target_{config.target_column}_{config.forecast_horizon}h",
        "lag_features": config.lag_hours,
        "rolling_windows": config.rolling_windows,
        "normalization_method": config.normalization_method if config.normalize_features else None,
    }
    metadata_path = gold_dir / "pipeline_metadata.json"
    with open(metadata_path, "w") as f:
        json.dump(metadata, f, indent=2)
    log.info("pipeline.metadata_saved", path=str(metadata_path))

    elapsed = time.time() - start_time
    log.info("pipeline.complete", elapsed_s=round(elapsed, 1))


if __name__ == "__main__":
    run_gold_pipeline()
