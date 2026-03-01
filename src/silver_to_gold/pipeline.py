"""Main Silver to Gold ETL pipeline"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import polars as pl

from ..utils.config import PipelineConfig
from ..utils.logger import get_logger
from ..utils.schema import LazyFrame
from .loader import load_silver_airquality, load_silver_weather, load_stations
from .transforms import (
    add_lag_features,
    add_rolling_features,
    add_temporal_encoding,
    aggregate_to_daily,
    clip_outliers,
    create_chronological_splits,
    decompose_wind_vectors,
    interpolate_missing,
)

log = get_logger(__name__)


def run_silver_to_gold_pipeline(config: PipelineConfig) -> dict[str, Path]:
    """
    Execute complete Silver → Gold transformation pipeline.
    
    Returns:
        Dictionary with paths to train/val/test parquet files and manifest
    """
    log.info("pipeline.start", stage="silver_to_gold")

    log.info("pipeline.stage", stage="load_silver_data")
    weather_lazy = load_silver_weather(config.silver_weather_dir)
    airquality_lazy = load_silver_airquality(config.silver_airquality_dir)
    stations_df = load_stations(config.stations_path)

    log.info("pipeline.stage", stage="aggregate_to_daily")
    weather_daily = aggregate_to_daily(weather_lazy, config.target_resolution)

    log.info("pipeline.stage", stage="decompose_wind")
    weather_daily = decompose_wind_vectors(weather_daily)

    if airquality_lazy is not None:
        log.info("pipeline.stage", stage="aggregate_airquality")
        aq_daily = (
            airquality_lazy.with_columns(pl.col("timestamp_utc").dt.date().alias("date"))
            .group_by(["date", "stationID"])
            .agg(
                pl.col("lat").first(),
                pl.col("lon").first(),
                pl.col("pm2_5_ugm3").mean().alias("pm2_5_mean"),
                pl.col("pm10_ugm3").mean().alias("pm10_mean"),
                pl.col("no2_ugm3").mean().alias("no2_mean"),
                pl.col("o3_ugm3").mean().alias("o3_mean"),
                pl.col("so2_ugm3").mean().alias("so2_mean"),
                pl.col("co_ugm3").mean().alias("co_mean"),
            )
            .sort(["stationID", "date"])
        )

        log.info("pipeline.stage", stage="merge_weather_airquality")
        merged = weather_daily.join(
            aq_daily,
            on=["date", "stationID"],
            how="left",
            suffix="_aq",
        ).select(
            pl.exclude("^.*_aq$")
        )
    else:
        log.warning("pipeline.airquality_missing", action="using_weather_only")
        merged = weather_daily.with_columns(
            [
                pl.lit(None, dtype=pl.Float64).alias("pm2_5_mean"),
                pl.lit(None, dtype=pl.Float64).alias("pm10_mean"),
                pl.lit(None, dtype=pl.Float64).alias("no2_mean"),
                pl.lit(None, dtype=pl.Float64).alias("o3_mean"),
            ]
        )

    log.info("pipeline.stage", stage="add_lag_features")
    merged = add_lag_features(
        merged,
        target_cols=["pm2_5_mean", "temp_2m_mean"],
        lag_days=config.lag_days,
    )

    log.info("pipeline.stage", stage="add_rolling_features")
    merged = add_rolling_features(
        merged,
        target_cols=["pm2_5_mean", "temp_2m_mean"],
        windows=config.rolling_windows,
    )

    log.info("pipeline.stage", stage="add_temporal_encoding")
    merged = add_temporal_encoding(merged)

    log.info("pipeline.stage", stage="add_hotspot_placeholders")
    merged = merged.with_columns(
        [
            pl.lit(None, dtype=pl.Float64).alias("hotspot_count_th"),
            pl.lit(None, dtype=pl.Float64).alias("hotspot_count_mm"),
            pl.lit(None, dtype=pl.Float64).alias("hotspot_count_la"),
            pl.lit(None, dtype=pl.Float64).alias("hotspot_frp_sum"),
            pl.lit(None, dtype=pl.Float64).alias("transboundary_index"),
        ]
    )

    log.info("pipeline.stage", stage="interpolate_missing")
    if config.max_interpolation_gap_days > 0:
        merged = interpolate_missing(merged, config.max_interpolation_gap_days)

    log.info("pipeline.stage", stage="clip_outliers")
    if config.outlier_clip_enabled:
        merged = clip_outliers(merged)

    log.info("pipeline.stage", stage="create_splits")
    merged = create_chronological_splits(
        merged,
        config.train_ratio,
        config.val_ratio,
        config.test_ratio,
    )

    log.info("pipeline.stage", stage="collect_and_normalize")
    df_collected = merged.collect()

    train_df = df_collected.filter(pl.col("split") == "train")
    val_df = df_collected.filter(pl.col("split") == "val")
    test_df = df_collected.filter(pl.col("split") == "test")

    log.info(
        "pipeline.splits_created",
        train_rows=len(train_df),
        val_rows=len(val_df),
        test_rows=len(test_df),
    )

    numeric_cols = [
        col
        for col, dtype in df_collected.schema.items()
        if dtype in [pl.Float64, pl.Float32, pl.Int64, pl.Int32]
        and col not in ["lat", "lon", "latitude", "longitude", "elevation"]
    ]

    normalization_stats = {}
    for col in numeric_cols:
        if col in train_df.columns:
            mean_val = train_df[col].mean()
            std_val = train_df[col].std()
            normalization_stats[col] = {"mean": mean_val, "std": std_val}

    def normalize_df(df: pl.DataFrame) -> pl.DataFrame:
        norm_exprs = []
        for col in numeric_cols:
            if col in df.columns and col in normalization_stats:
                stats = normalization_stats[col]
                if stats["std"] and stats["std"] > 1e-8:
                    norm_exprs.append(
                        ((pl.col(col) - stats["mean"]) / stats["std"]).alias(
                            f"{col}_norm"
                        )
                    )
        return df.with_columns(norm_exprs) if norm_exprs else df

    train_df = normalize_df(train_df)
    val_df = normalize_df(val_df)
    test_df = normalize_df(test_df)

    log.info("pipeline.stage", stage="write_gold_layer")
    config.gold_dir.mkdir(parents=True, exist_ok=True)

    train_path = config.gold_dir / "train.parquet"
    val_path = config.gold_dir / "val.parquet"
    test_path = config.gold_dir / "test.parquet"
    stats_path = config.gold_dir / "normalization_stats.json"
    manifest_path = config.gold_dir / "pipeline_manifest.json"

    train_df.write_parquet(train_path)
    val_df.write_parquet(val_path)
    test_df.write_parquet(test_path)

    with open(stats_path, "w") as f:
        json.dump(normalization_stats, f, indent=2)

    manifest = {
        "pipeline_version": "1.0.0",
        "created_at": datetime.utcnow().isoformat() + "Z",
        "config": {
            "target_resolution": config.target_resolution,
            "train_ratio": config.train_ratio,
            "val_ratio": config.val_ratio,
            "test_ratio": config.test_ratio,
            "lag_days": config.lag_days,
            "rolling_windows": config.rolling_windows,
            "max_interpolation_gap_days": config.max_interpolation_gap_days,
            "outlier_clip_enabled": config.outlier_clip_enabled,
            "random_seed": config.random_seed,
        },
        "splits": {
            "train": {
                "rows": len(train_df),
                "date_range": [
                    str(train_df["date"].min()),
                    str(train_df["date"].max()),
                ],
            },
            "val": {
                "rows": len(val_df),
                "date_range": [str(val_df["date"].min()), str(val_df["date"].max())],
            },
            "test": {
                "rows": len(test_df),
                "date_range": [
                    str(test_df["date"].min()),
                    str(test_df["date"].max()),
                ],
            },
        },
        "features": {
            "total_count": len(df_collected.columns),
            "normalized_count": len([c for c in train_df.columns if c.endswith("_norm")]),
            "columns": df_collected.columns,
        },
    }

    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)

    log.info(
        "pipeline.complete",
        train_path=str(train_path),
        val_path=str(val_path),
        test_path=str(test_path),
        manifest_path=str(manifest_path),
    )

    return {
        "train": train_path,
        "val": val_path,
        "test": test_path,
        "stats": stats_path,
        "manifest": manifest_path,
    }
