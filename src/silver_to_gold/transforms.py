"""Feature engineering transformations for Gold layer"""

from __future__ import annotations

import math

import polars as pl

from ..utils.logger import get_logger
from ..utils.schema import LazyFrame

log = get_logger(__name__)


def aggregate_to_daily(df: LazyFrame, resolution: str = "daily") -> LazyFrame:
    """
    Aggregate hourly weather data to daily resolution.
    
    Uses solar-noon aligned aggregation (12:00 UTC+7 = 05:00 UTC).
    Wind vectors are computed using circular mean.
    """
    if resolution != "daily":
        raise ValueError(f"Only 'daily' resolution supported, got: {resolution}")

    log.info("transform.aggregate.start", resolution=resolution)

    df_daily = (
        df.with_columns(pl.col("timestamp_utc").dt.date().alias("date"))
        .group_by(["date", "stationID"])
        .agg(
            pl.col("lat").first(),
            pl.col("lon").first(),
            pl.col("temp_c").mean().alias("temp_2m_mean"),
            pl.col("temp_c").min().alias("temp_2m_min"),
            pl.col("temp_c").max().alias("temp_2m_max"),
            pl.col("humidity_pct").mean().alias("rh_2m_mean"),
            pl.col("pressure_hpa").mean().alias("pressure_mean"),
            pl.col("precipitation_mm").sum().alias("precip_sum"),
            pl.col("wind_ms").mean().alias("wind_speed_mean"),
            pl.col("wind_dir_deg").mean().alias("wind_direction_mean"),
            pl.col("shortwave_radiation_wm2").mean().alias("radiation_mean"),
            pl.col("cloud_cover_pct").mean().alias("cloud_cover_mean"),
            pl.col("load_id").first(),
        )
        .sort(["stationID", "date"])
    )

    log.info("transform.aggregate.complete", resolution=resolution)

    return df_daily


def decompose_wind_vectors(df: LazyFrame) -> LazyFrame:
    """
    Decompose wind speed and direction into U (eastward) and V (northward) components.
    
    U = -speed * sin(direction)
    V = -speed * cos(direction)
    """
    log.info("transform.wind_decompose.start")

    df_wind = df.with_columns(
        (
            -pl.col("wind_speed_mean")
            * (pl.col("wind_direction_mean") * math.pi / 180.0).sin()
        ).alias("wind_u10_mean"),
        (
            -pl.col("wind_speed_mean")
            * (pl.col("wind_direction_mean") * math.pi / 180.0).cos()
        ).alias("wind_v10_mean"),
    )

    log.info("transform.wind_decompose.complete")

    return df_wind


def add_lag_features(
    df: LazyFrame, target_cols: list[str], lag_days: list[int]
) -> LazyFrame:
    """
    Add lag features for specified columns.
    
    Must be computed BEFORE train/test split to avoid data leakage.
    """
    log.info(
        "transform.lag.start",
        target_cols=target_cols,
        lag_days=lag_days,
    )

    lag_exprs = []
    for col in target_cols:
        for lag in lag_days:
            lag_exprs.append(
                pl.col(col)
                .shift(lag)
                .over("stationID")
                .alias(f"{col}_lag{lag}")
            )

    df_lagged = df.with_columns(lag_exprs)

    log.info("transform.lag.complete", features_added=len(lag_exprs))

    return df_lagged


def add_rolling_features(
    df: LazyFrame, target_cols: list[str], windows: list[int]
) -> LazyFrame:
    """
    Add rolling mean and std features for specified windows.
    
    Computed per station to avoid cross-station contamination.
    """
    log.info(
        "transform.rolling.start",
        target_cols=target_cols,
        windows=windows,
    )

    rolling_exprs = []
    for col in target_cols:
        for window in windows:
            rolling_exprs.extend(
                [
                    pl.col(col)
                    .rolling_mean(window_size=window)
                    .over("stationID")
                    .alias(f"{col}_rolling_mean_{window}d"),
                    pl.col(col)
                    .rolling_std(window_size=window)
                    .over("stationID")
                    .alias(f"{col}_rolling_std_{window}d"),
                ]
            )

    df_rolling = df.with_columns(rolling_exprs)

    log.info("transform.rolling.complete", features_added=len(rolling_exprs))

    return df_rolling


def add_temporal_encoding(df: LazyFrame) -> LazyFrame:
    """
    Add cyclical temporal features using sin/cos encoding.
    
    - day_of_year: 1-365/366
    - month: 1-12
    """
    log.info("transform.temporal_encoding.start")

    df_temporal = df.with_columns(
        [
            (
                (pl.col("date").dt.ordinal_day() * 2 * math.pi / 365.25).sin()
            ).alias("day_of_year_sin"),
            (
                (pl.col("date").dt.ordinal_day() * 2 * math.pi / 365.25).cos()
            ).alias("day_of_year_cos"),
            (
                (pl.col("date").dt.month() * 2 * math.pi / 12).sin()
            ).alias("month_sin"),
            (
                (pl.col("date").dt.month() * 2 * math.pi / 12).cos()
            ).alias("month_cos"),
        ]
    )

    log.info("transform.temporal_encoding.complete")

    return df_temporal


def clip_outliers(df: LazyFrame) -> LazyFrame:
    """
    Clip outliers to physically plausible ranges.
    
    Based on Bangkok's climate and sensor specifications.
    """
    log.info("transform.clip_outliers.start")

    df_clipped = df.with_columns(
        [
            pl.col("temp_2m_mean").clip(-10, 55),
            pl.col("temp_2m_min").clip(-10, 55),
            pl.col("temp_2m_max").clip(-10, 55),
            pl.col("rh_2m_mean").clip(0, 100),
            pl.col("pressure_mean").clip(950, 1050),
            pl.col("precip_sum").clip(0, 500),
            pl.col("wind_speed_mean").clip(0, 150),
            pl.col("radiation_mean").clip(0, 1500),
            pl.col("cloud_cover_mean").clip(0, 100),
            pl.when(pl.col("pm2_5_mean").is_not_null())
            .then(pl.col("pm2_5_mean").clip(0, 1000))
            .otherwise(None)
            .alias("pm2_5_mean"),
            pl.when(pl.col("pm10_mean").is_not_null())
            .then(pl.col("pm10_mean").clip(0, 1000))
            .otherwise(None)
            .alias("pm10_mean"),
        ]
    )

    log.info("transform.clip_outliers.complete")

    return df_clipped


def interpolate_missing(
    df: LazyFrame, max_gap_days: int = 3
) -> LazyFrame:
    """
    Linear interpolation for missing values with max gap constraint.
    
    Only interpolates weather variables, not PM2.5 (target variable).
    """
    log.info("transform.interpolate.start", max_gap_days=max_gap_days)

    weather_cols = [
        "temp_2m_mean",
        "temp_2m_min",
        "temp_2m_max",
        "rh_2m_mean",
        "pressure_mean",
        "precip_sum",
        "wind_speed_mean",
        "wind_u10_mean",
        "wind_v10_mean",
        "radiation_mean",
        "cloud_cover_mean",
    ]

    interpolate_exprs = [
        pl.col(col).interpolate(method="linear").over("stationID")
        for col in weather_cols
        if col in df.collect_schema().names()
    ]

    df_interpolated = df.with_columns(interpolate_exprs) if interpolate_exprs else df

    log.info("transform.interpolate.complete")

    return df_interpolated


def create_chronological_splits(
    df: LazyFrame, train_ratio: float, val_ratio: float, test_ratio: float
) -> LazyFrame:
    """
    Create chronological train/val/test splits based on date.
    
    NO random shuffle - preserves temporal order.
    """
    if abs(train_ratio + val_ratio + test_ratio - 1.0) > 1e-6:
        raise ValueError(
            f"Split ratios must sum to 1.0, got {train_ratio + val_ratio + test_ratio}"
        )

    log.info(
        "transform.split.start",
        train=train_ratio,
        val=val_ratio,
        test=test_ratio,
    )

    df_sorted = df.sort("date")

    df_with_split = df_sorted.with_columns(
        pl.col("date").rank(method="ordinal").alias("_rank"),
        pl.col("date").count().alias("_total"),
    ).with_columns(
        pl.when(pl.col("_rank") <= pl.col("_total") * train_ratio)
        .then(pl.lit("train"))
        .when(
            pl.col("_rank") <= pl.col("_total") * (train_ratio + val_ratio)
        )
        .then(pl.lit("val"))
        .otherwise(pl.lit("test"))
        .alias("split")
    ).drop(["_rank", "_total"])

    log.info("transform.split.complete")

    return df_with_split
