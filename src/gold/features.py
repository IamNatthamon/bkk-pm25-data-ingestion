"""
Feature Engineering for PM2.5 Forecasting

Creates features from raw air quality data for ML model training.
"""

from __future__ import annotations

import numpy as np
import polars as pl


def add_lag_features(
    df: pl.DataFrame,
    target_col: str,
    lag_hours: list[int],
    group_by: str = "stationID",
) -> pl.DataFrame:
    """
    Add lag features for time series forecasting.
    
    Creates features like pm2_5_lag_1h, pm2_5_lag_24h, etc.
    """
    print(f"  📊 Adding lag features: {lag_hours}", flush=True)
    
    lag_exprs = []
    for lag in lag_hours:
        lag_exprs.append(
            pl.col(target_col)
            .shift(lag)
            .over(group_by)
            .alias(f"{target_col}_lag_{lag}h")
        )
    
    df = df.with_columns(lag_exprs)
    
    return df


def add_rolling_features(
    df: pl.DataFrame,
    target_col: str,
    windows: list[int],
    group_by: str = "stationID",
) -> pl.DataFrame:
    """
    Add rolling window statistics.
    
    Creates features like pm2_5_rolling_mean_24h, pm2_5_rolling_std_24h, etc.
    """
    print(f"  📊 Adding rolling features: {windows}", flush=True)
    
    rolling_exprs = []
    for window in windows:
        # Rolling mean
        rolling_exprs.append(
            pl.col(target_col)
            .rolling_mean(window)
            .over(group_by)
            .alias(f"{target_col}_rolling_mean_{window}h")
        )
        
        # Rolling std
        rolling_exprs.append(
            pl.col(target_col)
            .rolling_std(window)
            .over(group_by)
            .alias(f"{target_col}_rolling_std_{window}h")
        )
        
        # Rolling min/max
        rolling_exprs.append(
            pl.col(target_col)
            .rolling_min(window)
            .over(group_by)
            .alias(f"{target_col}_rolling_min_{window}h")
        )
        
        rolling_exprs.append(
            pl.col(target_col)
            .rolling_max(window)
            .over(group_by)
            .alias(f"{target_col}_rolling_max_{window}h")
        )
    
    df = df.with_columns(rolling_exprs)
    
    return df


def add_temporal_features(
    df: pl.DataFrame,
    timestamp_col: str = "timestamp_utc",
) -> pl.DataFrame:
    """
    Add temporal features from timestamp.
    
    Creates: hour, day_of_week, month, is_weekend, cyclical encodings.
    """
    print(f"  📊 Adding temporal features", flush=True)
    
    df = df.with_columns([
        # Extract components
        pl.col(timestamp_col).dt.hour().alias("hour"),
        pl.col(timestamp_col).dt.day().alias("day"),
        pl.col(timestamp_col).dt.month().alias("month"),
        pl.col(timestamp_col).dt.weekday().alias("day_of_week"),
        pl.col(timestamp_col).dt.year().alias("year"),
        
        # Is weekend
        (pl.col(timestamp_col).dt.weekday() >= 5).alias("is_weekend"),
    ])
    
    # Cyclical encoding for hour (0-23)
    df = df.with_columns([
        (2 * np.pi * pl.col("hour") / 24).sin().alias("hour_sin"),
        (2 * np.pi * pl.col("hour") / 24).cos().alias("hour_cos"),
    ])
    
    # Cyclical encoding for month (1-12)
    df = df.with_columns([
        (2 * np.pi * (pl.col("month") - 1) / 12).sin().alias("month_sin"),
        (2 * np.pi * (pl.col("month") - 1) / 12).cos().alias("month_cos"),
    ])
    
    # Cyclical encoding for day of week (0-6)
    df = df.with_columns([
        (2 * np.pi * pl.col("day_of_week") / 7).sin().alias("dow_sin"),
        (2 * np.pi * pl.col("day_of_week") / 7).cos().alias("dow_cos"),
    ])
    
    return df


def add_target_variable(
    df: pl.DataFrame,
    target_col: str,
    forecast_horizon: int,
    group_by: str = "stationID",
) -> pl.DataFrame:
    """
    Create target variable for forecasting.
    
    Target = PM2.5 value at t+forecast_horizon
    """
    print(f"  🎯 Adding target: {target_col} at t+{forecast_horizon}h", flush=True)
    
    df = df.with_columns(
        pl.col(target_col)
        .shift(-forecast_horizon)
        .over(group_by)
        .alias(f"target_{target_col}_{forecast_horizon}h")
    )
    
    return df


def add_rate_of_change(
    df: pl.DataFrame,
    target_col: str,
    group_by: str = "stationID",
) -> pl.DataFrame:
    """Add rate of change features"""
    print(f"  📊 Adding rate of change features", flush=True)
    
    df = df.with_columns([
        # 1-hour change
        (pl.col(target_col) - pl.col(target_col).shift(1).over(group_by))
        .alias(f"{target_col}_diff_1h"),
        
        # 24-hour change
        (pl.col(target_col) - pl.col(target_col).shift(24).over(group_by))
        .alias(f"{target_col}_diff_24h"),
        
        # Percentage change
        ((pl.col(target_col) - pl.col(target_col).shift(1).over(group_by)) / 
         pl.col(target_col).shift(1).over(group_by) * 100)
        .alias(f"{target_col}_pct_change_1h"),
    ])
    
    return df


def interpolate_missing(
    df: pl.DataFrame,
    columns: list[str],
    method: str = "linear",
    group_by: str = "stationID",
) -> pl.DataFrame:
    """Interpolate missing values within each station"""
    print(f"  🔧 Interpolating missing values ({method})", flush=True)
    
    for col in columns:
        if col in df.columns:
            df = df.with_columns(
                pl.col(col).interpolate(method=method).over(group_by)
            )
    
    return df


if __name__ == "__main__":
    # Test feature engineering
    print("Testing feature engineering functions...")
    
    # Create sample data
    df = pl.DataFrame({
        "stationID": ["A"] * 100,
        "timestamp_utc": pl.datetime_range(
            start=pl.datetime(2023, 1, 1),
            end=pl.datetime(2023, 1, 5),
            interval="1h",
            time_zone="UTC",
            eager=True
        )[:100],
        "pm2_5_ugm3": np.random.rand(100) * 50 + 20,
    })
    
    print(f"\nOriginal shape: {df.shape}")
    
    df = add_lag_features(df, "pm2_5_ugm3", [1, 6, 24])
    df = add_rolling_features(df, "pm2_5_ugm3", [6, 24])
    df = add_temporal_features(df)
    df = add_target_variable(df, "pm2_5_ugm3", 24)
    
    print(f"After features: {df.shape}")
    print(f"Columns: {df.columns}")
