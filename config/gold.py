"""
Gold Layer Configuration for PM2.5 Forecasting Pipeline

This config defines all parameters for transforming Silver → Gold layer
and preparing data for ML model training.
"""

from __future__ import annotations

from pathlib import Path
from typing import TypeAlias

from pydantic_settings import BaseSettings

PathLike: TypeAlias = str | Path


class GoldPipelineConfig(BaseSettings):
    """Configuration for Gold layer transformation pipeline"""

    # Data paths
    project_root: Path = Path.cwd()
    silver_aq_path: Path = project_root / "data" / "silver" / "openmeteo_airquality"
    silver_weather_path: Path = project_root / "data" / "silver" / "openmeteo_weather"
    stations_path: Path = project_root / "data" / "stations" / "bangkok_stations.parquet"
    gold_output_path: Path = project_root / "data" / "gold"
    
    # Years to process (only complete years)
    target_years: list[int] = [2023, 2024, 2025]
    
    # Feature engineering
    lag_hours: list[int] = [1, 2, 3, 6, 12, 24]  # Lag features for PM2.5
    rolling_windows: list[int] = [3, 6, 12, 24]  # Rolling mean windows
    
    # Temporal features
    include_cyclical_time: bool = True  # hour_sin, hour_cos, month_sin, month_cos
    include_day_of_week: bool = True
    include_is_weekend: bool = True
    include_is_holiday: bool = True
    
    # Data quality
    max_missing_ratio: float = 0.3  # Max 30% missing values per station
    interpolate_missing: bool = True
    interpolation_method: str = "linear"
    
    # Train/Val/Test split
    train_ratio: float = 0.7
    val_ratio: float = 0.15
    test_ratio: float = 0.15
    
    # Ensure chronological split (no data leakage)
    chronological_split: bool = True
    
    # Target variable
    target_column: str = "pm2_5_ugm3"
    forecast_horizon: int = 24  # Predict 24 hours ahead
    
    # Normalization
    normalize_features: bool = True
    normalization_method: str = "standard"  # or "minmax"
    
    # Output format
    save_train_val_test_separately: bool = True
    save_normalization_stats: bool = True
    
    # Pipeline metadata
    pipeline_version: str = "1.0.0"
    
    class Config:
        env_prefix = "GOLD_"


# Global config instance
config = GoldPipelineConfig()


if __name__ == "__main__":
    print("Gold Pipeline Configuration:")
    print(f"  Target years: {config.target_years}")
    print(f"  Lag features: {config.lag_hours}")
    print(f"  Rolling windows: {config.rolling_windows}")
    print(f"  Train/Val/Test: {config.train_ratio}/{config.val_ratio}/{config.test_ratio}")
    print(f"  Forecast horizon: {config.forecast_horizon} hours")
