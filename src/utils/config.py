"""Pipeline configuration using Pydantic settings"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class PipelineConfig(BaseSettings):
    """Central configuration for Bangkok PM2.5 data pipeline (bronze/silver/gold + FIRMS)."""

    model_config = SettingsConfigDict(env_prefix="BKK_PM25_", case_sensitive=False)

    project_root: Path = Field(default_factory=lambda: Path.cwd())

    bronze_weather_dir: Path = Field(default=Path("data/bronze/openmeteo_weather"))
    bronze_airquality_dir: Path = Field(default=Path("data/bronze/openmeteo_airquality"))
    bronze_hotspot_dir: Path = Field(default=Path("data/bronze/raw_hotspot"))
    silver_weather_dir: Path = Field(default=Path("data/silver/openmeteo_weather"))
    silver_airquality_dir: Path = Field(default=Path("data/silver/openmeteo_airquality"))
    silver_firms_dir: Path = Field(default=Path("data/silver/firms_hotspot"))
    gold_dir: Path = Field(default=Path("data/gold/model_ready"))
    gold_aq_combined_dir: Path = Field(default=Path("data/gold/airquality_combined"))
    stations_path: Path = Field(default=Path("data/stations/bangkok_stations.parquet"))

    checkpoint_dir: Path = Field(default=Path("checkpoints"))
    logs_dir: Path = Field(default=Path("logs"))

    target_resolution: Literal["hourly", "daily"] = "daily"
    train_ratio: float = 0.70
    val_ratio: float = 0.15
    test_ratio: float = 0.15

    lag_days: list[int] = Field(default_factory=lambda: [1, 2, 3])
    rolling_windows: list[int] = Field(default_factory=lambda: [3, 7, 14])

    max_interpolation_gap_days: int = 3
    outlier_clip_enabled: bool = True

    random_seed: int = 42

    # FIRMS: Bangkok center (lat, lon), radius km for spatial filter
    firms_bangkok_lat: float = 13.7563
    firms_bangkok_lon: float = 100.5018
    firms_radius_km: float = 500.0
    firms_confidence_allow: list[str] = Field(default_factory=lambda: ["nominal", "high", "n", "h"])

    @model_validator(mode="after")
    def resolve_paths(self) -> "PipelineConfig":
        root = self.project_root if self.project_root.is_absolute() else Path.cwd() / self.project_root
        for path_field in [
            "bronze_weather_dir", "bronze_airquality_dir", "bronze_hotspot_dir",
            "silver_weather_dir", "silver_airquality_dir", "silver_firms_dir",
            "gold_dir", "gold_aq_combined_dir", "checkpoint_dir", "logs_dir",
        ]:
            path_val = getattr(self, path_field)
            if path_val and not path_val.is_absolute():
                setattr(self, path_field, root / path_val)
        if self.stations_path and not self.stations_path.is_absolute():
            self.stations_path = root / self.stations_path
        return self

    @property
    def split_ratios_valid(self) -> bool:
        return abs(self.train_ratio + self.val_ratio + self.test_ratio - 1.0) < 1e-6
