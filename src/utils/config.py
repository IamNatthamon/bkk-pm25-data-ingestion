"""Pipeline configuration using Pydantic settings"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class PipelineConfig(BaseSettings):
    """Central configuration for Bangkok PM2.5 data pipeline"""

    model_config = SettingsConfigDict(env_prefix="BKK_PM25_", case_sensitive=False)

    project_root: Path = Field(default_factory=lambda: Path.cwd())

    bronze_weather_dir: Path = Field(default=Path("data/bronze/openmeteo_weather"))
    bronze_airquality_dir: Path = Field(default=Path("data/bronze/openmeteo_airquality"))
    silver_weather_dir: Path = Field(default=Path("data/silver/openmeteo_weather"))
    silver_airquality_dir: Path = Field(default=Path("data/silver/openmeteo_airquality"))
    gold_dir: Path = Field(default=Path("data/gold/model_ready"))
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

    def __post_init__(self) -> None:
        for path_field in [
            "bronze_weather_dir",
            "bronze_airquality_dir",
            "silver_weather_dir",
            "silver_airquality_dir",
            "gold_dir",
            "checkpoint_dir",
            "logs_dir",
        ]:
            path_val = getattr(self, path_field)
            if not path_val.is_absolute():
                setattr(self, path_field, self.project_root / path_val)

        if not self.stations_path.is_absolute():
            self.stations_path = self.project_root / self.stations_path

    @property
    def split_ratios_valid(self) -> bool:
        return abs(self.train_ratio + self.val_ratio + self.test_ratio - 1.0) < 1e-6
