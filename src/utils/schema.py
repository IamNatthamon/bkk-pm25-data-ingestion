"""Data schemas and validation using Pydantic"""

from __future__ import annotations

from datetime import datetime
from typing import TypeAlias

import polars as pl
from pydantic import BaseModel, Field, field_validator

DataFrame: TypeAlias = pl.DataFrame
LazyFrame: TypeAlias = pl.LazyFrame


class WeatherRecord(BaseModel):
    """Schema for weather data records"""

    timestamp: datetime
    station_id: str
    latitude: float = Field(ge=-90, le=90)
    longitude: float = Field(ge=-180, le=180)
    elevation: float

    temperature_2m: float | None = Field(default=None, ge=-50, le=60)
    relative_humidity_2m: float | None = Field(default=None, ge=0, le=100)
    surface_pressure: float | None = Field(default=None, ge=800, le=1100)
    precipitation: float | None = Field(default=None, ge=0)
    wind_speed_10m: float | None = Field(default=None, ge=0)
    wind_direction_10m: float | None = Field(default=None, ge=0, le=360)
    shortwave_radiation: float | None = Field(default=None, ge=0)
    cloud_cover: float | None = Field(default=None, ge=0, le=100)
    u10_ms: float | None = Field(default=None)
    v10_ms: float | None = Field(default=None)

    load_id: str
    record_hash: str
    ingestion_timestamp_utc: datetime


class AirQualityRecord(BaseModel):
    """Schema for air quality data records"""

    timestamp: datetime
    station_id: str
    latitude: float = Field(ge=-90, le=90)
    longitude: float = Field(ge=-180, le=180)

    pm2_5_ugm3: float | None = Field(default=None, ge=0, le=1000)
    pm10_ugm3: float | None = Field(default=None, ge=0, le=1000)
    no2_ugm3: float | None = Field(default=None, ge=0)
    o3_ugm3: float | None = Field(default=None, ge=0)
    so2_ugm3: float | None = Field(default=None, ge=0)
    co_ugm3: float | None = Field(default=None, ge=0)

    load_id: str
    record_hash: str
    ingestion_timestamp_utc: datetime


class GoldFeatureSchema(BaseModel):
    """Schema for Gold layer model-ready features"""

    date: datetime
    station_id: str

    temp_2m_mean: float | None
    temp_2m_min: float | None
    temp_2m_max: float | None
    rh_2m_mean: float | None
    pressure_mean: float | None
    precip_sum: float | None
    wind_speed_mean: float | None
    wind_u10_mean: float | None
    wind_v10_mean: float | None
    radiation_mean: float | None
    cloud_cover_mean: float | None

    pm2_5_mean: float | None
    pm10_mean: float | None
    no2_mean: float | None
    o3_mean: float | None

    pm2_5_lag1: float | None
    pm2_5_lag2: float | None
    pm2_5_lag3: float | None

    temp_lag1: float | None
    temp_lag2: float | None

    pm2_5_rolling_mean_3d: float | None
    pm2_5_rolling_std_3d: float | None
    pm2_5_rolling_mean_7d: float | None
    pm2_5_rolling_std_7d: float | None
    pm2_5_rolling_mean_14d: float | None
    pm2_5_rolling_std_14d: float | None

    day_of_year_sin: float
    day_of_year_cos: float
    month_sin: float
    month_cos: float

    hotspot_count_th: float | None = None
    hotspot_count_mm: float | None = None
    hotspot_count_la: float | None = None
    hotspot_frp_sum: float | None = None
    transboundary_index: float | None = None

    split: str = Field(pattern="^(train|val|test)$")


WEATHER_SILVER_SCHEMA = {
    "stationID": pl.Utf8,
    "lat": pl.Float64,
    "lon": pl.Float64,
    "timestamp_utc": pl.Datetime("ns", "UTC"),
    "timestamp_unix_ms": pl.Int64,
    "temp_c": pl.Float32,
    "humidity_pct": pl.Float32,
    "pressure_hpa": pl.Float32,
    "precipitation_mm": pl.Float32,
    "wind_ms": pl.Float32,
    "wind_dir_deg": pl.Float32,
    "shortwave_radiation_wm2": pl.Float32,
    "cloud_cover_pct": pl.Float32,
    "u10_ms": pl.Float32,
    "v10_ms": pl.Float32,
    "data_source": pl.Utf8,
    "ingestion_timestamp_utc": pl.Datetime("us", "UTC"),
    "load_id": pl.Utf8,
    "pipeline_version": pl.Utf8,
    "record_hash": pl.Utf8,
}

AIRQUALITY_SILVER_SCHEMA = {
    "timestamp": pl.Datetime("us", "UTC"),
    "station_id": pl.Utf8,
    "latitude": pl.Float64,
    "longitude": pl.Float64,
    "pm2_5_ugm3": pl.Float64,
    "pm10_ugm3": pl.Float64,
    "no2_ugm3": pl.Float64,
    "o3_ugm3": pl.Float64,
    "so2_ugm3": pl.Float64,
    "co_ugm3": pl.Float64,
    "load_id": pl.Utf8,
    "record_hash": pl.Utf8,
    "ingestion_timestamp_utc": pl.Datetime("us", "UTC"),
}
