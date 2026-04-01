"""Unit tests for src.silver_to_gold.transforms — pure transformation functions."""

from __future__ import annotations

import math

import polars as pl
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from src.silver_to_gold.transforms import (
    add_lag_features,
    add_rolling_features,
    add_temporal_encoding,
    clip_outliers,
    decompose_wind_vectors,
)


# ── Fixtures ─────────────────────────────────────────────────────────────────

def _daily_df(n: int = 10) -> pl.LazyFrame:
    """Minimal daily DataFrame for transform tests."""
    return pl.LazyFrame({
        "stationID": ["A"] * n,
        "date": pl.date_range(
            start=pl.date(2024, 1, 1),
            end=pl.date(2024, 1, n),
            interval="1d",
            eager=True,
        ),
        "temp_2m_mean": [25.0 + i * 0.1 for i in range(n)],
        "temp_2m_min": [20.0] * n,
        "temp_2m_max": [32.0] * n,
        "rh_2m_mean": [70.0] * n,
        "pressure_mean": [1010.0] * n,
        "precip_sum": [0.0] * n,
        "wind_speed_mean": [3.0] * n,
        "wind_direction_mean": [90.0] * n,
        "radiation_mean": [200.0] * n,
        "cloud_cover_mean": [40.0] * n,
        "pm2_5_mean": [20.0 + i for i in range(n)],
        "pm10_mean": [30.0 + i for i in range(n)],
        "lat": [13.75] * n,
        "lon": [100.52] * n,
        "load_id": ["test"] * n,
    })


# ── add_lag_features ──────────────────────────────────────────────────────────

class TestAddLagFeatures:
    def test_creates_lag_columns(self):
        # Given
        df = _daily_df(10)
        # When
        result = add_lag_features(df, target_cols=["pm2_5_mean"], lag_days=[1, 3]).collect()
        # Then
        assert "pm2_5_mean_lag1" in result.columns
        assert "pm2_5_mean_lag3" in result.columns

    def test_lag1_value_matches_previous_row(self):
        # Given
        df = _daily_df(10)
        # When
        result = add_lag_features(df, target_cols=["pm2_5_mean"], lag_days=[1]).collect()
        # Then — row index 1 should have lag1 = value at index 0
        assert result["pm2_5_mean_lag1"][1] == pytest.approx(result["pm2_5_mean"][0])

    def test_lag_does_not_mutate_input(self):
        # Given
        df = _daily_df(10)
        original_cols = set(df.collect_schema().names())
        # When
        add_lag_features(df, target_cols=["pm2_5_mean"], lag_days=[1])
        # Then — original frame unchanged
        assert set(df.collect_schema().names()) == original_cols

    def test_creates_correct_number_of_columns(self):
        df = _daily_df(10)
        result = add_lag_features(df, target_cols=["pm2_5_mean", "pm10_mean"], lag_days=[1, 7]).collect()
        # 2 columns × 2 lags = 4 new columns
        new_cols = [c for c in result.columns if "lag" in c]
        assert len(new_cols) == 4


# ── add_rolling_features ──────────────────────────────────────────────────────

class TestAddRollingFeatures:
    def test_creates_mean_and_std_columns(self):
        df = _daily_df(30)
        result = add_rolling_features(df, target_cols=["pm2_5_mean"], windows=[7]).collect()
        assert "pm2_5_mean_rolling_mean_7d" in result.columns
        assert "pm2_5_mean_rolling_std_7d" in result.columns

    def test_rolling_mean_within_valid_range(self):
        df = _daily_df(30)
        result = add_rolling_features(df, target_cols=["pm2_5_mean"], windows=[3]).collect()
        valid = result["pm2_5_mean_rolling_mean_3d"].drop_nulls()
        assert (valid >= 0).all()


# ── decompose_wind_vectors ───────────────────────────────────────────────────

class TestDecomposeWindVectors:
    def test_creates_uv_columns(self):
        df = _daily_df(5)
        result = decompose_wind_vectors(df).collect()
        assert "wind_u10_mean" in result.columns
        assert "wind_v10_mean" in result.columns

    def test_wind_magnitude_preserved(self):
        """U² + V² should equal speed² for the decomposition to be correct."""
        df = _daily_df(5)
        result = decompose_wind_vectors(df).collect()
        speed = result["wind_speed_mean"]
        u = result["wind_u10_mean"]
        v = result["wind_v10_mean"]
        reconstructed = (u ** 2 + v ** 2).sqrt()
        for s, r in zip(speed.to_list(), reconstructed.to_list()):
            assert abs(s - r) < 1e-4


# ── add_temporal_encoding ────────────────────────────────────────────────────

class TestAddTemporalEncoding:
    def test_creates_cyclical_columns(self):
        df = _daily_df(5)
        result = add_temporal_encoding(df).collect()
        for col in ["day_of_year_sin", "day_of_year_cos", "month_sin", "month_cos"]:
            assert col in result.columns

    def test_cyclical_values_in_minus1_to_1(self):
        df = _daily_df(10)
        result = add_temporal_encoding(df).collect()
        for col in ["day_of_year_sin", "day_of_year_cos"]:
            vals = result[col].drop_nulls().to_list()
            assert all(-1.0 <= v <= 1.0 for v in vals), f"{col} out of [-1, 1]"

    def test_sin_cos_identity(self):
        """sin² + cos² should equal 1 (± numerical error)."""
        df = _daily_df(10)
        result = add_temporal_encoding(df).collect()
        sin_sq = result["day_of_year_sin"] ** 2
        cos_sq = result["day_of_year_cos"] ** 2
        identity = (sin_sq + cos_sq).to_list()
        assert all(abs(v - 1.0) < 1e-5 for v in identity)


# ── clip_outliers ─────────────────────────────────────────────────────────────

class TestClipOutliers:
    def test_temperature_clipped_to_valid_range(self):
        df = pl.LazyFrame({
            "stationID": ["A"],
            "date": [pl.date(2024, 1, 1)],
            "temp_2m_mean": [999.0],
            "temp_2m_min": [-999.0],
            "temp_2m_max": [100.0],
            "rh_2m_mean": [200.0],
            "pressure_mean": [1010.0],
            "precip_sum": [-5.0],
            "wind_speed_mean": [200.0],
            "radiation_mean": [5000.0],
            "cloud_cover_mean": [150.0],
            "pm2_5_mean": [5000.0],
            "pm10_mean": [5000.0],
        })
        result = clip_outliers(df).collect()
        assert result["temp_2m_mean"][0] <= 55.0
        assert result["temp_2m_min"][0] >= -10.0
        assert result["rh_2m_mean"][0] <= 100.0
        assert result["precip_sum"][0] >= 0.0
        assert result["pm2_5_mean"][0] <= 1000.0

    @given(temp=st.floats(min_value=-500, max_value=500, allow_nan=False, allow_infinity=False))
    @settings(max_examples=100)
    def test_clip_always_produces_valid_temperature(self, temp: float):
        """Property: any input temperature is clipped to [-10, 55]."""
        df = pl.LazyFrame({
            "stationID": ["A"],
            "date": [pl.date(2024, 1, 1)],
            "temp_2m_mean": [temp],
            "temp_2m_min": [temp],
            "temp_2m_max": [temp],
            "rh_2m_mean": [50.0],
            "pressure_mean": [1010.0],
            "precip_sum": [0.0],
            "wind_speed_mean": [3.0],
            "radiation_mean": [200.0],
            "cloud_cover_mean": [40.0],
            "pm2_5_mean": [20.0],
            "pm10_mean": [30.0],
        })
        result = clip_outliers(df).collect()
        assert -10.0 <= result["temp_2m_mean"][0] <= 55.0
