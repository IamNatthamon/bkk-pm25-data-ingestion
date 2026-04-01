"""Unit tests for compute_metrics — MAE, RMSE, R² per forecast horizon."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st


# ---------------------------------------------------------------------------
# Inline implementation — mirrors model_training.ipynb
# ---------------------------------------------------------------------------

def compute_metrics(
    preds: np.ndarray,
    targets: np.ndarray,
    horizons: list[int],
) -> pd.DataFrame:
    """Compute MAE, RMSE, R² per forecast horizon."""
    rows = []
    for i, h in enumerate(horizons):
        p = preds[:, i]
        t = targets[:, i]
        mae = np.mean(np.abs(p - t))
        rmse = np.sqrt(np.mean((p - t) ** 2))
        ss_res = np.sum((t - p) ** 2)
        ss_tot = np.sum((t - np.mean(t)) ** 2)
        r2 = 1 - ss_res / (ss_tot + 1e-8)
        rows.append({"horizon_days": h, "MAE": mae, "RMSE": rmse, "R2": r2})
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestComputeMetrics:

    def test_zero_metrics_for_perfect_prediction(self):
        # Given
        preds   = np.array([[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]], dtype=np.float32)
        targets = preds.copy()

        # When
        df = compute_metrics(preds, targets, horizons=[1, 3])

        # Then
        assert float(df.loc[0, "MAE"])  < 1e-6
        assert float(df.loc[0, "RMSE"]) < 1e-6
        assert float(df.loc[1, "MAE"])  < 1e-6

    def test_returns_correct_number_of_horizons(self):
        preds   = np.random.rand(10, 3).astype(np.float32)
        targets = np.random.rand(10, 3).astype(np.float32)

        df = compute_metrics(preds, targets, horizons=[1, 3, 7])

        assert len(df) == 3
        assert list(df["horizon_days"]) == [1, 3, 7]

    def test_mae_rmse_non_negative(self):
        preds   = np.random.rand(50, 2).astype(np.float32)
        targets = np.random.rand(50, 2).astype(np.float32)

        df = compute_metrics(preds, targets, horizons=[1, 3])

        assert (df["MAE"]  >= 0).all()
        assert (df["RMSE"] >= 0).all()

    def test_rmse_geq_mae(self):
        """RMSE ≥ MAE by Cauchy-Schwarz inequality."""
        rng = np.random.default_rng(0)
        preds   = rng.standard_normal((100, 2)).astype(np.float32)
        targets = rng.standard_normal((100, 2)).astype(np.float32)

        df = compute_metrics(preds, targets, horizons=[1, 3])

        for _, row in df.iterrows():
            assert row["RMSE"] >= row["MAE"] - 1e-6, "RMSE should be >= MAE"

    def test_r2_is_one_for_perfect_prediction(self):
        preds   = np.array([[5.0], [10.0], [15.0], [20.0]])
        targets = preds.copy()

        df = compute_metrics(preds, targets, horizons=[1])

        assert abs(float(df.loc[0, "R2"]) - 1.0) < 1e-3

    def test_r2_below_one_for_imperfect(self):
        preds   = np.array([[5.0], [10.0], [15.0], [20.0]])
        targets = np.array([[6.0], [9.0], [16.0], [21.0]])

        df = compute_metrics(preds, targets, horizons=[1])

        assert float(df.loc[0, "R2"]) < 1.0

    @given(
        n=st.integers(min_value=5, max_value=200),
        n_horizons=st.integers(min_value=1, max_value=5),
    )
    @settings(max_examples=30)
    def test_shape_invariant(self, n: int, n_horizons: int):
        """Property: output always has n_horizons rows regardless of sample count."""
        preds   = np.random.rand(n, n_horizons).astype(np.float32)
        targets = np.random.rand(n, n_horizons).astype(np.float32)
        horizons = list(range(1, n_horizons + 1))

        df = compute_metrics(preds, targets, horizons=horizons)

        assert len(df) == n_horizons
        assert set(df.columns) == {"horizon_days", "MAE", "RMSE", "R2"}
