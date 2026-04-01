"""Integration tests for the training pipeline: dataset → trainer → evaluator."""

from __future__ import annotations

from datetime import date, timedelta

import numpy as np
import polars as pl
import pytest
import torch

from src.training.config import TrainConfig
from src.training.dataset import PM25SequenceDataset, build_dataloaders
from src.training.evaluator import compute_metrics
from src.training.loss import CombinedLoss
from src.training.models import GRUForecaster, STUNN
from src.training.trainer import Trainer


# ── Fixtures ─────────────────────────────────────────────────────────────────

_BASE_DATE = date(2024, 1, 1)


def _make_synthetic_df(n_days: int = 100, n_stations: int = 2) -> pl.DataFrame:
    """Create a minimal synthetic daily DataFrame for testing.

    Uses datetime.date (not pl.date Expr) so the 'date' column has dtype pl.Date.
    """
    station_ids = [f"st{i:02d}" for i in range(n_stations)]

    rows = []
    for station in station_ids:
        for day in range(n_days):
            rows.append({
                "stationID": station,
                "date": _BASE_DATE + timedelta(days=day),
                "pm2_5_mean": float(20 + np.random.rand() * 30),
                "temp_2m_mean": float(25 + np.random.randn() * 3),
                "rh_2m_mean": float(70 + np.random.randn() * 5),
                "wind_speed_mean": float(3 + abs(np.random.randn())),
            })
    return pl.DataFrame(rows)


@pytest.fixture(scope="module")
def synthetic_df() -> pl.DataFrame:
    np.random.seed(0)
    return _make_synthetic_df(n_days=80, n_stations=2)


@pytest.fixture(scope="module")
def feature_cols() -> list[str]:
    return ["pm2_5_mean", "temp_2m_mean", "rh_2m_mean", "wind_speed_mean"]


@pytest.fixture(scope="module")
def tmp_model_dir(tmp_path_factory):
    return tmp_path_factory.mktemp("models")


# ── Dataset tests ─────────────────────────────────────────────────────────────

class TestPM25SequenceDataset:
    def test_builds_samples(self, synthetic_df, feature_cols):
        ds = PM25SequenceDataset(
            synthetic_df,
            feature_cols=feature_cols,
            target_col="pm2_5_mean",
            seq_len=10,
            horizons=[1, 3],
        )
        assert len(ds) > 0

    def test_sample_shapes(self, synthetic_df, feature_cols):
        seq_len = 10
        horizons = [1, 3]
        ds = PM25SequenceDataset(
            synthetic_df,
            feature_cols=feature_cols,
            target_col="pm2_5_mean",
            seq_len=seq_len,
            horizons=horizons,
        )
        x, y = ds[0]
        assert x.shape == (seq_len, len(feature_cols))
        assert y.shape == (len(horizons),)

    def test_no_nan_in_cached_samples(self, synthetic_df, feature_cols):
        ds = PM25SequenceDataset(
            synthetic_df,
            feature_cols=feature_cols,
            target_col="pm2_5_mean",
            seq_len=5,
            horizons=[1],
            cache=True,
        )
        for i in range(min(len(ds), 20)):
            x, y = ds[i]
            assert not torch.isnan(x).any()
            assert not torch.isnan(y).any()


# ── DataLoader tests ──────────────────────────────────────────────────────────

class TestBuildDataloaders:
    def test_creates_three_loaders(self, synthetic_df, feature_cols, tmp_model_dir):
        cfg = TrainConfig(
            data_dir=tmp_model_dir,
            output_dir=tmp_model_dir,
            batch_size=16,
            num_workers=0,
        )
        train, val, test = build_dataloaders(
            synthetic_df,
            synthetic_df,
            synthetic_df,
            feature_cols,
            "pm2_5_mean",
            seq_len=10,
            horizons=[1],
            cfg=cfg,
        )
        assert train is not None
        assert val is not None
        assert test is not None

    def test_batch_shape_correct(self, synthetic_df, feature_cols, tmp_model_dir):
        cfg = TrainConfig(
            data_dir=tmp_model_dir,
            output_dir=tmp_model_dir,
            batch_size=8,
            num_workers=0,
        )
        train, _, _ = build_dataloaders(
            synthetic_df, synthetic_df, synthetic_df,
            feature_cols, "pm2_5_mean", seq_len=5, horizons=[1, 3], cfg=cfg,
        )
        x_batch, y_batch = next(iter(train))
        assert x_batch.ndim == 3
        assert y_batch.shape[-1] == 2


# ── Trainer smoke test ────────────────────────────────────────────────────────

@pytest.mark.slow
class TestTrainer:
    def test_training_reduces_loss(self, synthetic_df, feature_cols, tmp_model_dir):
        """Training for a few epochs should decrease val_loss."""
        cfg = TrainConfig(
            data_dir=tmp_model_dir,
            output_dir=tmp_model_dir,
            epochs=5,
            patience=10,
            batch_size=16,
            num_workers=0,
            use_amp=False,
            compile_model=False,
        )
        train_loader, val_loader, _ = build_dataloaders(
            synthetic_df, synthetic_df, synthetic_df,
            feature_cols, "pm2_5_mean", seq_len=5, horizons=[1], cfg=cfg,
        )
        model = GRUForecaster(
            num_features=len(feature_cols),
            hidden_size=32,
            num_layers=1,
            num_horizons=1,
        )
        trainer = Trainer(model, cfg, model_name="test_gru")
        history = trainer.train(train_loader, val_loader)

        assert len(history["train_loss"]) == 5
        assert len(history["val_loss"]) == 5
        assert all(v is not None for v in history["val_loss"])

    def test_evaluator_metrics_shape(self, synthetic_df, feature_cols, tmp_model_dir):
        """Evaluator should return a DataFrame with correct row count."""
        cfg = TrainConfig(
            data_dir=tmp_model_dir,
            output_dir=tmp_model_dir,
            epochs=2,
            patience=10,
            batch_size=16,
            num_workers=0,
            use_amp=False,
            compile_model=False,
        )
        horizons = [1, 3]
        _, val_loader, _ = build_dataloaders(
            synthetic_df, synthetic_df, synthetic_df,
            feature_cols, "pm2_5_mean", seq_len=5, horizons=horizons, cfg=cfg,
        )
        model = GRUForecaster(
            num_features=len(feature_cols),
            hidden_size=32,
            num_layers=1,
            num_horizons=len(horizons),
        )
        trainer = Trainer(model, cfg, model_name="test_eval")
        trainer.train(val_loader, val_loader)
        _, preds, targets = trainer.evaluate(val_loader)

        metrics = compute_metrics(preds, targets, horizons=horizons)
        assert len(metrics) == len(horizons)
        assert "MAE" in metrics.columns
        assert "RMSE" in metrics.columns
        assert "R2" in metrics.columns
