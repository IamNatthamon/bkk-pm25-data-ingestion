"""Hyperparameter tuning with Optuna."""

from __future__ import annotations

from pathlib import Path
from typing import Callable

import numpy as np
import polars as pl
import torch

from src.training.config import TrainConfig
from src.training.dataset import build_dataloaders
from src.training.models import STUNN
from src.training.trainer import Trainer
from src.utils.logger import get_logger

log = get_logger(__name__)


def _build_objective(
    train_df: pl.DataFrame,
    val_df: pl.DataFrame,
    feature_cols: list[str],
    target_col: str,
    seq_len: int,
    horizons: list[int],
    base_cfg: TrainConfig,
) -> Callable:
    """Build an Optuna objective function for STUNN hyperparameter search."""

    def objective(trial) -> float:  # type: ignore[no-untyped-def]
        import optuna

        hidden_size = trial.suggest_int("hidden_size", 64, 256, step=32)
        num_layers = trial.suggest_int("num_layers", 1, 4)
        dropout = trial.suggest_float("dropout", 0.1, 0.5)
        learning_rate = trial.suggest_float("learning_rate", 1e-5, 1e-2, log=True)
        attention_heads = trial.suggest_categorical("attention_heads", [2, 4, 8])
        batch_size = trial.suggest_categorical("batch_size", [32, 64, 128])
        accumulation_steps = trial.suggest_int("accumulation_steps", 1, 4)

        cfg = TrainConfig(
            data_dir=base_cfg.data_dir,
            output_dir=base_cfg.output_dir / "optuna_trials",
            hidden_size=hidden_size,
            num_layers=num_layers,
            dropout=dropout,
            learning_rate=learning_rate,
            attention_heads=attention_heads,
            batch_size=batch_size,
            accumulation_steps=accumulation_steps,
            epochs=30,
            patience=8,
            use_amp=base_cfg.use_amp,
            num_workers=base_cfg.num_workers,
            seed=base_cfg.seed,
        )

        train_loader, val_loader, _ = build_dataloaders(
            train_df,
            val_df,
            val_df,
            feature_cols,
            target_col,
            seq_len,
            horizons,
            cfg,
        )

        model = STUNN(
            num_features=len(feature_cols),
            hidden_size=cfg.hidden_size,
            num_layers=cfg.num_layers,
            num_horizons=len(horizons),
            attention_heads=cfg.attention_heads,
            dropout=cfg.dropout,
        )

        trainer = Trainer(model, cfg, model_name=f"stunn_trial_{trial.number}")

        try:
            history = trainer.train(train_loader, val_loader)
            best_val_loss = min(history["val_loss"])
        except Exception as exc:
            log.warning("tuner.trial_failed", trial=trial.number, error=str(exc))
            raise optuna.exceptions.TrialPruned() from exc

        log.info(
            "tuner.trial_complete",
            trial=trial.number,
            val_loss=round(best_val_loss, 4),
            params=trial.params,
        )
        return best_val_loss

    return objective


def run_optuna_study(
    train_df: pl.DataFrame,
    val_df: pl.DataFrame,
    feature_cols: list[str],
    target_col: str,
    seq_len: int,
    horizons: list[int],
    base_cfg: TrainConfig,
    n_trials: int = 50,
    timeout_s: int = 3600,
    study_name: str = "pm25_stunn_tuning",
    storage: str | None = None,
) -> dict:
    """Run an Optuna study to find optimal STUNN hyperparameters.

    Args:
        train_df: Training data.
        val_df: Validation data.
        feature_cols: Feature column names.
        target_col: Target column name.
        seq_len: Sequence length for sliding window.
        horizons: Forecast horizons (days).
        base_cfg: Base TrainConfig with paths and seed.
        n_trials: Number of trials to run.
        timeout_s: Maximum wall-clock seconds.
        study_name: Study name (used for storage).
        storage: Optuna storage URL (e.g. "sqlite:///optuna.db"). None = in-memory.

    Returns:
        Best hyperparameters dict.
    """
    try:
        import optuna
    except ImportError as exc:
        raise ImportError(
            "optuna is required for hyperparameter tuning. Install with: uv add optuna"
        ) from exc

    optuna.logging.set_verbosity(optuna.logging.WARNING)

    study = optuna.create_study(
        direction="minimize",
        study_name=study_name,
        storage=storage,
        load_if_exists=True,
        pruner=optuna.pruners.MedianPruner(n_startup_trials=5, n_warmup_steps=10),
    )

    objective = _build_objective(
        train_df, val_df, feature_cols, target_col, seq_len, horizons, base_cfg
    )

    log.info("tuner.study_start", n_trials=n_trials, timeout_s=timeout_s)
    study.optimize(objective, n_trials=n_trials, timeout=timeout_s)

    best_params = study.best_params
    best_value = study.best_value
    log.info("tuner.study_complete", best_val_loss=round(best_value, 4), best_params=best_params)

    return best_params
