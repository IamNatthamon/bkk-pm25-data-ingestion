"""Training module for Bangkok PM2.5 forecasting."""

from src.training.config import TrainConfig
from src.training.dataset import PM25SequenceDataset, build_dataloaders
from src.training.evaluator import compute_metrics, compute_seasonal_metrics, compute_extreme_accuracy
from src.training.inference import load_deployment_bundle, load_all_checkpoints, load_model, predict
from src.training.loss import CombinedLoss
from src.training.models import (
    GRUForecaster,
    LSTMForecaster,
    MLPForecaster,
    PersistenceModel,
    STUNN,
    build_model_catalog,
    count_parameters,
)
from src.training.trainer import Trainer, seed_everything, select_device

__all__ = [
    "TrainConfig",
    "PM25SequenceDataset",
    "build_dataloaders",
    "compute_metrics",
    "compute_seasonal_metrics",
    "compute_extreme_accuracy",
    "load_deployment_bundle",
    "load_all_checkpoints",
    "load_model",
    "predict",
    "CombinedLoss",
    "GRUForecaster",
    "LSTMForecaster",
    "MLPForecaster",
    "PersistenceModel",
    "STUNN",
    "build_model_catalog",
    "count_parameters",
    "Trainer",
    "seed_everything",
    "select_device",
]
