"""Model versioning and MLflow experiment tracking."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import torch
import torch.nn as nn

from src.training.config import TrainConfig
from src.training.evaluator import compute_metrics
from src.utils.logger import get_logger

log = get_logger(__name__)


def start_mlflow_run(
    cfg: TrainConfig,
    model_name: str,
    experiment_name: str = "pm25_forecasting",
) -> Any:
    """Start an MLflow run and log config params. Returns the active run."""
    try:
        import mlflow
        from mlflow.models import infer_signature
    except ImportError as exc:
        raise ImportError(
            "mlflow is required for experiment tracking. Install with: uv add mlflow"
        ) from exc

    mlflow.set_experiment(experiment_name)
    run_name = f"{model_name}_{datetime.now(tz=timezone.utc):%Y%m%d_%H%M%S}"
    run = mlflow.start_run(run_name=run_name)
    mlflow.log_params(cfg.model_dump())
    log.info("mlflow.run_started", run_id=run.info.run_id, run_name=run_name)
    return run


def log_model_to_mlflow(
    model: nn.Module,
    model_name: str,
    x_sample: torch.Tensor,
    metrics: dict[str, float],
    register: bool = True,
) -> None:
    """Log a trained model artifact and evaluation metrics to MLflow."""
    try:
        import mlflow
        from mlflow.models import infer_signature
    except ImportError:
        log.warning("registry.mlflow_not_available")
        return

    with torch.no_grad():
        pred_sample = model(x_sample.cpu()).cpu().numpy()

    sig = infer_signature(x_sample.cpu().numpy(), pred_sample)
    mlflow.log_metrics(metrics)
    mlflow.pytorch.log_model(
        model,
        artifact_path="model",
        signature=sig,
        registered_model_name=f"pm25_{model_name}" if register else None,
    )
    log.info("registry.model_logged", model=model_name, register=register)


def save_run_manifest(
    model_name: str,
    cfg: TrainConfig,
    metrics: dict[str, Any],
    output_dir: Path | None = None,
) -> Path:
    """Save a JSON manifest of the training run for reproducibility."""
    output_dir = output_dir or cfg.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    manifest = {
        "model_name": model_name,
        "created_at": datetime.now(tz=timezone.utc).isoformat(),
        "config": cfg.model_dump(mode="json"),
        "metrics": metrics,
    }

    path = output_dir / f"{model_name}_run_manifest.json"
    with open(path, "w") as f:
        json.dump(manifest, f, indent=2, default=str)

    log.info("registry.manifest_saved", path=str(path))
    return path
