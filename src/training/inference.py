"""Production inference pipeline for PM2.5 forecasting."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import polars as pl
import torch
import torch.nn as nn

from src.training.config import TrainConfig
from src.training.dataset import PM25SequenceDataset
from src.training.trainer import select_device
from src.utils.logger import get_logger

log = get_logger(__name__)


def load_model(
    model: nn.Module,
    checkpoint_path: Path,
    device: torch.device | None = None,
) -> nn.Module:
    """Load a saved model checkpoint into an existing model instance.

    Handles two checkpoint formats:
    - Raw state dict (e.g. ``GRU_best.pt``, ``LSTM_best.pt``)
    - Wrapped deployment bundle with ``model_state_dict`` key (e.g. ``stunn_deployment.pt``)
    """
    checkpoint_path = Path(checkpoint_path)
    if not checkpoint_path.exists():
        raise FileNotFoundError(f"Checkpoint not found: {checkpoint_path}")

    device = device or select_device()
    raw = torch.load(checkpoint_path, map_location=device, weights_only=False)

    state_dict = raw["model_state_dict"] if isinstance(raw, dict) and "model_state_dict" in raw else raw

    model.load_state_dict(state_dict)
    model.eval()
    model.to(device)
    log.info("inference.model_loaded", path=str(checkpoint_path), device=str(device))
    return model


def load_deployment_bundle(
    bundle_path: Path,
    device: torch.device | None = None,
) -> tuple[nn.Module, dict[str, Any], list[str], list[int]]:
    """Load a full deployment bundle saved by model_training.ipynb.

    Bundle format (saved at ``models/stunn_deployment.pt``)::

        {
            "model_state_dict": ...,
            "config": {num_features, hidden_size, num_layers, num_horizons,
                       attention_heads, dropout, seq_len},
            "manifest": {...},
            "feature_cols": [...],
            "horizons": [1, 3],
        }

    Returns:
        (model, config_dict, feature_cols, horizons)
    """
    from src.training.models import STUNN

    bundle_path = Path(bundle_path)
    if not bundle_path.exists():
        raise FileNotFoundError(f"Deployment bundle not found: {bundle_path}")

    device = device or select_device()
    bundle = torch.load(bundle_path, map_location=device, weights_only=False)

    cfg = bundle["config"]
    model = STUNN.from_config(cfg)
    model.load_state_dict(bundle["model_state_dict"])
    model.eval()
    model.to(device)

    log.info(
        "inference.bundle_loaded",
        path=str(bundle_path),
        num_features=cfg["num_features"],
        hidden_size=cfg["hidden_size"],
        horizons=bundle["horizons"],
        device=str(device),
    )
    return model, cfg, bundle["feature_cols"], bundle["horizons"]


def load_all_checkpoints(
    models_dir: Path,
    num_features: int,
    seq_len: int,
    num_horizons: int,
    cfg: TrainConfig | None = None,
    device: torch.device | None = None,
) -> dict[str, nn.Module]:
    """Load all ``*_best.pt`` checkpoints from models_dir into model instances.

    Uses ``build_model_catalog`` to reconstruct architectures that match the
    training convention (MLP first-layer = hidden_size * mlp_hidden_multiplier).

    Returns:
        Dict mapping model name → loaded nn.Module in eval mode.
    """
    from src.training.models import build_model_catalog

    cfg = cfg or TrainConfig()
    device = device or select_device()

    catalog = build_model_catalog(
        num_features=num_features,
        seq_len=seq_len,
        num_horizons=num_horizons,
        hidden_size=cfg.hidden_size,
        num_layers=cfg.num_layers,
        dropout=cfg.dropout,
        attention_heads=cfg.attention_heads,
        mlp_hidden_multiplier=cfg.mlp_hidden_multiplier,
    )

    # Map from model name in catalog → checkpoint filename stem
    checkpoint_stems = {
        "MLP": "MLP_best",
        "LSTM": "LSTM_best",
        "GRU": "GRU_best",
        "ST-UNN": "ST-UNN_best",
    }

    loaded: dict[str, nn.Module] = {}
    for name, model in catalog.items():
        stem = checkpoint_stems.get(name, f"{name}_best")
        ckpt_path = Path(models_dir) / f"{stem}.pt"
        if not ckpt_path.exists():
            log.warning("inference.checkpoint_missing", model=name, path=str(ckpt_path))
            continue
        try:
            loaded[name] = load_model(model, ckpt_path, device=device)
        except Exception as exc:
            log.error("inference.load_failed", model=name, error=str(exc))

    log.info("inference.all_loaded", models=list(loaded.keys()))
    return loaded


@torch.no_grad()
def predict(
    model: nn.Module,
    df: pl.DataFrame,
    feature_cols: list[str],
    target_col: str,
    seq_len: int,
    horizons: list[int],
    batch_size: int = 256,
    device: torch.device | None = None,
) -> np.ndarray:
    """Run inference on a DataFrame, returning predictions array.

    Args:
        model: Trained nn.Module (already on device).
        df: Input DataFrame (must include feature_cols and target_col).
        feature_cols: Feature columns to use.
        target_col: Target column (used to build sliding window targets).
        seq_len: Sliding window length.
        horizons: Forecast horizons.
        batch_size: Inference batch size.
        device: Device to run inference on.

    Returns:
        np.ndarray of shape (N, len(horizons)).
    """
    device = device or select_device()
    model.eval()
    model.to(device)

    dataset = PM25SequenceDataset(
        df, feature_cols, target_col, seq_len, horizons, cache=True
    )
    loader = torch.utils.data.DataLoader(
        dataset,
        batch_size=batch_size,
        shuffle=False,
        num_workers=0,
        pin_memory=device.type == "cuda",
    )

    preds_list: list[np.ndarray] = []
    for x_batch, _ in loader:
        x_batch = x_batch.to(device, non_blocking=True)
        out = model(x_batch)
        preds_list.append(out.cpu().float().numpy())

    preds = np.concatenate(preds_list) if preds_list else np.empty((0, len(horizons)))
    log.info("inference.complete", samples=len(preds), horizons=horizons)
    return preds
