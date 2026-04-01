"""Training configuration — single source of truth for all hyperparameters."""

from __future__ import annotations

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings


def _project_root() -> Path:
    """Resolve project root (containing pyproject.toml) so paths work from any cwd."""
    cwd = Path.cwd()
    for candidate in [cwd, *cwd.parents]:
        if (candidate / "pyproject.toml").exists():
            return candidate
    return cwd


_ROOT = _project_root()


class TrainConfig(BaseSettings):
    """Training hyperparameters — all env vars prefixed with TRAIN_."""

    # Paths
    data_dir: Path = Field(default=_ROOT / "data/gold/model_ready")
    output_dir: Path = Field(default=_ROOT / "models")

    # Architecture
    hidden_size: int = Field(default=128, ge=32, le=1024)
    num_layers: int = Field(default=2, ge=1, le=8)
    dropout: float = Field(default=0.2, ge=0.0, le=0.8)
    attention_heads: int = Field(default=4, ge=1, le=16)
    # MLP first-layer width = hidden_size * mlp_hidden_multiplier (default 2 → 256)
    mlp_hidden_multiplier: int = Field(default=2, ge=1, le=8)

    # Training dynamics
    epochs: int = Field(default=100, ge=1)
    batch_size: int = Field(default=64, ge=1)
    learning_rate: float = Field(default=1e-3, gt=0.0)
    weight_decay: float = Field(default=1e-5, ge=0.0)
    patience: int = Field(default=15, ge=1)
    lr_scheduler_factor: float = Field(default=0.5, gt=0.0, lt=1.0)
    lr_scheduler_patience: int = Field(default=7, ge=1)
    gradient_clip_norm: float = Field(default=1.0, gt=0.0)

    # Gradient accumulation: effective_batch = batch_size * accumulation_steps
    accumulation_steps: int = Field(default=1, ge=1)

    # Mixed precision (AMP): set False for CPU-only or MPS
    use_amp: bool = Field(default=True)

    # torch.compile (requires PyTorch >= 2.0)
    compile_model: bool = Field(default=False)

    # DataLoader workers
    num_workers: int = Field(default=4, ge=0)

    # Loss weights (MAE primary, RMSE secondary)
    mae_weight: float = Field(default=0.7, ge=0.0, le=1.0)
    rmse_weight: float = Field(default=0.3, ge=0.0, le=1.0)

    # Reproducibility
    seed: int = Field(default=42)

    model_config = {"env_prefix": "TRAIN_"}
