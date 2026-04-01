"""Production trainer with mixed precision, gradient accumulation, and MLflow tracking."""

from __future__ import annotations

import os
import random
import time
from contextlib import nullcontext
from pathlib import Path
from typing import Any

import numpy as np
import torch
import torch.nn as nn
from torch.amp import GradScaler, autocast
from torch.utils.data import DataLoader

from src.training.config import TrainConfig
from src.training.loss import CombinedLoss
from src.utils.logger import get_logger

log = get_logger(__name__)


def seed_everything(seed: int) -> None:
    """Seed Python, NumPy, and PyTorch for reproducibility."""
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(seed)
    os.environ["PYTHONHASHSEED"] = str(seed)
    torch.backends.cudnn.deterministic = True
    torch.backends.cudnn.benchmark = False


def select_device() -> torch.device:
    """Auto-detect best available accelerator: CUDA/ROCm > MPS > CPU."""
    if torch.cuda.is_available():
        return torch.device("cuda")
    if hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
        return torch.device("mps")
    return torch.device("cpu")


class Trainer:
    """Production trainer with:
    - Mixed precision (AMP) for CUDA/GPU speedup
    - Gradient accumulation for larger effective batch sizes
    - Gradient clipping for stable training
    - Early stopping with best-model checkpointing
    - LR scheduling (ReduceLROnPlateau)
    - Optional MLflow experiment tracking
    - Optional torch.compile for PyTorch 2.0+ speed gains
    """

    def __init__(
        self,
        model: nn.Module,
        cfg: TrainConfig,
        device: torch.device | None = None,
        model_name: str = "model",
        mlflow_run: Any | None = None,
    ) -> None:
        self.cfg = cfg
        self.model_name = model_name
        self.device = device or select_device()
        self._mlflow_run = mlflow_run

        seed_everything(cfg.seed)

        # AMP only supported on CUDA; disable silently for CPU/MPS
        self._use_amp = cfg.use_amp and self.device.type == "cuda"
        self._scaler: GradScaler | None = GradScaler() if self._use_amp else None

        # Optionally compile model (PyTorch 2.0+)
        if cfg.compile_model:
            try:
                model = torch.compile(model, mode="reduce-overhead")  # type: ignore[assignment]
                log.info("trainer.model_compiled", model=model_name)
            except Exception as exc:
                log.warning("trainer.compile_failed", error=str(exc))

        self.model = model.to(self.device)
        self.criterion = CombinedLoss(cfg.mae_weight, cfg.rmse_weight).to(self.device)
        self.optimizer = torch.optim.AdamW(
            self.model.parameters(),
            lr=cfg.learning_rate,
            weight_decay=cfg.weight_decay,
        )
        self.scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(
            self.optimizer,
            mode="min",
            factor=cfg.lr_scheduler_factor,
            patience=cfg.lr_scheduler_patience,
        )

        n_params = sum(p.numel() for p in model.parameters() if p.requires_grad)
        log.info(
            "trainer.init",
            model=model_name,
            parameters=n_params,
            device=str(self.device),
            use_amp=self._use_amp,
            accumulation_steps=cfg.accumulation_steps,
        )

    def _amp_context(self):
        """Return autocast context or nullcontext depending on AMP availability."""
        if self._use_amp:
            return autocast(device_type="cuda")
        return nullcontext()

    def train_epoch(self, loader: DataLoader) -> float:
        """Train one epoch with gradient accumulation and AMP."""
        self.model.train()
        total_loss = 0.0
        n_updates = 0

        self.optimizer.zero_grad()

        for step, (x_batch, y_batch) in enumerate(loader):
            x_batch = x_batch.to(self.device, non_blocking=True)
            y_batch = y_batch.to(self.device, non_blocking=True)

            with self._amp_context():
                pred = self.model(x_batch)
                # Scale loss by accumulation steps for consistent gradient magnitude
                loss = self.criterion(pred, y_batch) / self.cfg.accumulation_steps

            if self._scaler is not None:
                self._scaler.scale(loss).backward()
            else:
                loss.backward()

            total_loss += loss.item() * self.cfg.accumulation_steps

            is_last_step = (step + 1) == len(loader)
            if (step + 1) % self.cfg.accumulation_steps == 0 or is_last_step:
                if self._scaler is not None:
                    self._scaler.unscale_(self.optimizer)
                    torch.nn.utils.clip_grad_norm_(
                        self.model.parameters(), self.cfg.gradient_clip_norm
                    )
                    self._scaler.step(self.optimizer)
                    self._scaler.update()
                else:
                    torch.nn.utils.clip_grad_norm_(
                        self.model.parameters(), self.cfg.gradient_clip_norm
                    )
                    self.optimizer.step()

                self.optimizer.zero_grad()
                n_updates += 1

        return total_loss / max(len(loader), 1)

    @torch.no_grad()
    def evaluate(
        self, loader: DataLoader
    ) -> tuple[float, np.ndarray, np.ndarray]:
        """Evaluate model, return (avg_loss, all_preds, all_targets)."""
        self.model.eval()
        total_loss = 0.0
        preds_list: list[np.ndarray] = []
        targets_list: list[np.ndarray] = []

        for x_batch, y_batch in loader:
            x_batch = x_batch.to(self.device, non_blocking=True)
            y_batch = y_batch.to(self.device, non_blocking=True)

            with self._amp_context():
                pred = self.model(x_batch)
                loss = self.criterion(pred, y_batch)

            total_loss += loss.item()
            preds_list.append(pred.cpu().float().numpy())
            targets_list.append(y_batch.cpu().float().numpy())

            if self.device.type == "cuda":
                torch.cuda.empty_cache()

        all_preds = np.concatenate(preds_list) if preds_list else np.empty((0,))
        all_targets = np.concatenate(targets_list) if targets_list else np.empty((0,))
        return total_loss / max(len(loader), 1), all_preds, all_targets

    def train(
        self,
        train_loader: DataLoader,
        val_loader: DataLoader,
    ) -> dict[str, list[float]]:
        """Full training loop with early stopping and checkpointing.

        Returns history dict with keys: train_loss, val_loss, lr.
        Saves best checkpoint to cfg.output_dir/{model_name}_best.pt.
        """
        cfg = self.cfg
        cfg.output_dir.mkdir(parents=True, exist_ok=True)
        ckpt_path = cfg.output_dir / f"{self.model_name}_best.pt"

        best_val_loss = float("inf")
        patience_counter = 0
        best_state: dict | None = None
        history: dict[str, list[float]] = {"train_loss": [], "val_loss": [], "lr": []}

        t_start = time.time()

        for epoch in range(1, cfg.epochs + 1):
            train_loss = self.train_epoch(train_loader)
            val_loss, _, _ = self.evaluate(val_loader)
            current_lr = self.optimizer.param_groups[0]["lr"]

            history["train_loss"].append(train_loss)
            history["val_loss"].append(val_loss)
            history["lr"].append(current_lr)

            self.scheduler.step(val_loss)

            if val_loss < best_val_loss:
                best_val_loss = val_loss
                patience_counter = 0
                best_state = {k: v.cpu().clone() for k, v in self.model.state_dict().items()}
            else:
                patience_counter += 1

            if epoch % 10 == 0 or epoch == 1:
                log.info(
                    "trainer.epoch",
                    model=self.model_name,
                    epoch=epoch,
                    total_epochs=cfg.epochs,
                    train_loss=round(train_loss, 4),
                    val_loss=round(val_loss, 4),
                    lr=f"{current_lr:.2e}",
                    patience=f"{patience_counter}/{cfg.patience}",
                )

            # MLflow logging
            if self._mlflow_run is not None:
                try:
                    import mlflow

                    mlflow.log_metrics(
                        {
                            "train_loss": train_loss,
                            "val_loss": val_loss,
                            "lr": current_lr,
                        },
                        step=epoch,
                    )
                except Exception:
                    pass

            if patience_counter >= cfg.patience:
                log.info(
                    "trainer.early_stop",
                    model=self.model_name,
                    epoch=epoch,
                    best_val_loss=round(best_val_loss, 4),
                )
                break

        elapsed = time.time() - t_start

        if best_state is not None:
            self.model.load_state_dict(best_state)
            torch.save(best_state, ckpt_path)
            log.info(
                "trainer.checkpoint_saved",
                path=str(ckpt_path),
                best_val_loss=round(best_val_loss, 4),
                elapsed_s=round(elapsed, 1),
            )

        return history

    def load_best(self) -> None:
        """Load best checkpoint from cfg.output_dir."""
        ckpt_path = self.cfg.output_dir / f"{self.model_name}_best.pt"
        if not ckpt_path.exists():
            raise FileNotFoundError(f"No checkpoint found at {ckpt_path}")
        state = torch.load(ckpt_path, map_location=self.device)
        self.model.load_state_dict(state)
        log.info("trainer.loaded_best", path=str(ckpt_path))
