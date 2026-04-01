"""PM2.5 sequence dataset and DataLoader factory."""

from __future__ import annotations

import numpy as np
import polars as pl
import torch
from torch.utils.data import DataLoader, Dataset

from src.training.config import TrainConfig
from src.utils.logger import get_logger

log = get_logger(__name__)


class PM25SequenceDataset(Dataset):
    """Sliding-window dataset: (seq_len × features) → PM2.5 at forecast horizons.

    Accepts a Polars DataFrame; converts to NumPy arrays internally.
    Sequences with any NaN are silently dropped.
    Sequences are pre-cached in RAM on construction for fast __getitem__.
    """

    def __init__(
        self,
        df: pl.DataFrame,
        feature_cols: list[str],
        target_col: str,
        seq_len: int,
        horizons: list[int],
        station_col: str = "stationID",
        cache: bool = True,
    ) -> None:
        self.seq_len = seq_len
        self.horizons = horizons
        self.max_h = max(horizons)

        self.samples: list[tuple[np.ndarray, np.ndarray]] = []

        for station_id in df[station_col].unique().to_list():
            sdf = df.filter(pl.col(station_col) == station_id).sort("date")
            feats = sdf.select(feature_cols).to_numpy().astype(np.float32)
            tgts = sdf.select(target_col).to_numpy().flatten().astype(np.float32)

            for i in range(seq_len, len(sdf) - self.max_h + 1):
                x = feats[i - seq_len : i]
                y = np.array([tgts[i + h - 1] for h in horizons], dtype=np.float32)
                if np.isnan(x).any() or np.isnan(y).any():
                    continue
                self.samples.append((x, y))

        log.info(
            "dataset.built",
            samples=len(self.samples),
            seq_len=seq_len,
            horizons=horizons,
            features=len(feature_cols),
        )

        # Pre-convert to tensors for faster __getitem__
        if cache and self.samples:
            self._x_cache = torch.from_numpy(
                np.stack([s[0] for s in self.samples])
            )
            self._y_cache = torch.from_numpy(
                np.stack([s[1] for s in self.samples])
            )
            self._cached = True
        else:
            self._cached = False

    def __len__(self) -> int:
        return len(self.samples)

    def __getitem__(self, idx: int) -> tuple[torch.Tensor, torch.Tensor]:
        if self._cached:
            return self._x_cache[idx], self._y_cache[idx]
        x, y = self.samples[idx]
        return torch.from_numpy(x.copy()), torch.from_numpy(y.copy())


def build_dataloaders(
    train_df: pl.DataFrame,
    val_df: pl.DataFrame,
    test_df: pl.DataFrame,
    feature_cols: list[str],
    target_col: str,
    seq_len: int,
    horizons: list[int],
    cfg: TrainConfig,
) -> tuple[DataLoader, DataLoader, DataLoader]:
    """Build train/val/test DataLoaders with optimal settings."""
    train_ds = PM25SequenceDataset(train_df, feature_cols, target_col, seq_len, horizons)
    val_ds = PM25SequenceDataset(val_df, feature_cols, target_col, seq_len, horizons)
    test_ds = PM25SequenceDataset(test_df, feature_cols, target_col, seq_len, horizons)

    pin_memory = torch.cuda.is_available()

    loader_kwargs: dict = {
        "num_workers": cfg.num_workers,
        "pin_memory": pin_memory,
        "persistent_workers": cfg.num_workers > 0,
        "prefetch_factor": 2 if cfg.num_workers > 0 else None,
    }

    train_loader = DataLoader(
        train_ds,
        batch_size=cfg.batch_size,
        shuffle=True,
        **loader_kwargs,
    )
    val_loader = DataLoader(
        val_ds,
        batch_size=cfg.batch_size * 2,
        shuffle=False,
        **loader_kwargs,
    )
    test_loader = DataLoader(
        test_ds,
        batch_size=cfg.batch_size * 2,
        shuffle=False,
        **loader_kwargs,
    )

    log.info(
        "dataloaders.built",
        train_samples=len(train_ds),
        val_samples=len(val_ds),
        test_samples=len(test_ds),
        batch_size=cfg.batch_size,
        num_workers=cfg.num_workers,
        pin_memory=pin_memory,
    )

    return train_loader, val_loader, test_loader
