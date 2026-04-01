"""Unit tests for PM25SequenceDataset — sliding window data loader."""
from __future__ import annotations

import numpy as np
import polars as pl
import pytest
import torch
from torch.utils.data import DataLoader


# ---------------------------------------------------------------------------
# Inline implementation — mirrors model_training.ipynb
# ---------------------------------------------------------------------------
from torch.utils.data import Dataset


class PM25SequenceDataset(Dataset):
    """Sliding-window dataset (replicated from model_training.ipynb)."""

    def __init__(
        self,
        df: pl.DataFrame,
        feature_cols: list[str],
        target_col: str,
        seq_len: int,
        horizons: list[int],
        station_col: str = "stationID",
    ):
        self.seq_len = seq_len
        self.horizons = horizons
        self.max_h = max(horizons)
        self.samples: list[tuple[np.ndarray, np.ndarray]] = []

        for station_id in df[station_col].unique().to_list():
            sdf = df.filter(pl.col(station_col) == station_id).sort("date")
            feats = sdf.select(feature_cols).to_numpy().astype(np.float32)
            tgts  = sdf.select(target_col).to_numpy().flatten().astype(np.float32)

            for i in range(seq_len, len(sdf) - self.max_h + 1):
                x = feats[i - seq_len : i]
                y = np.array([tgts[i + h - 1] for h in horizons], dtype=np.float32)
                if np.isnan(x).any() or np.isnan(y).any():
                    continue
                self.samples.append((x, y))

    def __len__(self) -> int:
        return len(self.samples)

    def __getitem__(self, idx: int) -> tuple[torch.Tensor, torch.Tensor]:
        x, y = self.samples[idx]
        return torch.from_numpy(x), torch.from_numpy(y)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_df(n_days: int = 60, n_stations: int = 2, n_features: int = 5) -> tuple[pl.DataFrame, list[str], str]:
    """Create a synthetic Polars DataFrame for testing."""
    import datetime

    feature_cols = [f"feat_{i}" for i in range(n_features)]
    target_col   = "pm2_5_ugm3"

    rows = []
    base_date = datetime.date(2023, 1, 1)
    rng = np.random.default_rng(42)

    for s in range(n_stations):
        for d in range(n_days):
            date = base_date + datetime.timedelta(days=d)
            row = {
                "stationID": f"ST{s:02d}",
                "date": date,
                target_col: float(rng.uniform(5, 80)),
            }
            for f in feature_cols:
                row[f] = float(rng.uniform(0, 1))
            rows.append(row)

    schema = {"stationID": pl.Utf8, "date": pl.Date, target_col: pl.Float32}
    for f in feature_cols:
        schema[f] = pl.Float32

    df = pl.DataFrame(rows, schema=schema)
    return df, feature_cols, target_col


def _make_df_with_nulls(n_days: int = 60) -> tuple[pl.DataFrame, list[str], str]:
    """DataFrame where half the rows have NaN features."""
    df, feature_cols, target_col = _make_df(n_days=n_days, n_stations=1)
    null_mask = np.zeros(len(df), dtype=bool)
    null_mask[::2] = True   # every second row is null

    nulled = df.with_columns([
        pl.when(pl.lit(null_mask).cast(pl.Boolean))
          .then(None)
          .otherwise(pl.col(feature_cols[0]))
          .alias(feature_cols[0])
    ])
    return nulled, feature_cols, target_col


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestPM25SequenceDataset:

    def test_correct_number_of_samples(self):
        # Given
        n_days, n_stations, seq_len = 60, 2, 10
        horizons = [1, 3]
        df, feat_cols, tgt = _make_df(n_days=n_days, n_stations=n_stations)

        # When
        ds = PM25SequenceDataset(df, feat_cols, tgt, seq_len=seq_len, horizons=horizons)

        # Then: each station contributes n_days - seq_len - max(horizons) + 1 samples
        max_h = max(horizons)
        expected_per_station = n_days - seq_len - max_h + 1
        assert len(ds) == n_stations * expected_per_station

    def test_input_shape(self):
        n_features = 5
        seq_len    = 10
        df, feat_cols, tgt = _make_df(n_days=50, n_stations=1, n_features=n_features)

        ds = PM25SequenceDataset(df, feat_cols, tgt, seq_len=seq_len, horizons=[1, 3])
        x, y = ds[0]

        assert x.shape == (seq_len, n_features)
        assert y.shape == (2,)  # 2 horizons

    def test_output_dtype_is_float32(self):
        df, feat_cols, tgt = _make_df()
        ds = PM25SequenceDataset(df, feat_cols, tgt, seq_len=10, horizons=[1])

        x, y = ds[0]

        assert x.dtype == torch.float32
        assert y.dtype == torch.float32

    def test_skips_nan_rows(self):
        """Sequences containing NaN features must be dropped."""
        df_nulls, feat_cols, tgt = _make_df_with_nulls(n_days=60)
        df_clean, _, _           = _make_df(n_days=60, n_stations=1)

        ds_nulls = PM25SequenceDataset(df_nulls, feat_cols, tgt, seq_len=10, horizons=[1])
        ds_clean = PM25SequenceDataset(df_clean, feat_cols, tgt, seq_len=10, horizons=[1])

        assert len(ds_nulls) < len(ds_clean), "NaN sequences should be skipped"

    def test_dataloader_integration(self):
        """Dataset must work with a standard DataLoader."""
        df, feat_cols, tgt = _make_df(n_days=80)
        ds = PM25SequenceDataset(df, feat_cols, tgt, seq_len=10, horizons=[1, 3])

        loader = DataLoader(ds, batch_size=16, shuffle=True)
        x_batch, y_batch = next(iter(loader))

        assert x_batch.ndim == 3          # (batch, seq_len, features)
        assert y_batch.ndim == 2          # (batch, horizons)
        assert x_batch.dtype == torch.float32

    def test_empty_dataset_for_too_short_series(self):
        """If series is shorter than seq_len + max_h, dataset should be empty."""
        df, feat_cols, tgt = _make_df(n_days=5, n_stations=1)

        ds = PM25SequenceDataset(df, feat_cols, tgt, seq_len=10, horizons=[1])

        assert len(ds) == 0

    def test_single_horizon(self):
        df, feat_cols, tgt = _make_df(n_days=50, n_stations=1)
        ds = PM25SequenceDataset(df, feat_cols, tgt, seq_len=10, horizons=[1])

        _, y = ds[0]

        assert y.shape == (1,)
