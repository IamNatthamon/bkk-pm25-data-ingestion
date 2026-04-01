"""Model architectures for PM2.5 forecasting.

All nn.Module classes follow (batch, seq_len, features) → (batch, num_horizons).
"""

from __future__ import annotations

import torch
import torch.nn as nn


class PersistenceModel:
    """Naive baseline: predict PM2.5(t+h) = PM2.5(t) for all horizons.

    Not a nn.Module — evaluated directly on DataLoader batches.
    """

    def __init__(self, pm25_feature_idx: int, num_horizons: int) -> None:
        self.pm25_feature_idx = pm25_feature_idx
        self.num_horizons = num_horizons

    def predict(self, x: torch.Tensor) -> torch.Tensor:
        """Return last observed PM2.5 repeated across all forecast horizons."""
        last_pm25 = x[:, -1, self.pm25_feature_idx]
        return last_pm25.unsqueeze(1).expand(-1, self.num_horizons)


class MLPForecaster(nn.Module):
    """Feed-forward baseline: flatten sequence → hidden layers → forecast.

    Simple but computationally cheap; useful as a strong non-temporal baseline.
    """

    def __init__(
        self,
        seq_len: int,
        num_features: int,
        hidden_size: int,
        num_horizons: int,
        dropout: float = 0.2,
    ) -> None:
        super().__init__()
        input_dim = seq_len * num_features
        self.net = nn.Sequential(
            nn.Flatten(),
            nn.Linear(input_dim, hidden_size),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(hidden_size, hidden_size // 2),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(hidden_size // 2, num_horizons),
        )

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return self.net(x)


class LSTMForecaster(nn.Module):
    """LSTM-based sequence forecaster with a FC regression head."""

    def __init__(
        self,
        num_features: int,
        hidden_size: int,
        num_layers: int,
        num_horizons: int,
        dropout: float = 0.2,
    ) -> None:
        super().__init__()
        self.lstm = nn.LSTM(
            input_size=num_features,
            hidden_size=hidden_size,
            num_layers=num_layers,
            batch_first=True,
            dropout=dropout if num_layers > 1 else 0.0,
        )
        self.head = nn.Sequential(
            nn.Linear(hidden_size, hidden_size // 2),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(hidden_size // 2, num_horizons),
        )

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        out, _ = self.lstm(x)
        return self.head(out[:, -1, :])


class GRUForecaster(nn.Module):
    """GRU-based sequence forecaster — lighter and faster alternative to LSTM."""

    def __init__(
        self,
        num_features: int,
        hidden_size: int,
        num_layers: int,
        num_horizons: int,
        dropout: float = 0.2,
    ) -> None:
        super().__init__()
        self.gru = nn.GRU(
            input_size=num_features,
            hidden_size=hidden_size,
            num_layers=num_layers,
            batch_first=True,
            dropout=dropout if num_layers > 1 else 0.0,
        )
        self.head = nn.Sequential(
            nn.Linear(hidden_size, hidden_size // 2),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(hidden_size // 2, num_horizons),
        )

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        out, _ = self.gru(x)
        return self.head(out[:, -1, :])


class SpatialAttention(nn.Module):
    """Multi-head self-attention over the temporal sequence.

    Dynamically weights features at each timestep — e.g. upwind hotspots
    vs local weather for PM2.5 transport.
    """

    def __init__(
        self,
        hidden_size: int,
        num_heads: int = 4,
        dropout: float = 0.1,
    ) -> None:
        super().__init__()
        self.attn = nn.MultiheadAttention(
            embed_dim=hidden_size,
            num_heads=num_heads,
            dropout=dropout,
            batch_first=True,
        )
        self.norm = nn.LayerNorm(hidden_size)

    def forward(self, x: torch.Tensor) -> tuple[torch.Tensor, torch.Tensor]:
        """Return (attended_output, attention_weights)."""
        attn_out, attn_weights = self.attn(x, x, x)
        return self.norm(x + attn_out), attn_weights


class GatedFusion(nn.Module):
    """Gated fusion of temporal encoding and attention-weighted representation.

    gate = sigmoid(W_t * temporal + W_a * attended + b)
    output = gate * temporal + (1 - gate) * attended
    """

    def __init__(self, hidden_size: int) -> None:
        super().__init__()
        self.gate = nn.Sequential(
            nn.Linear(hidden_size * 2, hidden_size),
            nn.Sigmoid(),
        )

    def forward(self, temporal: torch.Tensor, attended: torch.Tensor) -> torch.Tensor:
        combined = torch.cat([temporal, attended], dim=-1)
        g = self.gate(combined)
        return g * temporal + (1 - g) * attended


class STUNN(nn.Module):
    """Spatio-Temporal Unified Neural Network for PM2.5 forecasting.

    Architecture:
        Input (batch, seq_len, features)
          → Linear projection (features → hidden_size)
          → Multi-layer GRU temporal encoder
          → Multi-head spatial/feature attention
          → Gated fusion of temporal ⊕ attended representations
          → Regression head → (batch, num_horizons)

    The spatial attention module allows the model to learn which features
    (e.g. upwind hotspots, boundary layer height) matter most at each step.
    Gradient checkpointing is supported via use_checkpointing=True to trade
    compute for reduced VRAM on long sequences.
    """

    def __init__(
        self,
        num_features: int,
        hidden_size: int,
        num_layers: int,
        num_horizons: int,
        attention_heads: int = 4,
        dropout: float = 0.2,
        use_checkpointing: bool = False,
    ) -> None:
        super().__init__()
        self.use_checkpointing = use_checkpointing

        self.input_proj = nn.Linear(num_features, hidden_size)

        self.temporal_encoder = nn.GRU(
            input_size=hidden_size,
            hidden_size=hidden_size,
            num_layers=num_layers,
            batch_first=True,
            dropout=dropout if num_layers > 1 else 0.0,
        )

        self.spatial_attention = SpatialAttention(hidden_size, attention_heads, dropout)
        self.fusion = GatedFusion(hidden_size)

        self.regression_head = nn.Sequential(
            nn.Linear(hidden_size, hidden_size // 2),
            nn.GELU(),
            nn.Dropout(dropout),
            nn.Linear(hidden_size // 2, hidden_size // 4),
            nn.GELU(),
            nn.Linear(hidden_size // 4, num_horizons),
        )

        self._last_attn_weights: torch.Tensor | None = None

    def _temporal_encode(self, x: torch.Tensor) -> torch.Tensor:
        out, _ = self.temporal_encoder(x)
        return out

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        proj = self.input_proj(x)

        if self.use_checkpointing and self.training:
            from torch.utils.checkpoint import checkpoint

            temporal_out = checkpoint(self._temporal_encode, proj, use_reentrant=False)
        else:
            temporal_out = self._temporal_encode(proj)

        attended, attn_w = self.spatial_attention(temporal_out)
        self._last_attn_weights = attn_w.detach()

        temporal_last = temporal_out[:, -1, :]
        attended_last = attended[:, -1, :]
        fused = self.fusion(temporal_last, attended_last)

        return self.regression_head(fused)

    @property
    def attention_weights(self) -> torch.Tensor | None:
        """Last forward-pass attention weights for explainability."""
        return self._last_attn_weights

    @classmethod
    def from_config(cls, config: dict, **kwargs: object) -> "STUNN":
        """Reconstruct STUNN from a config dict (as stored in deployment bundle)."""
        return cls(
            num_features=config["num_features"],
            hidden_size=config["hidden_size"],
            num_layers=config["num_layers"],
            num_horizons=config["num_horizons"],
            attention_heads=config.get("attention_heads", 4),
            dropout=config.get("dropout", 0.2),
            **kwargs,
        )


def count_parameters(model: nn.Module) -> int:
    """Count trainable parameters."""
    return sum(p.numel() for p in model.parameters() if p.requires_grad)


def build_model_catalog(
    num_features: int,
    seq_len: int,
    num_horizons: int,
    hidden_size: int = 128,
    num_layers: int = 2,
    dropout: float = 0.2,
    attention_heads: int = 4,
    mlp_hidden_multiplier: int = 2,
) -> dict[str, nn.Module]:
    """Instantiate all model architectures.

    MLP first-layer width = hidden_size * mlp_hidden_multiplier (default → 256).
    This matches the training convention used in model_training.ipynb.
    """
    return {
        "MLP": MLPForecaster(
            seq_len=seq_len,
            num_features=num_features,
            hidden_size=hidden_size * mlp_hidden_multiplier,
            num_horizons=num_horizons,
            dropout=dropout,
        ),
        "LSTM": LSTMForecaster(
            num_features=num_features,
            hidden_size=hidden_size,
            num_layers=num_layers,
            num_horizons=num_horizons,
            dropout=dropout,
        ),
        "GRU": GRUForecaster(
            num_features=num_features,
            hidden_size=hidden_size,
            num_layers=num_layers,
            num_horizons=num_horizons,
            dropout=dropout,
        ),
        "ST-UNN": STUNN(
            num_features=num_features,
            hidden_size=hidden_size,
            num_layers=num_layers,
            num_horizons=num_horizons,
            attention_heads=attention_heads,
            dropout=dropout,
        ),
    }
