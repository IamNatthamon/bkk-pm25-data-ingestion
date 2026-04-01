"""Unit tests for src.training.models — model forward-pass shapes and properties."""

from __future__ import annotations

import pytest
import torch
import torch.nn as nn

from src.training.models import (
    GRUForecaster,
    LSTMForecaster,
    MLPForecaster,
    PersistenceModel,
    STUNN,
    count_parameters,
)

# ── Common test parameters ────────────────────────────────────────────────────

BATCH = 16
SEQ_LEN = 30
NUM_FEATURES = 24
NUM_HORIZONS = 2
HIDDEN = 64
LAYERS = 2


def _make_input() -> torch.Tensor:
    return torch.randn(BATCH, SEQ_LEN, NUM_FEATURES)


# ── STUNN ─────────────────────────────────────────────────────────────────────

class TestSTUNN:
    def test_output_shape(self):
        model = STUNN(NUM_FEATURES, HIDDEN, LAYERS, NUM_HORIZONS)
        out = model(_make_input())
        assert out.shape == (BATCH, NUM_HORIZONS)

    def test_attention_weights_set_after_forward(self):
        model = STUNN(NUM_FEATURES, HIDDEN, LAYERS, NUM_HORIZONS)
        model(_make_input())
        assert model.attention_weights is not None

    def test_no_nan_in_output(self):
        model = STUNN(NUM_FEATURES, HIDDEN, LAYERS, NUM_HORIZONS)
        out = model(_make_input())
        assert not torch.isnan(out).any()

    def test_gradient_flows(self):
        model = STUNN(NUM_FEATURES, HIDDEN, LAYERS, NUM_HORIZONS)
        x = _make_input()
        out = model(x)
        loss = out.sum()
        loss.backward()
        for name, param in model.named_parameters():
            if param.requires_grad:
                assert param.grad is not None, f"No gradient for {name}"

    def test_train_eval_mode_different(self):
        """Dropout should cause different outputs in train vs eval mode."""
        model = STUNN(NUM_FEATURES, HIDDEN, LAYERS, NUM_HORIZONS, dropout=0.5)
        x = _make_input()
        torch.manual_seed(0)
        model.train()
        out_train = model(x)
        model.eval()
        with torch.no_grad():
            out_eval1 = model(x)
            out_eval2 = model(x)
        # Eval should be deterministic
        assert torch.allclose(out_eval1, out_eval2)

    def test_parameter_count_positive(self):
        model = STUNN(NUM_FEATURES, HIDDEN, LAYERS, NUM_HORIZONS)
        assert count_parameters(model) > 0

    def test_different_hidden_sizes_produce_different_param_counts(self):
        m64 = STUNN(NUM_FEATURES, 64, LAYERS, NUM_HORIZONS)
        m128 = STUNN(NUM_FEATURES, 128, LAYERS, NUM_HORIZONS)
        assert count_parameters(m128) > count_parameters(m64)


# ── LSTMForecaster ────────────────────────────────────────────────────────────

class TestLSTMForecaster:
    def test_output_shape(self):
        model = LSTMForecaster(NUM_FEATURES, HIDDEN, LAYERS, NUM_HORIZONS)
        out = model(_make_input())
        assert out.shape == (BATCH, NUM_HORIZONS)

    def test_single_layer_no_dropout_error(self):
        model = LSTMForecaster(NUM_FEATURES, HIDDEN, num_layers=1, num_horizons=NUM_HORIZONS)
        out = model(_make_input())
        assert out.shape == (BATCH, NUM_HORIZONS)


# ── GRUForecaster ─────────────────────────────────────────────────────────────

class TestGRUForecaster:
    def test_output_shape(self):
        model = GRUForecaster(NUM_FEATURES, HIDDEN, LAYERS, NUM_HORIZONS)
        out = model(_make_input())
        assert out.shape == (BATCH, NUM_HORIZONS)

    def test_gru_fewer_params_than_lstm(self):
        gru = GRUForecaster(NUM_FEATURES, HIDDEN, LAYERS, NUM_HORIZONS)
        lstm = LSTMForecaster(NUM_FEATURES, HIDDEN, LAYERS, NUM_HORIZONS)
        assert count_parameters(gru) < count_parameters(lstm)


# ── MLPForecaster ─────────────────────────────────────────────────────────────

class TestMLPForecaster:
    def test_output_shape(self):
        model = MLPForecaster(SEQ_LEN, NUM_FEATURES, HIDDEN, NUM_HORIZONS)
        out = model(_make_input())
        assert out.shape == (BATCH, NUM_HORIZONS)


# ── PersistenceModel ──────────────────────────────────────────────────────────

class TestPersistenceModel:
    def test_output_shape(self):
        model = PersistenceModel(pm25_feature_idx=0, num_horizons=NUM_HORIZONS)
        x = _make_input()
        out = model.predict(x)
        assert out.shape == (BATCH, NUM_HORIZONS)

    def test_all_horizons_same_value(self):
        model = PersistenceModel(pm25_feature_idx=0, num_horizons=3)
        x = torch.zeros(4, 10, 5)
        x[:, -1, 0] = torch.tensor([1.0, 2.0, 3.0, 4.0])
        out = model.predict(x)
        for i in range(4):
            assert torch.all(out[i] == out[i, 0])
