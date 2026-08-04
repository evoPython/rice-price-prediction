"""
pipeline.py
-----------
Unified training pipeline that trains all four models and returns a
{model_name: model_object} dictionary. Handles both the regular
train/test workflow and the full-history forecast workflow.
"""

from __future__ import annotations

import numpy as np

from src.models.train_rf import train_rf
from src.models.train_xgb import train_xgb
from src.models.train_hadt import train_hadt
from src.models.train_lstm import train_lstm, predict_lstm
from src.utils.helpers import get_logger

logger = get_logger(__name__)


def train_all_models(
    preprocessed: dict,
    tune: bool = True,
    n_iter: int = 20,
    lstm_epochs: int = 100,
    lstm_batch: int = 16,
) -> dict:
    """Train all models on the regular chronological train/test split."""
    X_train = preprocessed["X_train"]
    X_test = preprocessed["X_test"]
    y_train = preprocessed["y_train"]
    y_test = preprocessed["y_test"]

    X_lstm_train = preprocessed["X_lstm_train"]
    X_lstm_test = preprocessed["X_lstm_test"]
    y_lstm_train = preprocessed["y_lstm_train"]
    y_lstm_test = preprocessed["y_lstm_test"]
    y_scaler = preprocessed["y_scaler"]

    models: dict[str, dict] = {}

    logger.info("─" * 50)
    rf = train_rf(X_train, y_train, tune=tune, n_iter=n_iter)
    models["rf"] = {
        "model": rf,
        "predict_fn": rf.predict,
        "X_test": X_test,
        "y_test": y_test,
    }

    logger.info("─" * 50)
    xgb = train_xgb(X_train, y_train, X_val=None, y_val=None, tune=tune, n_iter=n_iter)
    models["xgb"] = {
        "model": xgb,
        "predict_fn": xgb.predict,
        "X_test": X_test,
        "y_test": y_test,
    }

    logger.info("─" * 50)
    hadt = train_hadt(X_train, y_train, tune=tune, n_iter=n_iter)
    models["hadt"] = {
        "model": hadt,
        "predict_fn": hadt.predict,
        "X_test": X_test,
        "y_test": y_test,
    }

    logger.info("─" * 50)
    lstm_model, lstm_history = train_lstm(
        X_train=X_lstm_train,
        y_train=y_lstm_train,
        X_val=None,
        y_val=None,
        epochs=lstm_epochs,
        batch_size=lstm_batch,
    )

    def lstm_predict(X):
        return predict_lstm(lstm_model, X, y_scaler=y_scaler)

    models["lstm"] = {
        "model": lstm_model,
        "history": lstm_history,
        "predict_fn": lstm_predict,
        "X_test": X_lstm_test,
        "y_test": preprocessed["y_lstm_test_raw"],
    }

    logger.info("─" * 50)
    logger.info("All models trained successfully.")
    return models


def train_all_models_full(
    preprocessed: dict,
    tune: bool = True,
    n_iter: int = 20,
    lstm_epochs: int = 100,
    lstm_batch: int = 16,
) -> dict:
    """Train all models on the full historical dataset (no holdout split)."""
    X_full = preprocessed["X_full"]
    y_full = preprocessed["y_full"]
    X_lstm_full = preprocessed["X_lstm_full"]
    y_lstm_full = preprocessed["y_lstm_full"]
    y_scaler = preprocessed["y_scaler"]

    models: dict[str, dict] = {}

    logger.info("─" * 50)
    rf = train_rf(X_full, y_full, tune=tune, n_iter=n_iter, save=False)
    models["rf"] = {
        "model": rf,
        "predict_fn": rf.predict,
        "X_test": X_full,
        "y_test": y_full,
    }

    logger.info("─" * 50)
    xgb = train_xgb(X_full, y_full, X_val=None, y_val=None, tune=tune, n_iter=n_iter, save=False)
    models["xgb"] = {
        "model": xgb,
        "predict_fn": xgb.predict,
        "X_test": X_full,
        "y_test": y_full,
    }

    logger.info("─" * 50)
    hadt = train_hadt(X_full, y_full, tune=tune, n_iter=n_iter, save=False)
    models["hadt"] = {
        "model": hadt,
        "predict_fn": hadt.predict,
        "X_test": X_full,
        "y_test": y_full,
    }

    logger.info("─" * 50)
    lstm_model, lstm_history = train_lstm(
        X_train=X_lstm_full,
        y_train=y_lstm_full,
        X_val=None,
        y_val=None,
        epochs=lstm_epochs,
        batch_size=lstm_batch,
        save=False,
    )

    def lstm_predict(X):
        return predict_lstm(lstm_model, X, y_scaler=y_scaler)

    models["lstm"] = {
        "model": lstm_model,
        "history": lstm_history,
        "predict_fn": lstm_predict,
        "X_test": X_lstm_full,
        "y_test": preprocessed["y_lstm_full_raw"],
    }

    logger.info("─" * 50)
    logger.info("All full-history models trained successfully.")
    return models


def get_all_predictions(models: dict) -> dict:
    """Run each model's predict_fn on its stored evaluation set."""
    predictions: dict[str, dict] = {}
    for name, info in models.items():
        logger.info(f"Generating predictions: {name}")
        y_pred = info["predict_fn"](info["X_test"])
        predictions[name] = {
            "y_pred": np.asarray(y_pred),
            "y_test": np.asarray(info["y_test"]),
        }
    return predictions