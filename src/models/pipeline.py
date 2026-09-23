"""
pipeline.py
-----------
Unified training pipeline that trains all models and returns a
{model_name: model_object} dictionary.

MODELING APPROACH — predict the CHANGE, not the level
------------------------------------------------------
Tree-based models (RF, XGB, HADT) can never predict a value outside the
range of targets seen during training — a leaf's output is always an
average of training-set y-values. Rice prices in this dataset had a
sharp regime shift (the 2023 export-ban-driven price shock), and a
chronological holdout puts that shock almost entirely in the test set.
Modeling the raw price *level* meant every model flat-lined near the
training-set maximum once test-set prices moved past it.

Instead, models here are trained to predict `own_price_delta`
(this month's price minus last month's price, see
`add_own_price_lag_features` in preprocess.py), and predictions are
reconstructed back to price levels as `last_known_price + predicted_delta`.
Deltas are far more range-stable than levels, so this removes the
extrapolation ceiling. All downstream evaluation (metrics, DM/MCS tests,
plots) still operates on the reconstructed price level, in pesos.

A naive persistence baseline (`predicted price = last known price`,
i.e. assume zero change) is included as a permanent benchmark. Monthly
commodity prices are highly autocorrelated, so this trivial forecast is
a genuinely hard baseline to beat — any model that can't beat it isn't
adding value yet, and formalizing it here keeps that comparison honest.

NOTE: LSTM was dropped from this pipeline. With ~90-100 effective
training rows, the stacked LSTM (30k+ params) collapsed to predicting
a near-constant value close to the training-set mean rather than
learning a real input-output mapping (validation loss worsened every
epoch past epoch 1). Tree-based models are a much better fit for a
dataset this small.
"""

from __future__ import annotations

import numpy as np

from src.models.train_rf import train_rf
from src.models.train_xgb import train_xgb
from src.models.train_hadt import train_hadt
from src.utils.helpers import get_logger

logger = get_logger(__name__)

LAG1_COL = "own_price_lag1"


def _make_delta_predict_fn(model, default_lag1: np.ndarray):
    """
    Wraps a model trained on price-delta so it returns reconstructed
    price-level predictions: last_known_price + predicted_delta.

    `default_lag1` is used when the caller doesn't supply an override
    (the normal case: evaluating on a fixed test set). The forecast
    pipeline (genuinely unseen future months) passes `lag1=` explicitly
    since each future row has its own "last known price".
    """
    def _predict(X, lag1: np.ndarray | None = None):
        l1 = default_lag1 if lag1 is None else lag1
        delta_pred = model.predict(X)
        return np.asarray(l1) + delta_pred
    return _predict


def _make_naive_predict_fn(default_lag1: np.ndarray):
    """Persistence baseline: predicted price = last known price."""
    def _predict(X, lag1: np.ndarray | None = None):
        l1 = default_lag1 if lag1 is None else lag1
        return np.asarray(l1).copy()
    return _predict


def train_all_models(
    preprocessed: dict,
    tune: bool = True,
    n_iter: int = 20,
    save: bool = True,
    **_ignored,
) -> dict:
    """Train all models on the regular chronological train/test split."""
    X_train = preprocessed["X_train"]
    X_test = preprocessed["X_test"]
    y_train = preprocessed["y_train"]
    y_test = preprocessed["y_test"]

    lag1_train = preprocessed["X_train_raw"][LAG1_COL].values
    lag1_test = preprocessed["X_test_raw"][LAG1_COL].values
    y_train_delta = y_train - lag1_train

    models: dict[str, dict] = {}

    logger.info("─" * 50)
    logger.info("Training on price DELTA (this month − last month), "
                 "reconstructing levels via last_known_price + predicted_delta")
    logger.info("─" * 50)

    rf = train_rf(X_train, y_train_delta, tune=tune, n_iter=n_iter, save=save)
    models["rf"] = {
        "model": rf,
        "predict_fn": _make_delta_predict_fn(rf, lag1_test),
        "X_test": X_test,
        "y_test": y_test,
    }

    logger.info("─" * 50)
    xgb = train_xgb(X_train, y_train_delta, X_val=None, y_val=None, tune=tune, n_iter=n_iter, save=save)
    models["xgb"] = {
        "model": xgb,
        "predict_fn": _make_delta_predict_fn(xgb, lag1_test),
        "X_test": X_test,
        "y_test": y_test,
    }

    logger.info("─" * 50)
    hadt = train_hadt(X_train, y_train_delta, tune=tune, n_iter=n_iter, save=save)
    models["hadt"] = {
        "model": hadt,
        "predict_fn": _make_delta_predict_fn(hadt, lag1_test),
        "X_test": X_test,
        "y_test": y_test,
    }

    logger.info("─" * 50)
    models["naive"] = {
        "model": None,
        "predict_fn": _make_naive_predict_fn(lag1_test),
        "X_test": X_test,
        "y_test": y_test,
    }
    logger.info("Added naive persistence baseline (predicted price = last known price)")

    logger.info("─" * 50)
    logger.info("All models trained successfully.")
    return models


def train_all_models_full(
    preprocessed: dict,
    tune: bool = True,
    n_iter: int = 20,
    **_ignored,
) -> dict:
    """
    Train all models on the full historical dataset (no holdout split).
    Used to produce the final deployed model for genuine future forecasting.
    predict_fn requires an explicit `lag1=` array for any input other than
    the training data itself, since there is no fixed test set here.
    """
    X_full = preprocessed["X_full"]
    y_full = preprocessed["y_full"]

    lag1_full = preprocessed["X_full_raw"][LAG1_COL].values
    y_full_delta = y_full - lag1_full

    models: dict[str, dict] = {}

    logger.info("─" * 50)
    logger.info("Training on price DELTA (full-history / deployment mode)")
    logger.info("─" * 50)

    rf = train_rf(X_full, y_full_delta, tune=tune, n_iter=n_iter, save=False)
    models["rf"] = {
        "model": rf,
        "predict_fn": _make_delta_predict_fn(rf, lag1_full),
        "X_test": X_full,
        "y_test": y_full,
    }

    logger.info("─" * 50)
    xgb = train_xgb(X_full, y_full_delta, X_val=None, y_val=None, tune=tune, n_iter=n_iter, save=False)
    models["xgb"] = {
        "model": xgb,
        "predict_fn": _make_delta_predict_fn(xgb, lag1_full),
        "X_test": X_full,
        "y_test": y_full,
    }

    logger.info("─" * 50)
    hadt = train_hadt(X_full, y_full_delta, tune=tune, n_iter=n_iter, save=False)
    models["hadt"] = {
        "model": hadt,
        "predict_fn": _make_delta_predict_fn(hadt, lag1_full),
        "X_test": X_full,
        "y_test": y_full,
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
