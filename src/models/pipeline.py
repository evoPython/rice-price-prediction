"""
pipeline.py
-----------
Unified training pipeline that trains all four models and returns a
{model_name: model_object} dictionary.  Handles the LSTM's special
data requirements (outlier-cleaned + scaled targets) transparently.
"""

import numpy as np
from src.models.train_rf   import train_rf
from src.models.train_xgb  import train_xgb
from src.models.train_hadt import train_hadt
from src.models.train_lstm import train_lstm, predict_lstm
from src.utils.helpers     import get_logger

logger = get_logger(__name__)


def train_all_models(
    preprocessed: dict,
    tune: bool = True,
    n_iter: int = 20,
    lstm_epochs: int = 100,
    lstm_batch: int = 16,
) -> dict:
    """
    Trains RF, XGBoost, HADT, and LSTM on the preprocessed data splits.

    Parameters
    ----------
    preprocessed : output dict from src.data.preprocess.preprocess()
    tune         : whether to run hyperparameter search for tree models
    n_iter       : RandomizedSearchCV iterations
    lstm_epochs  : max LSTM training epochs
    lstm_batch   : LSTM mini-batch size

    Returns
    -------
    dict with keys 'rf', 'xgb', 'hadt', 'lstm'
      Each value is a dict: {'model': <fitted model>, 'predict_fn': <callable>}
    """
    X_train  = preprocessed["X_train"]
    X_test   = preprocessed["X_test"]
    y_train  = preprocessed["y_train"]
    y_test   = preprocessed["y_test"]

    X_lstm_train = preprocessed["X_lstm_train"]
    X_lstm_test  = preprocessed["X_lstm_test"]
    y_lstm_train = preprocessed["y_lstm_train"]   # scaled
    y_lstm_test  = preprocessed["y_lstm_test"]    # scaled
    y_scaler     = preprocessed["y_scaler"]

    models = {}

    # ------------------------------------------------------------------
    # Random Forest
    # ------------------------------------------------------------------
    logger.info("─" * 50)
    rf = train_rf(X_train, y_train, tune=tune, n_iter=n_iter)
    models["rf"] = {
        "model": rf,
        "predict_fn": rf.predict,
        "X_test": X_test,
        "y_test": y_test,
    }

    # ------------------------------------------------------------------
    # XGBoost
    # ------------------------------------------------------------------
    # ── FIX ──────────────────────────────────────────────────────────
    # PREVIOUSLY: X_test / y_test were passed as the XGBoost eval_set,
    #   meaning the test labels influenced n_estimators selection via
    #   early stopping (data leakage). Evaluation metrics were therefore
    #   slightly optimistic.
    #
    # FIX: pass X_val=None so early stopping is disabled and the number
    #   of estimators is taken from RandomizedSearchCV (which already
    #   uses TimeSeriesSplit CV on training data only).
    # ─────────────────────────────────────────────────────────────────
    logger.info("─" * 50)
    xgb = train_xgb(
        X_train, y_train,
        X_val=None, y_val=None,   # no test-set leakage
        tune=tune, n_iter=n_iter,
    )
    models["xgb"] = {
        "model": xgb,
        "predict_fn": xgb.predict,
        "X_test": X_test,
        "y_test": y_test,
    }

    # ------------------------------------------------------------------
    # HADT
    # ------------------------------------------------------------------
    logger.info("─" * 50)
    hadt = train_hadt(X_train, y_train, tune=tune, n_iter=n_iter)
    models["hadt"] = {
        "model": hadt,
        "predict_fn": hadt.predict,
        "X_test": X_test,
        "y_test": y_test,
    }

    # ------------------------------------------------------------------
    # LSTM
    # ------------------------------------------------------------------
    logger.info("─" * 50)
    lstm_model, lstm_history = train_lstm(
        X_train=X_lstm_train,
        y_train=y_lstm_train,
        X_val=None,
        y_val=None,
        epochs=lstm_epochs,
        batch_size=lstm_batch,
    )

    # Wrap prediction to include inverse-scaling
    def lstm_predict(X):
        return predict_lstm(lstm_model, X, y_scaler=y_scaler)

    models["lstm"] = {
        "model": lstm_model,
        "history": lstm_history,
        "predict_fn": lstm_predict,
        "X_test": X_lstm_test,
        "y_test": preprocessed["y_lstm_test_raw"],  # original scale for metrics
    }

    logger.info("─" * 50)
    logger.info("All models trained successfully.")
    return models


def get_all_predictions(models: dict) -> dict:
    """
    Runs each model's predict_fn on its test set.

    Returns
    -------
    dict  {model_name: {'y_pred': np.ndarray, 'y_test': np.ndarray}}
    """
    predictions = {}
    for name, info in models.items():
        logger.info(f"Generating predictions: {name}")
        y_pred = info["predict_fn"](info["X_test"])
        predictions[name] = {
            "y_pred": y_pred,
            "y_test": info["y_test"],
        }
    return predictions
