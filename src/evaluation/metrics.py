"""
metrics.py
----------
Evaluation metrics as defined in the manuscript:
  • MAE  – Mean Absolute Error
  • RMSE – Root Mean Square Error
  • MAPE – Mean Absolute Percentage Error
  • ME   – Mean Error (bias indicator)

All functions accept 1-D numpy arrays and return scalar floats.
"""

import numpy as np


def mae(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    """
    Mean Absolute Error.

    MAE = (1/N) Σ |y_i − ŷ_i|

    Measures average error magnitude without direction.
    Treats all errors equally regardless of size.
    """
    return float(np.mean(np.abs(y_true - y_pred)))


def rmse(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    """
    Root Mean Square Error.

    RMSE = sqrt( (1/N) Σ (y_i − ŷ_i)² )

    Penalises larger errors more heavily than MAE.
    Critical for detecting price volatility.
    """
    return float(np.sqrt(np.mean((y_true - y_pred) ** 2)))


def mape(y_true: np.ndarray, y_pred: np.ndarray, eps: float = 1e-8) -> float:
    """
    Mean Absolute Percentage Error.

    MAPE = (1/N) Σ |y_i − ŷ_i| / |y_i|  × 100%

    Expresses accuracy as a percentage, making it scale-independent
    and useful for comparing across different rice classifications.

    Parameters
    ----------
    eps : small constant to guard against zero-division
    """
    return float(np.mean(np.abs((y_true - y_pred) / (np.abs(y_true) + eps))) * 100)


def me(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    """
    Mean Error (bias).

    ME = (1/N) Σ (y_i − ŷ_i)

    Positive ME → model under-predicts (actual > predicted).
    Negative ME → model over-predicts (actual < predicted).
    """
    return float(np.mean(y_true - y_pred))


def evaluate_all(y_true: np.ndarray, y_pred: np.ndarray) -> dict:
    """
    Computes all four metrics in one call.

    Returns
    -------
    dict with keys: MAE, RMSE, MAPE, ME
    """
    y_true = np.asarray(y_true, dtype=float)
    y_pred = np.asarray(y_pred, dtype=float)

    return {
        "MAE":  mae(y_true, y_pred),
        "RMSE": rmse(y_true, y_pred),
        "MAPE": mape(y_true, y_pred),
        "ME":   me(y_true, y_pred),
    }
