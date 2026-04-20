"""
statistical_tests.py
--------------------
Statistical model comparison tools:
  1. Diebold-Mariano (DM) test  – pairwise significance test between model errors
  2. Model Confidence Set (MCS) – iteratively eliminates inferior models until
     only the set of statistically equivalent best models remains

References
----------
Diebold, F.X. & Mariano, R.S. (1995). Comparing Predictive Accuracy.
  Journal of Business & Economic Statistics, 13(3), 253–263.

Hansen, P.R., Lunde, A., & Nason, J.M. (2011). The Model Confidence Set.
  Econometrica, 79(2), 453–497.

Paul, R.K., et al. (2022). Machine learning techniques for forecasting
  agricultural prices. PLOS ONE.
"""

import numpy as np
import pandas as pd
from itertools import combinations
from scipy import stats

from src.utils.helpers import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Diebold-Mariano Test
# ---------------------------------------------------------------------------

def _dm_loss_diff(e1: np.ndarray, e2: np.ndarray, loss: str = "mse") -> np.ndarray:
    """Computes the loss differential d_t = L(e1_t) − L(e2_t)."""
    if loss == "mse":
        return e1 ** 2 - e2 ** 2
    elif loss == "mae":
        return np.abs(e1) - np.abs(e2)
    else:
        raise ValueError(f"Unknown loss type: '{loss}'. Choose 'mse' or 'mae'.")


def _newey_west_variance(d: np.ndarray, max_lag: int = None) -> float:
    """
    Newey-West HAC (heteroskedasticity and autocorrelation consistent)
    variance estimator for the loss differential series d.

    max_lag defaults to int(T^(1/3)) following the standard rule-of-thumb.
    """
    T = len(d)
    max_lag = max_lag or int(T ** (1 / 3))
    d_centered = d - d.mean()

    # Variance (lag 0)
    gamma_0 = np.dot(d_centered, d_centered) / T
    nw_var = gamma_0

    # Add weighted autocovariance terms
    for k in range(1, max_lag + 1):
        weight = 1 - k / (max_lag + 1)   # Bartlett kernel
        gamma_k = np.dot(d_centered[k:], d_centered[:-k]) / T
        nw_var += 2 * weight * gamma_k

    return max(nw_var, 1e-12)   # guard against numerical zero


def diebold_mariano(
    e1: np.ndarray,
    e2: np.ndarray,
    loss: str = "mse",
    alternative: str = "two-sided",
) -> dict:
    """
    Diebold-Mariano test for equal predictive accuracy between two models.

    H₀: E[L(e1)] = E[L(e2)]  (both models equally accurate)
    H₁: E[L(e1)] ≠ E[L(e2)] (one model is more accurate)

    Parameters
    ----------
    e1          : forecast errors of model 1  (y_true − y_pred_1)
    e2          : forecast errors of model 2  (y_true − y_pred_2)
    loss        : 'mse' or 'mae'
    alternative : 'two-sided', 'less' (e1 better), or 'greater' (e2 better)

    Returns
    -------
    dict with keys: dm_stat, p_value, significant_at_5pct,
                    mean_loss_e1, mean_loss_e2, winner
    """
    d = _dm_loss_diff(e1, e2, loss)
    T = len(d)
    d_bar = d.mean()
    nw_var = _newey_west_variance(d)
    dm_stat = d_bar / np.sqrt(nw_var / T)

    # Harvey, Leybourne & Newbold (1997) small-sample correction
    hln_factor = np.sqrt((T + 1) / T)
    dm_stat_adj = dm_stat * hln_factor

    # p-value from t-distribution with T-1 degrees of freedom
    if alternative == "two-sided":
        p_value = 2 * stats.t.sf(np.abs(dm_stat_adj), df=T - 1)
    elif alternative == "less":
        p_value = stats.t.cdf(dm_stat_adj, df=T - 1)
    elif alternative == "greater":
        p_value = stats.t.sf(dm_stat_adj, df=T - 1)
    else:
        raise ValueError(f"Unknown alternative: '{alternative}'")

    # Positive d_bar → model 1 has higher loss → model 2 is better
    winner = "model_2" if d_bar > 0 else "model_1"

    return {
        "dm_stat":           float(dm_stat_adj),
        "p_value":           float(p_value),
        "significant_at_5pct": bool(p_value < 0.05),
        "mean_loss_e1":      float(np.mean(e1 ** 2 if loss == "mse" else np.abs(e1))),
        "mean_loss_e2":      float(np.mean(e2 ** 2 if loss == "mse" else np.abs(e2))),
        "winner":            winner,
    }


def run_pairwise_dm(
    errors: dict,
    loss: str = "mse",
) -> pd.DataFrame:
    """
    Runs all pairwise DM tests across a set of models.

    Parameters
    ----------
    errors : {model_name: error_array}  where error = y_true − y_pred
    loss   : 'mse' or 'mae'

    Returns
    -------
    DataFrame with one row per model pair
    """
    model_names = list(errors.keys())
    rows = []

    for m1, m2 in combinations(model_names, 2):
        result = diebold_mariano(errors[m1], errors[m2], loss=loss)
        rows.append({
            "model_1":              m1,
            "model_2":              m2,
            "DM_stat":              result["dm_stat"],
            "p_value":              result["p_value"],
            "significant_at_5pct":  result["significant_at_5pct"],
            "winner":               m1 if result["winner"] == "model_1" else m2,
        })

    df = pd.DataFrame(rows)
    logger.info(f"Pairwise DM tests completed ({len(rows)} pairs).")
    return df


# ---------------------------------------------------------------------------
# Model Confidence Set (MCS)
# ---------------------------------------------------------------------------

def _mcs_statistic(loss_matrix: np.ndarray) -> tuple[float, int]:
    """
    Computes the Range MCS statistic T_R.

    loss_matrix shape: (T, M) — T periods, M models.
    Returns (T_R, worst_model_index).
    """
    T, M = loss_matrix.shape
    d_bar_i = np.zeros(M)   # mean relative loss for each model

    for i in range(M):
        d_i = loss_matrix[:, i] - loss_matrix.mean(axis=1)
        d_bar_i[i] = d_i.mean()

    # Variance using Newey-West
    vars_i = np.zeros(M)
    for i in range(M):
        d_i = loss_matrix[:, i] - loss_matrix.mean(axis=1)
        vars_i[i] = _newey_west_variance(d_i)

    # T_R = max over i of (d_bar_i / sqrt(var_i / T))
    t_stats = np.abs(d_bar_i) / np.sqrt(np.maximum(vars_i, 1e-12) / T)
    worst_idx = int(np.argmax(d_bar_i))   # highest mean loss = worst model
    T_R = t_stats[worst_idx]

    return T_R, worst_idx


def model_confidence_set(
    predictions: dict,
    y_true: np.ndarray,
    alpha: float = 0.10,
    loss: str = "mse",
    bootstrap_reps: int = 1000,
    random_state: int = 42,
) -> dict:
    """
    Model Confidence Set (MCS) procedure (Hansen et al., 2011).

    Iteratively eliminates the worst-performing model using a bootstrap-based
    significance test until the surviving set is statistically homogeneous.

    Parameters
    ----------
    predictions   : {model_name: y_pred_array}
    y_true        : true target values
    alpha         : significance level (default 10% as common in MCS literature)
    loss          : 'mse' or 'mae'
    bootstrap_reps: number of bootstrap replications for p-value estimation
    random_state  : RNG seed

    Returns
    -------
    dict with keys:
      mcs_set         : list of model names in the MCS
      eliminated      : list of eliminated model names (in elimination order)
      p_values        : {model_name: bootstrap p-value at elimination step}
    """
    rng = np.random.default_rng(random_state)
    model_names = list(predictions.keys())
    T = len(y_true)

    # Build loss matrix (T × M)
    def _loss(e):
        return e ** 2 if loss == "mse" else np.abs(e)

    loss_matrix = np.column_stack([
        _loss(y_true - predictions[m]) for m in model_names
    ])

    surviving = list(range(len(model_names)))
    eliminated = []
    p_values = {}

    logger.info(f"Running MCS with α={alpha}, {bootstrap_reps} bootstrap reps …")

    while len(surviving) > 1:
        sub_loss = loss_matrix[:, surviving]
        T_R_obs, local_worst = _mcs_statistic(sub_loss)

        # Bootstrap to estimate p-value
        T_R_boot = np.zeros(bootstrap_reps)
        for b in range(bootstrap_reps):
            boot_idx = rng.integers(0, T, size=T)
            boot_loss = sub_loss[boot_idx]
            T_R_boot[b], _ = _mcs_statistic(boot_loss)

        p_val = float(np.mean(T_R_boot > T_R_obs))
        worst_global = surviving[local_worst]
        worst_name = model_names[worst_global]
        p_values[worst_name] = p_val

        if p_val < alpha:
            # Reject H₀ → eliminate worst model
            logger.info(f"  Eliminated: {worst_name}  (p={p_val:.4f} < α={alpha})")
            eliminated.append(worst_name)
            surviving.pop(local_worst)
        else:
            # Fail to reject → all remaining models form the MCS
            logger.info(f"  Stopping: p={p_val:.4f} ≥ α={alpha}")
            break

    mcs = [model_names[i] for i in surviving]
    logger.info(f"MCS result: {mcs}")

    return {
        "mcs_set":   mcs,
        "eliminated": eliminated,
        "p_values":  p_values,
    }
