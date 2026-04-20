"""
train_xgb.py
------------
XGBoost Regressor training with early stopping and optional
RandomizedSearchCV tuning.
"""

import numpy as np
import xgboost as xgb
from sklearn.model_selection import RandomizedSearchCV, TimeSeriesSplit

from src.utils.helpers import get_logger, save_model

logger = get_logger(__name__)

XGB_PARAM_GRID = {
    "n_estimators":      [100, 200, 300, 500],
    "learning_rate":     [0.01, 0.05, 0.1, 0.2],
    "max_depth":         [3, 5, 7, 9],
    "min_child_weight":  [1, 3, 5],
    "subsample":         [0.6, 0.8, 1.0],
    "colsample_bytree":  [0.6, 0.8, 1.0],
    "gamma":             [0, 0.1, 0.2, 0.5],
    "reg_alpha":         [0, 0.01, 0.1],
    "reg_lambda":        [1, 1.5, 2],
}

RANDOM_STATE = 42


def train_xgb(
    X_train: np.ndarray,
    y_train: np.ndarray,
    X_val: np.ndarray = None,
    y_val: np.ndarray = None,
    tune: bool = True,
    n_iter: int = 30,
    cv_splits: int = 5,
    save: bool = True,
) -> xgb.XGBRegressor:
    """
    Trains an XGBoost Regressor.

    Parameters
    ----------
    X_train / y_train : training data
    X_val  / y_val    : optional validation set for early stopping
    tune              : run RandomizedSearchCV if True
    n_iter            : hyperparameter combinations to sample
    cv_splits         : time-series CV folds
    save              : persist model to models/xgb.pkl

    Returns
    -------
    Fitted XGBRegressor
    """
    logger.info("Training XGBoost …")

    if tune:
        tscv = TimeSeriesSplit(n_splits=cv_splits)
        base = xgb.XGBRegressor(
            objective="reg:squarederror",
            random_state=RANDOM_STATE,
            tree_method="hist",
            verbosity=0,
        )
        search = RandomizedSearchCV(
            estimator=base,
            param_distributions=XGB_PARAM_GRID,
            n_iter=n_iter,
            scoring="neg_mean_absolute_error",
            cv=tscv,
            verbose=1,
            random_state=RANDOM_STATE,
            n_jobs=-1,
        )
        search.fit(X_train, y_train)
        model = search.best_estimator_

        # Re-fit best params with early stopping if validation data is provided
        if X_val is not None and y_val is not None:
            best_params = search.best_params_.copy()
            best_params["n_estimators"] = 1000  # let early stopping decide
            model = xgb.XGBRegressor(
                objective="reg:squarederror",
                random_state=RANDOM_STATE,
                tree_method="hist",
                verbosity=0,
                **best_params,
            )
            model.fit(
                X_train, y_train,
                eval_set=[(X_val, y_val)],
                verbose=False,
            )
        logger.info(f"XGBoost best params: {search.best_params_}")
    else:
        model = xgb.XGBRegressor(
            objective="reg:squarederror",
            n_estimators=200,
            learning_rate=0.05,
            max_depth=5,
            random_state=RANDOM_STATE,
            tree_method="hist",
            verbosity=0,
        )
        fit_kwargs = {}
        if X_val is not None and y_val is not None:
            fit_kwargs["eval_set"] = [(X_val, y_val)]
            fit_kwargs["verbose"] = False
        model.fit(X_train, y_train, **fit_kwargs)

    logger.info("XGBoost training complete.")

    if save:
        save_model(model, "xgb")

    return model
