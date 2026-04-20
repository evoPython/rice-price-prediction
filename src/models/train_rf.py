"""
train_rf.py
-----------
Random Forest Regressor training with hyperparameter tuning via
RandomizedSearchCV.  Returns a fitted model.
"""

import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import RandomizedSearchCV, TimeSeriesSplit

from src.utils.helpers import get_logger, save_model

logger = get_logger(__name__)

# Default hyperparameter grid
RF_PARAM_GRID = {
    "n_estimators":      [100, 200, 300, 500],
    "max_depth":         [None, 10, 20, 30],
    "min_samples_split": [2, 5, 10],
    "min_samples_leaf":  [1, 2, 4],
    "max_features":      ["sqrt", "log2", 0.5],
    "bootstrap":         [True, False],
}

RANDOM_STATE = 42


def train_rf(
    X_train: np.ndarray,
    y_train: np.ndarray,
    tune: bool = True,
    n_iter: int = 30,
    cv_splits: int = 5,
    save: bool = True,
) -> RandomForestRegressor:
    """
    Trains a Random Forest Regressor.

    Parameters
    ----------
    X_train   : scaled feature array
    y_train   : target array
    tune      : if True, runs RandomizedSearchCV; otherwise uses sensible defaults
    n_iter    : number of hyperparameter combinations to try
    cv_splits : number of time-series cross-validation folds
    save      : persist the fitted model to models/rf.pkl

    Returns
    -------
    Fitted RandomForestRegressor
    """
    logger.info("Training Random Forest …")

    if tune:
        tscv = TimeSeriesSplit(n_splits=cv_splits)
        base = RandomForestRegressor(random_state=RANDOM_STATE, n_jobs=-1)
        search = RandomizedSearchCV(
            estimator=base,
            param_distributions=RF_PARAM_GRID,
            n_iter=n_iter,
            scoring="neg_mean_absolute_error",
            cv=tscv,
            verbose=1,
            random_state=RANDOM_STATE,
            n_jobs=-1,
        )
        search.fit(X_train, y_train)
        model = search.best_estimator_
        logger.info(f"RF best params: {search.best_params_}")
    else:
        model = RandomForestRegressor(
            n_estimators=200,
            max_depth=20,
            min_samples_split=5,
            random_state=RANDOM_STATE,
            n_jobs=-1,
        )
        model.fit(X_train, y_train)

    logger.info("Random Forest training complete.")

    if save:
        save_model(model, "rf")

    return model
