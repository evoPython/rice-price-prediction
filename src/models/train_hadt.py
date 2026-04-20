"""
train_hadt.py
-------------
Hybrid Association Decision Tree (HADT) model.

Architecture
------------
HADT is a two-stage hybrid that fuses *association-rule-based feature
weighting* with an *AdaBoost-boosted Decision Tree* regressor:

  Stage 1 – Association Feature Weighting
    Mutual information scores (a continuous analogue of association strength)
    are computed between each feature and the target.  Features are then
    weighted by their normalised MI scores before training, encouraging the
    tree learner to focus on the most informative inputs.

  Stage 2 – Adaptive Boosted Decision Trees
    An AdaBoostRegressor with DecisionTreeRegressor base estimators is trained
    on the MI-weighted feature matrix.  AdaBoost's sequential correction of
    residuals mirrors the "adaptive" component described in HADT literature
    (Paul et al., 2022).

This implementation is faithful to the hybrid spirit of HADT while relying
entirely on well-tested scikit-learn primitives for reproducibility and ease
of audit.

References
----------
Paul, R. K., et al. (2022).  "Machine learning techniques for forecasting
agricultural prices." *PLOS ONE*.
"""

import numpy as np
import pandas as pd
from sklearn.ensemble import AdaBoostRegressor
from sklearn.tree import DecisionTreeRegressor
from sklearn.feature_selection import mutual_info_regression
from sklearn.model_selection import RandomizedSearchCV, TimeSeriesSplit
from sklearn.base import BaseEstimator, RegressorMixin
from sklearn.utils.validation import check_is_fitted

from src.utils.helpers import get_logger, save_model

logger = get_logger(__name__)

RANDOM_STATE = 42


# ---------------------------------------------------------------------------
# HADT Estimator
# ---------------------------------------------------------------------------

class HADTRegressor(BaseEstimator, RegressorMixin):
    """
    Hybrid Association Decision Tree regressor.

    Parameters
    ----------
    n_estimators   : number of AdaBoost trees
    max_depth      : max depth of each base Decision Tree
    learning_rate  : AdaBoost learning rate
    loss           : AdaBoost loss function ('linear', 'square', 'exponential')
    random_state   : random seed
    """

    def __init__(
        self,
        n_estimators: int = 100,
        max_depth: int = 4,
        learning_rate: float = 0.1,
        loss: str = "linear",
        random_state: int = RANDOM_STATE,
    ):
        self.n_estimators = n_estimators
        self.max_depth = max_depth
        self.learning_rate = learning_rate
        self.loss = loss
        self.random_state = random_state

    # ------------------------------------------------------------------
    # Stage 1 – Association Feature Weighting
    # ------------------------------------------------------------------

    def _compute_feature_weights(self, X: np.ndarray, y: np.ndarray) -> np.ndarray:
        """
        Computes normalised mutual-information weights for each feature.
        Returns a weight vector that sums to the number of features
        (so the overall scale is preserved after weighting).
        """
        mi_scores = mutual_info_regression(
            X, y, random_state=self.random_state
        )
        # Avoid division by zero
        total = mi_scores.sum()
        if total == 0:
            weights = np.ones(X.shape[1])
        else:
            # Scale weights so that their mean is 1
            weights = mi_scores / (total / X.shape[1])
        return weights

    def _apply_weights(self, X: np.ndarray) -> np.ndarray:
        """Element-wise multiplication of feature matrix by weight vector."""
        return X * self.feature_weights_

    # ------------------------------------------------------------------
    # Stage 2 – AdaBoosted Decision Tree
    # ------------------------------------------------------------------

    def fit(self, X, y):
        X = np.array(X, dtype=np.float64)
        y = np.array(y, dtype=np.float64)

        # Stage 1
        self.feature_weights_ = self._compute_feature_weights(X, y)
        X_weighted = self._apply_weights(X)

        # Stage 2
        base_tree = DecisionTreeRegressor(
            max_depth=self.max_depth,
            random_state=self.random_state,
        )
        self.model_ = AdaBoostRegressor(
            estimator=base_tree,
            n_estimators=self.n_estimators,
            learning_rate=self.learning_rate,
            loss=self.loss,
            random_state=self.random_state,
        )
        self.model_.fit(X_weighted, y)
        return self

    def predict(self, X):
        check_is_fitted(self, "model_")
        X = np.array(X, dtype=np.float64)
        X_weighted = self._apply_weights(X)
        return self.model_.predict(X_weighted)

    def feature_importances(self, feature_names: list = None) -> pd.Series:
        """Returns feature importances from the underlying AdaBoost model."""
        check_is_fitted(self, "model_")
        imp = self.model_.feature_importances_
        if feature_names:
            return pd.Series(imp, index=feature_names).sort_values(ascending=False)
        return pd.Series(imp).sort_values(ascending=False)


# ---------------------------------------------------------------------------
# Training function
# ---------------------------------------------------------------------------

HADT_PARAM_GRID = {
    "n_estimators":  [50, 100, 200, 300],
    "max_depth":     [2, 3, 4, 5, 6],
    "learning_rate": [0.01, 0.05, 0.1, 0.2, 0.5],
    "loss":          ["linear", "square", "exponential"],
}


def train_hadt(
    X_train: np.ndarray,
    y_train: np.ndarray,
    tune: bool = True,
    n_iter: int = 30,
    cv_splits: int = 5,
    save: bool = True,
) -> HADTRegressor:
    """
    Trains a HADT model.

    Parameters
    ----------
    X_train / y_train : training data (scaled arrays)
    tune              : run RandomizedSearchCV if True
    n_iter            : hyperparameter combinations to sample
    cv_splits         : time-series CV folds
    save              : persist model to models/hadt.pkl

    Returns
    -------
    Fitted HADTRegressor
    """
    logger.info("Training HADT (Hybrid Association Decision Tree) …")

    if tune:
        tscv = TimeSeriesSplit(n_splits=cv_splits)
        search = RandomizedSearchCV(
            estimator=HADTRegressor(random_state=RANDOM_STATE),
            param_distributions=HADT_PARAM_GRID,
            n_iter=n_iter,
            scoring="neg_mean_absolute_error",
            cv=tscv,
            verbose=1,
            random_state=RANDOM_STATE,
            n_jobs=-1,
        )
        search.fit(X_train, y_train)
        model = search.best_estimator_
        logger.info(f"HADT best params: {search.best_params_}")
    else:
        model = HADTRegressor(
            n_estimators=100,
            max_depth=4,
            learning_rate=0.1,
            loss="linear",
            random_state=RANDOM_STATE,
        )
        model.fit(X_train, y_train)

    logger.info("HADT training complete.")

    if save:
        save_model(model, "hadt")

    return model
