"""
preprocess.py
-------------
Full preprocessing pipeline:
  1. Column selection / target definition
  2. Monthly regularization of the price timeline
  3. Linear interpolation of price columns only (gaps 1–6 months inclusive)
  4. Row-level exclusion of severely incomplete rows
  5. IQR-based outlier assessment (removal applied only to LSTM training split)
  6. Feature scaling (StandardScaler)
  7. Train / test split (80 / 20, chronological)

Output artefacts written to data/final/.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import joblib
from pathlib import Path
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer

from src.utils.helpers import get_logger

logger = get_logger(__name__)

FINAL_DIR = Path(__file__).resolve().parents[2] / "data" / "final"
DEFAULT_TARGET_COL = "price_regular_milled"
MAX_INTERPOLATION_GAP_MONTHS = 6


def get_target_artifact_dir(target_col: str) -> Path:
    """Returns the artifact directory dedicated to one target commodity."""
    return FINAL_DIR / target_col


def get_preprocessed_bundle_path(target_col: str | None = None) -> Path:
    """Returns the cached preprocessing bundle path for a target commodity."""
    target = target_col or DEFAULT_TARGET_COL
    return get_target_artifact_dir(target) / "preprocessed_bundle.joblib"


# Backward-compatible default path (regular milled).
PREPROCESSED_BUNDLE = get_preprocessed_bundle_path(DEFAULT_TARGET_COL)

# Fraction of columns that must be non-null for a row to be kept
ROW_COMPLETENESS_THRESHOLD = 0.50

# Drop very sparse columns before row-wise filtering to avoid destroying the sample size.
COLUMN_COMPLETENESS_THRESHOLD = 0.20  # drop columns with >80% missing values

# IQR multiplier for outlier detection
IQR_MULTIPLIER = 1.5

# Train / test split ratio
TEST_SIZE = 0.20
RANDOM_STATE = 42


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _drop_sparse_columns(
    df: pd.DataFrame,
    target_col: str,
    threshold: float = COLUMN_COMPLETENESS_THRESHOLD,
) -> pd.DataFrame:
    """Drops non-target columns whose non-null fraction is below the threshold."""
    keep = []
    dropped = []
    for col in df.columns:
        if col in {"year", "month", target_col}:
            keep.append(col)
            continue
        if df[col].notna().mean() >= threshold:
            keep.append(col)
        else:
            dropped.append(col)

    out = df[keep].copy()
    if dropped:
        logger.info(
            f"Sparse-column removal: dropped {len(dropped)} columns "
            f"(threshold={threshold:.0%} non-null): {dropped}"
        )
    else:
        logger.info("Sparse-column removal: none")
    return out


def _to_month_start_index(df: pd.DataFrame) -> pd.DatetimeIndex | None:
    """Builds a month-start datetime index from year/month columns."""
    if not {"year", "month"}.issubset(df.columns):
        return None
    dt = pd.to_datetime(
        dict(year=df["year"], month=df["month"], day=1),
        errors="coerce",
    )
    if dt.isna().all():
        return None
    return pd.DatetimeIndex(dt)


def _regularize_monthly_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Reindexes the data to a complete monthly timeline when year/month exist."""
    date_index = _to_month_start_index(df)
    if date_index is None:
        return df.copy()

    working = df.copy()
    working = working.loc[~date_index.isna()].copy()
    date_index = date_index[~date_index.isna()]
    working["_date"] = date_index
    working = working.sort_values("_date")

    if working.empty:
        return df.copy()

    full_index = pd.date_range(working["_date"].min(), working["_date"].max(), freq="MS")
    before = len(working)

    working = (
        working.drop(columns=["year", "month"], errors="ignore")
        .set_index("_date")
        .reindex(full_index)
    )
    working.index.name = "_date"
    working["year"] = working.index.year
    working["month"] = working.index.month
    working = working.reset_index(drop=True)

    inserted = len(working) - before
    if inserted > 0:
        logger.info(
            f"Monthly regularization: {before} → {len(working)} rows "
            f"(+{inserted} inserted missing months)"
        )
    else:
        logger.info(f"Monthly regularization: {before} rows (already contiguous)")
    return working


def _interpolate_series_with_gap_limit(series: pd.Series, max_gap: int = MAX_INTERPOLATION_GAP_MONTHS) -> tuple[pd.Series, int]:
    """Linearly fills only internal NaN runs whose size is within the gap limit."""
    values = pd.to_numeric(series, errors="coerce").astype(float).to_numpy(copy=True)
    valid_positions = np.flatnonzero(~np.isnan(values))

    filled_months = 0
    if len(valid_positions) < 2:
        return pd.Series(values, index=series.index, name=series.name), filled_months

    for left, right in zip(valid_positions[:-1], valid_positions[1:]):
        gap = right - left - 1
        if 1 <= gap <= max_gap:
            interpolated = np.linspace(values[left], values[right], gap + 2)[1:-1]
            values[left + 1:right] = interpolated
            filled_months += gap

    return pd.Series(values, index=series.index, name=series.name), filled_months


def interpolate_price_columns(
    df: pd.DataFrame,
    price_cols: list[str] | None = None,
    max_gap_months: int = MAX_INTERPOLATION_GAP_MONTHS,
) -> pd.DataFrame:

    """
    Regularizes the timeline and linearly interpolates:

      - price columns
      - weather columns

    Only internal gaps with sizes 1–6 months inclusive are filled.
    """

    working = _regularize_monthly_frame(df)

    price_cols = price_cols or [
        c for c in working.columns
        if c.startswith("price_")
    ]

    weather_cols = [
        "rainfall",
        "tmax",
        "tmin",
        "rh",
        "wind_speed",
    ]

    cols_to_interp = [
        c for c in price_cols + weather_cols
        if c in working.columns
    ]

    total_filled = 0

    for col in cols_to_interp:

        working[col], n_filled = _interpolate_series_with_gap_limit(
            working[col],
            max_gap=max_gap_months,
        )

        total_filled += n_filled

        if n_filled > 0:
            logger.info(
                f"  - {col}: filled {n_filled} month(s)"
            )

    logger.info(
        f"Interpolation complete: {total_filled} total filled"
    )

    return working


def _drop_sparse_rows(df: pd.DataFrame, threshold: float = ROW_COMPLETENESS_THRESHOLD) -> pd.DataFrame:
    """Removes rows where the fraction of non-null values is below threshold."""
    before = len(df)
    non_null_frac = df.notna().mean(axis=1)
    df = df[non_null_frac >= threshold].reset_index(drop=True)
    logger.info(f"Sparse-row removal: {before} → {len(df)} rows "
                f"(threshold={threshold:.0%})")
    return df


def _iqr_bounds(series: pd.Series, multiplier: float = IQR_MULTIPLIER):
    """Returns (lower, upper) IQR fences for a numeric Series."""
    q1, q3 = series.quantile(0.25), series.quantile(0.75)
    iqr = q3 - q1
    return q1 - multiplier * iqr, q3 + multiplier * iqr


def _flag_outliers(df: pd.DataFrame, cols: list) -> pd.Series:
    """Returns a boolean mask – True where any column is outside IQR fences."""
    mask = pd.Series(False, index=df.index)
    for col in cols:
        if col not in df.columns:
            continue
        lo, hi = _iqr_bounds(df[col])
        mask |= (df[col] < lo) | (df[col] > hi)
    return mask


def _save_bundle(bundle: dict, target_col: str) -> Path:
    """Persist the full preprocessing bundle for later reuse."""
    artifact_dir = get_target_artifact_dir(target_col)
    artifact_dir.mkdir(parents=True, exist_ok=True)
    bundle_path = get_preprocessed_bundle_path(target_col)
    joblib.dump(bundle, bundle_path)
    logger.info(f"Preprocessed bundle saved to {bundle_path}")
    return bundle_path


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def select_features_and_target(
    df: pd.DataFrame,
    target_col: str,
    drop_cols: list = None,
) -> tuple[pd.DataFrame, pd.Series]:
    """
    Drops identifier / leakage columns and returns (X, y).

    Parameters
    ----------
    df         : merged dataset
    target_col : name of the column to predict (rice price)
    drop_cols  : additional columns to exclude
    """
    default_drop = ["year", "month"]
    if drop_cols:
        default_drop += drop_cols

    feature_cols = [c for c in df.columns
                    if c != target_col and c not in default_drop]

    X = df[feature_cols].copy()
    y = df[target_col].copy()

    logger.info(f"Target: '{target_col}'  |  Features ({len(feature_cols)}): {feature_cols}")
    return X, y


def preprocess(
    df: pd.DataFrame,
    target_col: str,
    drop_cols: list = None,
    remove_outliers_for_lstm: bool = True,
    save_scaler: bool = True,
) -> dict:
    """
    Full preprocessing pipeline.

    Returns
    -------
    dict with keys:
      X_train, X_test, y_train, y_test          – scaled numpy arrays
      X_train_raw, X_test_raw                   – unscaled DataFrames (for tree models)
      scaler                                     – fitted StandardScaler
      feature_names                              – list of feature column names
      outlier_mask                               – boolean Series (True = outlier row)
      df_clean                                   – cleaned DataFrame before split
      df_lstm                                    – outlier-removed training DataFrame for LSTM
    """
    FINAL_DIR.mkdir(parents=True, exist_ok=True)
    artifact_dir = get_target_artifact_dir(target_col)
    artifact_dir.mkdir(parents=True, exist_ok=True)

    logger.info("=== Preprocessing ===")
    df = df.copy()

    # Keep time order stable for interpolation and splits.
    sort_cols = [c for c in ["year", "month"] if c in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols).reset_index(drop=True)

    # 1. Drop extremely sparse columns first (they create avoidable NaNs later)
    df = _drop_sparse_columns(df, target_col)

    # 2. Drop extremely sparse rows first (before interpolation to avoid propagation)
    

    # 3. Regularize the time axis and interpolate price columns only.
    df = interpolate_price_columns(df, max_gap_months=MAX_INTERPOLATION_GAP_MONTHS)

    df = _drop_sparse_rows(df)

    # 4. Feature / target split
    X, y = select_features_and_target(df, target_col, drop_cols)

    # Safety net: ensure the target and selected features are finite.
    df_clean = X.copy()
    df_clean[target_col] = y.values
    required_cols = list(X.columns) + [target_col]
    df_clean = df_clean.replace([np.inf, -np.inf], np.nan)

    # Keep rows where at least 70% of features exist
    feature_cols = required_cols[:-1]  # exclude target

    min_required = int(0.5 * len(feature_cols))

    valid_rows = (
        df_clean[feature_cols].notna().sum(axis=1)
        >= min_required
    )

    # Still require target to exist
    valid_rows &= df_clean[target_col].notna()

    df_clean = df_clean[valid_rows].reset_index(drop=True)

    logger.info(
        f"Rows after feature filtering: {len(df_clean)}"
    )

    logger.info(
        f"Target mean: {y.mean():.2f} | std: {y.std():.2f}"
    )

    print("Rows after relaxed filtering:", len(df_clean))
    X = df_clean[X.columns].copy()
    y = df_clean[target_col].copy()

    # 5. Outlier detection (IQR) — mask computed on full dataset for reference,
    #    but removal is applied to the LSTM training split only (see step 6).
    numeric_features = X.select_dtypes(include=[np.number]).columns.tolist()
    outlier_mask = _flag_outliers(df_clean, numeric_features + [target_col])
    n_outliers = outlier_mask.sum()
    logger.info(f"Outliers detected (IQR, multiplier={IQR_MULTIPLIER}): {n_outliers} rows")

    # 6. Train / test split – chronological (no shuffle) ---
    #    Using sklearn's train_test_split with shuffle=False to respect time order.
    def _split(X_in, y_in):
        return train_test_split(
            X_in, y_in,
            test_size=TEST_SIZE,
            shuffle=False,   # preserve time order
        )

    # Tree-model split (unchanged)
    X_train_raw, X_test_raw, y_train, y_test = _split(X, y)

    # Split first — same indices as tree models
    X_lstm_full = df_clean.drop(columns=[target_col])
    y_lstm_full = df_clean[target_col]
    X_lstm_train_raw, X_lstm_test, y_lstm_train_raw, y_lstm_test = _split(
        X_lstm_full, y_lstm_full
    )

    if remove_outliers_for_lstm:
        train_df_tmp = X_lstm_train_raw.copy()
        train_df_tmp[target_col] = y_lstm_train_raw.values
        outlier_mask_train = _flag_outliers(
            train_df_tmp, numeric_features + [target_col]
        )
        n_removed = outlier_mask_train.sum()
        logger.info(
            f"LSTM outlier removal (training only): {n_removed} rows removed "
            f"({len(X_lstm_train_raw) - n_removed} training rows kept)"
        )
        X_lstm_train = X_lstm_train_raw[~outlier_mask_train]
        y_lstm_train = y_lstm_train_raw[~outlier_mask_train]

        # --- FIX: Impute LSTM features properly ---

        lstm_imputer = SimpleImputer(strategy="median")

        X_lstm_train = pd.DataFrame(
            lstm_imputer.fit_transform(X_lstm_train),
            columns=X_lstm_train.columns
        )

        X_lstm_test = pd.DataFrame(
            lstm_imputer.transform(X_lstm_test),
            columns=X_lstm_test.columns
        )
    else:
        X_lstm_train = X_lstm_train_raw
        y_lstm_train = y_lstm_train_raw

    df_lstm = X_lstm_train.copy()
    df_lstm[target_col] = y_lstm_train.values
    logger.info(f"LSTM training dataset shape (outliers removed): {df_lstm.shape}")
    logger.info(f"LSTM test set size: {len(X_lstm_test)} (same as tree models)")

    imputer = SimpleImputer(strategy="median")

    X_train_raw = pd.DataFrame(
        imputer.fit_transform(X_train_raw),
        columns=X_train_raw.columns
    )

    X_test_raw = pd.DataFrame(
        imputer.transform(X_test_raw),
        columns=X_test_raw.columns
    )

    # 7. Scaling (StandardScaler fitted on train only)
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train_raw)
    X_test_scaled  = scaler.transform(X_test_raw)

    lstm_scaler = StandardScaler()
    X_lstm_train_scaled = lstm_scaler.fit_transform(X_lstm_train)
    X_lstm_test_scaled  = lstm_scaler.transform(X_lstm_test)

    y_scaler = StandardScaler()
    y_scaler.fit(y_lstm_train_raw.values.reshape(-1, 1))
    y_lstm_train_scaled = y_scaler.transform(
        y_lstm_train.values.reshape(-1, 1)
    ).ravel()
    y_lstm_test_scaled = y_scaler.transform(
        y_lstm_test.values.reshape(-1, 1)
    ).ravel()

    if save_scaler:
        joblib.dump(scaler,      artifact_dir / "scaler.pkl")
        joblib.dump(lstm_scaler, artifact_dir / "lstm_scaler.pkl")
        joblib.dump(y_scaler,    artifact_dir / "y_scaler.pkl")
        logger.info(f"Scalers saved to {artifact_dir}")

    # 8. Persist split datasets
    pd.DataFrame(X_train_scaled, columns=X.columns).to_csv(artifact_dir / "X_train.csv", index=False)
    pd.DataFrame(X_test_scaled,  columns=X.columns).to_csv(artifact_dir / "X_test.csv",  index=False)
    pd.Series(y_train.values, name=target_col).to_csv(artifact_dir / "y_train.csv", index=False)
    pd.Series(y_test.values,  name=target_col).to_csv(artifact_dir / "y_test.csv",  index=False)

    bundle = {
        # Tree-model splits (scaled)
        "X_train": X_train_scaled,
        "X_test":  X_test_scaled,
        "y_train": y_train.values,
        "y_test":  y_test.values,
        # Unscaled DataFrames (for SHAP / feature names)
        "X_train_raw": X_train_raw,
        "X_test_raw":  X_test_raw,
        # LSTM-specific splits
        "X_lstm_train": X_lstm_train_scaled,
        "X_lstm_test":  X_lstm_test_scaled,
        "y_lstm_train": y_lstm_train_scaled,
        "y_lstm_test":  y_lstm_test_scaled,
        "y_lstm_train_raw": y_lstm_train.values,
        "y_lstm_test_raw":  y_lstm_test.values,
        # Metadata
        "scaler":        scaler,
        "lstm_scaler":   lstm_scaler,
        "y_scaler":      y_scaler,
        "feature_names": list(X.columns),
        "outlier_mask":  outlier_mask,
        "df_clean":      df_clean,
        "df_lstm":       df_lstm,
        "target_col":    target_col,
    }

    _save_bundle(bundle, target_col)
    logger.info(f"Train/test splits saved to {FINAL_DIR}")
    logger.info(f"Train size: {len(X_train_raw)} | Test size: {len(X_test_raw)}")

    return bundle


def load_cached_preprocessed(target_col: str | None = None) -> dict | None:
    """Loads the saved preprocessing bundle if it exists."""
    bundle_path = get_preprocessed_bundle_path(target_col)
    if not bundle_path.exists():
        return None

    bundle = joblib.load(bundle_path)
    if target_col is not None and bundle.get("target_col") != target_col:
        return None
    return bundle


def reshape_for_lstm(X: np.ndarray, timesteps: int = 1) -> np.ndarray:
    """
    Reshapes a 2D array (samples, features) → (samples, timesteps, features)
    as required by Keras LSTM layers.
    """
    samples, features = X.shape
    return X.reshape(samples, timesteps, features)