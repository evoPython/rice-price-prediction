"""
preprocess.py
-------------
Full preprocessing pipeline:
  1. Column selection / target definition
  2. Monthly regularization of the price timeline
  3. Linear interpolation of price columns only (gaps 1–7 months inclusive)
  4. Row-level exclusion of severely incomplete rows
  5. IQR-based outlier detection; removal (training split only) is optional,
     controlled by preprocess(..., remove_outliers=...)
  6. Feature scaling (StandardScaler)
  7. Train / test split (80 / 20, chronological)

Output artefacts written to data/final/<target>[/<variant>]/.
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
MAX_INTERPOLATION_GAP_MONTHS = 7


def get_target_artifact_dir(target_col: str, variant: str | None = None) -> Path:
    """
    Returns the artifact directory dedicated to one target commodity, with
    an optional sub-variant (e.g. "outliers_kept" / "outliers_removed") so
    multiple experiment runs for the same target don't overwrite each other.
    """
    base = FINAL_DIR / target_col
    return base / variant if variant else base


def get_preprocessed_bundle_path(target_col: str | None = None, variant: str | None = None) -> Path:
    """Returns the cached preprocessing bundle path for a target commodity."""
    target = target_col or DEFAULT_TARGET_COL
    return get_target_artifact_dir(target, variant) / "preprocessed_bundle.joblib"


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
        "rain_hours",
        "tmax",
        "tmin",
        "tmean",
        "rh",
        "wind_speed",
        "soil_moisture",
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


LAG_MONTHS = (1, 2, 3)
ROLLING_WINDOW = 3


def add_own_price_lag_features(df: pd.DataFrame, target_col: str) -> pd.DataFrame:
    """
    Adds autoregressive features derived from the target's own history:
      own_price_lag1 / lag2 / lag3  – price at t-1, t-2, t-3
      own_price_roll3               – trailing 3-month mean of price at t-1..t-3

    Only past values are used (shift >= 1), so these are safe to use as
    predictive features with no lookahead leakage. Computed on the
    regularized/interpolated monthly timeline, BEFORE any row-completeness
    filtering, so lag values remain correct relative to true calendar
    months even if some rows are later dropped for sparsity.

    Also adds `own_price_delta` = target - lag1 (this month's change vs.
    last month). This is NOT a feature — it is only ever used as an
    alternative modeling target — and must be excluded from X explicitly.
    """
    df = df.copy()
    if target_col not in df.columns:
        logger.warning(f"Cannot build lag features: '{target_col}' not in df.")
        return df

    series = df[target_col]
    for lag in LAG_MONTHS:
        df[f"own_price_lag{lag}"] = series.shift(lag)

    df["own_price_roll3"] = series.shift(1).rolling(ROLLING_WINDOW).mean()
    df["own_price_delta"] = series - series.shift(1)

    logger.info(
        f"Added own-price lag features: "
        f"{[f'own_price_lag{l}' for l in LAG_MONTHS]} + own_price_roll3 "
        f"(own_price_delta kept aside as an alternative target, not a feature)"
    )
    return df


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


def _save_bundle(bundle: dict, target_col: str, variant: str | None = None) -> Path:
    """Persist the full preprocessing bundle for later reuse."""
    artifact_dir = get_target_artifact_dir(target_col, variant)
    artifact_dir.mkdir(parents=True, exist_ok=True)
    bundle_path = get_preprocessed_bundle_path(target_col, variant)
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

    # Drop sibling rice-grade price columns (e.g. price_well_milled,
    # price_superior_milled, ...) when they are not the chosen target.
    # These move in near-lockstep with the target because they're the same
    # commodity in the same market/month, so leaving them in as "features"
    # leaks target-adjacent information and lets the model shortcut around
    # the actual factors (weather, production, fertilizer) we care about.
    sibling_price_cols = [
        c for c in df.columns
        if c.startswith("price_") and c != target_col
    ]
    default_drop += sibling_price_cols

    # own_price_delta is target minus lag1 — pure leakage if used as a
    # feature (it algebraically reveals the target given lag1). It is only
    # ever used as an alternative *target* elsewhere, never as an X column.
    default_drop += ["own_price_delta"]

    # Contemporaneous (same-month) rice/headline inflation and rice
    # contribution figures are dropped by default — they're YoY % change
    # measures computed FROM that month's own price level, so using them
    # to predict that same month's price is target-adjacent leakage. Only
    # the _lag1 versions (last month's published inflation figures) are
    # kept as legitimate features. See load_rice_inflation() docstring.
    default_drop += ["headline_inflation", "rice_inflation", "rice_contribution"]

    # Yield and rice area (51% coverage, so ~half of every row's value for
    # these is median-imputed) were tested via ablation on price_regular_milled
    # and consistently HURT all three models (RF/XGB/HADT MAPE all rose when
    # they were included) — removing them was the one consistent, positive
    # change across models, unlike fertilizer or RH/wind (also sparse, but
    # ablation showed those net-help despite low coverage, so they're kept).
    default_drop += ["avg_yield_ton_per_ha", "rice_area_ha"]

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
    remove_outliers: bool = False,
    save_scaler: bool = True,
    variant: str | None = None,
) -> dict:
    """
    Full preprocessing pipeline.

    Parameters
    ----------
    remove_outliers : if True, rows flagged by the IQR rule (on features +
                       target) are dropped from the TRAINING split only
                       (never from test — test must reflect real conditions
                       models will face). Lets you run a clean A/B: same
                       target, same split, outliers in vs. out.
    variant         : optional label ("outliers_kept" / "outliers_removed")
                       appended to the artifact directory so multiple runs
                       for the same target don't overwrite each other.

    Returns
    -------
    dict with keys:
      X_train, X_test, y_train, y_test          – scaled numpy arrays
      X_train_raw, X_test_raw                   – unscaled DataFrames (for tree models)
      scaler                                     – fitted StandardScaler
      feature_names                              – list of feature column names
      outlier_mask                               – boolean Series (True = outlier row, full dataset)
      n_outliers_removed_train                   – int, rows dropped from training split
      remove_outliers                            – bool, echoed back for provenance
      df_clean                                   – cleaned DataFrame before split
    """
    FINAL_DIR.mkdir(parents=True, exist_ok=True)
    artifact_dir = get_target_artifact_dir(target_col, variant)
    artifact_dir.mkdir(parents=True, exist_ok=True)

    logger.info("=== Preprocessing ===")
    logger.info(f"Target: {target_col} | variant: {variant or '(default)'} | "
                f"remove_outliers: {remove_outliers}")
    df = df.copy()

    # Keep time order stable for interpolation and splits.
    sort_cols = [c for c in ["year", "month"] if c in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols).reset_index(drop=True)

    # 1. Drop extremely sparse columns first (they create avoidable NaNs later)
    df = _drop_sparse_columns(df, target_col)

    # 2. Regularize the time axis and interpolate price columns only.
    df = interpolate_price_columns(df, max_gap_months=MAX_INTERPOLATION_GAP_MONTHS)

    # 2b. Add autoregressive (own-price lag / rolling) features on the
    #     regularized timeline, before row filtering can break contiguity.
    df = add_own_price_lag_features(df, target_col)

    df = _drop_sparse_rows(df)

    # 3. Feature / target split
    X, y = select_features_and_target(df, target_col, drop_cols)

    # Safety net: ensure the target and selected features are finite.
    df_clean = X.copy()
    df_clean[target_col] = y.values
    required_cols = list(X.columns) + [target_col]
    df_clean = df_clean.replace([np.inf, -np.inf], np.nan)

    # Keep rows where at least 50% of features exist
    feature_cols = required_cols[:-1]  # exclude target
    min_required = int(0.5 * len(feature_cols))
    valid_rows = df_clean[feature_cols].notna().sum(axis=1) >= min_required
    valid_rows &= df_clean[target_col].notna()  # still require target to exist
    df_clean = df_clean[valid_rows].reset_index(drop=True)

    logger.info(f"Rows after feature filtering: {len(df_clean)}")
    logger.info(f"Target mean: {y.mean():.2f} | std: {y.std():.2f}")

    X = df_clean[X.columns].copy()
    y = df_clean[target_col].copy()

    # 4. Outlier detection (IQR) — mask computed on full dataset for
    #    reference/reporting regardless of whether removal is applied.
    numeric_features = X.select_dtypes(include=[np.number]).columns.tolist()
    outlier_mask = _flag_outliers(df_clean, numeric_features + [target_col])
    n_outliers = outlier_mask.sum()
    logger.info(f"Outliers detected (IQR, multiplier={IQR_MULTIPLIER}): {n_outliers} rows")

    # 5. Train / test split – chronological (no shuffle), test set NEVER touched by outlier removal.
    X_train_raw, X_test_raw, y_train, y_test = train_test_split(
        X, y, test_size=TEST_SIZE, shuffle=False,
    )

    n_outliers_removed_train = 0
    if remove_outliers:
        train_df_tmp = X_train_raw.copy()
        train_df_tmp[target_col] = y_train.values
        # Fences computed on the TRAINING split only (not full dataset),
        # so the test set can never leak into what counts as "normal".
        outlier_mask_train = _flag_outliers(train_df_tmp, numeric_features + [target_col])
        n_outliers_removed_train = int(outlier_mask_train.sum())
        logger.info(
            f"Removing outliers from training split: {n_outliers_removed_train} rows "
            f"({len(X_train_raw) - n_outliers_removed_train} kept of {len(X_train_raw)})"
        )
        X_train_raw = X_train_raw[~outlier_mask_train.values].reset_index(drop=True)
        y_train = y_train[~outlier_mask_train.values].reset_index(drop=True)

    # 6. Median-impute (fit on train only), then scale (fit on train only)
    imputer = SimpleImputer(strategy="median")
    X_train_raw = pd.DataFrame(imputer.fit_transform(X_train_raw), columns=X_train_raw.columns)
    X_test_raw  = pd.DataFrame(imputer.transform(X_test_raw),  columns=X_test_raw.columns)

    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train_raw)
    X_test_scaled  = scaler.transform(X_test_raw)

    if save_scaler:
        joblib.dump(scaler, artifact_dir / "scaler.pkl")
        logger.info(f"Scaler saved to {artifact_dir}")

    # 7. Persist split datasets
    pd.DataFrame(X_train_scaled, columns=X.columns).to_csv(artifact_dir / "X_train.csv", index=False)
    pd.DataFrame(X_test_scaled,  columns=X.columns).to_csv(artifact_dir / "X_test.csv",  index=False)
    pd.Series(y_train.values, name=target_col).to_csv(artifact_dir / "y_train.csv", index=False)
    pd.Series(y_test.values,  name=target_col).to_csv(artifact_dir / "y_test.csv",  index=False)

    bundle = {
        "X_train": X_train_scaled,
        "X_test":  X_test_scaled,
        "y_train": y_train.values,
        "y_test":  y_test.values,
        "X_train_raw": X_train_raw,   # unscaled DataFrames (for SHAP / tree models)
        "X_test_raw":  X_test_raw,
        "scaler":        scaler,
        "feature_names": list(X.columns),
        "outlier_mask":  outlier_mask,
        "n_outliers_removed_train": n_outliers_removed_train,
        "remove_outliers": remove_outliers,
        "df_clean":      df_clean,
        "target_col":    target_col,
        "variant":       variant,
    }

    _save_bundle(bundle, target_col, variant)
    logger.info(f"Train/test splits saved to {artifact_dir}")
    logger.info(f"Train size: {len(X_train_raw)} | Test size: {len(X_test_raw)}")

    return bundle


def load_cached_preprocessed(target_col: str | None = None, variant: str | None = None) -> dict | None:
    """Loads the saved preprocessing bundle if it exists."""
    bundle_path = get_preprocessed_bundle_path(target_col, variant)
    if not bundle_path.exists():
        return None

    bundle = joblib.load(bundle_path)
    if target_col is not None and bundle.get("target_col") != target_col:
        return None
    return bundle