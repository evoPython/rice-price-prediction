"""
eda.py
------
Exploratory data analysis helpers for the rice price dataset.
Generates PNG plots and compact summary artefacts under results/eda/.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

from src.utils.helpers import get_logger

logger = get_logger(__name__)

RESULTS_DIR = Path(__file__).resolve().parents[2] / "results"
EDA_DIR = RESULTS_DIR / "eda"

plt.rcParams.update({
    "figure.dpi": 150,
    "font.size": 10,
    "axes.spines.top": False,
    "axes.spines.right": False,
})


def _ensure_datetime(df: pd.DataFrame) -> pd.Series:
    if {"year", "month"}.issubset(df.columns):
        return pd.to_datetime(
            dict(year=df["year"], month=df["month"], day=1),
            errors="coerce",
        )
    if "date" in df.columns:
        return pd.to_datetime(df["date"], errors="coerce")
    return pd.RangeIndex(len(df))


def _top_numeric_factors(df: pd.DataFrame, target_col: str, max_features: int = 8) -> list[str]:
    numeric_cols = [
        c for c in df.select_dtypes(include=[np.number]).columns.tolist()
        if c not in {target_col, "year", "month"}
    ]
    if not numeric_cols:
        return []

    completeness = df[numeric_cols].notna().mean().sort_values(ascending=False)
    selected = list(completeness.head(max_features).index)
    return selected


def plot_missingness(df: pd.DataFrame, save_path: Path) -> Path:
    missing_pct = df.isna().mean().sort_values(ascending=False) * 100
    missing_pct = missing_pct[missing_pct > 0]

    fig, ax = plt.subplots(figsize=(max(10, len(missing_pct) * 0.5), 5))
    if len(missing_pct) == 0:
        ax.text(0.5, 0.5, "No missing values detected", ha="center", va="center", fontsize=12)
        ax.set_axis_off()
    else:
        ax.bar(missing_pct.index, missing_pct.values)
        ax.set_ylabel("Missing (%)")
        ax.set_title("Missingness by Column")
        ax.tick_params(axis="x", rotation=45)
        ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.0f%%"))
    plt.tight_layout()
    fig.savefig(save_path)
    plt.close(fig)
    return save_path


def plot_price_trends(df: pd.DataFrame, target_col: str, save_path: Path) -> Path:
    date_index = _ensure_datetime(df)
    price_cols = [c for c in df.columns if c.startswith("price_")]
    if target_col in df.columns and target_col not in price_cols:
        price_cols = [target_col] + price_cols
    price_cols = [c for c in dict.fromkeys(price_cols) if c in df.columns]

    fig, ax = plt.subplots(figsize=(12, 5))
    for col in price_cols:
        if pd.api.types.is_numeric_dtype(df[col]):
            ax.plot(date_index, df[col], label=col.replace("price_", "").replace("_", " "))
    ax.set_title("Rice Price Trends Over Time")
    ax.set_xlabel("Date")
    ax.set_ylabel("Price (₱/kg)")
    ax.legend(ncol=2, fontsize=8)
    ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("₱%.2f"))
    plt.tight_layout()
    fig.savefig(save_path)
    plt.close(fig)
    return save_path


def plot_target_distribution(df: pd.DataFrame, target_col: str, save_path: Path) -> Path:
    fig, ax = plt.subplots(figsize=(8, 5))
    if target_col in df.columns:
        series = pd.to_numeric(df[target_col], errors="coerce").dropna()
        ax.hist(series, bins=min(20, max(5, len(series) // 4)))
        ax.set_xlabel("Price (₱/kg)")
        ax.set_ylabel("Frequency")
        ax.set_title(f"Distribution of {target_col}")
        ax.xaxis.set_major_formatter(mticker.FormatStrFormatter("₱%.2f"))
    else:
        ax.text(0.5, 0.5, f"{target_col} not found", ha="center", va="center")
        ax.set_axis_off()
    plt.tight_layout()
    fig.savefig(save_path)
    plt.close(fig)
    return save_path


def plot_factor_trends(df: pd.DataFrame, target_col: str, save_path: Path, max_features: int = 8) -> Path:
    date_index = _ensure_datetime(df)
    cols = _top_numeric_factors(df, target_col, max_features=max_features)

    fig, ax = plt.subplots(figsize=(12, 5))
    plotted = 0
    for col in cols:
        series = pd.to_numeric(df[col], errors="coerce")
        if series.notna().sum() < 2:
            continue
        mean = series.mean()
        std = series.std(ddof=0)
        if not np.isfinite(std) or std == 0:
            continue
        z = (series - mean) / std
        ax.plot(date_index, z, label=col)
        plotted += 1

    if plotted == 0:
        ax.text(0.5, 0.5, "Not enough numeric factors to plot", ha="center", va="center")
        ax.set_axis_off()
    else:
        ax.set_title("Standardized Trends of Key Factors")
        ax.set_xlabel("Date")
        ax.set_ylabel("Z-score")
        ax.legend(ncol=2, fontsize=8)
    plt.tight_layout()
    fig.savefig(save_path)
    plt.close(fig)
    return save_path


def plot_correlation_heatmap(df: pd.DataFrame, save_path: Path) -> Path:
    numeric_df = df.select_dtypes(include=[np.number]).copy()
    corr = numeric_df.corr(numeric_only=True)

    fig, ax = plt.subplots(figsize=(max(8, 0.55 * len(corr.columns)), max(6, 0.55 * len(corr.columns))))
    if corr.empty:
        ax.text(0.5, 0.5, "No numeric columns available", ha="center", va="center")
        ax.set_axis_off()
    else:
        im = ax.imshow(corr.values, aspect="auto")
        ax.set_xticks(range(len(corr.columns)))
        ax.set_yticks(range(len(corr.columns)))
        ax.set_xticklabels(corr.columns, rotation=45, ha="right", fontsize=8)
        ax.set_yticklabels(corr.columns, fontsize=8)
        ax.set_title("Correlation Heatmap")
        fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
    plt.tight_layout()
    fig.savefig(save_path)
    plt.close(fig)
    return save_path


def export_summary(df: pd.DataFrame, target_col: str, out_dir: Path) -> Path:
    summary = {
        "rows": int(len(df)),
        "columns": int(len(df.columns)),
        "target_col": target_col,
        "year_min": int(df["year"].min()) if "year" in df.columns and df["year"].notna().any() else None,
        "year_max": int(df["year"].max()) if "year" in df.columns and df["year"].notna().any() else None,
        "missing_pct": {c: float(v) for c, v in (df.isna().mean() * 100).sort_values(ascending=False).to_dict().items()},
    }
    path = out_dir / "eda_summary.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    return path


def run_eda(df: pd.DataFrame, target_col: str, output_dir: Path | None = None) -> dict:
    """Run EDA and export a set of PNG plots plus a compact JSON summary."""
    out_dir = Path(output_dir) if output_dir else EDA_DIR
    out_dir.mkdir(parents=True, exist_ok=True)

    logger.info("=" * 55)
    logger.info("EXPLORATORY DATA ANALYSIS")
    logger.info("=" * 55)
    logger.info(f"Rows: {len(df)} | Columns: {len(df.columns)} | Target: {target_col}")

    paths = {
        "missingness": plot_missingness(df, out_dir / "missingness_by_column.png"),
        "price_trends": plot_price_trends(df, target_col, out_dir / "price_trends.png"),
        "factor_trends": plot_factor_trends(df, target_col, out_dir / "factor_trends.png"),
        "correlation": plot_correlation_heatmap(df, out_dir / "correlation_heatmap.png"),
        "target_distribution": plot_target_distribution(df, target_col, out_dir / "target_distribution.png"),
        "summary": export_summary(df, target_col, out_dir),
    }

    logger.info(f"EDA artefacts saved to {out_dir}")
    for name, path in paths.items():
        logger.info(f"  {name}: {path.name}")

    return {k: str(v) for k, v in paths.items()}
