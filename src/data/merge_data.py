"""
merge_data.py
-------------
Joins all six raw data sources into a single flat DataFrame keyed on
(year, month).  Uses a left join anchored on the rice price series so
every row has a target value, and other features are filled in where
the source data covers that period.

Data coverage summary (approximate):
  WFP rice prices      2000 – 2025   monthly
  Palay production     1987 – 2025   monthly (expanded from quarterly)
  Weather (Mactan)     2015 – 2024   monthly
  Yield (Region VII)   2018 – 2025   monthly (expanded from bi-annual)
  Rice area (Region VII) 2018 – 2025 monthly (expanded from bi-annual)
  Fertilizer prices    2021 – 2025   monthly

Rows where feature columns are NaN are handled downstream in preprocess.py
via linear interpolation; severely sparse rows are dropped there.

Output: data/processed/merged_dataset.csv
"""

import pandas as pd
from pathlib import Path

from src.data.load_data import (
    load_rice_price,
    load_fertilizer,
    load_palay_production,
    load_yield,
    load_rice_area,
    load_weather,
)
from src.utils.helpers import get_logger

logger = get_logger(__name__)

PROCESSED_DIR = Path(__file__).resolve().parents[2] / "data" / "processed"


def merge_datasets(out_path: Path = None) -> pd.DataFrame:
    """
    Loads every source and left-joins them on (year, month).

    Parameters
    ----------
    out_path : where to save the merged CSV (default: data/processed/merged_dataset.csv)

    Returns
    -------
    Merged DataFrame sorted chronologically.
    """
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    out_path = out_path or PROCESSED_DIR / "merged_dataset.csv"

    logger.info("=" * 55)
    logger.info("MERGING ALL DATA SOURCES")
    logger.info("=" * 55)

    # ── Load ──────────────────────────────────────────────────
    rice_df   = load_rice_price()
    fert_df   = load_fertilizer()
    palay_df  = load_palay_production()
    yield_df  = load_yield()
    area_df   = load_rice_area()
    weather_df = load_weather()

    # ── Merge (anchor = rice prices) ──────────────────────────
    merged = rice_df.copy()

    for name, df in [
        ("palay production", palay_df),
        ("weather",          weather_df),
        ("yield",            yield_df),
        ("rice area",        area_df),
        ("fertilizer",       fert_df),
    ]:
        before = len(merged)
        merged = merged.merge(df, on=["year", "month"], how="left")
        logger.info(
            f"  + {name:<22} → {len(merged)} rows  "
            f"({merged.isnull().mean().mean()*100:.1f}% NaN overall)"
        )
        assert len(merged) == before, \
            f"Row count changed after merging {name} — check for duplicate (year, month) keys."

    merged = merged.sort_values(["year", "month"]).reset_index(drop=True)

    logger.info(f"\nFinal merged shape : {merged.shape}")
    logger.info(f"Year range         : {merged['year'].min()} – {merged['year'].max()}")
    logger.info(f"Columns            : {list(merged.columns)}")

    # Coverage report: how many non-NaN values per column
    coverage = merged.notna().sum()
    logger.info("\nColumn coverage (non-NaN rows):")
    for col, cnt in coverage.items():
        pct = cnt / len(merged) * 100
        logger.info(f"  {col:<35} {cnt:>4} / {len(merged)}  ({pct:.0f}%)")

    merged.to_csv(out_path, index=False)
    logger.info(f"\nSaved merged dataset → {out_path}")

    return merged
