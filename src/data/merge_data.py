"""
merge_data.py
-------------
Joins the rice price series with every current factor source into a single
flat DataFrame keyed on (year, month). Uses a left join anchored on the rice
price series so every row has a target value.

  PSA rice prices        2011 – 2026   monthly (Premium / WMR / RMR current to
                                        most recent published month; Rice
                                        Special is too sparse to use post-2019)
  Rice stock (NATIONAL)  2010 – 2026   monthly (not Cebu-specific — PSA doesn't
                                        publish province-level stock data)
  Rice inflation (Cebu)  2011 – 2026   monthly (Ricelytics/DA-PhilRice, PSA CPI
                                        source; contemporaneous columns kept for
                                        reference, _lag1 versions are the ones
                                        actually meant to be used as features —
                                        see load_rice_inflation() docstring)
  Open-Meteo weather     2010 – 2026   daily → monthly (tmax/tmin/tmean,
                                        rainfall, rain_hours, soil_moisture)
  Mactan weather (RH/wind) 2015-2024   monthly (rh, wind_speed only — temp/
                                        rainfall come from Open-Meteo instead)
  Palay production       1987 – 2025   monthly (expanded from quarterly)
  Yield (Region VII)     2018 – 2025   monthly (expanded from bi-annual)
  Rice area (Region VII) 2018 – 2025   monthly (expanded from bi-annual)
  Fertilizer prices      2021 – 2025   monthly

Rows where feature columns are NaN are handled downstream in preprocess.py
via linear interpolation; severely sparse rows/columns are dropped there.

Output: data/processed/merged_dataset.csv
"""

import pandas as pd
from pathlib import Path

from src.data.load_data import (
    load_rice_price_psa,
    load_rice_stock,
    load_rice_inflation,
    load_weather_openmeteo,
    load_weather_mactan_humidity_wind,
    load_palay_production,
    load_yield,
    load_rice_area,
    load_fertilizer,
)
from src.utils.helpers import get_logger

logger = get_logger(__name__)

PROCESSED_DIR = Path(__file__).resolve().parents[2] / "data" / "processed"


def merge_datasets(out_path: Path = None) -> pd.DataFrame:
    """
    Loads every current factor source and left-joins them onto the rice
    price series on (year, month).

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
    rice_df       = load_rice_price_psa()
    stock_df      = load_rice_stock()
    inflation_df  = load_rice_inflation()
    weather_df    = load_weather_openmeteo()
    weather_rh_df = load_weather_mactan_humidity_wind()
    palay_df      = load_palay_production()
    yield_df      = load_yield()
    area_df       = load_rice_area()
    fert_df       = load_fertilizer()

    # ── Merge (anchor = rice prices) ──────────────────────────
    merged = rice_df.copy()

    for name, df in [
        ("rice stock (national)",  stock_df),
        ("rice inflation (Cebu)",  inflation_df),
        ("weather (open-meteo)",   weather_df),
        ("weather (rh/wind)",      weather_rh_df),
        ("palay production",       palay_df),
        ("yield",                  yield_df),
        ("rice area",              area_df),
        ("fertilizer",             fert_df),
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
