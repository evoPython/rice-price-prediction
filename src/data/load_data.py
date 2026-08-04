"""
load_data.py
------------
Loaders for every raw data file in data/raw/.
Each function returns a tidy DataFrame with (year, month) integer columns
so all sources can be joined in merge_data.py.

File layout expected under data/raw/:
  Price Data/
    wfp_food_prices_phl.csv
  Rice Factors Data/
    PALAY_PRODUCTION_quarterly.csv
    FERTILIZER_PRICES_monthly.csv
    YIELD_bi-annual.csv
    RICE_AREA_bi-annual.csv
  Weather Data/
    Mactan Monthly Data.csv
"""

import re
import numpy as np
import pandas as pd
from pathlib import Path

from src.utils.helpers import get_logger

logger = get_logger(__name__)

RAW_DIR       = Path(__file__).resolve().parents[2] / "data" / "raw"
PRICE_DIR     = RAW_DIR / "Price Data"
FACTORS_DIR   = RAW_DIR / "Rice Factors Data"
WEATHER_DIR   = RAW_DIR / "Weather Data"


# ─────────────────────────────────────────────────────────────────────────────
# 1. WFP Rice Prices  (monthly, Cebu / Region VII, 2000-2025)
# ─────────────────────────────────────────────────────────────────────────────

# Map from WFP commodity name → clean column suffix
_RICE_COMMODITY_MAP = {
    "rice (well milled)":    "well_milled",
    "rice (regular, milled)": "regular_milled",
    "rice (milled, superior)": "superior_milled",
    "rice (premium)":         "premium",
    "rice (special)":         "special",
}


def load_rice_price(path: Path = None) -> pd.DataFrame:
    """
    Loads the WFP food prices CSV, filters to Cebu / Region VII retail rice
    prices, and pivots to wide format: one column per rice classification.

    Returns
    -------
    DataFrame with columns: year, month, price_<classification>
    """
    path = path or PRICE_DIR / "wfp_food_prices_phl.csv"
    logger.info(f"Loading rice prices from: {path.name}")

    # Row 0 = real column headers; row 1 = HXL tag row (skip it)
    df = pd.read_csv(path, skiprows=[1], low_memory=False)
    df.columns = df.columns.str.strip().str.lower()

    # Keep only Cebu / Region VII
    region_mask = (
        df["admin1"].str.contains("region vii|central visayas", case=False, na=False)
        | df["admin2"].str.contains("cebu", case=False, na=False)
        | df["market"].str.contains("cebu", case=False, na=False)
    )
    # Keep only retail rice
    rice_mask = df["commodity"].str.lower().str.contains("rice", na=False)
    retail_mask = df["pricetype"].str.lower().str.strip() == "retail"

    df = df[region_mask & rice_mask & retail_mask].copy()

    # Parse date
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date", "price"])
    df["year"]  = df["date"].dt.year
    df["month"] = df["date"].dt.month

    # Normalise commodity name → clean label
    df["_comm"] = df["commodity"].str.lower().str.strip()
    df["_label"] = df["_comm"].map(_RICE_COMMODITY_MAP)

    # Drop unknown commodities
    df = df.dropna(subset=["_label"])

    # Monthly average per classification
    pivot = (
        df.groupby(["year", "month", "_label"])["price"]
        .mean()
        .unstack("_label")
        .reset_index()
    )
    pivot.columns.name = None
    pivot.columns = (
        ["year", "month"]
        + [f"price_{c}" for c in pivot.columns[2:]]
    )
    pivot = pivot.sort_values(["year", "month"]).reset_index(drop=True)

    logger.info(
        f"Rice price rows: {len(pivot)} | "
        f"Classifications: {[c for c in pivot.columns if c.startswith('price_')]}"
    )
    return pivot


# ─────────────────────────────────────────────────────────────────────────────
# 2. Fertilizer Prices  (monthly, Region VII, 2021-2025)
# ─────────────────────────────────────────────────────────────────────────────

_FERT_COLS = ["Urea_Prilled", "Urea_Granular", "Ammosul",
              "Complete", "Ammophos", "MOP", "DAP"]

_MONTH_MAP = {
    "january":1,"february":2,"march":3,"april":4,
    "may":5,"june":6,"july":7,"august":8,
    "september":9,"october":10,"november":11,"december":12,
}


def load_fertilizer(path: Path = None) -> pd.DataFrame:
    """
    Loads FERTILIZER_PRICES_monthly.csv, filters to Region VII (Cebu province
    where available, otherwise all R7 provinces), and returns a single monthly
    average per fertilizer type.

    Returns
    -------
    DataFrame with columns: year, month, fert_<type>
    """
    path = path or FACTORS_DIR / "FERTILIZER_PRICES_monthly.csv"
    logger.info(f"Loading fertilizer prices from: {path.name}")

    df = pd.read_csv(path)
    df.columns = df.columns.str.strip()

    # Normalise month string → integer
    df["month"] = df["Month"].str.lower().str.strip().map(_MONTH_MAP)
    df["year"]  = df["Year"].astype(int)

    # Filter Region VII; prefer Cebu province but fall back to all R7 if needed
    r7 = df[df["Region"].str.contains("REGION VII", case=False, na=False)].copy()
    cebu_only = r7[r7["Province"].str.contains("Cebu", case=False, na=False)]
    source = cebu_only if not cebu_only.empty else r7

    # Monthly mean across any remaining provinces
    agg = (
        source.groupby(["year", "month"])[_FERT_COLS]
        .mean()
        .reset_index()
    )
    agg.columns = (
        ["year", "month"]
        + [f"fert_{c.lower()}" for c in _FERT_COLS]
    )
    agg = agg.sort_values(["year", "month"]).reset_index(drop=True)

    logger.info(f"Fertilizer rows: {len(agg)}")
    return agg


# ─────────────────────────────────────────────────────────────────────────────
# 3. Palay Production  (quarterly → monthly, Cebu, 1987-2025)
# ─────────────────────────────────────────────────────────────────────────────

# Q1 = Jan-Mar, Q2 = Apr-Jun, Q3 = Jul-Sep, Q4 = Oct-Dec
_QUARTER_MONTHS = {1: [1, 2, 3], 2: [4, 5, 6], 3: [7, 8, 9], 4: [10, 11, 12]}


def _parse_palay_wide(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms the PSA wide-format palay table into a long monthly DataFrame
    for Cebu province (Irrigated + Rainfed combined).

    Column names look like: "1987 Quarter 1", "1987 Semester 2", "1988 Annual"
    We extract only the Quarter columns, discard Semester and Annual totals.
    """
    # Rows of interest: Cebu province only ("....Cebu" geolocation)
    cebu_mask = df_raw["Geolocation"].str.strip().str.lower() == "....cebu"
    df_cebu = df_raw[cebu_mask].copy()

    if df_cebu.empty:
        logger.warning("No Cebu rows found in PALAY_PRODUCTION. Check 'Geolocation' column.")
        return pd.DataFrame(columns=["year", "month", "palay_production_mt"])

    # Identify quarter columns
    quarter_cols = [
        c for c in df_raw.columns
        if re.match(r"^\d{4} Quarter [1-4]$", c.strip())
    ]

    rows = []
    for _, row in df_cebu.iterrows():
        for col in quarter_cols:
            match = re.match(r"^(\d{4}) Quarter ([1-4])$", col.strip())
            if not match:
                continue
            year, quarter = int(match.group(1)), int(match.group(2))
            val = row[col]
            try:
                val = float(val)
            except (TypeError, ValueError):
                val = np.nan

            for month in _QUARTER_MONTHS[quarter]:
                rows.append({"year": year, "month": month,
                             "ecosystem": str(row.get("Ecosystem/Croptype", "")).strip(),
                             "palay_mt": val})

    long = pd.DataFrame(rows)
    if long.empty:
        return pd.DataFrame(columns=["year", "month", "palay_production_mt"])

    # Sum irrigated + rainfed for each year-month
    monthly = (
        long.groupby(["year", "month"])["palay_mt"]
        .sum(min_count=1)          # NaN if all components are NaN
        .reset_index()
        .rename(columns={"palay_mt": "palay_production_mt"})
    )
    return monthly.sort_values(["year", "month"]).reset_index(drop=True)


def load_palay_production(path: Path = None) -> pd.DataFrame:
    """
    Loads PALAY_PRODUCTION_quarterly.csv and returns a monthly DataFrame
    with combined Irrigated + Rainfed palay production (metric tons) for Cebu.

    Returns
    -------
    DataFrame with columns: year, month, palay_production_mt
    """
    path = path or FACTORS_DIR / "PALAY_PRODUCTION_quarterly.csv"
    logger.info(f"Loading palay production from: {path.name}")

    # Row 0 = document title; row 1 = blank; row 2 = actual headers
    raw = pd.read_csv(path, skiprows=2)
    raw.columns = raw.columns.str.strip()

    monthly = _parse_palay_wide(raw)
    logger.info(f"Palay production rows (monthly): {len(monthly)}")
    return monthly


# ─────────────────────────────────────────────────────────────────────────────
# 4. Yield  (bi-annual → monthly, Region VII, 2018-2025)
# ─────────────────────────────────────────────────────────────────────────────

def load_yield(path: Path = None) -> pd.DataFrame:
    """
    Loads YIELD_bi-annual.csv and expands each semester to 6 monthly rows
    by forward-filling the semester value.

    Returns
    -------
    DataFrame with columns: year, month, avg_yield_ton_per_ha
    """
    path = path or FACTORS_DIR / "YIELD_bi-annual.csv"
    logger.info(f"Loading yield data from: {path.name}")

    df = pd.read_csv(path)
    rows = []
    for _, row in df.iterrows():
        start = 1 if int(row["semester"]) == 1 else 7
        for m in range(start, start + 6):
            rows.append({"year": int(row["year"]), "month": m,
                         "avg_yield_ton_per_ha": float(row["avg_yield_ton_per_ha"])})

    monthly = pd.DataFrame(rows).sort_values(["year", "month"]).reset_index(drop=True)
    logger.info(f"Yield rows (monthly): {len(monthly)}")
    return monthly


# ─────────────────────────────────────────────────────────────────────────────
# 5. Rice Area  (bi-annual → monthly, Region VII, 2018-2025)
# ─────────────────────────────────────────────────────────────────────────────

def load_rice_area(path: Path = None) -> pd.DataFrame:
    """
    Loads RICE_AREA_bi-annual.csv and expands each semester to 6 monthly rows.

    Returns
    -------
    DataFrame with columns: year, month, rice_area_ha
    """
    path = path or FACTORS_DIR / "RICE_AREA_bi-annual.csv"
    logger.info(f"Loading rice area data from: {path.name}")

    df = pd.read_csv(path)
    rows = []
    for _, row in df.iterrows():
        start = 1 if int(row["semester"]) == 1 else 7
        for m in range(start, start + 6):
            rows.append({"year": int(row["year"]), "month": m,
                         "rice_area_ha": float(row["rice_area_ha"])})

    monthly = pd.DataFrame(rows).sort_values(["year", "month"]).reset_index(drop=True)
    logger.info(f"Rice area rows (monthly): {len(monthly)}")
    return monthly


# ─────────────────────────────────────────────────────────────────────────────
# 6. Weather  (monthly, Mactan/Cebu station, 2015-2024)
# ─────────────────────────────────────────────────────────────────────────────

_WEATHER_COLS = ["RAINFALL", "TMAX", "TMIN", "RH", "WIND_SPEED"]
_SENTINEL     = -999          # missing-value code used in the CSV


def load_weather(path: Path = None) -> pd.DataFrame:
    """
    Loads Mactan Monthly Data.csv (Cebu weather station).
    Replaces the -999 sentinel with NaN.
    WIND_DIRECTION is dropped as it is a circular/categorical variable
    that requires special encoding — exclude unless the study explicitly needs it.

    Returns
    -------
    DataFrame with columns: year, month, rainfall, tmax, tmin, rh, wind_speed
    """
    path = path or WEATHER_DIR / "Mactan Monthly Data.csv"
    logger.info(f"Loading weather data from: {path.name}")

    df = pd.read_csv(path)
    df.columns = df.columns.str.strip().str.upper()

    # Replace sentinel -999 with NaN across all numeric columns
    df.replace(_SENTINEL, np.nan, inplace=True)

    df = df.rename(columns={"YEAR": "year", "MONTH": "month"})

    keep = ["year", "month"] + [c for c in _WEATHER_COLS if c in df.columns]
    df = df[keep].copy()

    # Lowercase feature column names for consistency
    df.columns = (
        ["year", "month"]
        + [c.lower() for c in df.columns[2:]]
    )
    df = df.sort_values(["year", "month"]).reset_index(drop=True)

    logger.info(f"Weather rows: {len(df)}")
    return df
