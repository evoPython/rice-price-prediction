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
# 1b. PSA OpenStat Wholesale Rice Prices  (monthly, Cebu, 2010-present)
# ─────────────────────────────────────────────────────────────────────────────
# This is now the PRIMARY rice price source (replaces WFP above). PSA OpenStat's
# "Cereals: Wholesale Selling Prices of Agricultural Commodities" table gives
# Cebu-specific monthly prices back to 2010, and — unlike WFP — is kept current:
# Premium / Well Milled / Regular Milled all have data through the most recent
# published month. Rice Special is NOT usable here: Cebu reporting for it is
# extremely sparse even 2011-2019 (multi-year gaps) and stops entirely after
# January 2019 — it is loaded but will very likely get dropped by preprocess.py's
# sparse-column/gap-limited-interpolation logic, which is the correct outcome.
#
# load_rice_price() (WFP, above) is kept for reference / provenance but is no
# longer called by merge_data.py.

PSA_MIN_YEAR = 2011
PSA_MAX_YEAR = 2026

_PSA_COMMODITY_MAP = {
    "rice special":                "special",
    "rice premium":                "premium",
    "well milled rice (wmr)":      "well_milled",
    "regular milled rice (rmr)":   "regular_milled",
}

_PSA_MONTH_MAP = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}


def load_rice_price_psa(path: Path = None,
                         min_year: int = PSA_MIN_YEAR,
                         max_year: int = PSA_MAX_YEAR) -> pd.DataFrame:
    """
    Loads the PSA OpenStat wholesale rice price export for Cebu
    ("Cereals: Wholesale Selling Prices of Agricultural Commodities"),
    a wide table with one row per commodity and one column per
    "<year> <month name>".

    Missing/unreported cells are coded ".." in the source and become NaN
    here. Gap-filling (linear interpolation, capped at
    MAX_INTERPOLATION_GAP_MONTHS) happens later in preprocess.py, alongside
    every other price source — not here — so both sources are treated with
    the same policy and we never silently extrapolate across multi-year gaps
    (which would otherwise happen for Rice Special).

    Returns
    -------
    DataFrame with columns: year, month, price_<classification>
    """
    path = path or PRICE_DIR / "psa_wholesale_rice_prices_cebu.csv"
    logger.info(f"Loading PSA wholesale rice prices from: {path.name}")

    # Row 0 = document title, row 1 = blank, row 2 = real header
    df = pd.read_csv(path, skiprows=2)
    df.columns = df.columns.str.strip()

    # Keep only Cebu rows (Geolocation looks like "....Cebu")
    cebu_mask = df["Geolocation"].str.strip().str.lower().str.contains("cebu", na=False)
    df = df[cebu_mask].copy()
    if df.empty:
        logger.warning("No Cebu rows found in PSA rice price export.")
        return pd.DataFrame(columns=["year", "month"])

    month_cols = [c for c in df.columns if c not in ("Geolocation", "Commodity")]

    long = df.melt(id_vars=["Geolocation", "Commodity"], value_vars=month_cols,
                    var_name="ym", value_name="price")

    # ".." (and any other non-numeric placeholder) -> NaN
    long["price"] = pd.to_numeric(long["price"].replace("..", pd.NA), errors="coerce")

    ym_split = long["ym"].str.strip().str.split(" ", n=1, expand=True)
    long["year"] = pd.to_numeric(ym_split[0], errors="coerce")
    long["month"] = ym_split[1].str.strip().str.lower().map(_PSA_MONTH_MAP)
    long = long.dropna(subset=["year", "month"])
    long["year"] = long["year"].astype(int)
    long["month"] = long["month"].astype(int)

    long["_label"] = long["Commodity"].str.strip().str.lower().map(_PSA_COMMODITY_MAP)
    unknown = long[long["_label"].isna()]["Commodity"].unique()
    if len(unknown):
        logger.warning(f"Unmapped PSA commodities (dropped): {list(unknown)}")
    long = long.dropna(subset=["_label"])

    long = long[(long["year"] >= min_year) & (long["year"] <= max_year)]

    pivot = (
        long.groupby(["year", "month", "_label"])["price"]
        .mean()
        .unstack("_label")
        .reset_index()
    )
    pivot.columns.name = None
    pivot.columns = ["year", "month"] + [f"price_{c}" for c in pivot.columns[2:]]

    # Drop trailing rows where every price column is NaN (i.e. months that
    # simply haven't been published yet, not real historical gaps).
    price_cols = [c for c in pivot.columns if c.startswith("price_")]
    pivot = pivot.sort_values(["year", "month"]).reset_index(drop=True)
    has_any = pivot[price_cols].notna().any(axis=1)
    if has_any.any():
        last_valid_idx = has_any[has_any].index.max()
        pivot = pivot.loc[:last_valid_idx].reset_index(drop=True)

    coverage = pivot[price_cols].notna().sum()
    logger.info(f"PSA rice price rows: {len(pivot)} ({pivot['year'].min()}-{pivot['year'].max()})")
    for c in price_cols:
        logger.info(f"  {c:<22} {coverage[c]:>4} / {len(pivot)} months present")

    return pivot




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
# 3b. Rice Stock Inventory  (monthly, NATIONAL — not Cebu-specific, 2010-present)
# ─────────────────────────────────────────────────────────────────────────────
# PSA does not publish province-level rice stock inventories, so this is a
# national aggregate (Philippines-wide), unlike the other Cebu/Region-VII
# sources. It's still a plausible macro signal — national supply pressure can
# feed through to local retail prices — but it is NOT a Cebu-specific factor,
# and that distinction is worth keeping in mind when interpreting results.
#
# The source table also has Household / Commercial / NFA stock breakdowns,
# which sum to Total Stock by construction. Only Total Stock is loaded here
# to avoid reintroducing the same kind of collinearity problem we already
# found and fixed once with the sibling rice-price columns (see
# preprocess.py's sibling_price_cols logic). If you want the sector
# breakdown for its own sake, load_rice_stock(..., sector=...) below.

_RICE_STOCK_SECTOR_MAP = {
    "Rice: Total Stock":      "rice_stock_total_mt",
    "Rice: Household Stock":  "rice_stock_household_mt",
    "Rice: Commercial Stock": "rice_stock_commercial_mt",
    "Rice: NFA Stock":        "rice_stock_nfa_mt",
}


def load_rice_stock(path: Path = None, sectors: list[str] | None = None) -> pd.DataFrame:
    """
    Loads PSA's "Rice and Corn: Monthly Total Stocks Inventory by Sector"
    export (national). By default returns only the Total Stock series as
    `rice_stock_total_mt` to avoid collinearity with its own sub-components.

    Parameters
    ----------
    sectors : subset of {"rice_stock_total_mt", "rice_stock_household_mt",
              "rice_stock_commercial_mt", "rice_stock_nfa_mt"}.
              Default: ["rice_stock_total_mt"] only.

    Returns
    -------
    DataFrame with columns: year, month, <requested sector columns>
    """
    path = path or FACTORS_DIR / "RICE_STOCK_INVENTORY_monthly.csv"
    sectors = sectors or ["rice_stock_total_mt"]
    logger.info(f"Loading rice stock inventory from: {path.name}")

    df = pd.read_csv(path, skiprows=2)
    df.columns = df.columns.str.strip()
    df["_label"] = df["Sector"].str.strip().map(_RICE_STOCK_SECTOR_MAP)
    df = df.dropna(subset=["_label"])
    df = df[df["_label"].isin(sectors)]

    month_cols = [c for c in df.columns if c not in ("Sector", "Year", "_label")]
    long = df.melt(id_vars=["Year", "_label"], value_vars=month_cols,
                    var_name="month_name", value_name="value")
    long["value"] = pd.to_numeric(long["value"].replace("..", pd.NA), errors="coerce")
    long["year"] = long["Year"].astype(int)
    long["month"] = long["month_name"].str.strip().str.lower().map(_PSA_MONTH_MAP)
    long = long.dropna(subset=["month"])
    long["month"] = long["month"].astype(int)

    pivot = (
        long.groupby(["year", "month", "_label"])["value"]
        .mean()
        .unstack("_label")
        .reset_index()
    )
    pivot.columns.name = None
    pivot = pivot.sort_values(["year", "month"]).reset_index(drop=True)

    # Drop trailing rows where every requested column is NaN (unpublished
    # future months), same logic as load_rice_price_psa.
    value_cols = [c for c in pivot.columns if c not in ("year", "month")]
    has_any = pivot[value_cols].notna().any(axis=1)
    if has_any.any():
        pivot = pivot.loc[:has_any[has_any].index.max()].reset_index(drop=True)

    logger.info(f"Rice stock rows: {len(pivot)} | columns: {value_cols}")
    return pivot


# ─────────────────────────────────────────────────────────────────────────────
# 3c. Rice Inflation (Ricelytics / DA-PhilRice, sourced from PSA CPI, Cebu, 2011-present)
# ─────────────────────────────────────────────────────────────────────────────
# Ricelytics (ricelytics.philrice.gov.ph) is a legitimate DA-PhilRice platform
# built on PSA data, with province-level filtering (Cebu is one of the listed
# provinces) — so this is plausibly a genuine Cebu-specific series, distinct
# from the PSA wholesale price series used elsewhere in this pipeline (CPI
# rice sub-index / retail basket vs. wholesale spot price — different
# methodology, so don't expect a 1:1 match with our own price series).
#
# IMPORTANT — timing/leakage caveat: rice_inflation is a year-over-year %
# change figure, which by construction is computed FROM that month's own
# price level. Using month t's inflation figure as a feature to predict
# month t's price is uncomfortably close to using the target to predict
# itself, even though it's a different underlying price series. This loader
# returns LAGGED versions (suffixed _lag1) as the ones actually intended for
# use as features — i.e. "last month's published inflation rate" — plus the
# raw contemporaneous columns for reference/diagnostics only. Prefer the
# _lag1 versions unless you've specifically checked the contemporaneous ones
# don't just inflate apparent accuracy through leakage.

def load_rice_inflation(path: Path = None, lag: int = 1) -> pd.DataFrame:
    """
    Loads the Ricelytics rice inflation export (year, month, headline_inflation,
    rice_inflation, rice_contribution) and returns both the raw contemporaneous
    columns and a lagged version of each (suffixed _lag{lag}).

    Returns
    -------
    DataFrame with columns: year, month, headline_inflation, rice_inflation,
    rice_contribution, headline_inflation_lag{lag}, rice_inflation_lag{lag},
    rice_contribution_lag{lag}
    """
    path = path or FACTORS_DIR / "RICE_INFLATION_CEBU_monthly.csv"
    logger.info(f"Loading rice inflation data from: {path.name}")

    df = pd.read_csv(path)
    df.columns = df.columns.str.strip()
    df = df.sort_values(["year", "month"]).reset_index(drop=True)

    value_cols = ["headline_inflation", "rice_inflation", "rice_contribution"]
    # Confirm monthly contiguity before shifting — a lag is only valid if
    # there's no gap between consecutive rows.
    expected = pd.date_range(
        f"{df['year'].iloc[0]}-{df['month'].iloc[0]:02d}-01",
        f"{df['year'].iloc[-1]}-{df['month'].iloc[-1]:02d}-01",
        freq="MS",
    )
    actual = pd.to_datetime(df["year"].astype(str) + "-" + df["month"].astype(str) + "-01")
    if len(expected) != len(actual) or not (expected == actual).all():
        logger.warning("Rice inflation data is not perfectly contiguous month-to-month; "
                       "lag features may be misaligned across the gap.")

    for c in value_cols:
        df[f"{c}_lag{lag}"] = df[c].shift(lag)

    logger.info(f"Rice inflation rows: {len(df)} ({df['year'].min()}-{df['year'].max()})")
    return df


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

    NOTE: as of the Open-Meteo integration below, this source's tmax/tmin/
    rainfall are superseded (Open-Meteo covers 2010-2026 vs. this station's
    2015-2024, and is directly re-downloadable via API). This function is
    kept because RH and wind speed aren't in the Open-Meteo query — see
    load_weather_mactan_humidity_wind() for the subset actually merged.

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


def load_weather_mactan_humidity_wind(path: Path = None) -> pd.DataFrame:
    """
    Subset of the legacy Mactan station data actually merged going forward:
    just relative humidity and wind speed, since Open-Meteo (below) covers
    temperature/rainfall with better date range and is the preferred source
    for those. Avoids merging two competing measurements of the same
    variable (e.g. two different "tmax" columns) side by side.

    Returns
    -------
    DataFrame with columns: year, month, rh, wind_speed
    """
    full = load_weather(path)
    keep = [c for c in ["year", "month", "rh", "wind_speed"] if c in full.columns]
    return full[keep].copy()


# ─────────────────────────────────────────────────────────────────────────────
# 6b. Open-Meteo Daily Weather → Monthly  (Cebu ~10.37N 123.80E, 2010-present)
# ─────────────────────────────────────────────────────────────────────────────
# Direct, re-downloadable API source (see PSA_HEADERS-style note in
# download_data.py). Broader range and more current than the Mactan station
# file, and adds soil moisture / precipitation-hours, which weren't
# available before and are directly agronomically relevant.

OPENMETEO_URL = (
    "https://archive-api.open-meteo.com/v1/archive"
    "?latitude=10.3333&longitude=123.75&start_date=2010-01-01&end_date=2026-08-07"
    "&daily=temperature_2m_mean,temperature_2m_max,temperature_2m_min,"
    "precipitation_sum,precipitation_hours,soil_moisture_0_to_100cm_mean"
    "&timezone=auto&format=csv"
)


def load_weather_openmeteo(path: Path = None) -> pd.DataFrame:
    """
    Loads the Open-Meteo daily archive CSV and aggregates to monthly:
      rainfall      = sum(daily precipitation_sum)
      rain_hours    = sum(daily precipitation_hours)
      tmax / tmin   = mean(daily max / min)
      tmean         = mean(daily mean)
      soil_moisture = mean(daily soil_moisture_0_to_100cm_mean)

    Any month that isn't fully covered by the data (i.e. the partial month
    at the tail end, since the source is a daily archive with a specific
    cutoff date) is dropped rather than aggregated from a handful of days —
    a partial-month rainfall SUM in particular would be badly misleading.

    Returns
    -------
    DataFrame with columns: year, month, rainfall, rain_hours, tmax, tmin,
                             tmean, soil_moisture
    """
    path = path or WEATHER_DIR / "open_meteo_daily_2010_2026.csv"
    logger.info(f"Loading Open-Meteo daily weather from: {path.name}")

    df = pd.read_csv(path, skiprows=3)
    df.columns = [c.split(" (")[0].strip() for c in df.columns]  # strip " (°C)" etc.

    df["time"] = pd.to_datetime(df["time"])
    df["year"] = df["time"].dt.year
    df["month"] = df["time"].dt.month
    df["_days_in_month"] = df["time"].dt.days_in_month

    agg = (
        df.groupby(["year", "month"])
        .agg(
            rainfall=("precipitation_sum", "sum"),
            rain_hours=("precipitation_hours", "sum"),
            tmax=("temperature_2m_max", "mean"),
            tmin=("temperature_2m_min", "mean"),
            tmean=("temperature_2m_mean", "mean"),
            soil_moisture=("soil_moisture_0_to_100cm_mean", "mean"),
            n_days=("time", "count"),
            days_in_month=("_days_in_month", "first"),
        )
        .reset_index()
    )

    complete = agg["n_days"] >= agg["days_in_month"]
    n_dropped = (~complete).sum()
    if n_dropped:
        dropped_months = agg.loc[~complete, ["year", "month"]].values.tolist()
        logger.info(f"Dropping {n_dropped} incomplete month(s) from Open-Meteo aggregation: {dropped_months}")
    agg = agg[complete].drop(columns=["n_days", "days_in_month"]).reset_index(drop=True)

    logger.info(f"Open-Meteo monthly rows: {len(agg)}")
    return agg
