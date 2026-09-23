import sys
import re
import requests
import pdfplumber
import pandas as pd
from pathlib import Path
from bs4 import BeautifulSoup
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt, Confirm
from rich.progress import Progress, BarColumn, DownloadColumn, TransferSpeedColumn, TimeRemainingColumn, TextColumn

console = Console()

# --- CONSTANTS ---
WFP_RICE_URL = "https://data.humdata.org/dataset/ea251823-8694-47b4-82d0-7d27f00e8aba/resource/9a842d72-0d7d-4922-ad0e-eb8106c1ab0e/download/wfp_food_prices_phl.csv"
FPA_BASE_URL = "https://fpa.da.gov.ph"
FPA_PAGE_URL = "https://fpa.da.gov.ph/weekly-prices/"
PRISM_BASE_URL = "https://prism.philrice.gov.ph/wp-dynamicreports/map/"
PRISM_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "X-Requested-With": "XMLHttpRequest",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Origin": "https://prism.philrice.gov.ph",
    "Referer": "https://prism.philrice.gov.ph/wp-dynamicreports/"
}

PSA_PALAY_API_URL = "https://openstat.psa.gov.ph/PXWeb/api/v1/en/DB/DB__2E__CS/0012E4EVCP0.px"
PSA_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Mobile Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Language": "en",
    "Origin": "https://openstat.psa.gov.ph",
    "Referer": "https://openstat.psa.gov.ph/PXWeb/pxweb/en/DB/DB__2E__CS/0012E4EVCP0.px/?rxid=bdf9d8da-96f1-4100-ae09-18cb3eaeb313",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

PSA_RICE_STOCK_SQ_URL = "https://openstat.psa.gov.ph:443/PXWeb/sq/3bb903d6-b28a-4b58-bcb3-905d29ecd300"

PSA_RICE_PRICE_SQ_URL = "https://openstat.psa.gov.ph:443/PXWeb/sq/2a91727a-050a-4bb1-80f4-5ba30383f5d3"

# Regex for exact Region VII match — avoids false-positive on "REGION VIII"
# \b ensures "VII" is a whole word, so "REGION VIII" is not matched
REGION_VII_PATTERN = re.compile(r"\bREGION\s+VII\b", re.IGNORECASE)


# ==========================================
# PSA WHOLESALE RICE PRICE DOWNLOADER (current primary source)
# ==========================================
def _download_psa_sq(sq_url: str, out_file: Path, title_fragments: list[str],
                      label: str) -> bool:
    """
    Shared best-effort downloader for PSA OpenStat "sq" (saved query)
    permalinks. See download_rice_price_psa for the verification caveat —
    this could not be tested against the live endpoint in development.
    """
    console.rule(f"[bold green]Downloading {label} (OpenStat)")
    out_file.parent.mkdir(parents=True, exist_ok=True)

    try:
        r = requests.get(
            sq_url,
            headers={**PSA_HEADERS, "Accept": "text/csv, application/csv, */*"},
            timeout=30,
        )
        r.raise_for_status()
    except Exception as e:
        console.print(f"[red]Request failed: {e}[/red]")
        console.print(
            "[yellow]Automated download didn't work. Please open the link below, "
            f"export as CSV, and save it to:[/yellow] {out_file}\n"
            f"  {sq_url}"
        )
        return False

    text_preview = r.text[:200] if r.text else ""
    looks_valid = all(frag in text_preview for frag in title_fragments)

    if not looks_valid:
        console.print(
            "[yellow]Response didn't look like the expected CSV export "
            "(this endpoint may return an HTML page rather than the raw file "
            "for a saved-query link). Please download manually instead:[/yellow]\n"
            f"  {sq_url}\n"
            f"  → save to: {out_file}"
        )
        return False

    out_file.write_bytes(r.content)
    console.print(f"[green]✔ Saved to:[/green] {out_file}")
    console.print(
        "[yellow]Please spot-check this file against a manual export at least once — "
        "this downloader path is unverified.[/yellow]\n"
    )
    return True


def download_rice_price_psa(out_dir: Path) -> bool:
    """
    Attempts to fetch the PSA OpenStat "Cereals: Wholesale Selling Prices of
    Agricultural Commodities" saved query (Cebu, Premium/WMR/RMR/Special,
    2010-present) directly from its PXWeb "sq" permalink.

    IMPORTANT — this could not be verified against the live PSA endpoint in
    development (network egress to openstat.psa.gov.ph was not available in
    that environment), so this is a best-effort attempt with a validated
    fallback, not a confirmed-working scraper. It checks that the response
    actually looks like the expected export (starts with the known title
    string) before saving, and clearly tells you to fall back to a manual
    download if that check fails — please verify the first run manually
    against the raw file before trusting an automated re-run.

    Manual fallback: open the link below in a browser, export/download as
    CSV, and save it to data/raw/Price Data/psa_wholesale_rice_prices_cebu.csv
      https://openstat.psa.gov.ph:443/PXWeb/sq/2a91727a-050a-4bb1-80f4-5ba30383f5d3
    """
    out_file = out_dir / "Price Data" / "psa_wholesale_rice_prices_cebu.csv"
    return _download_psa_sq(
        PSA_RICE_PRICE_SQ_URL, out_file,
        title_fragments=["Cereals", "Wholesale"],
        label="PSA Wholesale Rice Prices",
    )


def download_rice_stock(out_dir: Path) -> bool:
    """
    Attempts to fetch the PSA OpenStat "Rice and Corn: Monthly Total Stocks
    Inventory by Sector" saved query (national, 2010-present) directly from
    its PXWeb "sq" permalink. Same unverified-in-development caveat as
    download_rice_price_psa above.

    Manual fallback: open the link below, export as CSV, and save to
    data/raw/Rice Factors Data/RICE_STOCK_INVENTORY_monthly.csv
      https://openstat.psa.gov.ph:443/PXWeb/sq/3bb903d6-b28a-4b58-bcb3-905d29ecd300
    """
    out_file = out_dir / "Rice Factors Data" / "RICE_STOCK_INVENTORY_monthly.csv"
    return _download_psa_sq(
        PSA_RICE_STOCK_SQ_URL, out_file,
        title_fragments=["Rice and Corn", "Stocks"],
        label="PSA Rice Stock Inventory",
    )


def download_weather_openmeteo(out_dir: Path) -> bool:
    """
    Downloads the Open-Meteo daily archive directly — this one IS a plain,
    public, no-auth REST API (unlike the PSA endpoints above), so it's a
    normal GET rather than a best-effort scrape.
    """
    console.rule("[bold green]Downloading Open-Meteo Daily Weather Archive")
    out_file = out_dir / "Weather Data" / "open_meteo_daily_2010_2026.csv"
    out_file.parent.mkdir(parents=True, exist_ok=True)

    url = (
        "https://archive-api.open-meteo.com/v1/archive"
        "?latitude=10.3333&longitude=123.75&start_date=2010-01-01"
        f"&end_date={pd.Timestamp.today().date().isoformat()}"
        "&daily=temperature_2m_mean,temperature_2m_max,temperature_2m_min,"
        "precipitation_sum,precipitation_hours,soil_moisture_0_to_100cm_mean"
        "&timezone=auto&format=csv"
    )
    try:
        r = requests.get(url, timeout=60)
        r.raise_for_status()
    except Exception as e:
        console.print(f"[red]Request failed: {e}[/red]")
        return False

    out_file.write_bytes(r.content)
    console.print(f"[green]✔ Saved to:[/green] {out_file}\n")
    return True


# ==========================================
# WFP RICE PRICE DOWNLOADER (legacy — superseded by PSA above, kept for reference)
# ==========================================
def download_rice_price(out_dir: Path):
    console.rule("[bold green]Downloading WFP Rice Price Dataset")
    out_file = out_dir / "wfp_food_prices_phl.csv"

    with requests.get(WFP_RICE_URL, stream=True) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))

        columns = [
            TextColumn("[cyan]Downloading CSV"),
            BarColumn(),
            DownloadColumn(),
            TransferSpeedColumn(),
            TimeRemainingColumn(),
        ]

        with Progress(*columns, console=console) as progress:
            task = progress.add_task("download", total=total)
            with open(out_file, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        progress.update(task, advance=len(chunk))

    console.print(f"[green]✔ Saved to:[/green] {out_file}\n")


# ==========================================
# FERTILIZER DATA DOWNLOADER & PARSER
# ==========================================
def get_pdf_links():
    try:
        response = requests.get(FPA_PAGE_URL)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")
        links = []
        for a in soup.select("a[href$='.pdf']"):
            href = a['href']
            if "WFP" in href or "Weekly" in a.text or "FERTILIZER" in href.upper():
                full_url = href if href.startswith("http") else f"{FPA_BASE_URL}{href}"
                links.append(full_url)
        return list(set(links))
    except Exception as e:
        console.print(f"[red]Error fetching links: {e}[/red]")
        return []

def download_file(url, out_dir):
    filename = url.split("/")[-1]
    out_file = out_dir / filename

    if out_file.exists():
        return out_file

    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))

        columns = [
            TextColumn(f"[blue]{filename[:30]}..."),
            BarColumn(),
            DownloadColumn(),
            TransferSpeedColumn(),
            TimeRemainingColumn(),
        ]

        with Progress(*columns, console=console) as progress:
            task = progress.add_task("download", total=total)
            with open(out_file, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        progress.update(task, advance=len(chunk))
    return out_file

def clean_currency(value):
    if not value or pd.isna(value) or str(value).strip() == '':
        return None
    cleaned = str(value).replace(',', '').replace(' ', '').strip()
    try:
        return float(cleaned)
    except ValueError:
        return None

def create_entry(date, region, province, prices, filename):
    prices = (prices + [None]*7)[:7]
    return {
        "Date": date, "Month": date.month_name() if pd.notnull(date) else None, "Year": date.year if pd.notnull(date) else None,
        "Region": region, "Province": province, "Urea_Prilled": prices[0], "Urea_Granular": prices[1], "Ammosul": prices[2],
        "Complete": prices[3], "Ammophos": prices[4], "MOP": prices[5], "DAP": prices[6], "Source_File": filename
    }

def extract_date_from_filename(filename):
    filename_lower = filename.lower().replace('_', '-').replace('.', '-')
    year_match = re.search(r'(20\d{2})', filename_lower)
    year = year_match.group(1) if year_match else None

    months = ["january", "february", "march", "april", "may", "june", "july", "august", "september", "october", "november", "december", "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "sept", "oct", "nov", "dec"]
    months.sort(key=len, reverse=True)
    month_pattern = r'(' + '|'.join(months) + r')'

    month_match = re.search(month_pattern, filename_lower)
    month_str = month_match.group(1) if month_match else None

    if month_str and year:
        if month_str == 'sept': month_str = 'sep'
        try:
            return pd.to_datetime(f"{month_str} 1 {year}")
        except Exception:
            pass
    return pd.NaT

def parse_pdf(pdf_path):
    data = []
    file_date = extract_date_from_filename(pdf_path.name)

    if pd.isna(file_date): return pd.DataFrame()

    with pdfplumber.open(pdf_path) as pdf:
        current_region = "Unknown"
        for page in pdf.pages:
            for table in page.extract_tables():
                for row in table:
                    if not row or not str(row[0]).strip(): continue
                    raw_col = str(row[0]).strip()
                    first_col = raw_col.upper()

                    if "REGION" in first_col and "PROVINCE" in first_col: continue
                    if "FERTILIZER" in first_col or "UREA" in first_col: continue
                    if first_col.startswith("46-0-0") or first_col == "PRICE": continue

                    if "REGION" in first_col or first_col in ["CAR", "CARAGA", "BARMM"]:
                        lines = raw_col.split('\n')
                        current_region = lines[0].strip()

                        if len(lines) == 1:
                            prices = [clean_currency(c) for c in row[1:8]]
                            if any(prices): data.append(create_entry(file_date, current_region, "Regional Summary", prices, pdf_path.name))
                            continue
                        else:
                            province = " ".join(lines[1:]).strip()
                            prices = [clean_currency(c) for c in row[1:8]]
                            if any(prices): data.append(create_entry(file_date, current_region, province, prices, pdf_path.name))
                            continue

                    if first_col == "AVE" or first_col == "AVERAGE PRICE":
                        prices = [clean_currency(c) for c in row[1:8]]
                        if any(prices): data.append(create_entry(file_date, current_region, "Regional Average", prices, pdf_path.name))
                        continue

                    province = raw_col.replace('\n', ' ').strip()
                    prices = [clean_currency(c) for c in row[1:8]]

                    if any(prices): data.append(create_entry(file_date, current_region, province, prices, pdf_path.name))
    return pd.DataFrame(data)

def process_fertilizer_data(out_dir: Path):
    console.rule("[bold green]Fetching & Parsing FPA Fertilizer Data")
    raw_pdf_dir = out_dir / "fertilizer_pdfs"
    raw_pdf_dir.mkdir(parents=True, exist_ok=True)

    links = get_pdf_links()
    pdf_files = []

    if not links:
        console.print("[red]No PDF links found.[/red]")
        return

    console.print(f"Found [bold]{len(links)}[/bold] PDF links. Downloading...")
    for link in links:
        try:
            pdf_files.append(download_file(link, raw_pdf_dir))
        except Exception as e:
            console.print(f"[red]Failed to download {link}: {e}[/red]")

    all_data = []
    with Progress(console=console) as progress:
        task = progress.add_task("[cyan]Parsing PDFs to CSV...", total=len(pdf_files))
        for pdf_file in pdf_files:
            try:
                csv_name = pdf_file.with_suffix('.csv')
                if csv_name.exists():
                    df = pd.read_csv(csv_name)
                    df['Date'] = pd.to_datetime(df['Date'])
                    all_data.append(df)
                else:
                    df = parse_pdf(pdf_file)
                    if not df.empty:
                        df.to_csv(csv_name, index=False)
                        all_data.append(df)
            except Exception:
                pass
            progress.update(task, advance=1)

    if not all_data:
        console.print("[red]No data extracted.[/red]")
        return

    full_df = pd.concat(all_data, ignore_index=True)

    # FIX: Use word-boundary regex so "REGION VII" does not match "REGION VIII"
    region_7_df = full_df[
        full_df['Region'].str.contains(REGION_VII_PATTERN, na=False)
    ].copy()

    if region_7_df.empty:
        console.print("[yellow]No Region VII data found after extraction.[/yellow]")
        return

    # Filter to non-summary rows, then strictly to Cebu province only
    region_7_provinces = region_7_df[
        ~region_7_df['Province'].isin(['Regional Summary', 'Regional Average'])
    ].copy()

    # Strict Cebu match: strip, uppercase, exact equality
    region_7_provinces = region_7_provinces[
        region_7_provinces['Province'].str.strip().str.upper() == 'CEBU'
    ].copy()

    if region_7_provinces.empty:
        console.print("[yellow]No Cebu province data found in Region VII.[/yellow]")
        return

    monthly_df = region_7_provinces.groupby(['Year', 'Month', 'Region', 'Province']).agg({
        'Urea_Prilled': 'mean', 'Urea_Granular': 'mean', 'Ammosul': 'mean',
        'Complete': 'mean', 'Ammophos': 'mean', 'MOP': 'mean', 'DAP': 'mean'
    }).reset_index()

    monthly_df['Date'] = pd.to_datetime(monthly_df['Month'] + ' 1, ' + monthly_df['Year'].astype(str))
    cols = ['Date', 'Year', 'Month', 'Region', 'Province', 'Urea_Prilled', 'Urea_Granular', 'Ammosul', 'Complete', 'Ammophos', 'MOP', 'DAP']
    monthly_df = monthly_df[cols].sort_values(by=["Date", "Province"])

    out_file = out_dir / "cebu_monthly_fertilizer_prices.csv"
    monthly_df.to_csv(out_file, index=False)

    console.print(f"[green]✔ Cebu (Region VII) fertilizer data saved to:[/green] {out_file}\n")


# ==========================================
# YIELD DATA DOWNLOADER
# ==========================================
def extract_value_from_table(html, target_name, table_id="RA_table"):
    soup = BeautifulSoup(html, "html.parser")
    for row in soup.select(f"#{table_id} tbody tr"):
        cols = row.find_all(["th", "td"])
        if len(cols) >= 2:
            if cols[0].get_text(strip=True) == target_name:
                try: return float(cols[1].get_text(strip=True).replace(",", ""))
                except: return None
    return None

def download_yield_data(out_dir: Path):
    console.rule("[bold green]Scraping PRISM Yield & Rice Area Data")

    rice_area_results = []
    yield_results = []
    years = list(range(2026, 2017, -1))
    sems = [1, 2]

    with Progress(TextColumn("[cyan]{task.description}"), BarColumn(), TextColumn("[progress.percentage]{task.percentage:>3.0f}%"), TimeRemainingColumn(), console=console) as progress:
        task = progress.add_task("Querying PRISM Database...", total=len(years)*len(sems))

        for year in years:
            for sem in sems:
                date_label = f"{year}-S{sem}"

                try: # Rice Area
                    r_ra = requests.post(PRISM_BASE_URL + "RA", headers=PRISM_HEADERS, data={"year": year, "sem": sem, "region": 0}, timeout=15)
                    if r_ra.status_code == 200:
                        ra_value = extract_value_from_table(r_ra.text, "Region VII")
                        if ra_value is not None:
                            rice_area_results.append({"date": date_label, "year": year, "semester": sem, "region": "Region VII", "rice_area_ha": ra_value})
                except Exception: pass

                try: # Yield
                    r_yld = requests.post(PRISM_BASE_URL + "yield_nodrill", headers=PRISM_HEADERS, data={"year": year, "sem": sem, "region": 7}, timeout=15)
                    if r_yld.status_code == 200:
                        soup = BeautifulSoup(r_yld.text, "html.parser")
                        yields = [float(cols[1].get_text(strip=True).replace(",", "")) for row in soup.select("#RA_table tbody tr") if len(cols := row.find_all(["th", "td"])) >= 2]
                        if yields:
                            yield_results.append({"date": date_label, "year": year, "semester": sem, "region": "Region VII", "avg_yield_ton_per_ha": sum(yields)/len(yields)})
                except Exception: pass
                progress.update(task, advance=1)

    df_ra = pd.DataFrame(rice_area_results).sort_values(by=["year", "semester"]).reset_index(drop=True)
    file_ra = out_dir / "rice_area.csv"
    df_ra.to_csv(file_ra, index=False)

    df_yield = pd.DataFrame(yield_results).sort_values(by=["year", "semester"]).reset_index(drop=True)
    file_yield = out_dir / "yield.csv"
    df_yield.to_csv(file_yield, index=False)

    console.print(f"[green]✔ Yield data saved to:[/green] {file_yield}")
    console.print(f"[green]✔ Rice area data saved to:[/green] {file_ra}\n")


# ==========================================
# PSA PALAY PRODUCTION DOWNLOADER
# ==========================================
def download_palay_production(out_dir: Path):
    console.rule("[bold green]Downloading PSA Palay Production Data (OpenStat)")

    out_file = out_dir / "palay_production_psa.csv"

    console.print("[cyan]Fetching dataset metadata from PSA OpenStat...[/cyan]")

    try:
        # Step 1: GET metadata to discover all variable codes and their values
        meta_r = requests.get(PSA_PALAY_API_URL, headers=PSA_HEADERS, timeout=30)
        meta_r.raise_for_status()
        meta = meta_r.json()
    except Exception as e:
        console.print(f"[red]Failed to fetch metadata: {e}[/red]")
        return

    variables = meta.get("variables", [])
    if not variables:
        console.print("[red]No variables found in metadata response.[/red]")
        return

    console.print(f"[cyan]Found [bold]{len(variables)}[/bold] variable(s). Building query for all values...[/cyan]")

    # Step 2: Build a query that selects ALL values for every variable
    query = {
        "query": [
            {
                "code": var["code"],
                "selection": {
                    "filter": "all",
                    "values": var.get("values", ["*"])
                }
            }
            for var in variables
        ],
        "response": {
            "format": "csv"
        }
    }

    console.print("[cyan]Posting query to download full dataset...[/cyan]")

    try:
        with Progress(
            TextColumn("[cyan]Downloading palay production CSV"),
            BarColumn(),
            DownloadColumn(),
            TransferSpeedColumn(),
            TimeRemainingColumn(),
            console=console
        ) as progress:
            task = progress.add_task("download", total=None)

            dl_r = requests.post(
                PSA_PALAY_API_URL,
                json=query,
                headers={**PSA_HEADERS, "Content-Type": "application/json"},
                stream=True,
                timeout=60
            )
            dl_r.raise_for_status()

            total = int(dl_r.headers.get("content-length", 0)) or None
            progress.update(task, total=total)

            with open(out_file, "wb") as f:
                for chunk in dl_r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        progress.update(task, advance=len(chunk))

    except Exception as e:
        console.print(f"[red]Failed to download palay production data: {e}[/red]")
        return

    console.print(f"[green]✔ PSA palay production data saved to:[/green] {out_file}\n")


# ==========================================
# TERMINAL USER INTERFACE (TUI)
# ==========================================
def check_overwrite(expected_files: list[Path]) -> bool:
    """Checks if files exist and prompts user before proceeding."""
    existing = [f for f in expected_files if f.exists()]
    if existing:
        console.print("[yellow]⚠ The following files already exist in your output directory:[/yellow]")
        for f in existing:
            console.print(f"  - {f.name}")
        return Confirm.ask("Do you want to overwrite them?", default=False)
    return True

def main():
    root = Path(__file__).resolve().parent
    out_dir = root / "data" / "raw"

    while True:
        out_dir.mkdir(parents=True, exist_ok=True)
        console.print("\n")

        menu_text = (
            "[1] Download PSA Wholesale Rice Prices (Cebu, primary source)\n"
            "[2] Download & Parse FPA Fertilizer Data (Cebu, Region VII)\n"
            "[3] Download PRISM Yield & Rice Area Data\n"
            "[4] Download PSA Palay Production Data\n"
            "[5] Download PSA Rice Stock Inventory (national)\n"
            "[6] Download Open-Meteo Daily Weather Archive\n"
            "[7] Download & Parse ALL Data\n"
            f"[8] Change Output Directory (Current: [bold cyan]{out_dir}[/bold cyan])\n"
            "[0] Exit"
        )
        console.print(Panel(menu_text, title="🌾 Dataset Downloader", expand=False, border_style="green"))

        choice = Prompt.ask("Select an option", choices=["0", "1", "2", "3", "4", "5", "6", "7", "8"], default="0")

        if choice == "0":
            console.print("[cyan]Exiting program. Goodbye![/cyan]")
            sys.exit(0)

        elif choice == "1":
            target = [out_dir / "Price Data" / "psa_wholesale_rice_prices_cebu.csv"]
            if check_overwrite(target):
                download_rice_price_psa(out_dir)

        elif choice == "2":
            target = [out_dir / "cebu_monthly_fertilizer_prices.csv"]
            if check_overwrite(target):
                process_fertilizer_data(out_dir)

        elif choice == "3":
            targets = [out_dir / "rice_area.csv", out_dir / "yield.csv"]
            if check_overwrite(targets):
                download_yield_data(out_dir)

        elif choice == "4":
            target = [out_dir / "palay_production_psa.csv"]
            if check_overwrite(target):
                download_palay_production(out_dir)

        elif choice == "5":
            target = [out_dir / "Rice Factors Data" / "RICE_STOCK_INVENTORY_monthly.csv"]
            if check_overwrite(target):
                download_rice_stock(out_dir)

        elif choice == "6":
            target = [out_dir / "Weather Data" / "open_meteo_daily_2010_2026.csv"]
            if check_overwrite(target):
                download_weather_openmeteo(out_dir)

        elif choice == "7":
            targets = [
                out_dir / "Price Data" / "psa_wholesale_rice_prices_cebu.csv",
                out_dir / "cebu_monthly_fertilizer_prices.csv",
                out_dir / "rice_area.csv",
                out_dir / "yield.csv",
                out_dir / "palay_production_psa.csv",
                out_dir / "Rice Factors Data" / "RICE_STOCK_INVENTORY_monthly.csv",
                out_dir / "Weather Data" / "open_meteo_daily_2010_2026.csv",
            ]
            if check_overwrite(targets):
                download_rice_price_psa(out_dir)
                process_fertilizer_data(out_dir)
                download_yield_data(out_dir)
                download_palay_production(out_dir)
                download_rice_stock(out_dir)
                download_weather_openmeteo(out_dir)

        elif choice == "8":
            new_dir = Prompt.ask("Enter new output directory path")
            out_dir = Path(new_dir).resolve()
            console.print(f"[green]Output directory changed to:[/green] {out_dir}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        console.print("\n[red]Process interrupted by user. Exiting...[/red]")
        sys.exit(0)