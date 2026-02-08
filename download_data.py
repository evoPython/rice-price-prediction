import sys
import requests
from pathlib import Path
from rich.progress import Progress, BarColumn, DownloadColumn, TransferSpeedColumn, TimeRemainingColumn, TextColumn

URL = "https://data.humdata.org/dataset/ea251823-8694-47b4-82d0-7d27f00e8aba/resource/9a842d72-0d7d-4922-ad0e-eb8106c1ab0e/download/wfp_food_prices_phl.csv"

out_dir = Path("data/raw")
out_file = out_dir / "wfp_food_prices_phl.csv"

confirm = input("Download WFP rice price dataset? [y/N]: ").lower().strip()
if confirm != "y":
    print("Cancelled")
    sys.exit()

out_dir.mkdir(parents=True, exist_ok=True)

if out_file.exists():
    out_file.unlink()

with requests.get(URL, stream=True) as r:
    r.raise_for_status()
    total = int(r.headers.get("content-length", 0))

    columns = [
        TextColumn("Downloading"),
        BarColumn(),
        DownloadColumn(),
        TransferSpeedColumn(),
        TimeRemainingColumn(),
    ]

    with Progress(*columns) as progress:
        task = progress.add_task("download", total=total)

        with open(out_file, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    progress.update(task, advance=len(chunk))

print(f"Saved to {out_file}")
