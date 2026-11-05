"""Settings shared by every stage of the pipeline."""

from pathlib import Path

import pandas as pd

# --- Source data -------------------------------------------------------------
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
MONTHS = ("2025-01", "2025-02", "2025-03")
DOWNLOAD_TIMEOUT_SECONDS = 60
CHUNK_SIZE = 1024 * 1024  # 1 MiB

# --- Local folders (resolved from this file, so any working directory works) --
PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"

# --- Transform rules (decided in notebooks/explore.ipynb) ---------------------
PICKUP_COLUMN = "lpep_pickup_datetime"
REVENUE_COLUMN = "total_amount"
# Keep pickups from the first day of the first month up to (not including)
# the first day after the last month.
PERIOD_START = pd.Timestamp(f"{MONTHS[0]}-01")
PERIOD_END = pd.Timestamp(f"{MONTHS[-1]}-01") + pd.offsets.MonthBegin(1)


def raw_filename(month: str) -> str:
    """File name of one month of Green Taxi data, e.g. green_tripdata_2025-01.parquet."""
    return f"green_tripdata_{month}.parquet"


def raw_url(month: str) -> str:
    """Public download link for one month of Green Taxi data."""
    return f"{BASE_URL}/{raw_filename(month)}"
