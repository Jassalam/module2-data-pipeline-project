"""Extract stage: read the staged Parquet files into one DataFrame."""

from collections.abc import Iterable
from pathlib import Path

import pandas as pd

from .config import PICKUP_COLUMN, REVENUE_COLUMN, raw_filename

# Only the columns the transform needs; reading fewer columns is faster.
COLUMNS = [PICKUP_COLUMN, REVENUE_COLUMN]


def read_staged(months: Iterable[str], staging_dir: Path) -> pd.DataFrame:
    """Read and combine the staged files for the given months."""
    paths = [staging_dir / raw_filename(month) for month in months]
    missing = [p.name for p in paths if not p.exists()]
    if missing:
        raise FileNotFoundError(
            f"Missing staged files in {staging_dir}: {', '.join(missing)}. "
            "Run `uv run scripts/download_data.py` first."
        )
    frames = [pd.read_parquet(path, columns=COLUMNS) for path in paths]
    return pd.concat(frames, ignore_index=True)
