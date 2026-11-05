"""Run the ETL stages in order: extract -> transform -> load."""

from collections.abc import Iterable
from pathlib import Path

import pandas as pd

from .config import MONTHS, PROCESSED_DIR, RAW_DIR
from .extract import read_staged
from .load import write_output
from .transform import clean_trips, daily_revenue


def run(
    months: Iterable[str] = MONTHS,
    staging_dir: Path = RAW_DIR,
    output_dir: Path = PROCESSED_DIR,
) -> pd.DataFrame:
    """Run the full pipeline and return the daily revenue table."""
    months = tuple(months)

    print(f"[extract]   reading {len(months)} staged file(s) from {staging_dir}")
    trips = read_staged(months, staging_dir)
    print(f"            raw rows: {len(trips):>9,}")

    print("[transform] cleaning")
    clean, steps = clean_trips(trips)
    for step in steps:
        print(f"            - {step.rule:<40} removed {step.removed:>6,}  -> {step.remaining:>9,} left")
    print(
        f"            clean rows: {len(clean):,} of {len(trips):,} "
        f"({len(clean) / len(trips):.1%} kept)"
    )
    daily = daily_revenue(clean)
    print(f"            daily rows: {len(daily)} ({daily['date'].min():%Y-%m-%d} .. {daily['date'].max():%Y-%m-%d})")

    print(f"[load]      writing to {output_dir}")
    for path in write_output(daily, output_dir):
        print(f"            - {path.name}")

    print(f"Total revenue: ${daily['revenue'].sum():,.2f} from {daily['trip_count'].sum():,} trips")
    return daily
