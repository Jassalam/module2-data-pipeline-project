"""Step 4 (bonus): the same ETL pipeline, orchestrated with Prefect.

Each stage is a Prefect task and the whole run is a flow, so Prefect records
every run, shows which stage failed, and retries the step that talks to the
network. The stage logic itself still lives in download/extract/transform/load.
"""

from pathlib import Path

import pandas as pd
from prefect import flow, task
from prefect.cache_policies import NO_CACHE

from .config import MONTHS, PROCESSED_DIR, RAW_DIR
from .download import download_months
from .extract import read_staged
from .load import write_output
from .transform import clean_trips, daily_revenue


# Downloads are the only step that can fail for reasons outside our control
# (network blips, server errors), so they get retries with growing waits.
@task(retries=3, retry_delay_seconds=[5, 15, 30])
def download_task(months: tuple[str, ...], staging_dir: Path, force: bool = False) -> None:
    failures = download_months(months, staging_dir, force=force)
    if failures:
        # Raising makes Prefect retry; files already downloaded are skipped.
        raise RuntimeError(f"download failed for: {', '.join(failures)}")


# NO_CACHE: these tasks pass DataFrames around, and hashing them for Prefect's
# cache would be slow and pointless (the pipeline is cheap to rerun).
@task(cache_policy=NO_CACHE)
def extract_task(months: tuple[str, ...], staging_dir: Path) -> pd.DataFrame:
    trips = read_staged(months, staging_dir)
    print(f"raw rows: {len(trips):,}")
    return trips


@task(cache_policy=NO_CACHE)
def clean_task(trips: pd.DataFrame) -> pd.DataFrame:
    clean, steps = clean_trips(trips)
    for step in steps:
        print(f"{step.rule}: removed {step.removed:,} -> {step.remaining:,} left")
    print(f"clean rows: {len(clean):,} of {len(trips):,} ({len(clean) / len(trips):.1%} kept)")
    return clean


@task(cache_policy=NO_CACHE)
def aggregate_task(clean: pd.DataFrame) -> pd.DataFrame:
    daily = daily_revenue(clean)
    print(f"daily rows: {len(daily)}")
    return daily


@task(cache_policy=NO_CACHE)
def load_task(daily: pd.DataFrame, output_dir: Path) -> list[Path]:
    paths = write_output(daily, output_dir)
    print(f"wrote: {', '.join(p.name for p in paths)}")
    return paths


@flow(name="green-taxi-daily-revenue", log_prints=True)
def daily_revenue_flow(
    months: tuple[str, ...] = MONTHS,
    staging_dir: Path = RAW_DIR,
    output_dir: Path = PROCESSED_DIR,
    force_download: bool = False,
) -> pd.DataFrame:
    """Download -> extract -> clean -> aggregate -> load."""
    download_task(months, staging_dir, force=force_download)
    trips = extract_task(months, staging_dir)
    clean = clean_task(trips)
    daily = aggregate_task(clean)
    load_task(daily, output_dir)
    print(
        f"Total revenue: ${daily['revenue'].sum():,.2f} "
        f"from {daily['trip_count'].sum():,} trips"
    )
    return daily
