"""Download stage: fetch the raw Parquet files into the staging directory."""

from collections.abc import Iterable
from pathlib import Path

import requests

from .config import CHUNK_SIZE, DOWNLOAD_TIMEOUT_SECONDS, raw_filename, raw_url


def download_month(month: str, staging_dir: Path, force: bool = False) -> Path:
    """Download one month's Parquet file and return its local path.

    Files that already exist are skipped unless ``force`` is True.
    """
    staging_dir.mkdir(parents=True, exist_ok=True)
    destination = staging_dir / raw_filename(month)

    if destination.exists() and not force:
        print(f"[skip] {destination.name} already staged")
        return destination

    url = raw_url(month)
    # Write to a temporary file first so an interrupted download never
    # leaves a truncated Parquet file that looks complete.
    partial = destination.with_suffix(".parquet.part")
    print(f"[get ] {url}")
    try:
        with requests.get(url, stream=True, timeout=DOWNLOAD_TIMEOUT_SECONDS) as response:
            response.raise_for_status()
            with partial.open("wb") as f:
                for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                    f.write(chunk)
        partial.replace(destination)
    finally:
        partial.unlink(missing_ok=True)

    size_mb = destination.stat().st_size / 1_000_000
    print(f"[done] {destination.name} ({size_mb:.1f} MB)")
    return destination


def download_months(
    months: Iterable[str], staging_dir: Path, force: bool = False
) -> list[str]:
    """Download every month. Returns the months that failed (empty list = success)."""
    failures = []
    for month in months:
        try:
            download_month(month, staging_dir, force=force)
        except requests.RequestException as exc:
            print(f"[fail] {month}: {exc}")
            failures.append(month)
    return failures
