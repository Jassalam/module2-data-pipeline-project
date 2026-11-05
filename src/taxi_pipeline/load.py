"""Load stage: write the daily revenue table to the output folder."""

from pathlib import Path

import pandas as pd

OUTPUT_NAME = "daily_revenue"


def write_output(daily: pd.DataFrame, output_dir: Path) -> list[Path]:
    """Write the result as Parquet (for code) and CSV (for people). Returns the paths."""
    output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = output_dir / f"{OUTPUT_NAME}.parquet"
    csv_path = output_dir / f"{OUTPUT_NAME}.csv"

    daily.to_parquet(parquet_path, index=False)
    daily.to_csv(csv_path, index=False, date_format="%Y-%m-%d")
    return [parquet_path, csv_path]
