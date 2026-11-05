"""Run the ETL pipeline: read staged files, compute revenue per day, write results.

Usage:
    uv run scripts/run_pipeline.py

Run scripts/download_data.py first so the raw files are staged.
"""

import sys
from pathlib import Path

# The project is not installed as a package, so let Python find src/taxi_pipeline.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from taxi_pipeline.pipeline import run  # noqa: E402

if __name__ == "__main__":
    try:
        run()
    except FileNotFoundError as exc:
        print(f"error: {exc}", file=sys.stderr)
        sys.exit(1)
