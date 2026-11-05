"""Run the pipeline as a Prefect flow (Step 4, bonus).

Usage:
    uv run scripts/run_prefect_flow.py

Without a running Prefect server, Prefect starts a temporary local one for the run.
"""

import sys
from pathlib import Path

# The project is not installed as a package, so let Python find src/taxi_pipeline.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from taxi_pipeline.flow import daily_revenue_flow  # noqa: E402

if __name__ == "__main__":
    daily_revenue_flow()
