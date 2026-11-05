"""Download NYC Green Taxi trip data (Jan-Mar 2025) into a local staging directory.

Usage:
    uv run scripts/download_data.py            # download missing months
    uv run scripts/download_data.py --force    # re-download everything
"""

import argparse
import sys
from pathlib import Path

# The project is not installed as a package, so let Python find src/taxi_pipeline.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from taxi_pipeline.config import MONTHS, RAW_DIR  # noqa: E402
from taxi_pipeline.download import download_months  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--force", action="store_true", help="re-download files that already exist"
    )
    parser.add_argument(
        "--staging-dir",
        type=Path,
        default=RAW_DIR,
        help=f"where to store raw files (default: {RAW_DIR})",
    )
    args = parser.parse_args()

    failures = download_months(MONTHS, args.staging_dir, force=args.force)
    if failures:
        print(f"{len(failures)} month(s) failed: {', '.join(failures)}", file=sys.stderr)
        return 1
    print(f"All {len(MONTHS)} months staged in {args.staging_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
