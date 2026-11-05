"""Transform stage: clean the trips and aggregate revenue per day.

Cleaning rules come from notebooks/explore.ipynb (Step 2):
  1. Drop pickups outside the reporting period (they would add stray days).
  2. Drop trips with total_amount <= 0 (reversals, disputes, no-charge trips).
"""

from dataclasses import dataclass

import pandas as pd

from .config import PERIOD_END, PERIOD_START, PICKUP_COLUMN, REVENUE_COLUMN


@dataclass(frozen=True)
class CleaningStep:
    """How many rows one cleaning rule removed, and how many were left."""

    rule: str
    removed: int
    remaining: int


def clean_trips(
    trips: pd.DataFrame,
    start: pd.Timestamp = PERIOD_START,
    end: pd.Timestamp = PERIOD_END,
) -> tuple[pd.DataFrame, list[CleaningStep]]:
    """Apply the cleaning rules and report the row count after each one."""
    steps = []

    pickup = trips[PICKUP_COLUMN]
    in_period = trips[(pickup >= start) & (pickup < end)]
    steps.append(
        CleaningStep(
            f"pickup outside {start:%Y-%m-%d} .. {end - pd.Timedelta(days=1):%Y-%m-%d}",
            removed=len(trips) - len(in_period),
            remaining=len(in_period),
        )
    )

    paid = in_period[in_period[REVENUE_COLUMN] > 0]
    steps.append(
        CleaningStep(
            f"{REVENUE_COLUMN} <= 0",
            removed=len(in_period) - len(paid),
            remaining=len(paid),
        )
    )

    return paid.reset_index(drop=True), steps


def daily_revenue(
    trips: pd.DataFrame,
    start: pd.Timestamp = PERIOD_START,
    end: pd.Timestamp = PERIOD_END,
) -> pd.DataFrame:
    """Sum revenue per pickup day.

    Every day in the period gets a row, even if it had no trips (revenue 0),
    so gaps in the data show up instead of silently disappearing.
    """
    days = pd.date_range(start, end - pd.Timedelta(days=1), freq="D", name="date")
    grouped = trips.groupby(trips[PICKUP_COLUMN].dt.floor("D"))[REVENUE_COLUMN]

    result = pd.DataFrame(
        {
            "trip_count": grouped.size(),
            "revenue": grouped.sum(),
        }
    ).reindex(days, fill_value=0)

    result["trip_count"] = result["trip_count"].astype("int64")
    result["revenue"] = result["revenue"].round(2)
    result["avg_revenue_per_trip"] = (
        (result["revenue"] / result["trip_count"]).where(result["trip_count"] > 0, 0).round(2)
    )
    return result.reset_index()
