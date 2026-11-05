"""Tests for the cleaning rules and the daily revenue aggregation."""

import pandas as pd
import pytest

from taxi_pipeline.transform import clean_trips, daily_revenue

START = pd.Timestamp("2025-01-01")
END = pd.Timestamp("2025-01-04")  # period = Jan 1, 2, 3


def trips(*rows):
    return pd.DataFrame(
        rows, columns=["lpep_pickup_datetime", "total_amount"]
    ).astype({"lpep_pickup_datetime": "datetime64[us]"})


def test_clean_drops_out_of_period_and_non_positive():
    raw = trips(
        ("2024-12-31 23:59:59", 10.0),  # before period
        ("2025-01-01 00:00:00", 10.0),  # first second: kept
        ("2025-01-02 12:00:00", -5.0),  # negative
        ("2025-01-02 13:00:00", 0.0),  # zero
        ("2025-01-03 23:59:59", 20.0),  # last second: kept
        ("2025-01-04 00:00:00", 30.0),  # after period
    )
    clean, steps = clean_trips(raw, START, END)

    assert clean["total_amount"].tolist() == [10.0, 20.0]
    assert [(s.removed, s.remaining) for s in steps] == [(2, 4), (2, 2)]


def test_daily_revenue_sums_per_day_and_fills_missing_days():
    clean = trips(
        ("2025-01-01 08:00", 10.25),
        ("2025-01-01 23:30", 4.75),
        ("2025-01-03 09:00", 12.0),
    )
    daily = daily_revenue(clean, START, END)

    assert daily["date"].dt.strftime("%Y-%m-%d").tolist() == [
        "2025-01-01",
        "2025-01-02",
        "2025-01-03",
    ]
    assert daily["trip_count"].tolist() == [2, 0, 1]
    assert daily["revenue"].tolist() == pytest.approx([15.0, 0.0, 12.0])
    assert daily["avg_revenue_per_trip"].tolist() == pytest.approx([7.5, 0.0, 12.0])


def test_revenue_matches_total_of_clean_trips():
    clean = trips(("2025-01-01 01:00", 1.10), ("2025-01-02 01:00", 2.20))
    daily = daily_revenue(clean, START, END)
    assert daily["revenue"].sum() == pytest.approx(clean["total_amount"].sum())
