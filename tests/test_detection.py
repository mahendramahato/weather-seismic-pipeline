"""Unit tests for the weather anomaly detector (phase2/detection.py).

Runs the real Spark code on small synthetic histories with known answers:
a daily temperature cycle (≈9 °C at 03:00, ≈21 °C at 15:00) over several days.
"""
import math
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "phase2"))
from detection import MIN_SPREAD_C, Z_THRESHOLD, add_seasonal_scores  # noqa: E402

NOW = datetime(2026, 10, 10, 0, 0)
COLUMNS = ["station_id", "observed_at", "temperature_c"]


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("detection-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


# Daily cycle plus a small day-to-day wobble so the spread isn't zero.
def normal_temperature(t):
    return round(15 + 6 * math.sin(2 * math.pi * (t.hour - 9) / 24) + 0.6 * math.sin(t.day * 1.7), 1)


# Hourly readings for the given number of days before NOW.
def hourly_history(days, station="TEST", temperature=normal_temperature):
    times = [NOW - timedelta(hours=h) for h in range(days * 24, 0, -1)]
    return [(station, t, temperature(t)) for t in times]


# Scores `new_rows` the way the stream job does: the baseline candidates are
# the history plus the new rows themselves. Returns rows keyed by timestamp.
def score(spark, history_rows, new_rows):
    readings = spark.createDataFrame(history_rows + new_rows, COLUMNS)
    new = spark.createDataFrame(new_rows, COLUMNS)
    return {row.observed_at: row for row in add_seasonal_scores(new, readings).collect()}


def test_warm_night_is_flagged(spark):
    # 21 °C at 03:30 — normal for an afternoon, far above a normal night.
    t = NOW - timedelta(hours=20, minutes=30)
    result = score(spark, hourly_history(8), [("TEST", t, 21.0)])[t]
    assert result.is_anomaly
    assert result.z_score > Z_THRESHOLD
    assert result.baseline_days >= 5


def test_warm_afternoon_is_normal(spark):
    # The same 21 °C at 15:30 is a normal afternoon.
    t = NOW - timedelta(hours=8, minutes=30)
    result = score(spark, hourly_history(8), [("TEST", t, 21.0)])[t]
    assert not result.is_anomaly
    assert abs(result.z_score) < 1


def test_extreme_cold_is_flagged(spark):
    t = NOW - timedelta(minutes=30)
    result = score(spark, hourly_history(8), [("TEST", t, -10.0)])[t]
    assert result.is_anomaly
    assert result.z_score < -Z_THRESHOLD


def test_not_scored_until_enough_days(spark):
    # Only 3 previous days of same-hour history: "calibrating", never flagged.
    t = NOW - timedelta(minutes=30)
    result = score(spark, hourly_history(3), [("TEST", t, 45.0)])[t]
    assert result.baseline_days == 3
    assert result.z_score is None
    assert result.is_anomaly is False


def test_same_day_readings_are_not_in_the_baseline(spark):
    # Readings from earlier the same day are never part of the baseline.
    t = NOW - timedelta(minutes=30)
    same_day = [("TEST", NOW - timedelta(hours=h), 15.0) for h in range(1, 12)]
    result = score(spark, same_day, [("TEST", t, 15.0)])[t]
    assert result.baseline_days == 0
    assert result.z_score is None


def test_spread_has_a_floor(spark):
    # Identical history => MAD = 0; the spread floor (1 °C) avoids dividing by
    # zero, so a reading 2 °C above the median scores exactly z = 2.
    t = NOW - timedelta(minutes=30)
    flat = hourly_history(7, temperature=lambda _: 10.0)
    result = score(spark, flat, [("TEST", t, 10.0 + 2 * MIN_SPREAD_C)])[t]
    assert result.baseline_spread == 0
    assert result.z_score == pytest.approx(2.0)
    assert not result.is_anomaly


def test_stations_are_scored_separately(spark):
    # A hot station's history must not leak into a cool station's baseline.
    t = NOW - timedelta(minutes=30)
    hot = hourly_history(8, station="HOT", temperature=lambda _: 35.0)
    cool = hourly_history(8, station="COOL", temperature=lambda _: 5.0)
    result = score(spark, hot + cool, [("COOL", t, 5.5)])[t]
    assert result.baseline_median == pytest.approx(5.0)
    assert not result.is_anomaly
