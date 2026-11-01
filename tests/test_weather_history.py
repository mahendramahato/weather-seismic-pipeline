"""Tests for the weather stream's in-memory history (phase2/history.py).

The key check: scoring with the in-memory history gives exactly the same
result as the old way (re-reading the lake every batch), across a series of
batches written one after another.
"""
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_date

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "phase2"))
from detection import BASELINE_COLUMNS, HOUR_WINDOW_SECONDS, SEASONAL_DAYS, add_seasonal_scores  # noqa: E402
from history import KEEP, BaselineHistory  # noqa: E402

START = datetime(2026, 10, 1, 0, 0)
STATIONS = ["KBOI", "KDEN", "KSEA"]


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("history-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


# Hourly readings with a daily cycle, plus a bit of per-station variety.
def readings(first_hour, hours):
    rows = []
    for h in range(first_hour, first_hour + hours):
        at = START + timedelta(hours=h)
        for i, station in enumerate(STATIONS):
            rows.append((station, at, 10.0 + i + 6 * ((h % 24) / 24) + (h % 5) * 0.3))
    return rows


def as_batch(spark, rows):
    df = spark.createDataFrame(rows, "station_id string, observed_at timestamp, temperature_c double")
    return df.withColumn("date", to_date(col("observed_at")))


def write(batch, lake):
    batch.write.mode("append").partitionBy("date").parquet(str(lake))


# The old approach: read every lake file since `since` on every batch.
def score_old(spark, batch, lake):
    oldest = batch.agg({"observed_at": "min"}).first()[0]
    since = oldest - timedelta(days=SEASONAL_DAYS, seconds=HOUR_WINDOW_SECONDS)
    data = batch.select(*BASELINE_COLUMNS)
    if lake.exists():
        data = data.unionByName(
            spark.read.parquet(str(lake)).filter(col("observed_at") >= lit(since)).select(*BASELINE_COLUMNS)
        )
    return add_seasonal_scores(batch, data.dropDuplicates(["station_id", "observed_at"]))


# The new approach, as weather_stream.score_batch does it.
def score_new(batch, history):
    oldest = batch.agg({"observed_at": "min"}).first()[0]
    since = oldest - timedelta(days=SEASONAL_DAYS, seconds=HOUR_WINDOW_SECONDS)
    data = batch.select(*BASELINE_COLUMNS)
    past = history.readings_since(since)
    if past is not None:
        data = data.unionByName(past)
    return add_seasonal_scores(batch, data.dropDuplicates(["station_id", "observed_at"]))


def results(df):
    return sorted(
        (r.station_id, r.observed_at, r.z_score, r.baseline_days, r.is_anomaly)
        for r in df.select("station_id", "observed_at", "z_score", "baseline_days", "is_anomaly").collect()
    )


def test_in_memory_history_scores_like_reading_the_lake(spark, tmp_path):
    lake = tmp_path / "weather"
    # 16 days already in the lake before the job starts.
    write(as_batch(spark, readings(0, 16 * 24)), lake)

    history = BaselineHistory(spark, str(lake))
    hour = 16 * 24
    heatwave = START + timedelta(hours=hour + 2)
    anomalies = 0
    for size in [1, 3, 2, 1]:  # a few batches, one after another
        # One obviously hot reading, so the anomaly path is compared too.
        rows = [
            (s, at, 45.0 if (s == "KDEN" and at == heatwave) else temp)
            for s, at, temp in readings(hour, size)
        ]
        batch = as_batch(spark, rows)

        old = results(score_old(spark, batch, lake))
        new = results(score_new(batch, history))
        assert new == old
        assert all(r[3] >= 5 for r in new)  # enough history to be scored
        anomalies += sum(r[4] for r in new)

        write(batch, lake)
        history.add(batch.select(*BASELINE_COLUMNS).collect())
        hour += size

    assert anomalies == 1


def test_history_forgets_old_readings_but_reloads_if_needed(spark, tmp_path):
    lake = tmp_path / "weather"
    write(as_batch(spark, readings(0, 20 * 24)), lake)
    history = BaselineHistory(spark, str(lake))

    newest = START + timedelta(hours=20 * 24 - 1)
    history.readings_since(newest - KEEP)
    history.add([type("Row", (), {"station_id": "KBOI", "observed_at": newest, "temperature_c": 1.0})()])
    assert min(at for _, at in history.rows) >= newest - KEEP

    # A replay asking for older readings goes back to the lake for them.
    older = history.readings_since(START)
    assert older.count() == 20 * 24 * len(STATIONS)


def test_empty_lake_gives_no_history(spark, tmp_path):
    history = BaselineHistory(spark, str(tmp_path / "missing"))
    assert history.readings_since(START) is None
