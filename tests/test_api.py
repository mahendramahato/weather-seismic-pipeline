"""API tests (api/main.py) against a small Parquet lake built for each run.

Covers the live (DuckDB) endpoints; the 7-day Athena endpoint needs AWS and
is not called here.
"""
import importlib
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb
import pytest
from fastapi.testclient import TestClient

API_DIR = Path(__file__).resolve().parents[1] / "api"
NOW = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)


# Writes Hive-partitioned Parquet (date=YYYY-MM-DD folders), like the Spark jobs.
def write_table(path, columns, rows):
    con = duckdb.connect()
    placeholders = ", ".join(["?"] * len(columns))
    con.execute(f"CREATE TABLE t ({', '.join(columns)})")
    con.executemany(f"INSERT INTO t VALUES ({placeholders})", rows)
    con.execute("ALTER TABLE t ADD COLUMN date DATE")
    con.execute("UPDATE t SET date = CAST(" + columns[1].split()[0] + " AS DATE)")
    con.execute(f"COPY t TO '{path}' (FORMAT parquet, PARTITION_BY (date))")


@pytest.fixture(scope="module")
def client(tmp_path_factory):
    lake = tmp_path_factory.mktemp("lake")
    write_table(
        lake / "weather",
        [
            "station_id VARCHAR", "observed_at TIMESTAMP", "lat DOUBLE", "lon DOUBLE",
            "temperature_c DOUBLE", "humidity_pct DOUBLE", "wind_speed_kmh DOUBLE",
            "description VARCHAR", "ingested_at TIMESTAMP", "baseline_median DOUBLE",
            "baseline_spread DOUBLE", "baseline_count BIGINT", "baseline_days BIGINT",
            "z_score DOUBLE", "is_anomaly BOOLEAN",
        ],
        [
            # KBOI: an older unscored reading, then a scored normal one.
            ("KBOI", NOW - timedelta(hours=3), 43.6, -116.2, 14.0, 40.0, 5.0, "Clear", NOW, None, None, 0, 0, None, False),
            ("KBOI", NOW - timedelta(hours=1), 43.6, -116.2, 15.0, 40.0, 5.0, "Clear", NOW, 14.4, 0.6, 7, 7, 0.6, False),
            # KDEN: an anomaly.
            ("KDEN", NOW - timedelta(hours=1), 39.8, -104.7, 30.0, 20.0, 9.0, "Sunny", NOW, 20.0, 2.0, 7, 7, 5.0, True),
        ],
    )
    write_table(
        lake / "seismic",
        [
            "event_id VARCHAR", "event_time TIMESTAMP", "updated_at TIMESTAMP", "magnitude DOUBLE",
            "place VARCHAR", "lat DOUBLE", "lon DOUBLE", "depth_km DOUBLE", "ingested_at TIMESTAMP",
            "is_significant BOOLEAN",
        ],
        [
            # Two revisions of the same quake: the latest (M4.7) must win.
            ("eq1", NOW - timedelta(hours=2), NOW - timedelta(hours=2), 4.4, "Test Ridge", 10.0, 20.0, 5.0, NOW, False),
            ("eq1", NOW - timedelta(hours=2), NOW - timedelta(hours=1), 4.7, "Test Ridge", 10.0, 20.0, 5.0, NOW, True),
            ("eq2", NOW - timedelta(hours=1), NOW - timedelta(hours=1), 1.2, "Small Hill", 11.0, 21.0, 2.0, NOW, False),
        ],
    )

    os.environ["LAKE_DIR"] = str(lake)
    os.environ.setdefault("AWS_DEFAULT_REGION", "us-west-2")
    sys.path.insert(0, str(API_DIR))
    import main

    importlib.reload(main)
    return TestClient(main.app)


def test_stations_returns_latest_reading_and_status(client):
    stations = {s["station_id"]: s for s in client.get("/api/stations").json()}
    assert set(stations) == {"KBOI", "KDEN"}
    assert stations["KBOI"]["temperature_c"] == 15.0
    assert stations["KBOI"]["status"] == "normal"
    assert stations["KDEN"]["status"] == "anomaly"


def test_quakes_keep_only_the_latest_revision(client):
    quakes = {q["event_id"]: q for q in client.get("/api/quakes?hours=24").json()}
    assert set(quakes) == {"eq1", "eq2"}
    assert quakes["eq1"]["magnitude"] == 4.7
    assert quakes["eq1"]["is_significant"] is True


def test_hours_parameter_is_validated(client):
    assert client.get("/api/quakes?hours=999").status_code == 422
    assert client.get("/api/quakes?hours=abc").status_code == 422


def test_alerts_combine_weather_anomalies_and_significant_quakes(client):
    alerts = client.get("/api/alerts?hours=24").json()
    assert {(a["source"], a["id"]) for a in alerts} == {("weather", "KDEN"), ("seismic", "eq1")}


def test_history_band_matches_the_detector(client):
    rows = client.get("/api/weather/history?hours=24").json()
    boise = [r for r in rows if r["station_id"] == "KBOI"]
    unscored, scored = boise
    assert unscored["band_low"] is None
    # median ± 3.5 × max(spread, 1 °C) = 14.4 ± 3.5
    assert scored["band_low"] == pytest.approx(10.9)
    assert scored["band_high"] == pytest.approx(17.9)


def test_health_reports_both_datasets(client):
    health = client.get("/api/health").json()
    assert set(health) == {"weather", "seismic"}
