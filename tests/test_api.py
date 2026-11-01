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

    write_table(
        lake / "alerts",
        [
            "alert_id VARCHAR", "sent_at TIMESTAMP", "expires_at TIMESTAMP", "event VARCHAR",
            "severity VARCHAR", "urgency VARCHAR", "certainty VARCHAR", "headline VARCHAR",
            "area VARCHAR", "message_type VARCHAR", "replaces VARCHAR[]", "lat DOUBLE",
            "lon DOUBLE", "geometry VARCHAR", "ingested_at TIMESTAMP",
        ],
        [
            # Active severe warning with a polygon.
            ("a1", NOW - timedelta(hours=2), NOW + timedelta(hours=4), "Tornado Warning", "Severe",
             "Immediate", "Observed", "Tornado Warning for Ada County", "Ada, ID", "Alert", [], 43.6, -116.2,
             '{"type":"Polygon","coordinates":[[[-116,43],[-116.5,43],[-116.5,44],[-116,43]]]}', NOW),
            # a2 was updated by a3, so only a3 should show.
            ("a2", NOW - timedelta(hours=3), NOW + timedelta(hours=6), "Flood Watch", "Moderate",
             "Expected", "Possible", "Flood Watch", "King, WA", "Alert", [], None, None, None, NOW),
            ("a3", NOW - timedelta(hours=1), NOW + timedelta(hours=6), "Flood Watch", "Moderate",
             "Expected", "Possible", "Flood Watch extended", "King, WA", "Update", ["a2"], None, None,
             '{"type":"Polygon","coordinates":[[[0,0],[1,0],[1,1],[0,0]]]}', NOW),
            # Already expired.
            ("a4", NOW - timedelta(hours=5), NOW - timedelta(hours=1), "Wind Advisory", "Minor",
             "Expected", "Likely", "Wind Advisory", "Denver, CO", "Alert", [], None, None, None, NOW),
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


def test_pipeline_reports_stream_and_curated_status(client, monkeypatch):
    import main

    # No AWS in tests: pretend Athena is unreachable.
    monkeypatch.setattr(main, "curated_freshness", lambda: None)
    body = client.get("/api/pipeline").json()
    assert set(body["streams"]) == {"weather", "seismic", "alerts"}
    # The test lake was written moments ago, so both streams look fresh.
    assert body["streams"]["weather"]["status"] == "healthy"
    assert body["streams"]["seismic"]["age_minutes"] < 15
    assert body["curated"]["status"] == "unknown"


def test_curated_status_compares_against_expected_day():
    import main

    tables = {"weather": "2026-10-31", "seismic": "2026-10-31"}
    assert main.curated_status(tables, "2026-10-31") == "healthy"
    assert main.curated_status(tables, "2026-11-01") == "delayed"
    assert main.curated_status(tables, "2026-11-05") == "down"
    assert main.curated_status(None, "2026-11-01") == "unknown"


def test_nws_alerts_show_only_alerts_still_in_force(client):
    alerts = client.get("/api/nws-alerts").json()
    assert [a["alert_id"] for a in alerts] == ["a1", "a3"]  # severe first; a2 replaced, a4 expired
    # Polygons only for Extreme/Severe alerts.
    assert alerts[0]["geometry"] is not None
    assert alerts[1]["geometry"] is None


def test_stations_include_extra_fields_even_before_the_lake_has_them(client):
    station = client.get("/api/stations").json()[0]
    # The test lake predates the extra fields, so they come back as NULL.
    assert "dewpoint_c" in station and station["dewpoint_c"] is None
    assert "cloud_cover" in station


def test_forecast_reshapes_the_nws_hourly_periods(client, monkeypatch):
    import main

    def fake_nws(url):
        if "/points/" in url:
            return {"properties": {"forecastHourly": "https://nws.test/hourly"}}
        assert url.endswith("?units=si")
        return {"properties": {"periods": [
            {"startTime": "2026-11-01T13:00:00-07:00", "temperature": 14, "isDaytime": True,
             "probabilityOfPrecipitation": {"value": 20}, "windSpeed": "13 km/h",
             "windDirection": "NW", "shortForecast": "Partly Sunny"},
        ] * 30}}

    monkeypatch.setattr(main, "nws_get", fake_nws)
    hours = client.get("/api/forecast/KBOI").json()
    assert len(hours) == 24
    assert hours[0] == {
        "time": "2026-11-01T13:00:00-07:00", "temperature_c": 14, "precip_pct": 20,
        "wind": "13 km/h NW", "summary": "Partly Sunny", "is_day": True,
    }
    assert client.get("/api/forecast/KZZZ").status_code == 404
    assert client.get("/api/forecast/bad!").status_code == 422
