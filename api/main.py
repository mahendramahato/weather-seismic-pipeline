import functools
import os
import time
from datetime import datetime, timedelta, timezone

import boto3
import duckdb
from fastapi import FastAPI, Query

# --- Configuration ---
# Lake location (env var so the container can point elsewhere) and the Athena
# workgroup/database from Phase 5.
LAKE_DIR = os.environ.get("LAKE_DIR", "data/lake")
WEATHER = f"read_parquet('{LAKE_DIR}/weather/**/*.parquet', hive_partitioning = true)"
SEISMIC = f"read_parquet('{LAKE_DIR}/seismic/**/*.parquet', hive_partitioning = true)"
ATHENA_WORKGROUP = "weather-seismic"
ATHENA_DATABASE = "weather_seismic"

# The Athena client uses the standard AWS credential lookup: on the Mac the
# AWS_PROFILE=dashboard-reader profile, in the container its env vars.
athena = boto3.client("athena", region_name="us-west-2")

app = FastAPI(title="Weather & Seismic API")


# --- Cache helper ---
# Remembers a function's result per argument for `seconds`. Within that window
# repeat calls return the saved result without querying again — this caps how
# often DuckDB/Athena run no matter how many people load the page.
def ttl_cache(seconds):
    def decorator(func):
        saved = {}

        @functools.wraps(func)
        def wrapper(*args):
            now = time.monotonic()
            if args in saved and now - saved[args][0] < seconds:
                return saved[args][1]
            result = func(*args)
            saved[args] = (now, result)
            return result

        return wrapper

    return decorator


# --- DuckDB helpers ---
# Runs SQL over the local lake; values go in as ? parameters, never pasted in.
def query(sql, params=None):
    with duckdb.connect() as con:
        cursor = con.execute(sql, params or [])
        columns = [d[0] for d in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


def hours_ago(hours):
    return datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=hours)


# --- Athena helper ---
# Start a query, poll until it finishes, then read the result rows. Athena
# returns every value as text; the first row is the column names. Parameter
# values must be SQL literals, hence the quotes around each date string.
def athena_query(sql, params):
    query_id = athena.start_query_execution(
        QueryString=sql,
        WorkGroup=ATHENA_WORKGROUP,
        QueryExecutionContext={"Database": ATHENA_DATABASE},
        ExecutionParameters=[f"'{p}'" for p in params],
    )["QueryExecutionId"]

    while True:
        status = athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]["Status"]
        if status["State"] == "SUCCEEDED":
            break
        if status["State"] in ("FAILED", "CANCELLED"):
            raise RuntimeError(f"Athena query failed: {status.get('StateChangeReason')}")
        time.sleep(0.5)

    rows, header = [], None
    for page in athena.get_paginator("get_query_results").paginate(QueryExecutionId=query_id):
        for row in page["ResultSet"]["Rows"]:
            values = [cell.get("VarCharValue") for cell in row["Data"]]
            if header is None:
                header = values
                continue
            rows.append(dict(zip(header, values)))
    return rows


# --- Live data (DuckDB, cached 30 s) ---
@ttl_cache(30)
def live_stations():
    return query(f"""
        SELECT station_id, lat, lon, temperature_c, description,
               round(humidity_pct) AS humidity_pct,
               round(wind_speed_kmh) AS wind_speed_kmh,
               strftime(observed_at, '%Y-%m-%dT%H:%M:%SZ') AS observed_at,
               round(z_score, 1) AS z_score,
               CASE WHEN z_score IS NULL THEN 'unscored'
                    WHEN is_anomaly THEN 'anomaly'
                    ELSE 'normal' END AS status
        FROM {WEATHER}
        QUALIFY row_number() OVER (PARTITION BY station_id ORDER BY observed_at DESC) = 1
        ORDER BY station_id
    """)


@ttl_cache(30)
def live_quakes(hours):
    return query(f"""
        SELECT event_id,
               strftime(event_time, '%Y-%m-%dT%H:%M:%SZ') AS event_time,
               round(magnitude, 1) AS magnitude, place, lat, lon, depth_km, is_significant
        FROM {SEISMIC}
        WHERE event_time >= ?
        QUALIFY row_number() OVER (PARTITION BY event_id ORDER BY updated_at DESC) = 1
        ORDER BY event_time DESC
    """, [hours_ago(hours)])


@ttl_cache(30)
def live_alerts(hours):
    cutoff = hours_ago(hours)
    return query(f"""
        SELECT 'weather' AS source, station_id AS id,
               strftime(observed_at, '%Y-%m-%dT%H:%M:%SZ') AS event_time, lat, lon,
               printf('%s %.1f°C (z = %.1f)', station_id, temperature_c, z_score) AS detail
        FROM {WEATHER}
        WHERE is_anomaly AND observed_at >= ?

        UNION ALL

        SELECT 'seismic', event_id,
               strftime(event_time, '%Y-%m-%dT%H:%M:%SZ'), lat, lon,
               printf('M%.1f %s', magnitude, place)
        FROM (
            SELECT * FROM {SEISMIC}
            WHERE event_time >= ?
            QUALIFY row_number() OVER (PARTITION BY event_id ORDER BY updated_at DESC) = 1
        )
        WHERE is_significant

        ORDER BY event_time DESC
    """, [cutoff, cutoff])


# Every reading in the last `hours` with the detector's baseline, for the
# station sparklines (temperature line + the ±3σ "normal range" band).
@ttl_cache(30)
def live_weather_history(hours):
    return query(f"""
        SELECT station_id,
               strftime(observed_at, '%Y-%m-%dT%H:%M:%SZ') AS observed_at,
               temperature_c,
               round(baseline_avg, 2) AS baseline_avg,
               round(baseline_std, 2) AS baseline_std,
               is_anomaly
        FROM {WEATHER}
        WHERE observed_at >= ? AND temperature_c IS NOT NULL
        ORDER BY station_id, observed_at
    """, [hours_ago(hours)])


# --- History (Athena over curated tables, cached 10 min) ---
# Curated already holds one row per reading and only the latest quake revision,
# so no de-duplication is needed here. Filtering on `date` (the partition
# column) means Athena reads only those days' files. The time format matches
# the DuckDB one exactly, so the two sources can be merged.
@ttl_cache(600)
def curated_alerts(first_day, last_day):
    rows = athena_query("""
        SELECT 'weather' AS source, station_id AS id,
               date_format(observed_at, '%Y-%m-%dT%H:%i:%sZ') AS event_time, lat, lon,
               format('%s %.1f°C (z = %.1f)', station_id, temperature_c, z_score) AS detail
        FROM curated_weather
        WHERE is_anomaly AND date BETWEEN ? AND ?

        UNION ALL

        SELECT 'seismic', event_id,
               date_format(event_time, '%Y-%m-%dT%H:%i:%sZ'), lat, lon,
               format('M%.1f %s', magnitude, place)
        FROM curated_seismic
        WHERE is_significant AND date BETWEEN ? AND ?
    """, [first_day, last_day, first_day, last_day])
    # Athena returns text; turn coordinates back into numbers for the map.
    return [{**r, "lat": float(r["lat"]), "lon": float(r["lon"])} for r in rows]


# --- Endpoints ---
@app.get("/api/stations")
def stations():
    return live_stations()


@app.get("/api/quakes")
def quakes(hours: int = Query(24, ge=1, le=168)):
    return live_quakes(hours)


@app.get("/api/weather/history")
def weather_history(hours: int = Query(24, ge=1, le=168)):
    return live_weather_history(hours)


@app.get("/api/alerts")
def alerts(hours: int = Query(24, ge=1, le=168)):
    return live_alerts(hours)


# Last 7 days: days up to 2 days ago from curated (Athena), the last 48 h live
# (DuckDB) because yesterday may not be curated yet. The two can overlap by a
# few hours, so merge on (source, id, time) to drop duplicates, newest first.
@app.get("/api/alerts/week")
def alerts_week():
    today = datetime.now(timezone.utc).date()
    older = curated_alerts(
        (today - timedelta(days=7)).isoformat(),
        (today - timedelta(days=2)).isoformat(),
    )
    recent = live_alerts(48)
    merged = {(a["source"], a["id"], a["event_time"]): a for a in older + recent}
    return sorted(merged.values(), key=lambda a: a["event_time"], reverse=True)


@app.get("/api/health")
def health():
    rows = query(f"""
        SELECT 'weather' AS dataset,
               strftime(max(observed_at), '%Y-%m-%dT%H:%M:%SZ') AS latest
        FROM {WEATHER}
        UNION ALL
        SELECT 'seismic', strftime(max(ingested_at), '%Y-%m-%dT%H:%M:%SZ')
        FROM {SEISMIC}
    """)
    return {row["dataset"]: row["latest"] for row in rows}
