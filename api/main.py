import functools
import glob
import os
import time
from datetime import datetime, timedelta, timezone

import boto3
import duckdb
import threading
from fastapi import FastAPI, Query

# --- Configuration ---
# Lake location (env var so the container can point elsewhere) and the Athena
# workgroup/database from Phase 5.
LAKE_DIR = os.environ.get("LAKE_DIR", "data/lake")


# --- Which files to read ---
# The lake is split into one folder per day (date=YYYY-MM-DD). Every live query
# asks about a recent window (at most 7 days), so we hand DuckDB only those
# days' folders. Reading `**/*.parquet` instead made every request open every
# file the lake has ever collected (tens of thousands of small files), which
# was slow and is what pushed the API to 13+ GB of RAM.
def lake(dataset, hours):
    root = f"{LAKE_DIR}/{dataset}"
    now = datetime.now(timezone.utc)
    # One extra day of margin: rows near midnight, or a late update to an
    # older quake, can land in the previous day's folder.
    first = (now - timedelta(hours=hours)).date() - timedelta(days=1)
    days = [first + timedelta(days=i) for i in range((now.date() - first).days + 1)]
    patterns = [
        f"{root}/date={day.isoformat()}/*.parquet"
        for day in days
        if glob.glob(f"{root}/date={day.isoformat()}/*.parquet")
    ]
    if not patterns:
        # Nothing recent (streaming down, or an old dev lake): use the newest
        # day that exists so "latest reading" queries still show something.
        newest = sorted(glob.glob(f"{root}/date=*"))[-1:]
        patterns = [f"{folder}/*.parquet" for folder in newest] or [f"{root}/**/*.parquet"]
    files = ", ".join(f"'{p}'" for p in patterns)
    # union_by_name: files written before and after a detector change have
    # different baseline columns; match columns by name across all files, with
    # NULL where a file doesn't have one.
    return f"read_parquet([{files}], hive_partitioning = true, union_by_name = true)"

# Must match the Spark job, so the chart's shaded band is exactly the range the
# detector treats as normal. Current detector: same-hour median ± 3.5 × robust
# spread; rows from the older 24-hour detector: average ± 3 × std.
SEASONAL_Z_THRESHOLD = 3.5
LEGACY_Z_THRESHOLD = 3.0
MIN_SPREAD_C = 1.0
ATHENA_WORKGROUP = "weather-seismic"
ATHENA_DATABASE = "weather_seismic"

# The Athena client uses the standard AWS credential lookup: on the Mac the
# AWS_PROFILE=dashboard-reader profile, in the container its env vars.
athena = boto3.client("athena", region_name="us-west-2")

app = FastAPI(title="Weather & Seismic API")


# --- Cache helper ---
# Remembers a function's result per argument for `seconds`.
# Single-flight: if many requests miss at the same moment, only ONE runs the
# query; the rest wait on the lock, then return the result it just saved.
def ttl_cache(seconds):
    def decorator(func):
        saved = {}
        lock = threading.Lock()  # one lock per cached endpoint

        @functools.wraps(func)
        def wrapper(*args):
            # Fast path: a fresh result exists, so return it with no waiting.
            entry = saved.get(args)
            if entry and time.monotonic() - entry[0] < seconds:
                return entry[1]

            # Slow path: only one thread at a time gets past this line.
            with lock:
                # Re-check: while we waited, another thread may have just
                # refreshed it. If so, use its result instead of querying again.
                entry = saved.get(args)
                if entry and time.monotonic() - entry[0] < seconds:
                    return entry[1]

                result = func(*args)
                # Timestamp AFTER the query, so slow queries still get the full TTL.
                saved[args] = (time.monotonic(), result)
                return result

        return wrapper

    return decorator




# --- DuckDB helpers ---
# One DuckDB for the whole app, created once at startup and capped. Before,
# every query opened a fresh DuckDB that was allowed 80% of the VM's RAM and
# every CPU core; several at once is how the API took the whole server down.
# With the cap, a too-big query spills to disk or fails on its own instead.
db = duckdb.connect()
db.execute("SET memory_limit = '1GB'")
db.execute("SET threads = 2")


# Runs SQL over the local lake; values go in as ? parameters, never pasted in.
# cursor() gives each request thread its own handle on the shared, capped
# database (one handle can't be used by two threads at once).
def query(sql, params=None):
    with db.cursor() as con:
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
        FROM {lake('weather', 48)}
        QUALIFY row_number() OVER (PARTITION BY station_id ORDER BY observed_at DESC) = 1
        ORDER BY station_id
    """)


@ttl_cache(30)
def live_quakes(hours):
    return query(f"""
        SELECT event_id,
               strftime(event_time, '%Y-%m-%dT%H:%M:%SZ') AS event_time,
               round(magnitude, 1) AS magnitude, place, lat, lon, depth_km, is_significant
        FROM {lake('seismic', hours)}
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
        FROM {lake('weather', hours)}
        WHERE is_anomaly AND observed_at >= ?

        UNION ALL

        SELECT 'seismic', event_id,
               strftime(event_time, '%Y-%m-%dT%H:%M:%SZ'), lat, lon,
               printf('M%.1f %s', magnitude, place)
        FROM (
            SELECT * FROM {lake('seismic', hours)}
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
               round({band_edge('-')}, 2) AS band_low,
               round({band_edge('+')}, 2) AS band_high,
               is_anomaly
        FROM {lake('weather', hours)}
        WHERE observed_at >= ? AND temperature_c IS NOT NULL
        ORDER BY station_id, observed_at
    """, [hours_ago(hours)])


# Which columns exist in the last 7 days of the weather lake (refreshed every
# 5 minutes; 7 days = the widest window any endpoint asks for). Needed
# because a SQL query fails if it names a column no file has yet — e.g. right
# after a detector change, before any new-format file has been written.
@ttl_cache(300)
def weather_columns():
    return {row["column_name"] for row in query(f"DESCRIBE SELECT * FROM {lake('weather', 168)}")}


# SQL for one edge of the "normal range" band ('-' = lower, '+' = upper), for
# whichever detector scored each row. NULL for unscored rows: no band is drawn
# until the detector actually judges readings.
def band_edge(sign):
    columns = weather_columns()
    branches = []
    if "baseline_median" in columns:
        branches.append(
            f"WHEN baseline_median IS NOT NULL THEN baseline_median {sign} "
            f"{SEASONAL_Z_THRESHOLD} * greatest(baseline_spread, {MIN_SPREAD_C})"
        )
    if "baseline_avg" in columns:
        branches.append(
            f"WHEN baseline_avg IS NOT NULL THEN baseline_avg {sign} "
            f"{LEGACY_Z_THRESHOLD} * greatest(baseline_std, {MIN_SPREAD_C})"
        )
    if not branches:
        return "NULL"
    return f"CASE WHEN z_score IS NULL THEN NULL {' '.join(branches)} END"


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
        FROM {lake('weather', 48)}
        UNION ALL
        SELECT 'seismic', strftime(max(ingested_at), '%Y-%m-%dT%H:%M:%SZ')
        FROM {lake('seismic', 48)}
    """)
    return {row["dataset"]: row["latest"] for row in rows}
