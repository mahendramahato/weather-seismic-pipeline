import functools
import glob
import json
import os
import time
import urllib.request
from datetime import datetime, timedelta, timezone

import boto3
import duckdb
import threading
from fastapi import FastAPI, HTTPException, Path, Query

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
def athena_query(sql, params=None):
    options = {
        "QueryString": sql,
        "WorkGroup": ATHENA_WORKGROUP,
        "QueryExecutionContext": {"Database": ATHENA_DATABASE},
    }
    # Athena rejects an empty parameter list, so only send one when needed.
    if params:
        options["ExecutionParameters"] = [f"'{p}'" for p in params]
    query_id = athena.start_query_execution(**options)["QueryExecutionId"]

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


# Observation fields the producer added in Nov 2026. Files written before
# then don't have them, so each is selected only if the lake has it yet
# (NULL otherwise) — naming a column no file has would fail the query.
EXTRA_WEATHER_FIELDS = [
    "dewpoint_c", "feels_like_c", "wind_direction_deg", "wind_gust_kmh",
    "pressure_hpa", "visibility_km", "precip_last_hour_mm", "cloud_cover",
]


# NOAA stations send a full report once an hour and short "special" reports
# in between (e.g. when visibility changes), which often leave out dewpoint,
# pressure, visibility or clouds. These slow-changing fields are filled from
# the station's most recent report that had them, if it's under 3 hours old.
# Gusts, rain and wind direction are NOT filled: missing there usually means
# none (no gusts, no rain, calm wind), and an old value would mislead.
FILLABLE_WEATHER_FIELDS = ["dewpoint_c", "pressure_hpa", "visibility_km", "cloud_cover"]
FILL_HOURS = 3


def extra_weather_select():
    columns = weather_columns()
    parts = []
    for name in EXTRA_WEATHER_FIELDS:
        if name not in columns:
            parts.append(f"NULL AS {name}")
        elif name in FILLABLE_WEATHER_FIELDS:
            parts.append(f"coalesce(latest.{name}, filled.{name}) AS {name}")
        else:
            parts.append(f"latest.{name} AS {name}")
    return ", ".join(parts)


def fill_weather_select():
    columns = weather_columns()
    fields = [name for name in FILLABLE_WEATHER_FIELDS if name in columns]
    # max_by(x, t) FILTER (x IS NOT NULL) = the newest non-missing value.
    picks = [f"max_by({name}, observed_at) FILTER (WHERE {name} IS NOT NULL) AS {name}" for name in fields]
    return ", ".join(["station_id"] + picks)


# --- Live data (DuckDB, cached 30 s) ---
@ttl_cache(30)
def live_stations():
    return query(f"""
        WITH readings AS (SELECT * FROM {lake('weather', 48)}),
        latest AS (
            SELECT * FROM readings
            QUALIFY row_number() OVER (PARTITION BY station_id ORDER BY observed_at DESC) = 1
        ),
        filled AS (
            SELECT {fill_weather_select()}
            FROM readings
            WHERE observed_at >= ?
            GROUP BY station_id
        )
        SELECT latest.station_id, lat, lon, temperature_c, description,
               round(humidity_pct) AS humidity_pct,
               round(wind_speed_kmh) AS wind_speed_kmh,
               {extra_weather_select()},
               strftime(latest.observed_at, '%Y-%m-%dT%H:%M:%SZ') AS observed_at,
               round(z_score, 1) AS z_score,
               CASE WHEN z_score IS NULL THEN 'unscored'
                    WHEN is_anomaly THEN 'anomaly'
                    ELSE 'normal' END AS status
        FROM latest LEFT JOIN filled USING (station_id)
        ORDER BY latest.station_id
    """, [hours_ago(FILL_HOURS)])


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


# --- Pipeline status (for the dashboard's Pipeline page) ---
# A health picture built only from what the API can see itself: how recently
# each stream wrote a file to the lake, how far the curated (Athena) tables
# reach, and this process's own memory. Stage health on the page is inferred
# from these, not read from Kafka or Airflow directly.
# Minutes since the newest file before a stream counts as delayed / down.
# Weather and seismic write almost every minute; NWS alerts only when NWS
# issues something new, which can pause for a while on a quiet night.
STREAM_LIMITS = {
    "weather": (15, 120),
    "seismic": (15, 120),
    "alerts": (90, 360),
}


def stream_status(age_minutes, dataset="weather"):
    healthy_min, delayed_min = STREAM_LIMITS[dataset]
    if age_minutes is None or age_minutes > delayed_min:
        return "down"
    if age_minutes > healthy_min:
        return "delayed"
    return "healthy"


# Newest file and today's file count for one dataset. Only today's and
# yesterday's folders are looked at, so this stays cheap as the lake grows.
@ttl_cache(30)
def stream_freshness(dataset):
    now = datetime.now(timezone.utc)
    root = f"{LAKE_DIR}/{dataset}"
    today_files = glob.glob(f"{root}/date={now.date().isoformat()}/*.parquet")
    yesterday = (now.date() - timedelta(days=1)).isoformat()
    newest = None
    for path in today_files + glob.glob(f"{root}/date={yesterday}/*.parquet"):
        try:
            newest = max(newest or 0, os.path.getmtime(path))
        except OSError:
            continue  # file vanished between listing and reading its time
    age = None if newest is None else round((now.timestamp() - newest) / 60, 1)
    return {
        "newest_file": None if newest is None
        else datetime.fromtimestamp(newest, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "age_minutes": age,
        "files_today": len(today_files),
        "status": stream_status(age, dataset),
    }


# The last day each curated table holds. Cached 10 minutes; a failure (no AWS
# access, Athena down) is cached too, as None, so it can't slow every request.
@ttl_cache(600)
def curated_freshness():
    try:
        rows = athena_query("""
            SELECT (SELECT max(date) FROM curated_weather) AS weather,
                   (SELECT max(date) FROM curated_seismic) AS seismic
        """)
        return rows[0] if rows else None
    except Exception:
        return None


def curated_status(tables, expected):
    if not tables or not all(tables.values()):
        return "unknown"
    oldest = min(str(v)[:10] for v in tables.values())
    one_day_late = (datetime.fromisoformat(expected) - timedelta(days=1)).date().isoformat()
    if oldest >= expected:
        return "healthy"
    if oldest >= one_day_late:
        return "delayed"
    return "down"


# This process's memory and its container's limit (Linux only; None elsewhere).
def api_memory():
    used_mb = limit_mb = None
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    used_mb = round(int(line.split()[1]) / 1024)
    except OSError:
        pass
    try:
        with open("/sys/fs/cgroup/memory.max") as f:
            value = f.read().strip()
        if value != "max":
            limit_mb = round(int(value) / 1048576)
    except (OSError, ValueError):
        pass
    return {"memory_mb": used_mb, "memory_limit_mb": limit_mb}


# --- NWS alerts (DuckDB over the alerts lake, cached 60 s) ---
# Alerts still in force: not expired, not a cancellation, and not replaced by
# a newer alert (an NWS update is a new alert listing the ids it replaces).
# Polygons are sent only for Extreme/Severe alerts, which the globe draws;
# the rest are listed by area name, which keeps the response small.
SEVERITY_ORDER = "CASE severity WHEN 'Extreme' THEN 0 WHEN 'Severe' THEN 1 WHEN 'Moderate' THEN 2 WHEN 'Minor' THEN 3 ELSE 4 END"


@ttl_cache(60)
def active_nws_alerts():
    # No alerts lake yet (stream not started): nothing to show, not an error.
    if not glob.glob(f"{LAKE_DIR}/alerts/date=*/*.parquet"):
        return []
    # Long-running warnings (floods, fire weather) can be issued days before;
    # a week of partitions covers them.
    source = lake("alerts", 168)
    return query(f"""
        WITH alerts AS (SELECT * FROM {source}),
        replaced AS (SELECT DISTINCT unnest(replaces) AS alert_id FROM alerts)
        SELECT alert_id, event, severity, urgency, certainty, headline, area, lat, lon,
               strftime(sent_at, '%Y-%m-%dT%H:%M:%SZ') AS sent_at,
               strftime(expires_at, '%Y-%m-%dT%H:%M:%SZ') AS expires_at,
               CASE WHEN severity IN ('Extreme', 'Severe') THEN geometry END AS geometry
        FROM alerts
        WHERE coalesce(expires_at, sent_at + INTERVAL 1 DAY) > ?
          AND message_type <> 'Cancel'
          AND alert_id NOT IN (SELECT alert_id FROM replaced WHERE alert_id IS NOT NULL)
        QUALIFY row_number() OVER (PARTITION BY alert_id ORDER BY ingested_at DESC) = 1
        ORDER BY {SEVERITY_ORDER}, sent_at DESC
    """, [hours_ago(0)])


# --- Hourly forecast (straight from NWS, cached 30 min) ---
# Forecasts aren't part of the lake: the dashboard only ever needs the latest
# one, so the API asks NWS directly. NWS needs two calls: /points maps a
# location to its forecast grid (fixed, so remembered forever), then that
# grid's hourly forecast. units=si gives °C and km/h.
NWS_HEADERS = {
    "User-Agent": "weather-seismic-pipeline dashboard (mahendramahato33@gmail.com)",
    "Accept": "application/geo+json",
}
FORECAST_HOURS = 24
_forecast_urls = {}


def nws_get(url):
    request = urllib.request.Request(url, headers=NWS_HEADERS)
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.load(response)


@ttl_cache(1800)
def hourly_forecast(station_id):
    station = next((s for s in live_stations() if s["station_id"] == station_id), None)
    if station is None:
        return None
    if station_id not in _forecast_urls:
        point = nws_get(f"https://api.weather.gov/points/{station['lat']:.4f},{station['lon']:.4f}")
        _forecast_urls[station_id] = point["properties"]["forecastHourly"]
    periods = nws_get(f"{_forecast_urls[station_id]}?units=si")["properties"]["periods"]
    return [
        {
            # Station-local time with its offset, e.g. 2026-11-01T13:00:00-07:00.
            "time": p["startTime"],
            "temperature_c": p["temperature"],
            "precip_pct": (p.get("probabilityOfPrecipitation") or {}).get("value"),
            "wind": f"{p.get('windSpeed', '')} {p.get('windDirection', '')}".strip(),
            "summary": p["shortForecast"],
            "is_day": p["isDaytime"],
        }
        for p in periods[:FORECAST_HOURS]
    ]


# --- Endpoints ---
@app.get("/api/stations")
def stations():
    return live_stations()


@app.get("/api/forecast/{station_id}")
def forecast(station_id: str = Path(pattern=r"^[A-Z0-9]{3,5}$")):
    try:
        periods = hourly_forecast(station_id)
    except Exception as exc:  # NWS down or slow: say so instead of a bare 500
        raise HTTPException(status_code=502, detail="NWS forecast unavailable") from exc
    if periods is None:
        raise HTTPException(status_code=404, detail="Unknown station")
    return periods


@app.get("/api/quakes")
def quakes(hours: int = Query(24, ge=1, le=168)):
    return live_quakes(hours)


@app.get("/api/weather/history")
def weather_history(hours: int = Query(24, ge=1, le=168)):
    return live_weather_history(hours)


@app.get("/api/nws-alerts")
def nws_alerts():
    return active_nws_alerts()


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


@app.get("/api/pipeline")
def pipeline():
    now = datetime.now(timezone.utc)
    # The 00:30 UTC run curates the previous day; give it an hour to finish.
    expected = ((now - timedelta(hours=1, minutes=30)).date() - timedelta(days=1)).isoformat()
    tables = curated_freshness()
    return {
        "checked_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "streams": {name: stream_freshness(name) for name in STREAM_LIMITS},
        "curated": {
            "tables": tables,
            "expected": expected,
            "status": curated_status(tables, expected),
        },
        "api": api_memory(),
    }


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
