import os
from datetime import datetime, timedelta, timezone

import duckdb
from fastapi import FastAPI, Query

# --- where the lake is ---
LAKE_DIR = os.environ.get("LAKE_DIR", "data/lake")

# duckDB table expression over the parquet lake. hive_partitioning turns the
# date=yyyy-mm-dd folder structure into a date column.
WEATHER = f"read_parquet('{LAKE_DIR}/weather/**/*.parquet', hive_partitioning=true)"
SEISMIC = f"read_parquet('{LAKE_DIR}/seismic/**/*.parquet', hive_partitioning=true)"

app = FastAPI(title="Weather & Seismic Data API")

# --- Helpers ---
# runs a query and returns rows as a list of dicts
def query(sql, params=None):
    with duckdb.connect() as con:
        cursor = con.execute(sql, params or [])
        columns = [d[0] for d in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
    
# A UTC time N hours ago, without timezone info — the lake's timestamps are
# stored as plain UTC values, so both sides of a comparison must match.
def hours_ago(hours):
    return datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=hours)

# --- endpoints ---
# Each station's latest reading. status comes from the streaming detector:
# no z_score yet = 'unscored' (baseline too thin), else anomaly/normal.
# Times are formatted as ISO strings ending in Z so the browser knows they're UTC.
@app.get("/api/stations")
def stations():
    return query(f"""
        SELECT station_id, lat, lon, temperature_c, description,
            strftime(observed_at, '%Y-%m-%dT%H:%M:%SZ') AS observed_at,
            round(z_score, 1) AS z_score,
            CASE WHEN z_score IS NULL THEN 'unscored'
                WHEN is_anomaly THEN 'anomaly'
                ELSE 'normal' END AS status
        FROM {WEATHER}
        QUALIFY row_number() OVER (PARTITION BY station_id ORDER BY observed_at DESC) = 1
        ORDER BY station_id
    """)
    
# Quakes in the last `hours` (1–168), newest first. The raw lake keeps every
# USGS revision, so keep only each quake's latest version.
@app.get("/api/quakes")
def quakes(hours: int = Query(24, ge=1, le=168)):
    return query(f"""
        SELECT event_id,
            strftime(event_time, '%Y-%m-%dT%H:%M:%SZ') AS event_time,
            round(magnitude, 1) AS magnitude, place, lat, lon, depth_km, is_significant
        FROM {SEISMIC}
        WHERE event_time >= ?
        QUALIFY row_number() OVER (PARTITION BY event_id ORDER BY updated_at DESC) = 1
        ORDER BY event_time DESC
    """, [hours_ago(hours)])
    
# Everything the detectors flagged in the last `hours`: weather anomalies and
# significant quakes, combined into one list with the same columns
# (same idea as the Athena `alerts` view).
@app.get("/api/alerts")
def alerts(hours: int = Query(24, ge=1, le=168)):
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


# Newest data time per dataset, for the dashboard's "updated X min ago".
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