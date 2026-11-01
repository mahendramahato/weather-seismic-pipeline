# Project notes (read this first in a new session)

## Layout
- `phase0`–`phase6/` — pipeline phases: producer (NOAA + USGS → Kafka), Spark streams (`phase2/`), S3/Glue/Athena, Airflow DAG `phase6/dags/daily_lake_maintenance.py`
- `api/main.py` — FastAPI + DuckDB over the local lake (`data/lake/{weather,seismic}/date=YYYY-MM-DD/`), Athena for curated history
- `frontend/` — React + Vite + react-globe.gl dashboard; Caddy serves it (`frontend/Caddyfile`)
- `tests/test_api.py` — API tests on a tiny generated lake (`PYTHONPATH=api pytest tests/test_api.py`)

## How the dashboard works
- Globe view (`#/`): stats, stations, 3D globe (ripples for M3+, click a quake), replay scrubber (24 h / 7 days), quake detail, alerts
- Pipeline view (`#/pipeline`): `/api/pipeline` — stream freshness from lake file times, curated freshness from Athena (cached 10 min), API memory; incidents listed in `frontend/src/lib/incidents.js`
- Palette: sandstone (day) / deep moss (night), teal = weather, gold→crimson = quakes; tokens at top of `frontend/src/styles.css`

## Deploy
Mac = dev only. `git push` → on the Oracle VM: `git pull && docker compose up -d --build api web`

## Status (2026-11-01)
- Fixed: Airflow scheduler outage (restarted, Oct 20–31 backfilled), API memory blowup (single-flight cache, read only recent partitions, shared DuckDB capped 1 GB, `mem_limit: 2g`)
- Done: dashboard redesign + Pipeline page

## Open items
- `weather-stream` uses ~3.5 cores and rewrites state every batch (likely `dropDuplicates` without a watermark); also many tiny Parquet files
- Run the Airflow scheduler as its own service with `restart: unless-stopped` instead of `airflow standalone`; set `AIRFLOW__CORE__PARALLELISM=4`
- Add `mem_limit` to the other containers; VM disk is 82% full
