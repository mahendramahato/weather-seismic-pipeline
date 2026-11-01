# Project notes (read this first in a new session)

## Layout
- `phase0`–`phase6/` — pipeline phases: producer (NOAA obs + NWS alerts + USGS → Kafka topics weather/seismic/alerts), Spark streams (`phase2/`: weather, seismic, alerts), S3/Glue/Athena, Airflow DAG `phase6/dags/daily_lake_maintenance.py`
- `api/main.py` — FastAPI + DuckDB over the local lake (`data/lake/{weather,seismic}/date=YYYY-MM-DD/`), Athena for curated history
- `frontend/` — React + Vite + react-globe.gl dashboard; Caddy serves it (`frontend/Caddyfile`)
- `tests/test_api.py` — API tests on a tiny generated lake (`PYTHONPATH=api pytest tests/test_api.py`)

## How the dashboard works
- Globe view (`#/`): stats, stations, 3D globe (ripples for M3+, click a quake), replay scrubber (24 h / 7 days), quake detail, alerts
- Pipeline view (`#/pipeline`): `/api/pipeline` — stream freshness from lake file times, curated freshness from Athena (cached 10 min), API memory; incidents listed in `frontend/src/lib/incidents.js`
- NWS alerts: `/api/nws-alerts` = alerts in force (not expired, not cancelled, not replaced by an update); Severe/Extreme polygons drawn on the globe (winding flipped in `frontend/src/lib/nws.js`), list in `NwsAlerts.jsx`
- Palette: sandstone (day) / deep moss (night), teal = weather, gold→crimson = quakes; tokens at top of `frontend/src/styles.css`

## Deploy
Mac = dev only. `git push` → on the Oracle VM: `git pull && docker compose up -d --build producer alerts-stream api web`

## Status (2026-11-01)
- Fixed: Airflow scheduler outage (restarted, Oct 20–31 backfilled), API memory blowup (single-flight cache, read only recent partitions, shared DuckDB capped 1 GB, `mem_limit: 2g`)
- Fixed: weather-stream CPU — 14-day baseline history now kept in memory (`phase2/history.py`) instead of re-reading the lake every batch; one file per batch (`coalesce(1)`); `mem_limit: 3g`
- Done: dashboard redesign + Pipeline page; NWS alerts as a third stream (producer → Kafka `alerts` → `alerts-stream` → `data/lake/alerts`)

## Open items
- Run the Airflow scheduler as its own service with `restart: unless-stopped` instead of `airflow standalone`; set `AIRFLOW__CORE__PARALLELISM=4`
- Add `mem_limit` to the remaining containers; VM disk is 82% full
- Optional: compact old weather partitions (thousands of tiny files per day before Nov 1) into one file per day
