# Weather & Seismic Data Engineering Pipeline

Real-time pipeline ingesting live NOAA weather observations and USGS
earthquake data, detecting anomalies, and visualizing results on a
dashboard. Built to learn Kafka, Spark, Glue, Airflow, and Athena
hands-on.

## Architecture (target end state)

```
NOAA API + USGS API
    -> Python producer scripts
    -> Kafka (topics: weather, seismic)
    -> Spark Structured Streaming (real-time anomaly detection)
    -> S3 / MinIO raw data lake (Parquet, partitioned by date)
    -> Glue / PySpark batch ETL (clean, aggregate)
    -> Glue Data Catalog (schema registry)
    -> Athena (SQL query layer)
    -> React + Leaflet dashboard (live map, anomaly flags)

Airflow orchestrates the batch side (Glue ETL, cataloging, quality checks).
```

## Data sources

- NOAA Weather API: `https://api.weather.gov/stations/{station_id}/observations/latest`
  — no API key, requires a `User-Agent` header.
- USGS Earthquake feed: real-time GeoJSON, no auth required.

## Phases

- [x] **Phase 0** — pull one real reading from each API, confirm shape (`phase0/`)
- [ ] **Phase 1** — Kafka locally via Docker Compose, producer scripts
- [ ] **Phase 2** — Spark Structured Streaming reading from Kafka into a local data lake
- [ ] **Phase 3** — anomaly detection logic in the Spark job
- [ ] **Phase 4** — move to AWS: S3, Glue ETL, Glue Data Catalog
- [ ] **Phase 5** — Athena querying cataloged data
- [ ] **Phase 6** — Airflow DAG for the batch side
- [ ] **Phase 7** — React + Leaflet dashboard

## Cost notes

- S3 + Glue Data Catalog: within AWS Always Free tier at this scale.
- Glue ETL jobs: billed hourly — keep runs short and infrequent.
- Athena: no free tier, ~$5/TB scanned — cheap at this scale.
- Kafka, Spark (local), Airflow: self-hosted via Docker Compose, no AWS cost.

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

python phase0/fetch_samples.py
```

## Frontend preview

A minimal React + Leaflet dashboard that shows live NOAA stations and
recent USGS earthquakes on a map. It's a seed for the Phase 7 dashboard —
today it hits a local Flask API that fetches live data directly; later
phases swap the API's data source (Athena, etc.) without changing the
frontend.

```bash
# terminal 1 — backend API on :5001
source .venv/bin/activate
python backend/app.py

# terminal 2 — frontend on :5173
cd frontend
npm install
npm run dev
```
