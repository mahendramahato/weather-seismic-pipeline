# Low-Level Design — Weather & Seismic Data Pipeline

Component-level detail: schemas, algorithms, interfaces and configuration.
For the overview and design rationale see [HLD.md](HLD.md).

---

## 1. Producer — `phase1/producer.py`

| Setting | Value |
|---|---|
| Poll interval | 60 s |
| Stations | KBOI, KJFK, KLAX, KORD, KDEN, KSEA, KSFO, KPHX, KDFW, KMSP, KATL, KMIA, KBOS, PANC, PHNL |
| Sources | `api.weather.gov/stations/{id}/observations/latest` (User-Agent required); USGS `summary/all_hour.geojson` |
| Kafka | `BOOTSTRAP_SERVERS` env (`kafka:9092` in Docker, `localhost:9094` on the host) |
| Keys | weather → `station_id`; seismic → `event_id` |
| Serialization | JSON, UTF-8 bytes |

**Repeat suppression (in memory)**
- Weather: skip if `observed_at` equals the last value sent for that station.
- Seismic: skip if `(event_id, updated_at)` was in the previous poll's snapshot; the
  snapshot is replaced every poll, so memory stays bounded.

**Shutdown:** SIGTERM is converted to `KeyboardInterrupt`; `finally: producer.flush(10)`
delivers queued messages. Delivery guarantee: **at-least-once** (a restart resends
the latest readings).

### Message schemas

`weather`
```json
{"station_id": "KBOI", "observed_at": "2026-10-02T14:00:00+00:00", "lat": 43.57, "lon": -116.22,
 "temperature_c": 23.0, "humidity_pct": 28.0, "wind_speed_kmh": 6.0,
 "description": "Clear", "ingested_at": "2026-10-02T14:03:12+00:00"}
```
`seismic`
```json
{"event_id": "us6000abc", "event_time": "…+00:00", "updated_at": "…+00:00", "magnitude": 4.7,
 "place": "74 km S of Yonakuni, Japan", "lat": 23.8, "lon": 122.9, "depth_km": 10.0,
 "ingested_at": "…+00:00"}
```
NOAA values may be `null` (sensor gaps). USGS epoch-ms times are converted to ISO UTC.

## 2. Kafka

| Setting | Value |
|---|---|
| Mode | KRaft, single node (broker + controller), `apache/kafka:3.8.0` |
| Listeners | `PLAINTEXT://kafka:9092` (containers), `HOST://localhost:9094` (host, bound to 127.0.0.1) |
| Topics | `weather`, `seismic` — 1 partition, replication factor 1 |
| Retention | 7 days (default) |
| Storage | named volume `kafka-data` → `/var/lib/kafka/data` |

## 3. Weather stream — `phase2/weather_stream.py` + `phase2/detection.py`

**Pipeline per micro-batch (trigger: 1 minute)**
1. Read Kafka `weather` (`startingOffsets=earliest` applies only to a new checkpoint).
2. `from_json` with an explicit schema; drop rows with NULL `station_id`.
3. `withWatermark("observed_at", "3 hours").dropDuplicates(["station_id", "observed_at"])`.
4. Add `date = to_date(observed_at)` (session time zone UTC).
5. `foreachBatch`: persist → score (below) → append Parquet partitioned by `date` → print anomalies.

**Configuration**

| Env var | Default |
|---|---|
| `WEATHER_TOPIC` | `weather` |
| `LAKE_PATH` | `/opt/data/lake/weather` |
| `CHECKPOINT_PATH` | `/opt/data/checkpoints/weather` |

`spark.sql.shuffle.partitions = 4` (fixed by the checkpoint once a stateful query has run).

### 3.1 Anomaly detection algorithm (`detection.add_seasonal_scores`)

| Parameter | Value |
|---|---|
| `SEASONAL_DAYS` | 14 |
| `HOUR_WINDOW_MINUTES` | 60 |
| `MIN_BASELINE_DAYS` / `MIN_BASELINE_READINGS` | 5 / 5 |
| `MAD_TO_STD` | 1.4826 |
| `MIN_SPREAD_C` | 1.0 °C |
| `Z_THRESHOLD` | 3.5 |

For each new reading *r* of station *s* at time *t*:

1. **Candidates** = lake history since `t − 14 d − 1 h` (partition filter on `date`,
   only `station_id, observed_at, temperature_c`) ∪ the current batch, deduplicated.
2. **Pairs** = candidates *c* of station *s* where, with `gap = t − c.time` (seconds):
   - `gap ≥ 86400 − 3600` (previous days only — never the same day or itself),
   - `gap ≤ 14·86400 + 3600`,
   - circular time-of-day distance `min(gap mod 86400, 86400 − gap mod 86400) ≤ 3600`.
3. `baseline_median = median(c.temp)`; `MAD = median(|c.temp − baseline_median|)`;
   `baseline_spread = 1.4826 × MAD`; `baseline_count = |pairs|`;
   `baseline_days = count distinct round(gap / 86400)`.
4. If `baseline_days ≥ 5` and `baseline_count ≥ 5`:
   `z_score = (r.temp − baseline_median) / max(baseline_spread, 1.0)`; else NULL ("calibrating").
5. `is_anomaly = coalesce(|z_score| > 3.5, false)`.

### 3.2 Output schema (`lake/weather/date=YYYY-MM-DD/*.parquet`)

| Column | Type | Notes |
|---|---|---|
| station_id | string | |
| observed_at | timestamp | event time (UTC) |
| lat, lon | double | |
| temperature_c, humidity_pct, wind_speed_kmh | double | nullable |
| description | string | |
| ingested_at | timestamp | processing time |
| baseline_median, baseline_spread | double | seasonal detector |
| baseline_count, baseline_days | bigint | |
| z_score | double | NULL = not scored |
| is_anomaly | boolean | |
| date | date | partition (from folder name) |

Files written before October 2026 carry the earlier 24-hour detector's columns
(`baseline_avg`, `baseline_std`, `baseline_hours`) instead; all readers merge by name.

## 4. Seismic stream — `phase2/seismic_stream.py`

1. Read Kafka `seismic`; parse; drop NULL `event_id`.
2. `updated_at = coalesce(updated_at, event_time)` (messages from before the field existed).
3. `withWatermark("updated_at", "3 hours").dropDuplicates(["event_id", "updated_at"])` —
   a new `updated_at` is a USGS revision and is kept.
4. `is_significant = coalesce(magnitude ≥ 4.5, false)`.
5. `date = to_date(event_time)` (all revisions of a quake land in the same day).
6. Built-in Parquet file sink (exactly-once via `_spark_metadata`), checkpoint
   `/opt/data/checkpoints/seismic`, trigger 1 minute.

## 5. Batch layer

### 5.1 S3 layout — `s3://weather-seismic-lake-mahendra/`

| Prefix | Contents | Written by |
|---|---|---|
| `raw/weather/date=…/`, `raw/seismic/date=…/` | Lake files as streamed | `aws s3 sync` (Airflow) |
| `curated/weather/`, `curated/seismic/`, `curated/weather_daily/` | One file per day per table | Glue job |
| `scripts/curate_job.py` | Glue job script | CI/CD |
| `athena-results/` | Query results (lifecycle: delete after 7 days) | Athena |

Sync (`phase4/sync_to_s3.sh`) excludes `*.crc`, `*_SUCCESS`, `*_temporary/*`,
`*_spark_metadata/*`.

### 5.2 Glue job `curate-daily` — `phase4/curate_job.py`

| Setting | Value |
|---|---|
| Glue version / workers | 5.0 / 2 × G.1X |
| Timeout / retries | 10 min / 0 |
| Arguments | `--DATE YYYY-MM-DD` (per run), `--BUCKET` (default) |

Algorithm for `RUN_DATE`:
1. Read `raw/<table>/date=RUN_DATE/` **by S3 path** with `basePath` and `mergeSchema=true`
   (not via the catalog: Spark ignores Athena partition projection).
2. Fail if raw weather has 0 rows.
3. `latest_per_key` = `row_number() over (partition by keys order by col desc) = 1`:
   - weather: keys `(station_id, observed_at)`, newest `ingested_at`;
   - seismic: key `event_id`, newest `updated_at`.
4. `weather_daily`: per `(date, station_id)` — readings, min/max/avg temperature,
   `count(z_score)` as scored readings, sum of `is_anomaly`.
5. Write each with `coalesce(1)`, `partitionBy("date")`, `mode("overwrite")` and
   `partitionOverwriteMode=dynamic` → replaces only that day (idempotent).
6. Print `date=… weather: N raw -> M curated | seismic: …` to CloudWatch.

### 5.3 Catalog and Athena

| Table | Location | Partitioning |
|---|---|---|
| raw_weather, raw_seismic | `raw/…` | projection on `date` (`2026-09-27,NOW`, daily) |
| curated_weather, curated_seismic, curated_weather_daily | `curated/…` | projection on `date` |
| alerts (view) | — | weather anomalies ∪ latest significant quakes |

Workgroup `weather-seismic`: results to `athena-results/`, 100 MB per-query scan cutoff.
DDL: `phase5/ddl/*.sql`.

### 5.4 Airflow DAG `daily_lake_maintenance` — `phase6/dags/`

| Setting | Value |
|---|---|
| Schedule | `CronTriggerTimetable("30 0 * * *", UTC)` |
| catchup / max_active_runs | False / 1 |
| Retries | 1, after 5 min |
| Param | `date`: `null` or `YYYY-MM-DD` (manual backfill) |

```
check_freshness ──► sync_to_s3 ──► curate_day
target_date ─────────────────────► curate_day   (XCom)
```

| Task | Implementation | Fails when |
|---|---|---|
| check_freshness | newest `*.parquet` mtime per dataset | older than 3 h |
| sync_to_s3 | BashOperator: `bash …/sync_to_s3.sh ` | non-zero exit |
| target_date | `params.date` or `(logical_date or run_after) − 1 day` | — |
| curate_day | GlueJobOperator `curate-daily`, `aws_conn_id=None`, waits for completion | Glue run fails |

Runtime: `airflow standalone` + Postgres, LocalExecutor; runs as the host UID
(`AIRFLOW_UID`); AWS credentials mounted read-only and located via
`AWS_SHARED_CREDENTIALS_FILE` / `AWS_CONFIG_FILE`.

## 6. Serving layer

### 6.1 API — `api/main.py` (FastAPI + DuckDB)

Reads `read_parquet('<LAKE_DIR>/<table>/**/*.parquet', hive_partitioning, union_by_name)`.
Values are always bound as `?` parameters.

| Endpoint | Params | Source | Cache | Returns |
|---|---|---|---|---|
| `GET /api/stations` | — | DuckDB | 30 s | latest reading per station + `status` (normal/anomaly/unscored) |
| `GET /api/quakes` | `hours` 1–168 (24) | DuckDB | 30 s | quakes in window, latest revision each |
| `GET /api/weather/history` | `hours` 1–168 (24) | DuckDB | 30 s | readings + `band_low`/`band_high` for scored rows |
| `GET /api/alerts` | `hours` 1–168 (24) | DuckDB | 30 s | weather anomalies ∪ significant quakes |
| `GET /api/alerts/week` | — | Athena (curated, days −7…−2) + DuckDB (last 48 h) | 10 min (Athena) | merged, deduplicated on (source, id, time) |
| `GET /api/health` | — | DuckDB | — | newest time per dataset |

Out-of-range `hours` → **422**. Band: `baseline_median ± 3.5 × max(baseline_spread, 1)`
for seasonal rows (`± 3 × max(std, 1)` for older rows). The set of available columns
is read with `DESCRIBE` (cached 5 min) so queries never name a missing column.

### 6.2 Web — `frontend/` (Caddy + React)

**Caddy (`frontend/Caddyfile`, custom build with `caddy-ratelimit`)**

| Route | Behaviour |
|---|---|
| `/api/*` | rate limit 120 requests / 1 min per client IP → 429 + `Retry-After`; then `reverse_proxy api:8000` |
| everything else | static React build from `/srv`, SPA fallback to `index.html` |
| all | gzip; HSTS, `X-Content-Type-Options`, `Referrer-Policy`; `Server` header removed |

Site address from `SITE_ADDRESS` (automatic Let's Encrypt certificate).

**React app**

| Module | Role |
|---|---|
| `App.jsx` | loads stations, quakes, history, health every 60 s; layout; stat tiles; Live/Delayed badge (> 2 h) |
| `components/EarthGlobe.jsx` | react-globe.gl (lazy-loaded); day/night shader using the subsolar point; station pins; quake columns and rings |
| `components/StationCard.jsx`, `Sparkline.jsx` | conditions, status, 24 h chart with the API's normal band |
| `components/AlertsFeed.jsx` | 24 h / 7 days tabs |
| `lib/sun.js` | subsolar latitude/longitude (declination + equation of time) |
| `lib/useDayTheme.js` | light 06:00–18:00 visitor-local time, dark otherwise |

## 7. Infrastructure and configuration

### 7.1 Docker Compose services

| Service | Image / build | Ports (host) | Notes |
|---|---|---|---|
| kafka | apache/kafka:3.8.0 | 127.0.0.1:9094 | volume `kafka-data` |
| kafka-ui | provectuslabs/kafka-ui | 127.0.0.1:8080 | |
| producer | `phase1/` | — | `BOOTSTRAP_SERVERS=kafka:9092` |
| weather-stream, seismic-stream | apache/spark:4.1.1-python3 | 127.0.0.1:4040 / 4041 | code mounted from `phase2/` |
| spark | apache/spark:4.1.1-python3 | — | idle container for pyspark shells |
| airflow, airflow-postgres | `phase6/`, postgres:16 | 127.0.0.1:8081 | volume `airflow-db` |
| api | `api/` | — | lake mounted read-only; `.env.dashboard` |
| web | `frontend/` | 80, 443 | volumes `caddy-data`, `caddy-config` |

All services: `restart: unless-stopped`.

### 7.2 Server-only files (git-ignored)

| File | Contents |
|---|---|
| `.env` | `AIRFLOW_UID`, `SITE_ADDRESS`, `COPILOT_ADDRESS` |
| `.env.dashboard` | AWS keys for `dashboard-reader` |
| `~/.aws/` | AWS keys for `pipeline-vm` |

### 7.3 IAM

| Principal | Allowed | Policy file |
|---|---|---|
| `pipeline-vm` (user) | `s3:ListBucket`; `s3:PutObject` on `raw/*`; Glue Get/Start job run on `curate-daily` | `phase6/iam/` |
| `dashboard-reader` (user) | Athena query in workgroup; Glue GetDatabase/GetTable; `s3:GetObject` on `curated/*`; read/write `athena-results/*` | `phase7/iam/` |
| `AWSGlueServiceRole-weather-seismic` (role) | AWSGlueServiceRole + read/write the bucket | console |
| `github-deploy-weather-seismic` (role, OIDC) | `s3:PutObject` on `scripts/curate_job.py` only; assumable only by this repo's `main` | `cicd/iam/` |

## 8. CI/CD — `.github/workflows/ci-cd.yml`

| Job | Trigger | Steps |
|---|---|---|
| python | push, PR | Python 3.12 + Java 21; `pip install -r requirements-dev.txt`; `ruff check .`; `pytest` |
| frontend | push, PR | Node 22; `npm ci`; `npm run lint`; `npm run build` |
| docker | push, PR | `docker compose config -q`; build producer, api, web images |
| deploy | push to `main`, after all CI jobs | OIDC → upload Glue script; SSH (pinned host key) → `git pull --ff-only` → `scripts/deploy.sh <old-commit>` |

`scripts/deploy.sh`: `docker compose up -d --build` → restart `weather-stream` and
`seismic-stream` if `phase2/` changed → verify every long-running service is running
→ poll `http://api:8000/api/health` from the web container (≤ 60 s). Concurrency
group `production` serialises deploys.

### Tests (`tests/`)

| File | Covers |
|---|---|
| `test_detection.py` | warm night flagged; warm afternoon normal; cold extreme; not scored under 5 days; same-day readings excluded; spread floor; stations isolated |
| `test_api.py` | latest reading/status; latest quake revision; `hours` validation (422); alerts union; history band maths; health |

## 9. Failure handling

| Failure | Detection | Behaviour |
|---|---|---|
| NOAA/USGS request error | exception per station / poll | skipped, logged; next poll retries |
| Malformed Kafka message | NULL key after parsing | dropped |
| Duplicate reading | watermark dedup; Glue `latest_per_key` | dropped |
| Container crash / reboot | Docker restart policy | resumes from checkpoint |
| Lake stops growing | `check_freshness` > 3 h; dashboard "Delayed" > 2 h | DAG stops before Glue |
| Empty raw day | Glue row-count check | job fails (red in Airflow) |
| Concurrent Glue runs | `max_active_runs=1` | runs queue |
| API abuse | Caddy rate limit | 429 + Retry-After |
| Bad change pushed | CI | not deployed |
| Bad deploy | smoke test | workflow fails with service logs |
