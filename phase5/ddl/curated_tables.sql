-- Curated tables written by the curate-daily Glue job (phase4/curate_job.py).
-- Partition projection: Athena computes the date partitions from the rule
-- below (every day from 2026-09-27 up to today), so new days are queryable
-- immediately with no crawler run. Days with no folder are simply empty.
-- `date` is in backticks because it's a reserved word in DDL.
-- Run each statement separately in the Athena query editor.

-- One row per weather reading, duplicates removed
CREATE EXTERNAL TABLE IF NOT EXISTS curated_weather (
    station_id      string,
    observed_at     timestamp,
    lat             double,
    lon             double,
    temperature_c   double,
    humidity_pct    double,
    wind_speed_kmh  double,
    description     string,
    ingested_at     timestamp,
    baseline_avg    double,
    baseline_std    double,
    baseline_count  bigint,
    baseline_hours  int,
    z_score         double,
    is_anomaly      boolean,
    -- Seasonal detector (same-hour median/MAD), from October 2026 onward.
    -- Older rows have the 24-hour columns above instead; Athena shows NULL for
    -- whichever set a file doesn't have.
    baseline_median double,
    baseline_spread double,
    baseline_days   bigint
)
PARTITIONED BY (`date` string)
STORED AS PARQUET
LOCATION 's3://weather-seismic-lake-mahendra/curated/weather/'
TBLPROPERTIES (
    'projection.enabled'            = 'true',
    'projection.date.type'          = 'date',
    'projection.date.format'        = 'yyyy-MM-dd',
    'projection.date.range'         = '2026-09-27,NOW',
    'projection.date.interval'      = '1',
    'projection.date.interval.unit' = 'DAYS',
    'storage.location.template'     = 's3://weather-seismic-lake-mahendra/curated/weather/date=${date}/'
);

-- One row per earthquake: only the latest USGS revision.
CREATE EXTERNAL TABLE IF NOT EXISTS curated_seismic (
    event_id        string,
    event_time      timestamp,
    updated_at      timestamp,
    magnitude       double,
    place           string,
    lat             double,
    lon             double,
    depth_km        double,
    ingested_at     timestamp,
    is_significant  boolean
)
PARTITIONED BY (`date` string)
STORED AS PARQUET
LOCATION 's3://weather-seismic-lake-mahendra/curated/seismic/'
TBLPROPERTIES (
    'projection.enabled'            = 'true',
    'projection.date.type'          = 'date',
    'projection.date.format'        = 'yyyy-MM-dd',
    'projection.date.range'         = '2026-09-27,NOW',
    'projection.date.interval'      = '1',
    'projection.date.interval.unit' = 'DAYS',
    'storage.location.template'     = 's3://weather-seismic-lake-mahendra/curated/seismic/date=${date}/'
);

-- One row per station per day: reading counts, temperature range, anomalies.
CREATE EXTERNAL TABLE IF NOT EXISTS curated_weather_daily (
    station_id       string,
    readings         bigint,
    min_temp_c       double,
    max_temp_c       double, 
    avg_temp_c       double,
    scored_readings  bigint,
    anomalies        bigint
)
PARTITIONED BY (`date` string)
STORED AS PARQUET
LOCATION 's3://weather-seismic-lake-mahendra/curated/weather_daily/'
TBLPROPERTIES (
    'projection.enabled'            = 'true',
    'projection.date.type'          = 'date',
    'projection.date.format'        = 'yyyy-MM-dd',
    'projection.date.range'         = '2026-09-27,NOW',
    'projection.date.interval'      = '1',
    'projection.date.interval.unit' = 'DAYS',
    'storage.location.template'     = 's3://weather-seismic-lake-mahendra/curated/weather_daily/date=${date}/'
);

