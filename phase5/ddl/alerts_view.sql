-- One list of every anomaly from both sources, newest first when queried.
-- Built on the RAW tables so it includes data as soon as it's synced (curated
-- lags a day behind). A view stores no data: it re-runs this query each time.

CREATE OR REPLACE VIEW alerts AS

-- Weather anomalies flagged by the streaming z-score detector.
SELECT
    'weather'                                              AS source,
    station_id                                             AS id,
    observed_at                                            AS event_time,
    lat,
    lon,
    format('%.1f°C (z = %.1f)', temperature_c, z_score)    AS detail
FROM raw_weather
WHERE is_anomaly

-- UNION ALL stacks the two result sets; both sides need the same columns.
UNION ALL

-- Significant quakes. Raw keeps every USGS revision, so number each quake's
-- versions newest-first and keep only version 1 (same idea as the Glue job).
SELECT
    'seismic',
    event_id,
    event_time,
    lat,
    lon,
    format('M%.1f %s', magnitude, place)
FROM (
    SELECT *,
    row_number() OVER (PARTITION BY event_id ORDER BY updated_at DESC) AS version
    FROM raw_seismic
)
WHERE version = 1
AND is_significant;