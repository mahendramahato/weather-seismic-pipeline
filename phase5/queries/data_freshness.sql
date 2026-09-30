
-- How far behind is the data? If minutes_behind keeps growing, something
-- upstream has stopped: the producer, a streaming job, or the S3 sync.
SELECT
    station_id,
    MAX(observed_at)                                                        AS latest_reading,
    date_diff('minute', MAX(observed_at), current_timestamp AT TIME ZONE 'UTC') AS minutes_behind
FROM raw_weather
GROUP BY station_id
ORDER BY station_id;
