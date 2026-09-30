-- Each station's daily temperature range and anomaly count for the last 7 days.
-- Filtering on the partition column (date) means Athena reads only those days.
-- date is a string (yyyy-MM-dd), so compare it with a formatted string.
SELECT
    date,
    station_id,
    readings,
    min_temp_c,
    max_temp_c,
    avg_temp_c,
    anomalies
FROM curated_weather_daily
WHERE date >= date_format(current_date - INTERVAL '7' DAY, '%Y-%m-%d')
ORDER BY station_id, date;
