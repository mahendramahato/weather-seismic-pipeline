-- The raw tables were created by the Glue crawler (Step 4.3), which only
-- registers partitions that exist when it runs. Switching them to partition
-- projection means new days are queryable as soon as sync_to_s3.sh uploads
-- them, with no crawler run. Run each statement separately in Athena.

ALTER TABLE raw_weather SET TBLPROPERTIES (
    'projection.enabled'            = 'true',
    'projection.date.type'          = 'date',
    'projection.date.format'        = 'yyyy-MM-dd',
    'projection.date.range'         = '2026-09-27,NOW',
    'projection.date.interval'      = '1',
    'projection.date.interval.unit' = 'DAYS',
    'storage.location.template'     = 's3://weather-seismic-lake-mahendra/raw/weather/date=${date}/'
);

ALTER TABLE raw_seismic SET TBLPROPERTIES (
    'projection.enabled'            = 'true',
    'projection.date.type'          = 'date',
    'projection.date.format'        = 'yyyy-MM-dd',
    'projection.date.range'         = '2026-09-27,NOW',
    'projection.date.interval'      = '1',
    'projection.date.interval.unit' = 'DAYS',
    'storage.location.template'     = 's3://weather-seismic-lake-mahendra/raw/seismic/date=${date}/'
);