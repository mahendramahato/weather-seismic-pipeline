-- Adds the seasonal (same-hour, median/MAD) detector's columns to the weather
-- tables. Older files keep their 24-hour-baseline columns (baseline_avg,
-- baseline_std, baseline_hours) and show NULL for these; newer files show NULL
-- for the old ones. Athena matches Parquet columns by name, so both work.
-- Run each statement separately in Athena (DDL is free).

ALTER TABLE raw_weather ADD COLUMNS (
    baseline_median  double,
    baseline_spread  double,
    baseline_days    bigint
);

ALTER TABLE curated_weather ADD COLUMNS (
    baseline_median  double,
    baseline_spread  double,
    baseline_days    bigint
);
