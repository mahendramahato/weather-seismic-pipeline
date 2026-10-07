"""Weather anomaly detection: same-hour seasonal baseline with robust statistics.

Kept separate from weather_stream.py (which starts a streaming query as soon
as it runs) so the logic can be imported and unit-tested on its own.
"""
from pyspark.sql.functions import (
    abs as spark_abs,
    coalesce,
    col,
    count,
    countDistinct,
    first,
    greatest,
    least,
    lit,
    percentile_approx,
    round as spark_round,
    when,
)

# --- Anomaly rule: same-hour seasonal baseline, robust statistics ---
# Each reading is compared only with the same station's readings at the same
# time of day (± HOUR_WINDOW_MINUTES) on the previous SEASONAL_DAYS days, so a
# 3 pm reading is judged against earlier 3 pm readings — the daily temperature
# cycle no longer distorts the baseline.
SEASONAL_DAYS = 14
HOUR_WINDOW_MINUTES = 60

# Median and MAD (median absolute deviation) instead of mean and standard
# deviation: one extreme reading in the history barely moves them.
# 1.4826 × MAD estimates the standard deviation for normal data, so
# (reading − median) / (1.4826 × MAD) is the "modified z-score"; 3.5 is the
# standard cut-off for it (Iglewicz & Hoaglin).
MAD_TO_STD = 1.4826
Z_THRESHOLD = 3.5

# Not scored until the baseline has same-hour readings from at least this many
# previous days (and at least this many readings in total).
MIN_BASELINE_DAYS = 5
MIN_BASELINE_READINGS = 5

# Floor for the spread: some stations report whole degrees, so tiny spreads
# are rounding noise, and a spread of 0 would divide by zero.
MIN_SPREAD_C = 1.0

SECONDS_PER_DAY = 86400
HOUR_WINDOW_SECONDS = HOUR_WINDOW_MINUTES * 60

# The only history columns the baseline needs.
BASELINE_COLUMNS = ["station_id", "observed_at", "temperature_c"]


# --- Seasonal baseline (DETECT) ---
# For each new reading, finds the same station's readings at the same time of
# day (± 1 hour) on each of the previous 14 days, then scores the reading
# against their median and MAD.
def add_seasonal_scores(new_rows, readings):
    targets = new_rows.select("station_id", col("observed_at").alias("target_at"))
    baseline = (
        readings
        .filter(col("temperature_c").isNotNull())
        .select("station_id", col("observed_at").alias("base_at"), col("temperature_c").alias("base_temp"))
    )

    # Seconds between the baseline reading and the new one, and how far apart
    # their times of day are (wrapping around midnight: 23:30 vs 00:10 = 40 min).
    gap = col("target_at").cast("long") - col("base_at").cast("long")
    offset = gap % SECONDS_PER_DAY
    clock_distance = least(offset, lit(SECONDS_PER_DAY) - offset)

    # Pair each new reading with its same-hour readings from previous days only
    # (at least ~23 h earlier — never today's, never itself).
    pairs = (
        targets.join(baseline, "station_id")
        .filter(gap >= SECONDS_PER_DAY - HOUR_WINDOW_SECONDS)
        .filter(gap <= SEASONAL_DAYS * SECONDS_PER_DAY + HOUR_WINDOW_SECONDS)
        .filter(clock_distance <= HOUR_WINDOW_SECONDS)
        .withColumn("days_back", spark_round(gap / SECONDS_PER_DAY))
    )

    # Median first; then MAD = median of each reading's distance from it.
    keys = ["station_id", "target_at"]
    medians = pairs.groupBy(keys).agg(percentile_approx("base_temp", 0.5).alias("baseline_median"))
    stats = (
        pairs.join(medians, keys)
        .groupBy(keys)
        .agg(
            first("baseline_median").alias("baseline_median"),
            percentile_approx(spark_abs(col("base_temp") - col("baseline_median")), 0.5).alias("mad"),
            count("*").alias("baseline_count"),
            countDistinct("days_back").alias("baseline_days"),
        )
        # Spread on the same scale as a standard deviation.
        .withColumn("baseline_spread", col("mad") * MAD_TO_STD)
        .drop("mad")
        .withColumnRenamed("target_at", "observed_at")
    )

    return (
        new_rows.join(stats, ["station_id", "observed_at"], "left")
        # Readings with no same-hour history get 0, not NULL, for clarity.
        .withColumn("baseline_count", coalesce(col("baseline_count"), lit(0)))
        .withColumn("baseline_days", coalesce(col("baseline_days"), lit(0)))
        # Modified z-score; NULL (= "calibrating") until enough days of history.
        .withColumn(
            "z_score",
            when(
                (col("baseline_days") >= MIN_BASELINE_DAYS)
                & (col("baseline_count") >= MIN_BASELINE_READINGS),
                (col("temperature_c") - col("baseline_median"))
                / greatest(col("baseline_spread"), lit(MIN_SPREAD_C)),
            ),
        )
        # Anomaly = beyond the threshold in either direction (too hot or too
        # cold). NULL z (not scored) counts as not an anomaly.
        .withColumn("is_anomaly", coalesce(spark_abs(col("z_score")) > Z_THRESHOLD, lit(False)))
    )
