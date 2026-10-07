import os

from datetime import timedelta

from pyspark.errors import AnalysisException
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    abs as spark_abs,
    coalesce,
    col,
    count,
    countDistinct,
    first,
    from_json,
    greatest,
    least,
    lit,
    min as spark_min,
    percentile_approx,
    round as spark_round,
    to_date,
    when,
)
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType
)

# --- Schema ---
# The columns and types we expect inside each Kafka message. Names must match
# the JSON keys the producer sends exactly, or that column comes back NULL.
# A stream must declare this up front: the data hasn't arrived yet when the
# job starts, so Spark can't look at it to work the columns out.
WEATHER_SCHEMA = StructType([
    StructField("station_id", StringType()),
    StructField("observed_at", TimestampType()),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("temperature_c", DoubleType()),
    StructField("humidity_pct", DoubleType()),
    StructField("wind_speed_kmh", DoubleType()),
    StructField("description", StringType()),
    StructField("ingested_at", TimestampType())
])

# --- Topic and output locations ---
# Defaults are the real pipeline. Tests override them with environment
# variables (docker exec -e NAME=value ...) to run this exact code against a
# separate topic, lake and checkpoint, so test data never touches real data.
# /opt/data inside the container is the project's data/ folder on the Mac.
WEATHER_TOPIC = os.environ.get("WEATHER_TOPIC", "weather")
LAKE_PATH = os.environ.get("LAKE_PATH", "/opt/data/lake/weather")
CHECKPOINT_PATH = os.environ.get("CHECKPOINT_PATH", "/opt/data/checkpoints/weather")


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

# --- Spark session ---
# Entry point to Spark. UTC timezone so to_date() assigns every reading to the
# same calendar day no matter where the job runs. shuffle.partitions = 4
# because the default (200) is sized for big clusters: with our small data it
# splits each batch into ~200 tiny pieces and writes a tiny file for each.
spark = (
    SparkSession.builder
    .appName("weather-stream")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# --- Read (ASK + READ) ---
# Stream from the weather topic. kafka:9092 is the internal Docker address.
# "earliest" = start from offset 0 the first time the job runs.
raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", WEATHER_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

# --- Parse (PARSE) ---
# value bytes -> string -> struct named "data" (using the schema),
# then data.* unpacks each field into its own column.
# Messages that don't match the schema come out as all NULLs instead of crashing.
parsed = (
    raw
    .select(from_json(col("value").cast("string"), WEATHER_SCHEMA).alias("data"))
    .select("data.*")
)

# drop junk rows
# A real weather record always has a station_id; NULL means junk.
valid = parsed.filter(col("station_id").isNotNull())

# --- Remove duplicates (CLEAN) ---
# Same station + same observed_at = the same measurement, even if the producer
# sent it twice (at-least-once delivery). Spark remembers readings it has seen
# so it can recognise repeats; the watermark caps that memory at 3 hours behind
# the newest observed_at, so state doesn't grow forever. Readings older than
# the watermark are dropped as too late.
deduped = (
    valid
    .withWatermark("observed_at", "3 hours")
    .dropDuplicates(["station_id", "observed_at"])
)


# --- Add partition column ---
# Derive the calendar day from event time (observed_at), not ingest time,
# so each reading lands in the day it was actually measured.
with_date = deduped.withColumn("date", to_date(col("observed_at")))

# --- History lookup ---
# The lake's readings since `since` (only the columns the baseline needs).
# Filtering on the `date` partition first means only the last ~2 weeks of
# folders are opened, however large the lake grows.
# Returns None on the very first batch, when the lake folder doesn't exist yet.
def load_history(since):
    try:
        return (
            spark.read.parquet(LAKE_PATH)
            .filter(col("date") >= lit(since.date()))
            .filter(col("observed_at") >= lit(since))
            .select(*BASELINE_COLUMNS)
        )
    except AnalysisException:
        return None


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


# --- Anomaly scoring for one micro-batch ---
# Baseline candidates = the lake's last 2 weeks plus this batch itself (on a
# replay, the whole history arrives in one batch), de-duplicated in case a
# retried batch is already in the lake.
def score_batch(batch_df):
    oldest = batch_df.agg(spark_min("observed_at")).first()[0]
    since = oldest - timedelta(days=SEASONAL_DAYS, seconds=HOUR_WINDOW_SECONDS)

    readings = batch_df.select(*BASELINE_COLUMNS)
    history = load_history(since)
    if history is not None:
        readings = readings.unionByName(history)
    readings = readings.dropDuplicates(["station_id", "observed_at"])

    return add_seasonal_scores(batch_df, readings)


# --- Per-batch processing ---
# Spark calls this once per micro-batch. batch_df is a normal (non-streaming)
# DataFrame holding only this batch's new rows; batch_id counts up 0, 1, 2 ...
def process_batch(batch_df, batch_id):
    # keep the batch in memory: its used several times below
    batch_df = batch_df.persist()
    row_count = batch_df.count()
    print(f"batch {batch_id}: {row_count} new readings")

    # Spark sometimes runs empty batches (e.g. to move the watermark forward)
    if row_count == 0:
        batch_df.unpersist()
        return

    # score and keep the result in memory: without persist, printing the 
    # alerts after the write would recompute the scores, re-reading a lake
    # that by then already contains this batch.
    scored = score_batch(batch_df).persist()

    # Append this batch to the lake, one folder per day
    (
        scored.write
        .mode("append")
        .partitionBy("date")
        .parquet(LAKE_PATH)
    )

    # print an alert line for each anomaly in this batch
    for row in scored.filter(col("is_anomaly")).collect():
        print(
            f"  ANOMALY {row.station_id} {row.observed_at} {row.temperature_c}°C "
            f"(same-hour median {row.baseline_median:.1f}, spread {row.baseline_spread:.1f}, "
            f"{row.baseline_days} days, z = {row.z_score:.1f})"
        )
    # free the memory
    scored.unpersist()
    batch_df.unpersist()

# --- Start the stream (RECORD) ---
# Every minute, hand the new rows to process_batch. After the function returns, 
# Spark saves the finished Kafka offsets to CHECKPOINT_PATH
query = (
    with_date.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime="1 minute")
    .start()
)

# keeps the script running; without it the script would exit and stop the stream
query.awaitTermination()