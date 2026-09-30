import os

from datetime import timedelta

from pyspark.errors import AnalysisException
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    abs as spark_abs,
    avg,
    coalesce,
    col,
    count,
    from_json,
    greatest,
    lit,
    min as spark_min,
    stddev,
    to_date,
    when,
    collect_set,
    date_trunc,
    size,
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


# --- Anomaly rule ---
# A reading is an anomaly if it's more than Z_THRESHOLD standard deviations
# from its station's average over the previous BASELINE_HOURS.
BASELINE_HOURS = 24
Z_THRESHOLD = 3.0

# Fewer readings than this = baseline too thin to judge; the reading isn't scored.
MIN_BASELINE_READINGS = 12

# The baseline must cover at least this many distinct hours of the previous 24.
# Temperature follows a daily cycle; a baseline of only night-time readings
# makes every normal afternoon look like an anomaly (gaps in collection cause this).
MIN_BASELINE_HOURS = 18

# Floor for the standard deviation: some stations report whole degrees, so tiny
# deviations are rounding noise, and a 0 deivation would divide by zero.
MIN_STD_C = 1.0

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
# Reads the lake's readings since `since`, with the same columns as the batch
# (the lake also stores score columns, which we don't want mixed in).
# Returns None on the very first batch, when the lake folder doesn't exist yet.
def load_history(since, columns):
    try:
        return (
            spark.read.parquet(LAKE_PATH)
            .filter(col("observed_at") >= lit(since))
            .select(*columns)
        )
    except AnalysisException:
        return None

# --- Anomaly scoring (DETECT) ---
# Combines the new readings with the last 24h of history, computes each new
# reading's rolling baseline from the readings before it, then its z-score
def score_batch(batch_df):
    # how far back history is needed: 24h before the oldest reading in the batch
    oldest = batch_df.agg(spark_min("observed_at")).first()[0]
    since = oldest - timedelta(hours=BASELINE_HOURS)

    # new rows are marked is_new=True, history rows False, so we can keep only 
    # the new ones after the window are computed
    combined = batch_df.withColumn("is_new", lit(True))
    history = load_history(since, batch_df.columns)
    if history is not None:
        combined = combined.unionByName(history.withColumn("is_new", lit(False)))

    # Rolling window: same station, from 24h before each reading upto 1 second
    # before it - so a reading is never part of its own baseline.
    previous_24h = (
        Window.partitionBy("station_id")
        .orderBy(col("observed_at").cast("long"))
        .rangeBetween(-BASELINE_HOURS * 3600, -1)
    )

    return (
        combined
        .withColumn("baseline_avg", avg("temperature_c").over(previous_24h))
        .withColumn("baseline_std", stddev("temperature_c").over(previous_24h))
        .withColumn("baseline_count", count("temperature_c").over(previous_24h))
        # Distinct hours covered by the baseline: round each time down to the hour,
        # collect the unique values, count them.
        .withColumn(
            "baseline_hours",
            size(collect_set(date_trunc("hour", col("observed_at"))).over(previous_24h)),
        )
        .filter(col("is_new"))
        .drop("is_new")

        # z = how many standard deviations from the baseline average.
        # NULL when the baseline is too thin (too few readings) or too lopsided
        # (too few distinct hours) to judge.
        .withColumn(
            "z_score",
            when(
                (col("baseline_count") >= MIN_BASELINE_READINGS)
                & (col("baseline_hours") >= MIN_BASELINE_HOURS),
                (col("temperature_c") - col("baseline_avg"))
                / greatest(col("baseline_std"), lit(MIN_STD_C)),
            ),
        )
        # anomaly = z beyond the threshold in either direction (too hot or too cold)
        # NULL z (not scored) counts as not an anomaly
        .withColumn(
            "is_anomaly",
            coalesce(spark_abs(col("z_score")) > Z_THRESHOLD, lit(False)),
        )
    )
                                    
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
            f"(baseline {row.baseline_avg:.1f} ± {row.baseline_std:.1f}, z = {row.z_score:.1f})"
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