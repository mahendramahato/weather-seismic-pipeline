from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_date, coalesce
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
SEISMIC_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_time", TimestampType()),
    StructField("updated_at", TimestampType()),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("magnitude", DoubleType()),
    StructField("place", StringType()),
    StructField("depth_km", DoubleType()),
    StructField("ingested_at", TimestampType())
])

# --- Output locations ---
# /opt/data inside the container is the project's data/ folder on the Mac.
# LAKE_PATH holds the Parquet files; CHECKPOINT_PATH is where Spark records
# which Kafka offsets it has already written, so a restart continues from there.
LAKE_PATH = "/opt/data/lake/seismic"
CHECKPOINT_PATH = "/opt/data/checkpoints/seismic"


# --- Spark session ---
# Entry point to Spark. WARN hides the noisy INFO logs.
# UTC timezone so to_date() assigns every earthquake to the
# same calendar day no matter where the job runs.
spark = (
    SparkSession.builder
    .appName("seismic-stream")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# --- Read (ASK + READ) ---
# Stream from the seismic topic. kafka:9092 is the internal Docker address.
# "earliest" = start from offset 0 the first time the job runs.
raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "seismic")
    .option("startingOffsets", "earliest")
    .load()
)

# --- Parse (PARSE) ---
# value bytes -> string -> struct named "data" (using the schema),
# then data.* unpacks each field into its own column.
# Messages that don't match the schema come out as all NULLs instead of crashing.
parsed = (
    raw
    .select(from_json(col("value").cast("string"), SEISMIC_SCHEMA).alias("data"))
    .select("data.*")
)

# drop junk rows
# A real earthquake record always has an event_id; NULL means junk.
valid = parsed.filter(col("event_id").isNotNull())

# --- Handle old messages (schema evolution) ---
# Messages sent before the producer added updated_at (Step 1.4) don't have it,
# so it parses as NULL. Fill it with event_time: an unrevised quake was last
# "updated" when it happened. coalesce() returns the first non-NULL value.
filled = valid.withColumn("updated_at", coalesce(col("updated_at"), col("event_time")))

# --- Remove duplicates (CLEAN) ---
# Same event_id + same updated_at = the same version of a quake, even if the
# producer sent it twice (at-least-once delivery). A different updated_at is a
# USGS revision (e.g. new magnitude), so it's kept. Spark remembers versions it
# has seen so it can recognise repeats; the watermark caps that memory at
# 3 hours behind the newest updated_at, so state doesn't grow forever.
# Versions older than the watermark are dropped as too late.
deduped = (
    filled
    .withWatermark("updated_at", "3 hours")
    .dropDuplicates(["event_id", "updated_at"])
)


# --- Add partition column ---
# Derive the calendar day from event time (event_time), not update or ingest
# time, so each quake — and all its revisions — lands in the day it happened.
with_date = deduped.withColumn("date", to_date(col("event_time")))

# --- Write (WRITE + RECORD) ---
# Every minute, append the new rows as Parquet files under LAKE_PATH,
# one folder per day (date=YYYY-MM-DD). After each batch, Spark saves the
# Kafka offsets it finished to CHECKPOINT_PATH.
# append = only write new rows; never rewrite files that already exist.
query = (
    with_date.writeStream
    .format("parquet")
    .option("path", LAKE_PATH)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .partitionBy("date")
    .outputMode("append")
    .trigger(processingTime="1 minute")
    .start()
)

# Keeps the script running; without it, the script would exit and stop the stream.
query.awaitTermination()

