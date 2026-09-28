from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_date
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

# --- Output locations ---
# /opt/data inside the container is the project's data/ folder on the Mac.
# LAKE_PATH holds the Parquet files; CHECKPOINT_PATH is where Spark records
# which Kafka offsets it has already written, so a restart continues from there.
LAKE_PATH = "/opt/data/lake/weather"
CHECKPOINT_PATH = "/opt/data/checkpoints/weather"


# --- Spark session ---
# Entry point to Spark. WARN hides the noisy INFO logs.
# UTC timezone so to_date() assigns every reading to the
# same calendar day no matter where the job runs.
spark = (
    SparkSession.builder
    .appName("weather-stream")
    .config("spark.sql.session.timeZone", "UTC")
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
    .option("subscribe", "weather")
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

