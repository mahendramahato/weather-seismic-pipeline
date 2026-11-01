from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_date
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# --- Schema ---
# The fields the producer sends for each NWS alert. Names must match the JSON
# keys exactly, or that column comes back NULL.
ALERTS_SCHEMA = StructType([
    StructField("alert_id", StringType()),
    StructField("event", StringType()),
    StructField("severity", StringType()),
    StructField("urgency", StringType()),
    StructField("certainty", StringType()),
    StructField("headline", StringType()),
    StructField("area", StringType()),
    StructField("sent_at", TimestampType()),
    StructField("expires_at", TimestampType()),
    StructField("message_type", StringType()),
    StructField("replaces", ArrayType(StringType())),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("geometry", StringType()),
    StructField("ingested_at", TimestampType()),
])

# --- Output locations ---
LAKE_PATH = "/opt/data/lake/alerts"
CHECKPOINT_PATH = "/opt/data/checkpoints/alerts"

# --- Spark session ---
# UTC so to_date() puts every alert in the same calendar day wherever it runs;
# 2 shuffle partitions because alerts are a small stream.
spark = (
    SparkSession.builder
    .appName("alerts-stream")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# --- Read ---
raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "alerts")
    .option("startingOffsets", "earliest")
    .load()
)

# --- Parse ---
# Messages that don't match the schema come out as NULLs; an alert without an
# id is junk and dropped.
parsed = (
    raw
    .select(from_json(col("value").cast("string"), ALERTS_SCHEMA).alias("data"))
    .select("data.*")
    .filter(col("alert_id").isNotNull())
)

# --- Remove duplicates ---
# An NWS alert never changes after it's issued (an update is a new alert with
# a new id), so the id alone identifies it. The producer may still send one
# twice (at-least-once, e.g. after a restart). The watermark keeps Spark's
# memory of seen ids to 2 days behind the newest sent_at, so state stays
# small instead of growing forever.
deduped = (
    parsed
    .withWatermark("sent_at", "2 days")
    .dropDuplicates(["alert_id"])
)

# --- Partition column ---
# The day NWS issued the alert.
with_date = deduped.withColumn("date", to_date(col("sent_at")))

# --- Write ---
# Same as the other streams: append Parquet once a minute, one folder per day,
# with Kafka offsets saved to the checkpoint after each batch.
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

query.awaitTermination()
