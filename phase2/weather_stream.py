from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
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

# --- Spark session ---
# Entry point to Spark. WARN hides the noisy INFO logs.
spark = SparkSession.builder.appName("weather-stream").getOrCreate()
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

valid.printSchema()

# --- Output ---
# Print each micro-batch to the terminal every 10 seconds (Parquet in Step 2.4).
# Nothing runs until start(); awaitTermination() keeps the script alive.
query = (
    valid.writeStream
    .format("console")
    .option("truncate", False)
    .trigger(processingTime="10 seconds")
    .start()
)

query.awaitTermination()
