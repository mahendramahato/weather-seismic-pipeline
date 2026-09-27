from pyspark.sql import SparkSession

# Entry point to Spark. WARN hides the noisy INFO logs.
spark = SparkSession.builder.appName("weather-stream-raw").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# --- Read from Kafka ---
# readStream (not read) makes this a continuous stream.
# kafka:9092 is the internal Docker address; "earliest" = start from offset 0.
raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "weather")
    .option("startingOffsets", "earliest")
    .load()
)

# Kafka's fixed 7 columns: key, value, topic, partition, offset, timestamp, timestampType.
raw.printSchema()

# --- Print raw messages ---
# key and value arrive as bytes, so CAST them to strings to make them readable.
# Every 10 seconds, new messages are printed as one micro-batch.
# truncate=True cuts long values to 20 characters so the table fits on screen.
query = (
    raw.selectExpr(
        "CAST(key AS STRING) AS key",
        "CAST(value AS STRING) AS value",
        "partition",
        "offset",
        "timestamp",
    )
    .writeStream
    .format("console")
    .option("truncate", True)
    .trigger(processingTime="10 seconds")
    .start()
)

# Keeps the script running; without it, the script would exit and stop the stream.
query.awaitTermination()
