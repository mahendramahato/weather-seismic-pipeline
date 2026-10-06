import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import Window
from pyspark.sql.functions import (
    avg,
    col,
    count,
    max as spark_max,
    min as spark_min,
    round as spark_round,
    row_number,
    sum as spark_sum,
    when,
)

# --- Job parameters ---
# Passed in when the job is started: --DATE 2026-09-29 --BUCKET <name>.
# JOB_NAME is supplied by Glue automatically.
args = getResolvedOptions(sys.argv, ["JOB_NAME", "DATE", "BUCKET"])
RUN_DATE = args["DATE"]
CURATED_PATH = f"s3://{args['BUCKET']}/curated"
RAW_PATH = f"s3://{args['BUCKET']}/raw"


# --- Glue + Spark setup ---
# GlueContext wraps Spark and connects it to the Glue Data Catalog.
# dynamic overwrite: "overwrite" replaces only the date partitions being
# written, not the whole table — this is what makes re-runs safe (idempotent).
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# --- Helpers ---
# Keeps one row per key: the one with the highest value in order_col.
# row_number numbers each key's rows 1, 2, 3 ... newest first; keep row 1.
def latest_per_key(df, keys, order_col):
    newest_first = Window.partitionBy(*keys).orderBy(col(order_col).desc())
    return (
        df.withColumn("row_num", row_number().over(newest_first))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )


# Writes one day as a single Parquet file into curated/<name>/date=<RUN_DATE>/,
# replacing that day if it already exists (dynamic overwrite).
def write_day(df, name):
    (
        df.coalesce(1)
        .write
        .mode("overwrite")
        .partitionBy("date")
        .parquet(f"{CURATED_PATH}/{name}")
    )

# Reads one day of raw data straight from its S3 folder — not through the
# catalog. Spark only sees partitions that are *registered* in the Glue Data
# Catalog, and partition projection (an Athena-only feature) never registers
# new days, so reading via the catalog returned 0 rows for every new day.
# basePath keeps `date` as a column. If the day's folder doesn't exist, Spark
# raises an error — a missing day should fail loudly, not "succeed" empty.
def read_raw_day(name):
    return (
        spark.read
        .option("basePath", f"{RAW_PATH}/{name}/")
        .parquet(f"{RAW_PATH}/{name}/date={RUN_DATE}/")
    )


raw_weather = read_raw_day("weather")
# A day with no weather readings means something upstream broke (sync,
# streaming, producer). Fail so Airflow shows red, instead of a green run that
# wrote nothing — which is how this bug stayed hidden for four nights.
if raw_weather.count() == 0:
    raise RuntimeError(f"No raw weather rows for {RUN_DATE}; refusing to write an empty day")
# --- Weather: remove leftover duplicates ---
# foreachBatch is at-least-once, so a retried batch can leave the same reading
# twice; keep the most recently ingested copy of each (station, observed_at).
weather = latest_per_key(raw_weather, ["station_id", "observed_at"], "ingested_at")
write_day(weather, "weather")

# --- Seismic: keep only the latest revision of each quake ---
# The raw zone keeps every USGS revision; curated keeps the newest updated_at.
raw_seismic = read_raw_day("seismic")

seismic = latest_per_key(raw_seismic, ["event_id"], "updated_at")
write_day(seismic, "seismic")

# --- Weather daily summary ---
# One row per station per day: how many readings, temperature range, and how
# many readings were scored / flagged by the streaming anomaly detector.
weather_daily = (
    weather.groupBy("date", "station_id")
    .agg(
        count("*").alias("readings"),
        spark_round(spark_min("temperature_c"), 1).alias("min_temp_c"),
        spark_round(spark_max("temperature_c"), 1).alias("max_temp_c"),
        spark_round(avg("temperature_c"), 1).alias("avg_temp_c"),
        count("z_score").alias("scored_readings"),
        spark_sum(when(col("is_anomaly"), 1).otherwise(0)).alias("anomalies"),
    )
)
write_day(weather_daily, "weather_daily")

# --- Report ---
# Printed lines appear in the job's CloudWatch output log.
print(
    f"date={RUN_DATE} "
    f"weather: {raw_weather.count()} raw -> {weather.count()} curated | "
    f"seismic: {raw_seismic.count()} raw -> {seismic.count()} curated"
)

# Marks the run as successful in Glue.
job.commit()
