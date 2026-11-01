"""In-memory baseline history for the weather stream.

The anomaly detector compares each new reading with the same station's
readings from the previous 14 days. Those readings are tiny — station, time
and temperature, roughly 15 stations x a few dozen readings a day, about
10,000 rows — but they live in the lake as thousands of small Parquet files.
Re-reading them every minute (and listing the WHOLE lake to find them) kept
weather-stream at ~3.5 CPU cores and made batches fall behind.

So the history is read from the lake once at startup and then kept up to date
from each batch, in a plain Python dict. The lake is only read again if a
batch needs older readings than memory holds (e.g. replaying from Kafka).

Kept separate from weather_stream.py so it can be tested without a stream.
"""
import glob
import os
from datetime import timedelta

from pyspark.sql.functions import col, lit
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

from detection import BASELINE_COLUMNS, HOUR_WINDOW_SECONDS, SEASONAL_DAYS

HISTORY_SCHEMA = StructType([
    StructField("station_id", StringType()),
    StructField("observed_at", TimestampType()),
    StructField("temperature_c", DoubleType()),
])

# How far back a reading's baseline looks (same as detection.py).
LOOKBACK = timedelta(days=SEASONAL_DAYS, seconds=HOUR_WINDOW_SECONDS)
# Keep one extra day in memory, so a batch whose oldest reading is a little
# older than the newest one still finds all its history here.
KEEP = LOOKBACK + timedelta(days=1)


class BaselineHistory:
    def __init__(self, spark, lake_path):
        self.spark = spark
        self.lake_path = lake_path
        # (station_id, observed_at) -> temperature_c. Keyed, so a reading
        # seen twice (lake + a retried batch) is stored once.
        self.rows = {}
        # Memory holds every lake reading from this time onward (None = nothing loaded yet).
        self.complete_from = None

    # Lake readings since `since`, opening only the day folders that can hold
    # them — never listing the whole lake.
    def _read_lake(self, since):
        first_folder = f"date={since.date().isoformat()}"
        folders = sorted(
            path for path in glob.glob(os.path.join(self.lake_path, "date=*"))
            if os.path.basename(path) >= first_folder
        )
        if not folders:
            return []
        return (
            self.spark.read.option("basePath", self.lake_path)
            .parquet(*folders)
            .filter(col("observed_at") >= lit(since))
            .select(*BASELINE_COLUMNS)
            .collect()
        )

    # Make sure memory covers everything from `since` onward.
    def _ensure(self, since):
        if self.complete_from is not None and since >= self.complete_from:
            return
        for row in self._read_lake(since):
            self.rows.setdefault((row.station_id, row.observed_at), row.temperature_c)
        self.complete_from = since

    # History readings since `since` as a small DataFrame (None if there are none).
    def readings_since(self, since):
        self._ensure(since)
        data = [(station, at, temp) for (station, at), temp in self.rows.items() if at >= since]
        return self.spark.createDataFrame(data, HISTORY_SCHEMA) if data else None

    # Remember a batch that was just written to the lake, then forget readings
    # too old to ever be needed again.
    def add(self, rows):
        newest = None
        for row in rows:
            self.rows[(row.station_id, row.observed_at)] = row.temperature_c
            if newest is None or row.observed_at > newest:
                newest = row.observed_at
        if newest is None:
            return
        cutoff = newest - KEEP
        for key in [key for key in self.rows if key[1] < cutoff]:
            del self.rows[key]
        # Memory is still complete from the cutoff (or later, if it never went back that far).
        if self.complete_from is None or self.complete_from < cutoff:
            self.complete_from = cutoff
