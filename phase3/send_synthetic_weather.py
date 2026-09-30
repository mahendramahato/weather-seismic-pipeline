import json
import math
from datetime import datetime, timedelta, timezone

from confluent_kafka import Producer

# --- Config ---
# Test topic only — never the real "weather" topic, or fake readings would end
# up in the real lake permanently.
BOOTSTRAP_SERVERS = "localhost:9094"
TEST_TOPIC = "weather-test"
HISTORY_HOURS = 30

# Start of the current hour, UTC; all synthetic times are relative to this.
now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)


# Same fields and format as the real producer, so the Spark job parses it
# exactly like real data.
def reading(station_id, observed_at, temperature_c):
    return {
        "station_id": station_id,
        "observed_at": observed_at.isoformat(),
        "lat": 0.0,
        "lon": 0.0,
        "temperature_c": temperature_c,
        "humidity_pct": 50.0,
        "wind_speed_kmh": 10.0,
        "description": "synthetic",
        "ingested_at": now.isoformat(),
    }


# A realistic daily cycle: coldest (9°C) at 03:00, warmest (21°C) at 15:00.
# Using a cycle, not a flat line, means the test also proves normal daily
# swings are NOT flagged.
def normal_temperature(t):
    return round(15 + 6 * math.sin(2 * math.pi * (t.hour - 9) / 24), 1)


# --- Build the test data ---
# Two stations, each with 29 hourly "normal" readings covering the past day
# (enough to pass the 18-hour coverage check), then the test cases.
messages = []
for station_id in ["TESTHOT", "TESTCOLD"]:
    for hours_ago in range(HISTORY_HOURS, 1, -1):
        t = now - timedelta(hours=hours_ago)
        messages.append(reading(station_id, t, normal_temperature(t)))

# Test cases, each with the answer we expect.
messages.append(reading("TESTHOT", now - timedelta(minutes=90), 20.0))   # expect: normal
messages.append(reading("TESTHOT", now - timedelta(minutes=60), 45.0))   # expect: ANOMALY (hot)
messages.append(reading("TESTCOLD", now - timedelta(minutes=60), -10.0)) # expect: ANOMALY (cold)

# --- Send ---
# Keyed by station, like the real producer. flush() waits until everything is
# delivered before the script exits.
producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
for message in messages:
    producer.produce(
        TEST_TOPIC,
        key=message["station_id"].encode("utf-8"),
        value=json.dumps(message).encode("utf-8"),
    )
producer.flush(10)
print(f"sent {len(messages)} synthetic readings to {TEST_TOPIC}")

