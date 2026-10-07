import json
import math
from datetime import datetime, timedelta, timezone

from confluent_kafka import Producer

# --- Config ---
# Test topic only — never the real "weather" topic, or fake readings would end
# up in the real lake permanently.
BOOTSTRAP_SERVERS = "localhost:9094"
TEST_TOPIC = "weather-test"

# The seasonal detector needs same-hour readings from at least 5 previous
# days; 8 days of hourly history gives it a full baseline.
HISTORY_DAYS = 8

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


# A realistic daily cycle — coldest (~9 °C) at 03:00, warmest (~21 °C) at
# 15:00 — plus a small day-to-day wobble so the baseline spread isn't zero.
def normal_temperature(t):
    daily_cycle = 6 * math.sin(2 * math.pi * (t.hour - 9) / 24)
    wobble = 0.6 * math.sin(t.day * 1.7)
    return round(15 + daily_cycle + wobble, 1)


# The most recent HH:30 that's at least an hour ago (so it sits inside the
# test data's last day, between the hourly history readings).
def most_recent(hour):
    t = now.replace(hour=hour) + timedelta(minutes=30)
    while t > now - timedelta(hours=1):
        t -= timedelta(days=1)
    return t


# --- Build the test data ---
# Two stations, each with 8 days of hourly "normal" readings ...
messages = []
for station_id in ["TESTHOT", "TESTCOLD"]:
    for hours_ago in range(HISTORY_DAYS * 24, 1, -1):
        t = now - timedelta(hours=hours_ago)
        messages.append(reading(station_id, t, normal_temperature(t)))

# ... then the test cases, each with the answer we expect.
# 1. A warm afternoon: 21 °C at 15:30 is normal for an afternoon.
messages.append(reading("TESTHOT", most_recent(15), 21.0))                # expect: normal
# 2. The same 21 °C at 03:30 is far above a normal night (~9 °C). The old
#    24-hour average detector missed this; the same-hour baseline catches it.
messages.append(reading("TESTHOT", most_recent(3), 21.0))                 # expect: ANOMALY (warm night)
# 3. and 4. Extremes in both directions.
messages.append(reading("TESTHOT", now - timedelta(minutes=50), 45.0))    # expect: ANOMALY (hot)
messages.append(reading("TESTCOLD", now - timedelta(minutes=50), -10.0))  # expect: ANOMALY (cold)

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
