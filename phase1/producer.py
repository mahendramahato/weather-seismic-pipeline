import json
import time
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer

# --- Config ---
# localhost:9094 is Kafka's HOST listener: this script runs on the Mac, outside Docker.
BOOTSTRAP_SERVERS = "localhost:9094"
WEATHER_TOPIC = "weather"
SEISMIC_TOPIC = "seismic"

# NOAA requires a User-Agent identifying the app and a contact.
NOAA_HEADERS = {"User-Agent": "mahendramahato33@gmail.com"}
STATIONS = ["KBOI", "KJFK", "KLAX", "KORD", "KDEN"]
POLL_INTERVAL_SECONDS = 60  # seconds
USGS_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson"


# Current time in UTC as an ISO string — used for the ingested_at field.
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# --- Fetch + clean ---
# Calls NOAA for one station's latest observation and flattens the large raw
# response into a small record with only the fields we need.
def fetch_weather_data(station_id: str) -> dict:
    url = f"https://api.weather.gov/stations/{station_id}/observations/latest"
    response = requests.get(url, headers=NOAA_HEADERS, timeout=10)
    response.raise_for_status()
    obs = response.json()
    props = obs["properties"]
    # GeoJSON stores coordinates as [lon, lat] — longitude first.
    lon, lat = obs["geometry"]["coordinates"]
    return {
        "station_id": station_id,
        "observed_at": props["timestamp"],  # event time: when it was measured
        "lat": lat,
        "lon": lon,
        "temperature_c": props["temperature"]["value"],
        "humidity_pct": props["relativeHumidity"]["value"],
        "wind_speed_kmh": props["windSpeed"]["value"],
        "description": props["textDescription"],
        "ingested_at": utc_now_iso(),  # processing time: when we fetched it
    }


# Calls the USGS past-hour feed and turns each earthquake into its own record.
# USGS times are epoch milliseconds, so they're converted to ISO strings to
# match NOAA's format.
def fetch_earthquakes() -> list[dict]:
    response = requests.get(USGS_URL, timeout=10)
    response.raise_for_status()
    records = []
    for feature in response.json()["features"]:
        props = feature["properties"]
        # [lon, lat, depth] — longitude first again.
        lon, lat, depth_km = feature["geometry"]["coordinates"]
        records.append(
            {
                "event_id": feature["id"],
                "event_time": datetime.fromtimestamp(props["time"] / 1000, tz=timezone.utc).isoformat(),
                # Changes whenever USGS revises the quake (e.g. new magnitude).
                "updated_at": datetime.fromtimestamp(props["updated"] / 1000, tz=timezone.utc).isoformat(),
                "magnitude": props["mag"],
                "place": props["place"],
                "lat": lat,
                "lon": lon,
                "depth_km": depth_km,
                "ingested_at": utc_now_iso(),
            }
        )
    return records


# --- Kafka sending ---
# Delivery callback: Kafka calls this later (during poll/flush) with the
# result of each send — success with its offset, or the error.
def delivery_report(err, msg):
    if err is not None:
        print(f"FAILED  {msg.topic()} key={msg.key()}: {err}")
    else:
        print(f"OK      {msg.topic()} [partition {msg.partition()}] "
            f"offset {msg.offset()} key={msg.key().decode()}")


# Serializes a record to JSON bytes and queues it for sending. produce() is
# asynchronous: it returns immediately; a background thread does the sending.
def send(producer: Producer, topic: str, key: str, record:dict) -> None:
    producer.produce(
        topic=topic,
        key=key.encode("utf-8"),
        value=json.dumps(record).encode("utf-8"),
        callback=delivery_report,
    )


# --- Polling with de-duplication ---
# Fetches every station and sends only readings we haven't sent before.
# last_weather remembers each station's last observed_at and is updated in
# place, so the caller keeps the changes.
def poll_weather(producer: Producer, last_weather: dict[str, str]) -> None:
    sent = 0
    for station_id in STATIONS:
        # One failing station shouldn't stop the others.
        try:
            record = fetch_weather_data(station_id)
        except requests.RequestException as exc:
            print(f"SKIP    weather {station_id}: {exc}")
            continue
        # Same observed_at as last time = same measurement, skip it.
        if last_weather.get(station_id) == record["observed_at"]:
            continue
        last_weather[station_id] = record["observed_at"]
        send(producer, WEATHER_TOPIC, station_id, record)
        sent += 1
    print(f"{utc_now_iso()}  weather: sent {sent} new")


# Fetches the past-hour quake feed and sends only new or revised quakes.
# Returns a fresh snapshot (event_id -> updated_at) of the current feed, so
# quakes that drop out of the feed are forgotten and memory stays small.
def poll_seismic(producer: Producer, last_quakes: dict[str, str]) -> dict[str, str]:
    try:
        quakes = fetch_earthquakes()
    except requests.RequestException as exc:
        print(f"SKIP    seismic: {exc}")
        # Keep the old snapshot; returning {} would resend everything next time.
        return last_quakes

    current = {}
    sent = 0
    for quake in quakes:
        current[quake["event_id"]] = quake["updated_at"]
        # Same event and same updated_at = nothing changed, skip it.
        if last_quakes.get(quake["event_id"]) == quake["updated_at"]:
            continue
        send(producer, SEISMIC_TOPIC, quake["event_id"], quake)
        sent += 1
    print(f"{utc_now_iso()}  seismic: sent {sent} new ({len(quakes)} in feed)")
    return current


# --- Main loop ---
# Polls both sources every POLL_INTERVAL_SECONDS until Ctrl+C.
def main():
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
    # "Already sent" memory. Lives only in RAM, so a restart resends once
    # (at-least-once delivery).
    last_weather: dict[str, str] = {}
    last_quakes: dict[str, str] = {}

    print(f"polling every {POLL_INTERVAL_SECONDS} seconds... - Ctrl+C to stop")
    try:
        while True:
            poll_weather(producer, last_weather)
            last_quakes = poll_seismic(producer, last_quakes)
            # Runs pending delivery callbacks without waiting.
            producer.flush(0)
            time.sleep(POLL_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        # Ctrl+C raises KeyboardInterrupt — catch it to shut down cleanly.
        print("stopping...")
    finally:
        # Always runs, even on a crash: send anything still queued in memory,
        # otherwise it's silently lost.
        producer.flush(10)

if __name__ == "__main__":
    main()
