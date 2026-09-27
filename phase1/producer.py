import json
import time
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer

BOOTSTRAP_SERVERS = "localhost:9094"
WEATHER_TOPIC = "weather"
SEISMIC_TOPIC = "seismic"

NOAA_HEADERS = {"User-Agent": "mahendramahato33@gmail.com"}
STATIONS = ["KBOI", "KJFK", "KLAX", "KORD", "KDEN"]
POLL_INTERVAL_SECONDS = 60  # seconds
USGS_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson"

# fetch weather data from NOAA API
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def fetch_weather_data(station_id: str) -> dict:
    url = f"https://api.weather.gov/stations/{station_id}/observations/latest"
    response = requests.get(url, headers=NOAA_HEADERS, timeout=10)
    response.raise_for_status()
    obs = response.json()
    props = obs["properties"]
    lon, lat = obs["geometry"]["coordinates"]
    return {
        "station_id": station_id,
        "observed_at": props["timestamp"],
        "lat": lat,
        "lon": lon,
        "temperature_c": props["temperature"]["value"],
        "humidity_pct": props["relativeHumidity"]["value"],
        "wind_speed_kmh": props["windSpeed"]["value"],
        "description": props["textDescription"],
        "ingested_at": utc_now_iso(),
    }

def fetch_earthquakes() -> list[dict]:
    response = requests.get(USGS_URL, timeout=10)
    response.raise_for_status()
    records = []
    for feature in response.json()["features"]:
        props = feature["properties"]
        lon, lat, depth_km = feature["geometry"]["coordinates"]
        records.append(
            {
                "event_id": feature["id"],
                "event_time": datetime.fromtimestamp(props["time"] / 1000, tz=timezone.utc).isoformat(),
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

# deliver messages to Kafka
def delivery_report(err, msg):
    if err is not None:
        print(f"FAILED  {msg.topic()} key={msg.key()}: {err}")
    else:
        print(f"OK      {msg.topic()} [partition {msg.partition()}] "
            f"offset {msg.offset()} key={msg.key().decode()}")

def send(producer: Producer, topic: str, key: str, record:dict) -> None:
    producer.produce(
        topic=topic,
        key=key.encode("utf-8"),
        value=json.dumps(record).encode("utf-8"),
        callback=delivery_report,
    )

def poll_weather(producer: Producer, last_weather: dict[str, str]) -> None:
    sent = 0
    for station_id in STATIONS:
        try: 
            record = fetch_weather_data(station_id)
        except requests.RequestException as exc:
            print(f"SKIP    weather {station_id}: {exc}")
            continue
        if last_weather.get(station_id) == record["observed_at"]:
            continue
        last_weather[station_id] = record["observed_at"]
        send(producer, WEATHER_TOPIC, station_id, record)
        sent += 1
    print(f"{utc_now_iso()}  weather: sent {sent} new")

def poll_seismic(producer: Producer, last_quakes: dict[str, str]) -> dict[str, str]:
    try:
        quakes = fetch_earthquakes()
    except requests.RequestException as exc:
        print(f"SKIP    seismic: {exc}")
        return last_quakes

    current = {}
    sent = 0
    for quake in quakes:
        current[quake["event_id"]] = quake["updated_at"]
        if last_quakes.get(quake["event_id"]) == quake["updated_at"]:
            continue
        send(producer, SEISMIC_TOPIC, quake["event_id"], quake)
        sent += 1
    print(f"{utc_now_iso()}  seismic: sent {sent} new ({len(quakes)} in feed)")
    return current

def main():
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
    last_weather: dict[str, str] = {}
    last_quakes: dict[str, str] = {}

    print(f"polling every {POLL_INTERVAL_SECONDS} seconds... - Ctrl+C to stop")
    try:
        while True:
            poll_weather(producer, last_weather)
            last_quakes = poll_seismic(producer, last_quakes)
            producer.flush(0)
            time.sleep(POLL_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        print("stopping...")
    finally:
        producer.flush(10)

if __name__ == "__main__":
    main()