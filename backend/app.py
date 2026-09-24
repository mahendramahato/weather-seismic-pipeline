"""
Minimal local API serving live NOAA + USGS readings as JSON, for the
frontend to display. This is a preview seed for the Phase 7 dashboard —
later phases will swap this out for Athena-backed endpoints, but the
frontend contract (station list + earthquake list) stays the same.
"""

import sys
from pathlib import Path

from flask import Flask, jsonify
from flask_cors import CORS

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "phase0"))
from fetch_samples import fetch_earthquakes, fetch_weather  # noqa: E402

app = Flask(__name__)
CORS(app)

NOAA_STATIONS = {
    "KBOI": (43.57, -116.22, "Boise, ID"),
    "KJFK": (40.64, -73.78, "New York, NY"),
    "KLAX": (33.94, -118.41, "Los Angeles, CA"),
}


@app.get("/api/weather")
def weather():
    stations = []
    for station_id, (lat, lon, name) in NOAA_STATIONS.items():
        try:
            obs = fetch_weather(station_id)
            props = obs["properties"]
            stations.append(
                {
                    "stationId": station_id,
                    "name": name,
                    "lat": lat,
                    "lon": lon,
                    "temperatureC": props["temperature"]["value"],
                    "description": props["textDescription"],
                    "timestamp": props["timestamp"],
                }
            )
        except Exception as exc:  # noqa: BLE001
            stations.append({"stationId": station_id, "name": name, "error": str(exc)})
    return jsonify(stations)


@app.get("/api/earthquakes")
def earthquakes():
    data = fetch_earthquakes()
    quakes = [
        {
            "id": f["id"],
            "place": f["properties"]["place"],
            "mag": f["properties"]["mag"],
            "time": f["properties"]["time"],
            "lon": f["geometry"]["coordinates"][0],
            "lat": f["geometry"]["coordinates"][1],
            "depthKm": f["geometry"]["coordinates"][2],
        }
        for f in data["features"]
    ]
    return jsonify(quakes)


if __name__ == "__main__":
    app.run(port=5001, debug=True)
