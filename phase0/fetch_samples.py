"""
Phase 0: pull one real reading from the NOAA weather API and the USGS
earthquake feed, and print the raw JSON so we can look at the actual
shape of the data before building anything on top of it.

NOAA requires a descriptive User-Agent header (no API key). USGS needs
no auth at all.
"""

import json

import requests

NOAA_STATION_ID = "KBOI"  # Boise, ID
NOAA_HEADERS = {
    "User-Agent": "weather-seismic-pipeline (mahendramahato33@gmail.com)"
}

USGS_URL = (
    "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson"
)


def fetch_weather(station_id: str = NOAA_STATION_ID) -> dict:
    url = f"https://api.weather.gov/stations/{station_id}/observations/latest"
    response = requests.get(url, headers=NOAA_HEADERS, timeout=10)
    response.raise_for_status()
    return response.json()


def fetch_earthquakes() -> dict:
    response = requests.get(USGS_URL, timeout=10)
    response.raise_for_status()
    return response.json()


def main() -> None:
    print(f"=== NOAA weather observation ({NOAA_STATION_ID}) ===")
    weather = fetch_weather()
    print(json.dumps(weather, indent=2))

    print("\n=== USGS earthquakes (past hour) ===")
    earthquakes = fetch_earthquakes()
    print(f"count: {earthquakes['metadata']['count']}")
    if earthquakes["features"]:
        print(json.dumps(earthquakes["features"][0], indent=2))
    else:
        print("no earthquakes in the last hour")


if __name__ == "__main__":
    main()
