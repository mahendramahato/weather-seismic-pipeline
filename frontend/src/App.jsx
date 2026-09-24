import { useEffect, useState } from "react";
import { MapContainer, TileLayer, CircleMarker, Popup } from "react-leaflet";
import "leaflet/dist/leaflet.css";
import "./App.css";

const API_BASE = "http://localhost:5001";

function magColor(mag) {
  if (mag >= 4) return "#d62728";
  if (mag >= 2) return "#ff7f0e";
  return "#1f77b4";
}

export default function App() {
  const [stations, setStations] = useState([]);
  const [quakes, setQuakes] = useState([]);
  const [error, setError] = useState(null);

  useEffect(() => {
    async function load() {
      try {
        const [weatherRes, quakeRes] = await Promise.all([
          fetch(`${API_BASE}/api/weather`),
          fetch(`${API_BASE}/api/earthquakes`),
        ]);
        setStations(await weatherRes.json());
        setQuakes(await quakeRes.json());
      } catch (err) {
        setError("Could not reach the backend API — is it running on :5001?");
      }
    }
    load();
    const interval = setInterval(load, 60000);
    return () => clearInterval(interval);
  }, []);

  return (
    <div className="app">
      <header>
        <h1>Weather &amp; Seismic Monitor</h1>
        <p className="subtitle">Live NOAA stations + USGS earthquakes (past hour)</p>
      </header>

      {error && <div className="error">{error}</div>}

      <div className="layout">
        <div className="map-wrap">
          <MapContainer center={[39, -98]} zoom={4} style={{ height: "100%", width: "100%" }}>
            <TileLayer
              url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
              attribution="&copy; OpenStreetMap contributors"
            />
            {stations.map((s) =>
              s.error ? null : (
                <CircleMarker
                  key={s.stationId}
                  center={[s.lat, s.lon]}
                  radius={9}
                  pathOptions={{ color: "#2ca02c", fillOpacity: 0.8 }}
                >
                  <Popup>
                    <strong>{s.name}</strong>
                    <br />
                    {s.temperatureC}°C, {s.description}
                  </Popup>
                </CircleMarker>
              )
            )}
            {quakes.map((q) => (
              <CircleMarker
                key={q.id}
                center={[q.lat, q.lon]}
                radius={Math.max(4, q.mag * 3)}
                pathOptions={{ color: magColor(q.mag), fillOpacity: 0.6 }}
              >
                <Popup>
                  <strong>M{q.mag}</strong> — {q.place}
                  <br />
                  depth: {q.depthKm} km
                </Popup>
              </CircleMarker>
            ))}
          </MapContainer>
        </div>

        <div className="panel">
          <section>
            <h2>Stations</h2>
            <ul className="card-list">
              {stations.map((s) => (
                <li key={s.stationId} className="card">
                  <strong>{s.name}</strong>
                  {s.error ? (
                    <span className="card-error">{s.error}</span>
                  ) : (
                    <span>
                      {s.temperatureC}°C · {s.description}
                    </span>
                  )}
                </li>
              ))}
            </ul>
          </section>

          <section>
            <h2>Recent earthquakes ({quakes.length})</h2>
            <ul className="card-list">
              {quakes.slice(0, 10).map((q) => (
                <li key={q.id} className="card">
                  <strong style={{ color: magColor(q.mag) }}>M{q.mag}</strong>{" "}
                  {q.place}
                </li>
              ))}
              {quakes.length === 0 && <li className="card">No earthquakes in the past hour</li>}
            </ul>
          </section>
        </div>
      </div>
    </div>
  );
}
