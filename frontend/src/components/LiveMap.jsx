import { CircleMarker, MapContainer, Popup, TileLayer } from 'react-leaflet'
import { timeAgo } from '../api.js'

// Station marker colour by the streaming detector's verdict.
const STATUS_COLORS = { normal: '#2e7d32', anomaly: '#c62828', unscored: '#9e9e9e' }

// Bigger quakes get bigger circles; a minimum so tiny ones stay visible.
function quakeRadius(magnitude) {
  return Math.max(3, magnitude * 3)
}

export default function LiveMap({ stations, quakes }) {
  return (
    // Centred on North America, zoomed out enough to see Pacific quakes too.
    <MapContainer center={[30, -100]} zoom={3} className="map">
      {/* Base map tiles from OpenStreetMap — free, no API key. Their usage
          policy requires this attribution and allows light use like a
          portfolio site (heavy-traffic apps should use a paid tile provider). */}
      <TileLayer
        url="https://tile.openstreetmap.org/{z}/{x}/{y}.png"
        attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
      />

      {/* Quakes first, so station markers are drawn on top of them.
          key = a stable unique id, so React can track each marker. */}
      {quakes.map((q) => (
        <CircleMarker
          key={q.event_id}
          center={[q.lat, q.lon]}
          radius={quakeRadius(q.magnitude)}
          pathOptions={{
            color: q.is_significant ? '#e65100' : '#5c6bc0',
            fillOpacity: 0.4,
            weight: q.is_significant ? 2 : 1,
          }}
        >
          <Popup>
            <strong>M{q.magnitude}</strong> {q.place}
            <br />
            {timeAgo(q.event_time)}, depth {q.depth_km} km
          </Popup>
        </CircleMarker>
      ))}

      {stations.map((s) => (
        <CircleMarker
          key={s.station_id}
          center={[s.lat, s.lon]}
          radius={9}
          pathOptions={{ color: '#ffffff', fillColor: STATUS_COLORS[s.status], fillOpacity: 1, weight: 2 }}
        >
          <Popup>
            <strong>{s.station_id}</strong>: {s.temperature_c}°C, {s.description}
            <br />
            Status: {s.status}
            {s.z_score !== null && ` (z = ${s.z_score})`}
            <br />
            {timeAgo(s.observed_at)}
          </Popup>
        </CircleMarker>
      ))}
    </MapContainer>
  )
}

