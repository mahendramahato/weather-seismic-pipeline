import { timeAgo, toFahrenheit } from '../lib/format.js'
import { STATION_NAMES, STATUS_LABELS } from '../lib/stations.js'
import Sparkline from './Sparkline.jsx'

// One weather station: current conditions, the detector's verdict, and the
// last 24 hours of temperature against its normal range.
export default function StationCard({ station, history, selected, onSelect }) {
  const hasTemperature = station.temperature_c !== null

  return (
    <button
      type="button"
      className={`station-card ${station.status}${selected ? ' selected' : ''}`}
      onClick={() => onSelect(station.station_id)}
    >
      <div className="card-head">
        <div>
          <div className="city">{STATION_NAMES[station.station_id] ?? station.station_id}</div>
          <div className="card-sub">
            {station.station_id} · {timeAgo(station.observed_at)}
          </div>
        </div>
        <span
          className={`status-chip ${station.status}`}
          title={station.status === 'unscored' ? 'Needs 18 of the last 24 hours of readings before it can be judged' : undefined}
        >
          {STATUS_LABELS[station.status]}
          {station.z_score !== null && ` · z ${station.z_score > 0 ? '+' : ''}${station.z_score}`}
        </span>
      </div>

      <div className="card-body">
        <div className="temp">
          {hasTemperature ? Math.round(station.temperature_c) : '–'}
          <span className="unit">°C</span>
        </div>
        <div className="conditions">
          <div>{station.description || 'No description'}</div>
          {hasTemperature && <div className="muted">{toFahrenheit(station.temperature_c)}°F</div>}
        </div>
        <dl className="metrics">
          <div>
            <dt>Humidity</dt>
            <dd>{station.humidity_pct === null ? '–' : `${station.humidity_pct}%`}</dd>
          </div>
          <div>
            <dt>Wind</dt>
            <dd>{station.wind_speed_kmh === null ? '–' : `${station.wind_speed_kmh} km/h`}</dd>
          </div>
        </dl>
      </div>

      <Sparkline points={history} />
    </button>
  )
}
