import { timeAgo, toFahrenheit } from '../lib/format.js'
import { STATION_NAMES, STATUS_LABELS } from '../lib/stations.js'
import Sparkline from './Sparkline.jsx'

// Most interesting first: anomalies, then normal stations furthest from their
// usual temperature, then stations still calibrating (alphabetically).
function byImportance(a, b) {
  const rank = { anomaly: 0, normal: 1, unscored: 2 }
  if (rank[a.status] !== rank[b.status]) return rank[a.status] - rank[b.status]
  if (a.status !== 'unscored') return Math.abs(b.z_score) - Math.abs(a.z_score)
  return (STATION_NAMES[a.station_id] ?? a.station_id).localeCompare(STATION_NAMES[b.station_id] ?? b.station_id)
}

function formatTemp(celsius) {
  return celsius === null ? '–' : `${Math.round(celsius)}°`
}

// Signed z-score, e.g. "+1.4" / "−2.1".
function formatZ(z) {
  return `${z > 0 ? '+' : z < 0 ? '−' : ''}${Math.abs(z).toFixed(1)}`
}

// One compact row; the selected station expands to show its details and a
// full 24 h chart.
function StationRow({ station, history, selected, onSelect }) {
  const name = STATION_NAMES[station.station_id] ?? station.station_id
  return (
    <li className={`station-row ${station.status}${selected ? ' selected' : ''}`}>
      <button type="button" className="station-row-main" onClick={() => onSelect(selected ? null : station.station_id)}>
        <span className="status-dot" aria-hidden="true" />
        <span className="station-name" title={`${name} (${station.station_id})`}>
          {name}
        </span>
        <span className="station-spark" aria-hidden="true">
          <Sparkline points={history} compact />
        </span>
        <span className="station-temp">{formatTemp(station.temperature_c)}</span>
        <span className={`z-chip ${station.status}`}>
          {station.status === 'unscored' ? 'calibrating' : `z ${formatZ(station.z_score)}`}
        </span>
      </button>

      {selected && (
        <div className="station-details">
          <dl>
            <div>
              <dt>Conditions</dt>
              <dd>{station.description || '–'}</dd>
            </div>
            <div>
              <dt>Temperature</dt>
              <dd>
                {formatTemp(station.temperature_c)}C
                {station.temperature_c !== null && ` · ${toFahrenheit(station.temperature_c)}°F`}
              </dd>
            </div>
            <div>
              <dt>Humidity</dt>
              <dd>{station.humidity_pct === null ? '–' : `${station.humidity_pct}%`}</dd>
            </div>
            <div>
              <dt>Wind</dt>
              <dd>{station.wind_speed_kmh === null ? '–' : `${station.wind_speed_kmh} km/h`}</dd>
            </div>
            <div>
              <dt>Station</dt>
              <dd>{station.station_id}</dd>
            </div>
            <div>
              <dt>Status</dt>
              <dd>{STATUS_LABELS[station.status]}</dd>
            </div>
            <div>
              <dt>Updated</dt>
              <dd>{timeAgo(station.observed_at)}</dd>
            </div>
          </dl>
          <Sparkline points={history} />
          {station.status === 'unscored' && (
            <p className="muted small">
              Needs readings at this time of day from at least 5 previous days before it can be judged.
            </p>
          )}
        </div>
      )}
    </li>
  )
}

export default function StationList({ stations, historyByStation, selectedId, onSelect }) {
  const sorted = [...stations].sort(byImportance)
  const scoring = stations.filter((s) => s.status !== 'unscored').length

  return (
    <section className="panel">
      <div className="panel-head">
        <h2>Stations</h2>
        <span className="muted small">
          {stations.length} live · {scoring} scored
        </span>
      </div>
      <ul className="station-list">
        {sorted.map((s) => (
          <StationRow
            key={s.station_id}
            station={s}
            history={historyByStation[s.station_id] ?? []}
            selected={s.station_id === selectedId}
            onSelect={onSelect}
          />
        ))}
      </ul>
    </section>
  )
}
