import { STATION_NAMES } from '../lib/stations.js'
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

// One compact row; selecting it opens the station panel on the right.
function StationRow({ station, history, selected, onSelect }) {
  const name = STATION_NAMES[station.station_id] ?? station.station_id
  return (
    <li className={`station-row ${station.status}${selected ? ' selected' : ''}`}>
      <button
        type="button"
        className="station-row-main"
        aria-pressed={selected}
        onClick={() => onSelect(selected ? null : station.station_id)}
      >
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
    </li>
  )
}

export default function StationList({ stations, historyByStation, selectedId, onSelect }) {
  const sorted = [...stations].sort(byImportance)
  const scoring = stations.filter((s) => s.status !== 'unscored').length

  return (
    <section className="panel stations-panel">
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
