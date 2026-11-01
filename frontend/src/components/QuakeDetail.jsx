import { formatUtc, timeAgo } from '../lib/format.js'
import { depthClass, magnitudeColor } from '../lib/quakes.js'

// Deepest earthquakes are about 700 km down; the bar is scaled to that.
const DEPTH_MAX_KM = 700

function formatCoord(value, positive, negative) {
  return `${Math.abs(value).toFixed(1)}° ${value >= 0 ? positive : negative}`
}

// Details for one quake: the one clicked on the globe, or (when nothing is
// selected) the most recent significant / largest one, so the panel is never empty.
export default function QuakeDetail({ quake, label, onClose }) {
  if (!quake) {
    return (
      <section className="panel detail-panel quake-detail">
        <div className="panel-head">
          <h2>Earthquakes</h2>
        </div>
        <p className="muted">No earthquakes in this period yet.</p>
      </section>
    )
  }

  const color = magnitudeColor(quake.magnitude)
  const depth = Math.max(quake.depth_km ?? 0, 0)

  return (
    <section className="panel detail-panel quake-detail" aria-label="Earthquake details">
      <div className="panel-head">
        <span className="kicker">{label}</span>
        {onClose && (
          <button type="button" className="icon-btn ghost" aria-label="Clear selection" onClick={onClose}>
            <svg width="14" height="14" viewBox="0 0 14 14" aria-hidden="true">
              <path d="M2 2 L12 12 M12 2 L2 12" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" />
            </svg>
          </button>
        )}
      </div>

      <div className="quake-headline">
        <span className="quake-mag" style={{ color }}>
          {quake.magnitude.toFixed(1)}
        </span>
        <span className="quake-place">{quake.place}</span>
      </div>
      <p className="muted quake-when">
        {formatUtc(quake.event_time)} · {timeAgo(quake.event_time)}
      </p>

      <div className="depth">
        <div className="depth-head">
          <span>Depth</span>
          <span className="mono">
            {Math.round(depth)} km · {depthClass(depth)}
          </span>
        </div>
        <div className="depth-track" aria-hidden="true">
          <div
            className="depth-fill"
            style={{ width: `${Math.max((depth / DEPTH_MAX_KM) * 100, 2)}%`, background: color }}
          />
        </div>
        <div className="depth-scale" aria-hidden="true">
          <span>surface</span>
          <span>{DEPTH_MAX_KM} km</span>
        </div>
      </div>

      <dl className="quake-facts">
        <div>
          <dt>Location</dt>
          <dd className="mono">
            {formatCoord(quake.lat, 'N', 'S')}, {formatCoord(quake.lon, 'E', 'W')}
          </dd>
        </div>
        <div>
          <dt>Significant</dt>
          <dd>{quake.is_significant ? 'Yes, M4.5+' : 'No'}</dd>
        </div>
        <div>
          <dt>Event ID</dt>
          <dd className="mono">{quake.event_id}</dd>
        </div>
        <div>
          <dt>Source</dt>
          <dd>USGS</dd>
        </div>
      </dl>

      <a
        className="btn-outline"
        href={`https://earthquake.usgs.gov/earthquakes/eventpage/${encodeURIComponent(quake.event_id)}`}
        target="_blank"
        rel="noreferrer"
      >
        View on USGS
      </a>
    </section>
  )
}
