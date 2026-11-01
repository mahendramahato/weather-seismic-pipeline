import { useState } from 'react'
import { formatUtc } from '../lib/format.js'
import { SEVERITIES, severityColor } from '../lib/nws.js'

// How many rows show before "Show all".
const COLLAPSED_ROWS = 6

// Official National Weather Service warnings in force right now, most severe
// first, with a count per severity along the top.
export default function NwsAlerts({ alerts }) {
  const [expanded, setExpanded] = useState(false)
  const counts = Object.fromEntries(SEVERITIES.map((s) => [s, 0]))
  alerts.forEach((a) => (counts[a.severity] = (counts[a.severity] ?? 0) + 1))
  const rows = expanded ? alerts : alerts.slice(0, COLLAPSED_ROWS)

  return (
    <section className="panel nws" aria-label="National Weather Service alerts">
      <div className="panel-head">
        <h2>NWS warnings</h2>
        <span className="muted small">{alerts.length} in force</span>
      </div>

      {alerts.length === 0 ? (
        <p className="muted">No active alerts right now.</p>
      ) : (
        <>
          <div className="severity-counts">
            {SEVERITIES.filter((s) => counts[s]).map((s) => (
              <span key={s} className="severity-chip" style={{ '--sev': severityColor(s) }}>
                <i aria-hidden="true" />
                {counts[s]} {s.toLowerCase()}
              </span>
            ))}
          </div>
          <ul className="nws-list">
            {rows.map((a) => (
              <li key={a.alert_id} style={{ '--sev': severityColor(a.severity) }}>
                <span className="nws-bar" aria-hidden="true" />
                <span className="nws-text">
                  <span className="nws-event">{a.event}</span>
                  <span className="nws-area" title={a.area}>
                    {a.area}
                  </span>
                </span>
                <span className="nws-until">
                  {a.expires_at ? `until ${formatUtc(a.expires_at, { withDay: false })}` : a.severity}
                </span>
              </li>
            ))}
          </ul>
          {alerts.length > COLLAPSED_ROWS && (
            <button type="button" className="chip-btn show-all" onClick={() => setExpanded((v) => !v)}>
              {expanded ? 'Show fewer' : `Show all ${alerts.length}`}
            </button>
          )}
        </>
      )}
    </section>
  )
}
