import { formatUtc } from '../lib/format.js'
import { SEVERITIES, severityColor } from '../lib/nws.js'

// Official National Weather Service alerts in force right now, most severe
// first, with a count per severity along the top. Shown as a tab of the
// Alerts panel, which scrolls the list inside the panel.
export default function NwsAlerts({ alerts }) {
  if (alerts.length === 0) {
    return <p className="muted">No active NWS alerts right now.</p>
  }

  const counts = Object.fromEntries(SEVERITIES.map((s) => [s, 0]))
  alerts.forEach((a) => (counts[a.severity] = (counts[a.severity] ?? 0) + 1))

  return (
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
        {alerts.map((a) => (
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
    </>
  )
}
