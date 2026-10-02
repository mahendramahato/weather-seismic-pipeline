import { useEffect, useState } from 'react'
import { getJson, timeAgo } from '../api.js'

// The two tabs and which API endpoint each one reads:
// 24 hours = live lake (DuckDB); 7 days = curated (Athena) + live merged.
const TABS = {
  day: { label: '24 hours', path: '/api/alerts?hours=24' },
  week: { label: '7 days', path: '/api/alerts/week' },
}

export default function AlertsFeed() {
  const [tab, setTab] = useState('day')
  const [alerts, setAlerts] = useState([])
  const [loading, setLoading] = useState(true)

  // Reload whenever the selected tab changes ([tab] = run again when tab changes).
  // `cancelled` ignores a slow response that arrives after the user already
  // switched tabs, so an old answer can't overwrite the newer one.
  useEffect(() => {
    let cancelled = false
    setLoading(true)
    getJson(TABS[tab].path)
      .then((data) => {
        if (!cancelled) setAlerts(data)
      })
      .catch(() => {
        if (!cancelled) setAlerts([])
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })
    return () => {
      cancelled = true
    }
  }, [tab])

  return (
    <aside className="alerts">
      <div className="tabs">
        {Object.entries(TABS).map(([key, { label }]) => (
          <button key={key} className={key === tab ? 'active' : ''} onClick={() => setTab(key)}>
            {label}
          </button>
        ))}
      </div>

      {loading && <p className="muted">Loading…</p>}
      {!loading && alerts.length === 0 && <p className="muted">No alerts in this period.</p>}

      <ul>
        {alerts.map((a) => (
          <li key={`${a.source}-${a.id}-${a.event_time}`} className={a.source}>
            <span className="badge">{a.source === 'seismic' ? 'Quake' : 'Weather'}</span>
            <span className="detail">{a.detail}</span>
            <span className="when">{timeAgo(a.event_time)}</span>
          </li>
        ))}
      </ul>
    </aside>
  )
}
