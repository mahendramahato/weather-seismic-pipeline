import { useEffect, useState } from 'react'
import { getJson, timeAgo } from '../lib/format.js'

// The two tabs and which API endpoint each one reads:
// 24 hours = live lake (DuckDB); 7 days = curated (Athena) + live merged.
const TABS = {
  day: { label: '24 hours', path: '/api/alerts?hours=24' },
  week: { label: '7 days', path: '/api/alerts/week' },
}

export default function AlertsFeed() {
  const [tab, setTab] = useState('day')
  // The last answer received, remembering which tab it belongs to.
  const [result, setResult] = useState({ tab: null, alerts: [] })

  // Reload when the tab changes; ignore a slow reply from a tab the user
  // already left, so an old answer can't overwrite the newer one.
  useEffect(() => {
    let cancelled = false
    getJson(TABS[tab].path)
      .then((data) => !cancelled && setResult({ tab, alerts: data }))
      .catch(() => !cancelled && setResult({ tab, alerts: [] }))
    return () => {
      cancelled = true
    }
  }, [tab])

  // Still loading while the answer we hold is for a different tab.
  const loading = result.tab !== tab
  const alerts = loading ? [] : result.alerts

  return (
    <section className="panel alerts">
      <div className="panel-head">
        <h2>Alerts</h2>
        <div className="tabs" role="tablist">
          {Object.entries(TABS).map(([key, { label }]) => (
            <button
              key={key}
              type="button"
              role="tab"
              aria-selected={key === tab}
              className={key === tab ? 'active' : ''}
              onClick={() => setTab(key)}
            >
              {label}
            </button>
          ))}
        </div>
      </div>

      {loading && <p className="muted">Loading…</p>}
      {!loading && alerts.length === 0 && <p className="muted">Nothing flagged in this period.</p>}

      <ul className="alert-list">
        {alerts.map((a) => (
          <li key={`${a.source}-${a.id}-${a.event_time}`} className={a.source}>
            <span className="alert-icon" aria-hidden="true" />
            <span className="alert-text">
              <span className="alert-kind">{a.source === 'seismic' ? 'Earthquake' : 'Weather anomaly'}</span>
              {a.detail}
            </span>
            <span className="alert-when">{timeAgo(a.event_time)}</span>
          </li>
        ))}
      </ul>
    </section>
  )
}
