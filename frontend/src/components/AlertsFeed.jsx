import { useEffect, useState } from 'react'
import { getJson, timeAgo } from '../lib/format.js'
import { magnitudeColor } from '../lib/quakes.js'
import { STATION_NAMES } from '../lib/stations.js'
import NwsAlerts from './NwsAlerts.jsx'

// Our own detections (24 hours = live lake; 7 days = curated + live merged)
// and, in a third tab, the official NWS alerts the page already loaded.
// The list scrolls inside the panel, so a long one never stretches the page.
const TABS = {
  day: { label: '24 h', path: '/api/alerts?hours=24' },
  week: { label: '7 days', path: '/api/alerts/week' },
  nws: { label: 'NWS', path: null },
}

// Splits an alert's text into a badge (magnitude or temperature), a title and
// a subtitle. The API sends e.g. "M5.5 west of Macquarie Island" or
// "KDEN 30.0°C (z = 5.0)"; anything unexpected falls back to the raw text.
function describe(alert) {
  if (alert.source === 'seismic') {
    const match = /^M([\d.]+)\s+(.*)$/.exec(alert.detail)
    return {
      badge: match ? `M${match[1]}` : 'Quake',
      color: match ? magnitudeColor(Number(match[1])) : '#d63f3f',
      title: match ? match[2] : alert.detail,
      subtitle: 'Earthquake',
    }
  }
  const match = /^(\S+)\s+(-?[\d.]+)°C\s+\(z = (-?[\d.]+)\)/.exec(alert.detail)
  const code = match ? match[1] : alert.id
  const name = STATION_NAMES[code] ?? code
  // A positive z-score means warmer than usual for that hour, negative colder.
  const warmer = match && Number(match[3]) > 0
  return {
    badge: match ? `${Math.round(Number(match[2]))}°` : '!',
    // Warm anomalies in amber, cold ones in a cool slate-teal.
    color: !match ? '#c93c72' : warmer ? '#e0712f' : '#3f8fa6',
    title: match ? `${name}: ${warmer ? 'warmer' : 'colder'} than usual` : `${name}: unusual temperature`,
    subtitle: match ? `Weather anomaly · z ${match[3]}` : 'Weather anomaly',
  }
}

// A station usually stays anomalous for a while, producing one alert per
// reading. Consecutive alerts from the same station (the list is newest
// first) are folded into one row that counts them.
function groupRuns(alerts) {
  const groups = []
  for (const alert of alerts) {
    const last = groups[groups.length - 1]
    if (alert.source === 'weather' && last && last.alert.source === 'weather' && last.alert.id === alert.id) {
      last.count += 1
    } else {
      groups.push({ alert, count: 1 })
    }
  }
  return groups
}

export default function AlertsFeed({ nwsAlerts = [] }) {
  const [tab, setTab] = useState('day')
  // The last answer received, remembering which tab it belongs to.
  const [result, setResult] = useState({ tab: null, alerts: [] })

  // Reload when the tab changes; ignore a slow reply from a tab the user
  // already left, so an old answer can't overwrite the newer one.
  useEffect(() => {
    if (!TABS[tab].path) return undefined // the NWS tab needs no request
    let cancelled = false
    getJson(TABS[tab].path)
      .then((data) => !cancelled && setResult({ tab, alerts: data }))
      .catch(() => !cancelled && setResult({ tab, alerts: [] }))
    return () => {
      cancelled = true
    }
  }, [tab])

  // Still loading while the answer we hold is for a different tab.
  const isNws = tab === 'nws'
  const loading = !isNws && result.tab !== tab
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
              {key === 'nws' && nwsAlerts.length > 0 && <span className="tab-count">{nwsAlerts.length}</span>}
            </button>
          ))}
        </div>
      </div>

      <div className="panel-scroll">
        {isNws ? (
          <NwsAlerts alerts={nwsAlerts} />
        ) : (
          <>
            {loading && <p className="muted">Loading…</p>}
            {!loading && alerts.length === 0 && <p className="muted">Nothing flagged in this period.</p>}

            <ul className="alert-list">
              {groupRuns(alerts).map(({ alert: a, count }) => {
                const { badge, color, title, subtitle } = describe(a)
                return (
                  <li key={`${a.source}-${a.id}-${a.event_time}`} className={a.source}>
                    <span className="alert-badge" style={{ '--badge': color }}>
                      {badge}
                    </span>
                    <span className="alert-text">
                      <span className="alert-title">{title}</span>
                      <span className="alert-kind">
                        {subtitle}
                        {count > 1 && ` · ${count} readings`}
                      </span>
                    </span>
                    <span className="alert-when">{timeAgo(a.event_time)}</span>
                  </li>
                )
              })}
            </ul>
          </>
        )}
      </div>
    </section>
  )
}
