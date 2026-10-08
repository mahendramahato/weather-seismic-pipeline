import { Suspense, lazy, useEffect, useMemo, useState } from 'react'
import AlertsFeed from './components/AlertsFeed.jsx'
import StationList from './components/StationList.jsx'
import { getJson, timeAgo } from './lib/format.js'
import { useDayTheme } from './lib/useDayTheme.js'

// The 3D globe (three.js) is most of the JavaScript: load it as a separate
// file so the rest of the page shows immediately.
const EarthGlobe = lazy(() => import('./components/EarthGlobe.jsx'))

// Reload live data every minute — the streaming pipeline updates about that often.
const REFRESH_MS = 60_000

// If the newest reading is older than this, the pipeline is behind: show
// "Delayed" instead of "Live" (same idea as the Airflow freshness check).
const STALE_AFTER_MS = 2 * 60 * 60 * 1000

function SunIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <circle cx="12" cy="12" r="4.5" />
      {[0, 45, 90, 135, 180, 225, 270, 315].map((angle) => (
        <line key={angle} x1="12" y1="2" x2="12" y2="4.5" transform={`rotate(${angle} 12 12)`} />
      ))}
    </svg>
  )
}

function MoonIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path d="M20 14.5A8 8 0 0 1 9.5 4a8 8 0 1 0 10.5 10.5Z" />
    </svg>
  )
}

export default function App() {
  const theme = useDayTheme()
  const [stations, setStations] = useState([])
  const [quakes, setQuakes] = useState([])
  const [history, setHistory] = useState([])
  const [health, setHealth] = useState({})
  // When the data was last fetched — staleness is judged against this, not
  // against the clock during rendering (rendering must not depend on "now").
  const [fetchedAt, setFetchedAt] = useState(null)
  const [selectedId, setSelectedId] = useState(null)
  const [error, setError] = useState(null)

  // Load everything in parallel now, then every minute; stop on unmount.
  useEffect(() => {
    async function load() {
      try {
        const [s, q, h, hl] = await Promise.all([
          getJson('/api/stations'),
          getJson('/api/quakes?hours=24'),
          getJson('/api/weather/history?hours=24'),
          getJson('/api/health'),
        ])
        setStations(s)
        setQuakes(q)
        setHistory(h)
        setHealth(hl)
        setFetchedAt(Date.now())
        setError(null)
      } catch (err) {
        setError(err.message)
      }
    }
    load()
    const timer = setInterval(load, REFRESH_MS)
    return () => clearInterval(timer)
  }, [])

  // Group the flat history rows by station for the sparklines.
  const historyByStation = useMemo(() => {
    const groups = {}
    history.forEach((row) => (groups[row.station_id] ??= []).push(row))
    return groups
  }, [history])

  // Headline numbers for the overlay on the globe.
  const largest = quakes.reduce((best, q) => (!best || q.magnitude > best.magnitude ? q : best), null)
  const significantCount = quakes.filter((q) => q.is_significant).length
  const anomalyCount = stations.filter((s) => s.status === 'anomaly').length
  const isStale = !health.weather || fetchedAt - Date.parse(health.weather) > STALE_AFTER_MS

  return (
    <div className="app">
      <header className="topbar">
        <div className="brand">
          <span className="brand-mark" aria-hidden="true" />
          <h1>Weather &amp; Seismic Monitor</h1>
        </div>
        <div className="topbar-meta">
          <span className={`live-badge${isStale ? ' stale' : ''}`}>
            <span className="live-dot" aria-hidden="true" />
            {isStale ? 'Delayed' : 'Live'} · updated {timeAgo(health.weather)}
          </span>
          <span className="theme-badge" title="Light by day, dark by night (your local time)">
            {theme === 'dark' ? <MoonIcon /> : <SunIcon />}
            {theme === 'dark' ? 'Night' : 'Day'}
          </span>
        </div>
      </header>

      {error && <div className="error-bar">Could not load data: {error}</div>}

      <main className="layout">
        <section className="globe-panel" aria-label="Globe of stations and earthquakes">
          <Suspense fallback={<div className="globe-loading">Loading globe…</div>}>
            <EarthGlobe
              stations={stations}
              quakes={quakes}
              selectedId={selectedId}
              onSelectStation={setSelectedId}
              theme={theme}
            />
          </Suspense>

          <div className="stats">
            <div className="stat">
              <span className="stat-value">{quakes.length}</span>
              <span className="stat-label">quakes · 24 h</span>
            </div>
            <div className="stat">
              <span className="stat-value">{largest ? `M${largest.magnitude}` : '–'}</span>
              <span className="stat-label">{largest ? largest.place : 'largest quake'}</span>
            </div>
            <div className="stat">
              <span className="stat-value">{significantCount}</span>
              <span className="stat-label">M4.5+ significant</span>
            </div>
            <div className="stat">
              <span className="stat-value">{anomalyCount}</span>
              <span className="stat-label">weather anomalies now</span>
            </div>
          </div>

          <div className="legend">
            <span><i className="lg-quake" /> Earthquake</span>
            <span><i className="lg-sig" /> M4.5+ (pulsing)</span>
            <span><i className="lg-normal" /> Normal</span>
            <span><i className="lg-anomaly" /> Anomaly</span>
            <span><i className="lg-unscored" /> Calibrating</span>
          </div>
        </section>

        <aside className="sidebar">
          {/* Alerts first, so they're visible without scrolling. */}
          <AlertsFeed />

          <StationList
            stations={stations}
            historyByStation={historyByStation}
            selectedId={selectedId}
            onSelect={setSelectedId}
          />

          <footer className="credits">
            Data: NOAA weather.gov &amp; USGS · Imagery: NASA
          </footer>
        </aside>
      </main>
    </div>
  )
}
