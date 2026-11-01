import { Suspense, lazy, useCallback, useEffect, useMemo, useRef, useState } from 'react'
import AlertsFeed from './components/AlertsFeed.jsx'
import PipelineView from './components/PipelineView.jsx'
import QuakeDetail from './components/QuakeDetail.jsx'
import StationDetail from './components/StationDetail.jsx'
import StationList from './components/StationList.jsx'
import StatsGrid from './components/StatsGrid.jsx'
import TimeScrubber from './components/TimeScrubber.jsx'
import { getJson, timeAgo } from './lib/format.js'
import { loadLayers, saveLayers, temperatureColor } from './lib/layers.js'
import { alertPolygons } from './lib/nws.js'
import { largest, latestSignificant, magnitudeColor } from './lib/quakes.js'
import { useDayTheme } from './lib/useDayTheme.js'
import { useHashView } from './lib/useHashView.js'
import { WINDOWS } from './lib/windows.js'

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

function BrandMark() {
  return (
    <svg className="brand-mark" width="30" height="30" viewBox="0 0 28 28" aria-hidden="true">
      <circle cx="14" cy="14" r="11" fill="none" stroke="var(--accent)" strokeWidth="2" />
      <path
        d="M3 14 h5 l3 -6 l4 12 l3 -6 h7"
        fill="none"
        stroke="#e0712f"
        strokeWidth="2"
        strokeLinejoin="round"
        strokeLinecap="round"
      />
    </svg>
  )
}

export default function App() {
  const theme = useDayTheme()
  const view = useHashView()

  // Stations, sparkline history and freshness: refreshed every minute.
  const [live, setLive] = useState({ stations: [], history: [], health: {}, nws: [], fetchedAt: null })
  // Quakes for the chosen window (24 h or 7 days), with when they were fetched.
  const [windowKey, setWindowKey] = useState('day')
  const [quakeData, setQuakeData] = useState({ quakes: [], fetchedAt: null })
  const [selectedStationId, setSelectedStationId] = useState(null)
  const [selectedQuakeId, setSelectedQuakeId] = useState(null)
  // Globe layers switched on (remembered in this browser).
  const [layers, setLayers] = useState(loadLayers)
  const toggleLayer = useCallback((key) => {
    setLayers((current) => {
      const next = { ...current, [key]: !current[key] }
      saveLayers(next)
      return next
    })
  }, [])
  // Which of the two the right-hand detail panel shows: whichever was picked last.
  const [detailKind, setDetailKind] = useState('quake')
  // Replay time in ms; null = live (show everything).
  const [cursor, setCursor] = useState(null)
  const [error, setError] = useState(null)
  // Significant quakes already seen, so a NEW one can be flown to.
  const seenSignificant = useRef(null)

  useEffect(() => {
    async function load() {
      try {
        const [stations, history, health, nws] = await Promise.all([
          getJson('/api/stations'),
          getJson('/api/weather/history?hours=24'),
          getJson('/api/health'),
          // NWS warnings are a bonus layer: if that call fails, show none
          // rather than failing the whole page.
          getJson('/api/nws-alerts').catch(() => []),
        ])
        setLive({ stations, history, health, nws, fetchedAt: Date.now() })
        setError(null)
      } catch (err) {
        setError(err.message)
      }
    }
    load()
    const timer = setInterval(load, REFRESH_MS)
    return () => clearInterval(timer)
  }, [])

  // Quakes reload when the window changes and every minute. A slow reply for
  // a window the user already left is ignored.
  useEffect(() => {
    let cancelled = false
    async function load() {
      try {
        const quakes = await getJson(`/api/quakes?hours=${WINDOWS[windowKey].hours}`)
        if (cancelled) return
        // A significant quake we haven't seen before (not on the first load):
        // select it, which flies the globe there.
        const seen = seenSignificant.current
        if (seen) {
          const fresh = quakes.find((q) => q.is_significant && !seen.has(q.event_id))
          if (fresh) setSelectedQuakeId(fresh.event_id) // flies there; the panel stays put
        }
        seenSignificant.current = new Set(quakes.filter((q) => q.is_significant).map((q) => q.event_id))
        setQuakeData({ quakes, fetchedAt: Date.now() })
      } catch (err) {
        if (!cancelled) setError(err.message)
      }
    }
    load()
    const timer = setInterval(load, REFRESH_MS)
    return () => {
      cancelled = true
      clearInterval(timer)
    }
  }, [windowKey])

  const selectStation = useCallback((id) => {
    setSelectedStationId(id)
    setDetailKind(id ? 'station' : 'quake')
  }, [])

  const selectQuake = useCallback((id) => {
    setSelectedQuakeId(id)
    setDetailKind('quake')
  }, [])

  const changeWindow = useCallback((key) => {
    seenSignificant.current = null // a new window isn't "new quakes"
    setCursor(null)
    setWindowKey(key)
  }, [])

  // Group the flat history rows by station for the sparklines.
  const historyByStation = useMemo(() => {
    const groups = {}
    live.history.forEach((row) => (groups[row.station_id] ??= []).push(row))
    return groups
  }, [live.history])

  // Severe/Extreme NWS warnings with a drawn area, for the globe.
  const nwsPolygons = useMemo(() => alertPolygons(live.nws), [live.nws])

  const { quakes } = quakeData
  // While replaying, only quakes up to the replay time are shown.
  const visibleQuakes = useMemo(
    () => (cursor === null ? quakes : quakes.filter((q) => Date.parse(q.event_time) <= cursor)),
    [quakes, cursor],
  )

  const selectedQuake = visibleQuakes.find((q) => q.event_id === selectedQuakeId) ?? null
  const spotlight = latestSignificant(visibleQuakes)
  const fallbackQuake = spotlight ?? largest(visibleQuakes)
  const detailQuake = selectedQuake ?? fallbackQuake
  const detailLabel = selectedQuake ? 'Selected quake' : spotlight ? 'Latest significant' : 'Largest quake'

  const selectedStation = live.stations.find((s) => s.station_id === selectedStationId) ?? null
  const showStation = detailKind === 'station' && selectedStation

  const isStale = !live.health.weather || live.fetchedAt - Date.parse(live.health.weather) > STALE_AFTER_MS

  return (
    <div className="app">
      <header className="topbar">
        <div className="brand">
          <BrandMark />
          <h1>Weather &amp; Seismic Monitor</h1>
        </div>
        <nav className="view-tabs" aria-label="Views">
          <a href="#/" aria-current={view === 'globe' ? 'page' : undefined}>
            Globe
          </a>
          <a href="#/pipeline" aria-current={view === 'pipeline' ? 'page' : undefined}>
            Pipeline
          </a>
        </nav>
        <div className="spacer" />
        <div className="topbar-meta">
          <span className={`live-badge${isStale ? ' stale' : ''}`}>
            <span className="live-dot" aria-hidden="true" />
            {isStale ? 'Delayed' : 'Live'} · updated {timeAgo(live.health.weather)}
          </span>
          <span className="theme-badge" title="Light by day, dark by night (your local time)">
            {theme === 'dark' ? <MoonIcon /> : <SunIcon />}
            {theme === 'dark' ? 'Night' : 'Day'}
          </span>
        </div>
      </header>

      {error && <div className="error-bar">Could not load data: {error}</div>}

      {view === 'pipeline' ? (
        <PipelineView />
      ) : (
        <main className="layout">
          <section className="col col-left" aria-label="Summary and stations">
            <StatsGrid quakes={visibleQuakes} stations={live.stations} windowLabel={WINDOWS[windowKey].label} />
            <StationList
              stations={live.stations}
              historyByStation={historyByStation}
              selectedId={selectedStationId}
              onSelect={selectStation}
            />
          </section>

          <section className="col col-center" aria-label="Globe of stations and earthquakes">
            <div className="globe-panel">
              <Suspense fallback={<div className="globe-loading">Loading globe…</div>}>
                <EarthGlobe
                  stations={live.stations}
                  quakes={visibleQuakes}
                  nwsPolygons={nwsPolygons}
                  layers={layers}
                  onToggleLayer={toggleLayer}
                  selectedStationId={selectedStationId}
                  selectedQuakeId={selectedQuakeId}
                  onSelectStation={selectStation}
                  onSelectQuake={selectQuake}
                  theme={theme}
                />
              </Suspense>

              {spotlight && (
                <button
                  type="button"
                  className="callout"
                  style={{ '--tone': magnitudeColor(spotlight.magnitude) }}
                  onClick={() => selectQuake(spotlight.event_id)}
                >
                  <span className="callout-kicker">
                    <span className="live-dot" aria-hidden="true" />
                    Latest significant quake
                  </span>
                  <span className="callout-title">
                    M{spotlight.magnitude} · {spotlight.place}
                  </span>
                  <span className="callout-sub">
                    {timeAgo(spotlight.event_time)} · {Math.round(spotlight.depth_km ?? 0)} km deep
                  </span>
                </button>
              )}

              <div className="legend">
                <span>
                  <i className="lg-small" /> M &lt; 2.5
                </span>
                <span>
                  <i className="lg-mid" /> M 2.5–4.5
                </span>
                <span>
                  <i className="lg-sig" /> M 4.5+
                </span>
                <span>
                  <i className="lg-normal" /> Normal
                </span>
                <span>
                  <i className="lg-anomaly" /> Anomaly
                </span>
                {layers.nws && (
                  <span>
                    <i className="lg-nws" /> NWS warning
                  </span>
                )}
                {layers.plates && (
                  <span>
                    <i className="lg-plate" /> Plate boundary
                  </span>
                )}
                {layers.heat && (
                  <span>
                    <i
                      className="lg-temp"
                      style={{
                        background: `linear-gradient(90deg, ${temperatureColor(-10)}, ${temperatureColor(10)}, ${temperatureColor(25)}, ${temperatureColor(38)})`,
                      }}
                    />{' '}
                    Cold → hot
                  </span>
                )}
              </div>
            </div>

            <TimeScrubber
              quakes={quakes}
              windowKey={windowKey}
              now={quakeData.fetchedAt}
              cursor={cursor}
              onCursor={setCursor}
              onWindow={changeWindow}
            />
          </section>

          <aside className="col col-right" aria-label="Quake details and alerts">
            {showStation ? (
              <StationDetail
                station={selectedStation}
                history={historyByStation[selectedStation.station_id] ?? []}
                onClose={() => selectStation(null)}
              />
            ) : (
              <QuakeDetail
                quake={detailQuake}
                label={detailLabel}
                onClose={selectedQuake ? () => setSelectedQuakeId(null) : null}
              />
            )}
            <AlertsFeed nwsAlerts={live.nws} />
          </aside>
        </main>
      )}
      <footer className="credits">
        Data: NOAA weather.gov &amp; NWS · USGS · Plates: Bird (2003) PB2002 · Imagery: NASA
      </footer>
    </div>
  )
}
