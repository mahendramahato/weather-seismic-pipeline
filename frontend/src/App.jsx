import { useEffect, useState } from 'react'
import { getJson, timeAgo } from './api.js'
import AlertsFeed from './components/AlertsFeed.jsx'
import LiveMap from './components/LiveMap.jsx'

// Reload live data every minute — the streaming pipeline updates about that often.
const REFRESH_MS = 60_000

export default function App() {
  // State: React redraws whatever uses these whenever they change.
  const [stations, setStations] = useState([])
  const [quakes, setQuakes] = useState([])
  const [health, setHealth] = useState({})
  const [error, setError] = useState(null)

  // After the first render: load the data, then reload on a timer.
  // The returned function stops the timer when the component goes away.
  useEffect(() => {
    async function load() {
      try {
        // Fetch all three at the same time instead of one after another.
        const [s, q, h] = await Promise.all([
          getJson('/api/stations'),
          getJson('/api/quakes?hours=24'),
          getJson('/api/health'),
        ])
        setStations(s)
        setQuakes(q)
        setHealth(h)
        setError(null)
      } catch (err) {
        setError(err.message)
      }
    }
    load()
    const timer = setInterval(load, REFRESH_MS)
    return () => clearInterval(timer)
  }, [])

  return (
    <div className="app">
      <header>
        <h1>Weather &amp; Seismic Monitor</h1>
        <span className="updated">Data updated {timeAgo(health.weather)}</span>
      </header>
      {error && <div className="error">Could not load data: {error}</div>}
      <main>
        <LiveMap stations={stations} quakes={quakes} />
        <AlertsFeed />
      </main>
    </div>
  )
}
