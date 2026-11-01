import { useEffect, useMemo, useRef, useState } from 'react'
import { formatUtc } from '../lib/format.js'
import { magnitudeColor } from '../lib/quakes.js'
import { WINDOWS } from '../lib/windows.js'


// A full replay takes this long, advancing every TICK_MS.
const PLAY_MS = 12_000
const TICK_MS = 100

// Activity chart + replay slider under the globe. `cursor` is the replay time
// in ms (null = live). `now` is when the quakes were fetched — the right edge
// of the chart — so rendering never reads the clock itself.
export default function TimeScrubber({ quakes, windowKey, now, cursor, onCursor, onWindow }) {
  const { hours, buckets } = WINDOWS[windowKey]
  const end = now ?? 0
  const start = end - hours * 3_600_000
  const bucketMs = (end - start) / buckets
  const [playing, setPlaying] = useState(false)

  // Quakes per bar, and the strongest one (it picks the bar's colour).
  const bars = useMemo(() => {
    const out = Array.from({ length: buckets }, () => ({ count: 0, max: 0 }))
    for (const q of quakes) {
      const index = Math.floor((Date.parse(q.event_time) - start) / bucketMs)
      if (index >= 0 && index < buckets) {
        out[index].count += 1
        out[index].max = Math.max(out[index].max, q.magnitude)
      }
    }
    return out
  }, [quakes, start, bucketMs, buckets])
  const peak = Math.max(1, ...bars.map((b) => b.count))

  // The interval reads the latest cursor through a ref, so it isn't
  // restarted on every tick.
  const cursorRef = useRef(cursor)
  useEffect(() => {
    cursorRef.current = cursor
  }, [cursor])

  useEffect(() => {
    if (!playing) return undefined
    const step = (end - start) / (PLAY_MS / TICK_MS)
    const timer = setInterval(() => {
      const next = (cursorRef.current ?? start) + step
      if (next >= end) {
        setPlaying(false)
        onCursor(null) // replay finished: back to live
      } else {
        onCursor(next)
      }
    }, TICK_MS)
    return () => clearInterval(timer)
  }, [playing, start, end, onCursor])

  function togglePlay() {
    if (playing) {
      setPlaying(false)
      return
    }
    if (cursor === null) onCursor(start)
    setPlaying(true)
  }

  function pickWindow(key) {
    setPlaying(false)
    onWindow(key)
  }

  const position = cursor ?? end
  const percent = end > start ? ((position - start) / (end - start)) * 100 : 100
  const labels = [0, 0.25, 0.5, 0.75].map((f) => start + f * (end - start))

  return (
    <section className="panel scrubber" aria-label="Replay earthquakes over time">
      <div className="scrub-head">
        <button
          type="button"
          className="play-btn"
          aria-label={playing ? 'Pause replay' : 'Play replay'}
          onClick={togglePlay}
          disabled={!now}
        >
          {playing ? (
            <svg width="16" height="16" viewBox="0 0 16 16" aria-hidden="true">
              <rect x="3" y="2" width="3.5" height="12" rx="1" fill="currentColor" />
              <rect x="9.5" y="2" width="3.5" height="12" rx="1" fill="currentColor" />
            </svg>
          ) : (
            <svg width="16" height="16" viewBox="0 0 16 16" aria-hidden="true">
              <path d="M4 2 L13 8 L4 14 Z" fill="currentColor" />
            </svg>
          )}
        </button>
        <div className="scrub-readout">
          <span className="scrub-title">{cursor === null ? 'Live' : 'Replay'}</span>
          <span className="mono muted">{now ? formatUtc(position) : 'loading…'}</span>
        </div>
        {cursor !== null && (
          <button
            type="button"
            className="chip-btn"
            onClick={() => {
              setPlaying(false)
              onCursor(null)
            }}
          >
            Back to live
          </button>
        )}
        <div className="spacer" />
        <div className="tabs" role="group" aria-label="Time window">
          {Object.entries(WINDOWS).map(([key, { label }]) => (
            <button
              key={key}
              type="button"
              aria-pressed={key === windowKey}
              className={key === windowKey ? 'active' : ''}
              onClick={() => pickWindow(key)}
            >
              {label}
            </button>
          ))}
        </div>
      </div>

      <div className="scrub-chart">
        <div className="scrub-bars" aria-hidden="true">
          {bars.map((bar, i) => (
            <span
              key={i}
              className={`scrub-bar${start + i * bucketMs > position ? ' future' : ''}`}
              style={{
                height: `${bar.count ? 12 + (bar.count / peak) * 88 : 4}%`,
                background: bar.count ? magnitudeColor(bar.max) : undefined,
              }}
            />
          ))}
        </div>
        <span className="scrub-cursor" style={{ left: `${percent}%` }} aria-hidden="true" />
        <input
          type="range"
          className="scrub-range"
          aria-label="Replay time"
          min={start}
          max={end}
          step={60_000}
          value={position}
          disabled={!now}
          onChange={(e) => {
            setPlaying(false)
            const value = Number(e.target.value)
            onCursor(value >= end ? null : value)
          }}
        />
      </div>
      <div className="scrub-axis mono" aria-hidden="true">
        {labels.map((t) => (
          <span key={t}>
            {/* 24 h: the clock time ("16:00"); 7 days: the day ("Mon 26 Oct"). */}
            {!now ? '' : windowKey === 'week' ? formatUtc(t).split(' · ')[0] : formatUtc(t, { withDay: false }).replace(' UTC', '')}
          </span>
        ))}
        <span>Now</span>
      </div>
    </section>
  )
}
