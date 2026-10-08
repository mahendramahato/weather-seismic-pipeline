// A tiny 24h temperature chart. The shaded band is the anomaly detector's
// "normal range" for each reading (band_low..band_high, computed by the API
// for whichever detector scored it); red dots are readings it flagged.
// `compact` = a small inline version for list rows: no caption or dots.
const WIDTH = 260
const HEIGHT = 56
const PAD = 5

export default function Sparkline({ points, compact = false }) {
  if (points.length < 2) {
    return compact ? null : <div className="spark-empty">Collecting readings…</div>
  }

  const times = points.map((p) => Date.parse(p.observed_at))
  const banded = points.filter((p) => p.band_low !== null && p.band_high !== null)

  // Scale to the temperatures (at least a 4 °C span) rather than the band, so
  // the line keeps its shape. The band may run past the top/bottom edge and is
  // clipped there — a reading outside the band then stands out clearly.
  const temps = points.map((p) => p.temperature_c)
  const middle = (Math.min(...temps) + Math.max(...temps)) / 2
  const half = Math.max((Math.max(...temps) - Math.min(...temps)) / 2, 2) * 1.25
  const low = middle - half
  const high = middle + half
  const start = Math.min(...times)
  const span = Math.max(Math.max(...times) - start, 1)

  const x = (t) => PAD + ((t - start) / span) * (WIDTH - 2 * PAD)
  const y = (v) => HEIGHT - PAD - ((v - low) / Math.max(high - low, 1)) * (HEIGHT - 2 * PAD)

  const line = points
    .map((p, i) => `${i ? 'L' : 'M'}${x(times[i]).toFixed(1)},${y(p.temperature_c).toFixed(1)}`)
    .join(' ')

  // Band polygon: along the top edge left-to-right, back along the bottom.
  const bandTop = banded.map((p) => `${x(Date.parse(p.observed_at)).toFixed(1)},${y(p.band_high).toFixed(1)}`)
  const bandBottom = banded.map((p) => `${x(Date.parse(p.observed_at)).toFixed(1)},${y(p.band_low).toFixed(1)}`)
  const band = [...bandTop, ...bandBottom.reverse()].join(' ')

  const last = points.at(-1)

  if (compact) {
    return (
      <svg className="spark compact" viewBox={`0 0 ${WIDTH} ${HEIGHT}`} preserveAspectRatio="none">
        {banded.length > 1 && <polygon className="spark-band" points={band} />}
        <path className="spark-line" d={line} />
      </svg>
    )
  }

  return (
    <figure className="spark-wrap">
      <svg
        className="spark"
        viewBox={`0 0 ${WIDTH} ${HEIGHT}`}
        preserveAspectRatio="none"
        role="img"
        aria-label="Temperature over the last 24 hours"
      >
        {banded.length > 1 && <polygon className="spark-band" points={band} />}
        <path className="spark-line" d={line} />
        {points.map((p, i) =>
          p.is_anomaly ? <circle key={i} className="spark-anomaly" cx={x(times[i])} cy={y(p.temperature_c)} r="3" /> : null,
        )}
        <circle className="spark-now" cx={x(times.at(-1))} cy={y(last.temperature_c)} r="2.6" />
      </svg>
      <figcaption>
        <span>24 h</span>
        <span>
          {Math.round(Math.min(...temps))}° – {Math.round(Math.max(...temps))}°C
        </span>
        <span>now</span>
      </figcaption>
    </figure>
  )
}
