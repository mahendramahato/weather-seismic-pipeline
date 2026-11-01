import { largest, magnitudeColor } from '../lib/quakes.js'

// The four headline numbers. They follow the replay: while scrubbing back in
// time they describe what had happened up to that moment.
export default function StatsGrid({ quakes, stations, windowLabel }) {
  const biggest = largest(quakes)
  const significant = quakes.filter((q) => q.is_significant).length
  const anomalies = stations.filter((s) => s.status === 'anomaly').length

  return (
    <div className="stats-grid">
      <div className="stat">
        <span className="stat-label">Quakes · {windowLabel}</span>
        <span className="stat-value">{quakes.length}</span>
        <span className="stat-sub">recorded by USGS</span>
      </div>
      <div className="stat">
        <span className="stat-label">Largest</span>
        <span className="stat-value" style={biggest ? { color: magnitudeColor(biggest.magnitude) } : undefined}>
          {biggest ? `M${biggest.magnitude}` : '–'}
        </span>
        <span className="stat-sub" title={biggest?.place}>
          {biggest ? biggest.place : 'no quakes yet'}
        </span>
      </div>
      <div className="stat">
        <span className="stat-label">M4.5+ significant</span>
        <span className="stat-value">{significant}</span>
        <span className="stat-sub">in this period</span>
      </div>
      <div className="stat">
        <span className="stat-label">Weather anomalies</span>
        <span className={`stat-value${anomalies ? ' tone-anomaly' : ''}`}>{anomalies}</span>
        <span className="stat-sub">of {stations.length} stations, now</span>
      </div>
    </div>
  )
}
