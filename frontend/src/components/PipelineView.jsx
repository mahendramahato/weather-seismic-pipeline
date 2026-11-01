import { useEffect, useState } from 'react'
import { formatDay, getJson, timeAgo } from '../lib/format.js'
import { INCIDENTS } from '../lib/incidents.js'

const REFRESH_MS = 60_000

const LABELS = { healthy: 'Healthy', delayed: 'Delayed', down: 'Down', unknown: 'Unknown' }
// Worse statuses win when several signals feed one card.
const RANK = { healthy: 0, unknown: 1, delayed: 2, down: 3 }
const worst = (...statuses) => statuses.reduce((a, b) => (RANK[b] > RANK[a] ? b : a), 'healthy')

function ageText(minutes) {
  if (minutes === null || minutes === undefined) return 'no data'
  if (minutes < 1) return 'under 1 min'
  if (minutes < 60) return `${Math.round(minutes)} min`
  return `${Math.round(minutes / 60)} h`
}

function StatusPill({ status }) {
  return (
    <span className={`status-pill status-${status}`}>
      <span className="status-dot" aria-hidden="true" />
      {LABELS[status]}
    </span>
  )
}

// One step of the pipeline. Children = a line or two about what it's doing.
function Stage({ name, status, children }) {
  return (
    <div className={`stage status-${status}`}>
      <StatusPill status={status} />
      <h3>{name}</h3>
      <p>{children}</p>
    </div>
  )
}

// Animated dashes between stages: data moving left to right.
function Connector() {
  return <span className="connector" aria-hidden="true" />
}

// Live health of the whole pipeline, from /api/pipeline. Statuses are
// inferred from data freshness: a stream that keeps writing files means its
// source, producer, Kafka topic and Spark job are all doing their part.
export default function PipelineView() {
  const [data, setData] = useState(null)
  const [error, setError] = useState(null)

  useEffect(() => {
    async function load() {
      try {
        setData(await getJson('/api/pipeline'))
        setError(null)
      } catch (err) {
        setError(err.message)
      }
    }
    load()
    const timer = setInterval(load, REFRESH_MS)
    return () => clearInterval(timer)
  }, [])

  if (!data) {
    return (
      <main className="pipeline">
        <p className="muted">{error ? `Could not load pipeline status: ${error}` : 'Loading pipeline status…'}</p>
      </main>
    )
  }

  const { weather, seismic } = data.streams
  const streaming = worst(weather.status, seismic.status)
  const batch = data.curated.status
  const overall = worst(streaming, batch)
  const tables = data.curated.tables ?? {}
  const curatedDays = Object.values(tables).filter(Boolean).map((d) => String(d).slice(0, 10))
  const curatedThrough = curatedDays.length ? curatedDays.sort()[0] : null
  const ages = [weather.age_minutes, seismic.age_minutes].filter((a) => a !== null)
  const freshest = ages.length ? Math.min(...ages) : null
  const { memory_mb: memory, memory_limit_mb: limit } = data.api

  return (
    <main className="pipeline">
      <div className="pipeline-head">
        <div>
          <h2>Pipeline health</h2>
          <p className="muted">Checked {timeAgo(data.checked_at)} · refreshes every minute</p>
        </div>
        <StatusPill status={overall} />
      </div>

      <div className="tiles">
        <div className="stat">
          <span className="stat-label">Raw data freshness</span>
          <span className="stat-value">{ageText(freshest)}</span>
          <span className="stat-sub">since the newest lake file</span>
        </div>
        <div className="stat">
          <span className="stat-label">Curated (Athena) through</span>
          <span className="stat-value">{formatDay(curatedThrough)}</span>
          <span className="stat-sub">expected {formatDay(data.curated.expected)}</span>
        </div>
        <div className="stat">
          <span className="stat-label">Files written today</span>
          <span className="stat-value">{(weather.files_today + seismic.files_today).toLocaleString()}</span>
          <span className="stat-sub">
            {weather.files_today.toLocaleString()} weather · {seismic.files_today.toLocaleString()} seismic
          </span>
        </div>
        <div className="stat">
          <span className="stat-label">API memory</span>
          <span className="stat-value">{memory === null ? '–' : `${memory} MB`}</span>
          <span className="stat-sub">{limit ? `of a ${limit} MB limit` : 'no container limit visible'}</span>
        </div>
      </div>

      <section className="flow-section" aria-label="Streaming path">
        <div className="flow-head">
          <h3>Streaming path</h3>
          <span className="muted">every minute, source to dashboard</span>
        </div>
        <div className="flow-row">
          <Stage name="Sources" status={streaming}>
            NOAA weather.gov and the USGS earthquake feed
          </Stage>
          <Connector />
          <Stage name="Producer" status={streaming}>
            Polls both feeds every minute
          </Stage>
          <Connector />
          <Stage name="Kafka" status={streaming}>
            Topics <code>weather</code> and <code>seismic</code>
          </Stage>
          <Connector />
          <Stage name="Spark Streaming" status={streaming}>
            weather-stream: last file {ageText(weather.age_minutes)} ago
            <br />
            seismic-stream: last file {ageText(seismic.age_minutes)} ago
          </Stage>
          <Connector />
          <Stage name="Parquet lake" status={streaming}>
            Daily partitions · {(weather.files_today + seismic.files_today).toLocaleString()} files today
          </Stage>
          <Connector />
          <Stage name="API" status="healthy">
            FastAPI + DuckDB{memory !== null && ` · ${memory} MB`}
          </Stage>
        </div>
      </section>

      <section className="flow-section" aria-label="Nightly batch path">
        <div className="flow-head">
          <h3>Nightly batch path</h3>
          <span className="muted">Airflow · daily_lake_maintenance · 00:30 UTC</span>
        </div>
        <div className="flow-row">
          <Stage name="Airflow" status={batch}>
            Curates the previous day each night
          </Stage>
          <Connector />
          <Stage name="S3 raw" status={batch}>
            Uploads each finished day of the lake
          </Stage>
          <Connector />
          <Stage name="Glue ETL" status={batch}>
            Cleans and de-duplicates into curated tables
          </Stage>
          <Connector />
          <Stage name="Athena" status={batch}>
            {curatedThrough ? `Curated through ${formatDay(curatedThrough)}` : 'Not reachable from the API'}
          </Stage>
        </div>
      </section>

      <p className="muted small">
        Stage health is inferred from data freshness: streaming from how recently each Spark job wrote a file,
        the batch path from how far the curated tables reach.
      </p>

      {INCIDENTS.map((incident) => (
        <section key={incident.title} className="panel incident" aria-label={`Incident: ${incident.title}`}>
          <div className="panel-head">
            <h2>Incident: {incident.title}</h2>
            <span className="status-pill status-healthy">Resolved {formatDay(incident.resolved)}</span>
          </div>
          <ol className="timeline">
            {incident.events.map((event) => (
              <li key={event.when + event.what} className={`tone-${event.tone}`}>
                <span className="timeline-dot" aria-hidden="true" />
                <div>
                  <div className="mono muted small">{event.when}</div>
                  <p>
                    <b>{event.what}</b> {event.detail}
                  </p>
                </div>
              </li>
            ))}
          </ol>
        </section>
      ))}
    </main>
  )
}
