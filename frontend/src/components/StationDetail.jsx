import { useEffect, useState } from 'react'
import { getJson, timeAgo, toFahrenheit } from '../lib/format.js'
import { STATION_NAMES, STATUS_LABELS } from '../lib/stations.js'
import { subsolarPoint } from '../lib/sun.js'
import Sparkline from './Sparkline.jsx'
import { conditionKind } from '../lib/conditions.js'
import { FieldIcon, WeatherIcon } from './WeatherIcon.jsx'

const CLOUD_LABELS = {
  SKC: 'Clear',
  CLR: 'Clear',
  FEW: 'Few clouds',
  SCT: 'Scattered',
  BKN: 'Broken',
  OVC: 'Overcast',
  VV: 'Obscured',
}

const COMPASS = ['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW']

// Was the sun up at this place and time? True when the station is less than
// 90° (a quarter of the globe) away from the point the sun is overhead.
function isDaylight(lat, lon, isoTime) {
  const [sunLon, sunLat] = subsolarPoint(new Date(isoTime))
  const rad = Math.PI / 180
  const cosDistance =
    Math.sin(lat * rad) * Math.sin(sunLat * rad) +
    Math.cos(lat * rad) * Math.cos(sunLat * rad) * Math.cos((lon - sunLon) * rad)
  return cosDistance > 0
}

const fmt = (value, unit, digits = 0) =>
  value === null || value === undefined ? '–' : `${Number(value).toFixed(digits)}${unit}`

// "2026-11-01T13:00:00-07:00" -> "1 PM", in the station's own time zone (the
// time string already carries it).
function hourLabel(isoLocal) {
  const hour = Number(isoLocal.slice(11, 13))
  const suffix = hour < 12 ? 'AM' : 'PM'
  return `${hour % 12 || 12} ${suffix}`
}

// Everything about one station: conditions now with icons, the last 24 hours
// and NWS's hourly forecast for the next 24.
export default function StationDetail({ station, history, onClose }) {
  const id = station.station_id
  const [forecast, setForecast] = useState({ id: null, hours: [], error: null })

  useEffect(() => {
    let cancelled = false
    getJson(`/api/forecast/${id}`)
      .then((hours) => !cancelled && setForecast({ id, hours, error: null }))
      .catch((err) => !cancelled && setForecast({ id, hours: [], error: err.message }))
    return () => {
      cancelled = true
    }
  }, [id])

  const name = STATION_NAMES[id] ?? id
  const day = isDaylight(station.lat, station.lon, station.observed_at)
  const feelsLike = station.feels_like_c ?? station.temperature_c
  const direction = station.wind_direction_deg
  const loadingForecast = forecast.id !== id

  const fields = [
    { icon: 'humidity', label: 'Humidity', value: fmt(station.humidity_pct, '%') },
    { icon: 'dewpoint', label: 'Dew point', value: fmt(station.dewpoint_c, '°') },
    {
      icon: 'wind',
      label: 'Wind',
      value: (
        <>
          {fmt(station.wind_speed_kmh, ' km/h')}
          {direction !== null && direction !== undefined && (
            <span className="wind-dir" title={`from ${Math.round(direction)}°`}>
              {/* The arrow points where the wind is blowing TO. */}
              <svg
                width="12"
                height="12"
                viewBox="0 0 12 12"
                style={{ transform: `rotate(${direction + 180}deg)` }}
                aria-hidden="true"
              >
                <path d="M6 1 L9.5 10 L6 8 L2.5 10 Z" fill="currentColor" />
              </svg>
              {COMPASS[Math.round(direction / 45) % 8]}
            </span>
          )}
        </>
      ),
    },
    { icon: 'gust', label: 'Gusts', value: fmt(station.wind_gust_kmh, ' km/h') },
    { icon: 'pressure', label: 'Pressure', value: fmt(station.pressure_hpa, ' hPa') },
    { icon: 'visibility', label: 'Visibility', value: fmt(station.visibility_km, ' km', 1) },
    { icon: 'cloud', label: 'Clouds', value: CLOUD_LABELS[station.cloud_cover] ?? '–' },
    { icon: 'precip', label: 'Rain 1 h', value: fmt(station.precip_last_hour_mm, ' mm', 1) },
    {
      icon: 'thermometer',
      label: 'In °F',
      value: station.temperature_c === null ? '–' : `${toFahrenheit(station.temperature_c)}°F`,
    },
  ]

  return (
    <section className="panel detail-panel station-detail" aria-label={`${name} weather`}>
      <div className="panel-head">
        <span className="kicker">Station · {id}</span>
        <button type="button" className="icon-btn ghost" aria-label="Close station" onClick={onClose}>
          <svg width="14" height="14" viewBox="0 0 14 14" aria-hidden="true">
            <path d="M2 2 L12 12 M12 2 L2 12" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" />
          </svg>
        </button>
      </div>

      <div className="now">
        <WeatherIcon
          kind={conditionKind(station.description, day)}
          size={56}
          label={station.description || 'Conditions'}
        />
        <div className="now-main">
          <div className="now-title">
            <h2 className="now-name">{name}</h2>
            <span className={`z-chip ${station.status}`}>
              {station.status === 'unscored'
                ? 'calibrating'
                : `${STATUS_LABELS[station.status]} · z ${station.z_score > 0 ? '+' : ''}${station.z_score}`}
            </span>
          </div>
          <div className="now-temp">
            {station.temperature_c === null ? '–' : `${Math.round(station.temperature_c)}°`}
            <span className="now-desc">{station.description || '—'}</span>
          </div>
          <div className="muted small">
            Feels like {fmt(feelsLike, '°')} · updated {timeAgo(station.observed_at)}
          </div>
        </div>
      </div>

      <dl className="field-grid">
        {fields.map((f) => (
          <div key={f.label} className="field">
            <dt>
              <FieldIcon name={f.icon} />
              {f.label}
            </dt>
            <dd>{f.value}</dd>
          </div>
        ))}
      </dl>

      <h3 className="sub-head">Next 24 hours</h3>
      {loadingForecast ? (
        <p className="muted small">Loading forecast…</p>
      ) : forecast.error ? (
        <p className="muted small">Forecast unavailable right now.</p>
      ) : (
        <ol className="forecast-strip" aria-label="Hourly forecast">
          {forecast.hours.map((h, i) => (
            <li key={h.time} title={`${h.summary} · wind ${h.wind}`}>
              <span className="fc-hour">{i === 0 ? 'Now' : hourLabel(h.time)}</span>
              <WeatherIcon kind={conditionKind(h.summary, h.is_day)} size={26} label={h.summary} />
              <span className="fc-temp">{Math.round(h.temperature_c)}°</span>
              <span className={`fc-rain${h.precip_pct ? '' : ' none'}`}>{h.precip_pct ? `${h.precip_pct}%` : '–'}</span>
            </li>
          ))}
        </ol>
      )}

      <h3 className="sub-head">Last 24 hours</h3>
      <Sparkline points={history} />
      {station.status === 'unscored' && (
        <p className="muted small">
          Needs readings at this time of day from at least 5 previous days before it can be judged.
        </p>
      )}
    </section>
  )
}
