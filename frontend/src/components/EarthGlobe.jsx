import { useEffect, useMemo, useRef, useState } from 'react'
import Globe from 'react-globe.gl'
import * as THREE from 'three'
import { escapeHtml, timeAgo } from '../lib/format.js'
import { STATION_NAMES } from '../lib/stations.js'
import { LAYERS, temperatureColor } from '../lib/layers.js'
import { severityColor } from '../lib/nws.js'
import { magnitudeColor, withAlpha } from '../lib/quakes.js'
import { subsolarPoint } from '../lib/sun.js'

// --- Day/night globe shader ---
// Each point on the sphere mixes the daytime and night-lights textures
// depending on how directly it faces the sun. The normal is taken in world
// space, so lighting stays fixed to the Earth while the camera orbits.
const vertexShader = `
  varying vec3 vNormal;
  varying vec2 vUv;
  void main() {
    vNormal = normalize(mat3(modelMatrix) * normal);
    vUv = uv;
    gl_Position = projectionMatrix * modelViewMatrix * vec4(position, 1.0);
  }
`

const fragmentShader = `
  uniform sampler2D dayTexture;
  uniform sampler2D nightTexture;
  uniform vec2 sunPosition;
  varying vec3 vNormal;
  varying vec2 vUv;

  // [lng, lat] -> direction, using the same convention as the globe library.
  vec3 toDirection(vec2 lngLat) {
    float theta = radians(90.0 - lngLat.x);
    float phi = radians(90.0 - lngLat.y);
    return vec3(sin(phi) * cos(theta), cos(phi), sin(phi) * sin(theta));
  }

  void main() {
    float sunlight = dot(normalize(vNormal), toDirection(sunPosition));
    vec4 day = texture2D(dayTexture, vUv);
    // Night side: city lights plus a faint copy of the daytime map, so the
    // continents stay readable instead of fading to black.
    vec4 night = texture2D(nightTexture, vUv) + day * 0.22;
    // Soft band around the day/night line instead of a hard edge (twilight).
    gl_FragColor = mix(night, day, smoothstep(-0.12, 0.12, sunlight));
  }
`

// Where the globe starts, and where "Reset view" returns to: North America.
const HOME_VIEW = { lat: 32, lng: -98, altitude: 2.1 }

// Atmosphere glow per theme: sea-glass by day, teal at night.
const ATMOSPHERE = { light: '#8fd3c3', dark: '#5ccfb9' }

// A station marker: a small glowing dot in its status colour. The name and
// temperature appear on hover (and stay visible when selected), so 15
// stations never pile their labels on top of each other.
// Optional extras: a soft glow coloured by temperature, and an arrow showing
// where the wind is blowing (longer = stronger).
// Built as a real DOM element so it stays crisp and clickable on the globe.
function stationPin(station, selected, onSelect, layers) {
  const pin = document.createElement('button')
  const name = STATION_NAMES[station.station_id] ?? station.station_id
  const temperature = station.temperature_c === null ? '–' : `${Math.round(station.temperature_c)}°C`
  pin.className = `station-pin ${station.status}${selected ? ' selected' : ''}`
  pin.setAttribute('aria-label', `${name}, ${temperature}`)

  let extras = ''
  if (layers.heat && station.temperature_c !== null) {
    extras += `<span class="pin-heat" style="background: radial-gradient(circle, ${temperatureColor(station.temperature_c, 0.75)} 0%, ${temperatureColor(station.temperature_c, 0)} 70%)"></span>`
  }
  const direction = station.wind_direction_deg
  if (layers.wind && direction !== null && direction !== undefined && station.wind_speed_kmh) {
    // NOAA gives the direction the wind comes FROM; the arrow points where it goes.
    const length = Math.round(Math.min(14 + station.wind_speed_kmh * 0.9, 46))
    extras += `<span class="pin-wind" style="transform: rotate(${direction + 180}deg)"><i style="height: ${length}px"></i></span>`
  }
  pin.innerHTML = `${extras}<span class="pin-dot"></span><span class="pin-label">${escapeHtml(name)} <b>${temperature}</b></span>`
  pin.style.pointerEvents = 'auto'
  pin.onclick = () => onSelect(station.station_id)
  return pin
}

// Ripples: every M3+ quake, plus whichever one is selected. Bigger quakes
// spread further and more slowly, so a strong one is obvious at a glance.
function rippleQuakes(quakes, selectedQuakeId) {
  return quakes.filter((q) => q.magnitude >= 3 || q.event_id === selectedQuakeId)
}

export default function EarthGlobe({
  stations,
  quakes,
  nwsPolygons = [],
  layers,
  onToggleLayer,
  selectedStationId,
  selectedQuakeId,
  onSelectStation,
  onSelectQuake,
  theme,
}) {
  const globeRef = useRef()
  const containerRef = useRef()
  const stationsRef = useRef(stations)
  const quakesRef = useRef(quakes)
  const [size, setSize] = useState({ width: 0, height: 0 })
  const [plates, setPlates] = useState([])
  const [menuOpen, setMenuOpen] = useState(false)

  // Plate boundary lines (Bird 2003, PB2002 model), loaded once on demand.
  useEffect(() => {
    if (!layers.plates || plates.length) return undefined
    let cancelled = false
    fetch('/data/plates.json')
      .then((r) => r.json())
      .then((lines) => !cancelled && setPlates(lines))
      .catch(() => {})
    return () => {
      cancelled = true
    }
  }, [layers.plates, plates.length])

  // Latest data for the fly-to effects below, without re-flying every time
  // the data refreshes.
  useEffect(() => {
    stationsRef.current = stations
    quakesRef.current = quakes
  }, [stations, quakes])

  // The globe material: created once; only the sun position changes later.
  const material = useMemo(() => {
    const loader = new THREE.TextureLoader()
    return new THREE.ShaderMaterial({
      uniforms: {
        dayTexture: { value: loader.load('/textures/earth-day.jpg') },
        nightTexture: { value: loader.load('/textures/earth-night.jpg') },
        sunPosition: { value: new THREE.Vector2(...subsolarPoint()) },
      },
      vertexShader,
      fragmentShader,
    })
  }, [])

  // Move the day/night line with the real sun, once a minute.
  useEffect(() => {
    const timer = setInterval(() => {
      material.uniforms.sunPosition.value.set(...subsolarPoint())
    }, 60_000)
    return () => clearInterval(timer)
  }, [material])

  // The globe needs explicit pixel dimensions: follow the container's size.
  useEffect(() => {
    const observer = new ResizeObserver(([entry]) => {
      const { width, height } = entry.contentRect
      setSize({ width: Math.round(width), height: Math.round(height) })
    })
    observer.observe(containerRef.current)
    return () => observer.disconnect()
  }, [])

  // Start over North America and turn slowly until someone picks something.
  function handleGlobeReady() {
    const globe = globeRef.current
    // Pull back further on narrow screens so the whole globe fits.
    globe.pointOfView({ ...HOME_VIEW, altitude: size.width < 600 ? 2.9 : HOME_VIEW.altitude })
    const controls = globe.controls()
    controls.autoRotate = true
    controls.autoRotateSpeed = 0.3
  }

  function flyTo(lat, lng, altitude) {
    const globe = globeRef.current
    if (!globe) return
    globe.controls().autoRotate = false
    globe.pointOfView({ lat, lng, altitude }, 1200)
  }

  // Fly to a station or a quake when it's selected (from a list, a pin or a
  // new significant quake arriving).
  useEffect(() => {
    const station = stationsRef.current.find((s) => s.station_id === selectedStationId)
    if (station) flyTo(station.lat, station.lon, 1.3)
  }, [selectedStationId])

  useEffect(() => {
    const quake = quakesRef.current.find((q) => q.event_id === selectedQuakeId)
    if (quake) flyTo(quake.lat, quake.lon, 1.5)
  }, [selectedQuakeId])

  // --- Controls overlay ---
  function zoom(factor) {
    const globe = globeRef.current
    if (!globe) return
    const view = globe.pointOfView()
    globe.pointOfView({ ...view, altitude: Math.min(Math.max(view.altitude * factor, 0.5), 4) }, 400)
  }

  function resetView() {
    const globe = globeRef.current
    if (!globe) return
    globe.pointOfView(HOME_VIEW, 900)
  }

  function toggleRotation() {
    const controls = globeRef.current?.controls()
    if (controls) controls.autoRotate = !controls.autoRotate
  }

  return (
    <div ref={containerRef} className="globe-wrap">
      {size.width > 0 && (
        <Globe
          ref={globeRef}
          width={size.width}
          height={size.height}
          onGlobeReady={handleGlobeReady}
          globeMaterial={material}
          backgroundColor="rgba(0,0,0,0)"
          atmosphereColor={ATMOSPHERE[theme] ?? ATMOSPHERE.light}
          atmosphereAltitude={0.22}
          // Quakes: short columns, taller and redder with magnitude; click
          // one for its details.
          pointsData={quakes}
          pointLat="lat"
          pointLng="lon"
          pointAltitude={(q) => 0.006 + Math.max(q.magnitude, 0) * 0.006 + (q.event_id === selectedQuakeId ? 0.04 : 0)}
          pointRadius={(q) => 0.12 + Math.max(q.magnitude, 0) * 0.07}
          pointColor={(q) => magnitudeColor(q.magnitude)}
          pointResolution={16}
          pointLabel={(q) =>
            `<div class="globe-tip"><b>M${q.magnitude}</b> ${escapeHtml(q.place)}<br/>${timeAgo(q.event_time)} · ${Math.round(q.depth_km ?? 0)} km deep</div>`
          }
          onPointClick={(q) => onSelectQuake(q.event_id)}
          // Ripples spreading out from each M3+ quake.
          ringsData={layers.ripples ? rippleQuakes(quakes, selectedQuakeId) : []}
          ringLat="lat"
          ringLng="lon"
          ringColor={(q) => {
            const color = magnitudeColor(q.magnitude)
            return (t) => withAlpha(color, 1 - t)
          }}
          ringMaxRadius={(q) => Math.max(q.magnitude, 1) * 1.4}
          ringPropagationSpeed={(q) => 1 + Math.max(q.magnitude, 1) * 0.35}
          ringRepeatPeriod={(q) => 700 + Math.max(q.magnitude, 1) * 260}
          // NWS Severe/Extreme warning areas: translucent violet shapes
          // lying just above the surface, with the warning on hover.
          polygonsData={layers.nws ? nwsPolygons : []}
          polygonGeoJsonGeometry="geometry"
          polygonCapColor={(a) => withAlpha(severityColor(a.severity), 0.35)}
          polygonSideColor={(a) => withAlpha(severityColor(a.severity), 0.15)}
          polygonStrokeColor={(a) => severityColor(a.severity)}
          polygonAltitude={0.004}
          polygonLabel={(a) => `<div class="globe-tip"><b>${escapeHtml(a.event)}</b><br/>${escapeHtml(a.area)}</div>`}
          // Tectonic plate boundaries: thin warm lines. Most quakes happen right
          // on these edges, which is why they cluster where they do.
          pathsData={layers.plates ? plates : []}
          pathPoints={(line) => line}
          pathPointLat={(p) => p[1]}
          pathPointLng={(p) => p[0]}
          pathPointAlt={0.003}
          pathColor={() => (theme === 'dark' ? 'rgba(255, 160, 105, 0.9)' : 'rgba(255, 140, 80, 0.95)')}
          pathStroke={2}
          pathTransitionDuration={0}
          // Stations: clickable dots with hover labels.
          htmlElementsData={stations}
          htmlLat="lat"
          htmlLng="lon"
          htmlAltitude={0.02}
          htmlElement={(s) => stationPin(s, s.station_id === selectedStationId, onSelectStation, layers)}
          // Hide pins for stations on the far side of the globe.
          htmlElementVisibilityModifier={(el, isVisible) => {
            el.style.opacity = isVisible ? 1 : 0
            el.style.pointerEvents = isVisible ? 'auto' : 'none'
          }}
        />
      )}

      <div className="globe-controls">
        <button type="button" className="icon-btn" aria-label="Zoom in" onClick={() => zoom(0.7)}>
          <svg width="18" height="18" viewBox="0 0 18 18" aria-hidden="true">
            <path d="M9 3v12M3 9h12" stroke="currentColor" strokeWidth="2" strokeLinecap="round" />
          </svg>
        </button>
        <button type="button" className="icon-btn" aria-label="Zoom out" onClick={() => zoom(1.4)}>
          <svg width="18" height="18" viewBox="0 0 18 18" aria-hidden="true">
            <path d="M3 9h12" stroke="currentColor" strokeWidth="2" strokeLinecap="round" />
          </svg>
        </button>
        <button type="button" className="icon-btn" aria-label="Reset view" onClick={resetView}>
          <svg width="18" height="18" viewBox="0 0 18 18" aria-hidden="true">
            <circle cx="9" cy="9" r="5" fill="none" stroke="currentColor" strokeWidth="1.6" />
            <path d="M9 1v3M9 14v3M1 9h3M14 9h3" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" />
          </svg>
        </button>
        <button
          type="button"
          className={`icon-btn${menuOpen ? ' on' : ''}`}
          aria-label="Layers"
          aria-expanded={menuOpen}
          onClick={() => setMenuOpen((open) => !open)}
        >
          <svg width="18" height="18" viewBox="0 0 18 18" aria-hidden="true">
            <path
              d="M9 2 L16 6 L9 10 L2 6 Z M2 10 L9 14 L16 10"
              fill="none"
              stroke="currentColor"
              strokeWidth="1.6"
              strokeLinejoin="round"
            />
          </svg>
        </button>
        <button type="button" className="icon-btn" aria-label="Start or stop rotation" onClick={toggleRotation}>
          <svg width="18" height="18" viewBox="0 0 18 18" aria-hidden="true">
            <path d="M15 9a6 6 0 1 1-2-4.5" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" />
            <path
              d="M13.5 1.5v3.5H10"
              fill="none"
              stroke="currentColor"
              strokeWidth="1.6"
              strokeLinecap="round"
              strokeLinejoin="round"
            />
          </svg>
        </button>
      </div>

      {menuOpen && (
        <div className="layers-menu" role="group" aria-labelledby="layers-title">
          <span id="layers-title" className="layers-title">
            Layers
          </span>
          {LAYERS.map(({ key, label }) => (
            <label key={key}>
              <input type="checkbox" checked={layers[key]} onChange={() => onToggleLayer(key)} />
              {label}
            </label>
          ))}
        </div>
      )}
    </div>
  )
}
