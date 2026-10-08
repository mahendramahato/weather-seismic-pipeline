import { useEffect, useMemo, useRef, useState } from 'react'
import Globe from 'react-globe.gl'
import * as THREE from 'three'
import { escapeHtml, timeAgo } from '../lib/format.js'
import { STATION_NAMES } from '../lib/stations.js'
import { magnitudeColor } from '../lib/quakes.js'
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

// A station marker: a small glowing dot in its status colour. The name and
// temperature appear on hover (and stay visible when selected), so 15
// stations never pile their labels on top of each other.
// Built as a real DOM element so it stays crisp and clickable on the globe.
function stationPin(station, selected, onSelect) {
  const pin = document.createElement('button')
  const name = STATION_NAMES[station.station_id] ?? station.station_id
  const temperature = station.temperature_c === null ? '–' : `${Math.round(station.temperature_c)}°C`
  pin.className = `station-pin ${station.status}${selected ? ' selected' : ''}`
  pin.setAttribute('aria-label', `${name}, ${temperature}`)
  pin.innerHTML = `<span class="pin-dot"></span><span class="pin-label">${escapeHtml(name)} <b>${temperature}</b></span>`
  pin.style.pointerEvents = 'auto'
  pin.onclick = () => onSelect(station.station_id)
  return pin
}

export default function EarthGlobe({ stations, quakes, selectedId, onSelectStation, theme }) {
  const globeRef = useRef()
  const containerRef = useRef()
  const stationsRef = useRef(stations)
  const [size, setSize] = useState({ width: 0, height: 0 })

  // Latest stations for the fly-to effect below, without re-flying every time
  // the data refreshes.
  useEffect(() => {
    stationsRef.current = stations
  }, [stations])

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

  // Start over North America and turn slowly until someone picks a station.
  function handleGlobeReady() {
    const globe = globeRef.current
    globe.pointOfView({ lat: 32, lng: -98, altitude: 2.1 })
    const controls = globe.controls()
    controls.autoRotate = true
    controls.autoRotateSpeed = 0.3
  }

  // Fly to a station when it's selected (from a card or a pin).
  useEffect(() => {
    const station = stationsRef.current.find((s) => s.station_id === selectedId)
    const globe = globeRef.current
    if (!station || !globe) return
    globe.controls().autoRotate = false
    globe.pointOfView({ lat: station.lat, lng: station.lon, altitude: 1.3 }, 1200)
  }, [selectedId])

  const significant = quakes.filter((q) => q.is_significant)

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
          atmosphereColor={theme === 'dark' ? '#6fb3ff' : '#8fc4ff'}
          atmosphereAltitude={0.2}
          // Quakes: flat dots, bigger and redder with magnitude.
          pointsData={quakes}
          pointLat="lat"
          pointLng="lon"
          pointAltitude={0.004}
          pointRadius={(q) => 0.1 + Math.max(q.magnitude, 0) * 0.07}
          pointColor={(q) => magnitudeColor(q.magnitude)}
          pointResolution={16}
          pointLabel={(q) =>
            `<div class="globe-tip"><b>M${q.magnitude}</b> ${escapeHtml(q.place)}<br/>${timeAgo(q.event_time)}</div>`
          }
          // Significant quakes (M4.5+) also send out pulsing rings.
          ringsData={significant}
          ringLat="lat"
          ringLng="lon"
          ringColor={() => (t) => `rgba(239, 68, 68, ${1 - t})`}
          ringMaxRadius={(q) => q.magnitude * 1.3}
          ringPropagationSpeed={2.2}
          ringRepeatPeriod={1500}
          // Stations: clickable dots with hover labels.
          htmlElementsData={stations}
          htmlLat="lat"
          htmlLng="lon"
          htmlAltitude={0.02}
          htmlElement={(s) => stationPin(s, s.station_id === selectedId, onSelectStation)}
          // Hide pins for stations on the far side of the globe.
          htmlElementVisibilityModifier={(el, isVisible) => {
            el.style.opacity = isVisible ? 1 : 0
            el.style.pointerEvents = isVisible ? 'auto' : 'none'
          }}
        />
      )}
    </div>
  )
}
