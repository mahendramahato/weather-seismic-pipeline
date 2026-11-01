// Globe layers the viewer can switch on and off from the Layers menu. The
// choice is remembered in this browser only (a convenience; if storage is
// blocked, the defaults are used).
export const LAYERS = [
  { key: 'ripples', label: 'Quake ripples' },
  { key: 'plates', label: 'Tectonic plate boundaries' },
  { key: 'heat', label: 'Temperature glow' },
  { key: 'wind', label: 'Wind arrows' },
  { key: 'nws', label: 'NWS warning areas' },
]

export const DEFAULT_LAYERS = { ripples: true, plates: true, heat: true, wind: true, nws: true }

const STORAGE_KEY = 'globe-layers'

export function loadLayers() {
  try {
    const saved = JSON.parse(localStorage.getItem(STORAGE_KEY))
    return { ...DEFAULT_LAYERS, ...(saved ?? {}) }
  } catch {
    return DEFAULT_LAYERS
  }
}

export function saveLayers(layers) {
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(layers))
  } catch {
    // storage unavailable: the choice just won't be remembered
  }
}

// Temperature -> colour for the glow around stations: blue (cold) through
// teal and green to amber and red (hot). Stops in °C.
const TEMP_STOPS = [
  [-15, [91, 127, 209]],
  [0, [108, 192, 201]],
  [10, [143, 209, 139]],
  [20, [242, 193, 78]],
  [30, [232, 116, 59]],
  [40, [200, 50, 60]],
]

export function temperatureColor(celsius, alpha = 1) {
  if (celsius === null || celsius === undefined) return `rgba(150, 150, 150, ${alpha})`
  const t = Math.min(Math.max(celsius, TEMP_STOPS[0][0]), TEMP_STOPS.at(-1)[0])
  let i = 0
  while (t > TEMP_STOPS[i + 1][0]) i += 1
  const [t0, c0] = TEMP_STOPS[i]
  const [t1, c1] = TEMP_STOPS[i + 1]
  const f = (t - t0) / (t1 - t0)
  const [r, g, b] = c0.map((v, k) => Math.round(v + (c1[k] - v) * f))
  return `rgba(${r}, ${g}, ${b}, ${alpha})`
}
