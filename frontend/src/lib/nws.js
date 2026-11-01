// NWS alert severities, most serious first, with their colours: violets so
// they never get confused with the earthquake reds/oranges.
export const SEVERITIES = ['Extreme', 'Severe', 'Moderate', 'Minor', 'Unknown']

export const SEVERITY_COLORS = {
  Extreme: '#6d3fa8',
  Severe: '#8f6bd6',
  Moderate: '#c79a2e',
  Minor: '#8a9e92',
  Unknown: '#8a9e92',
}

export function severityColor(severity) {
  return SEVERITY_COLORS[severity] ?? SEVERITY_COLORS.Unknown
}

// Signed area of a [lon, lat] ring (shoelace formula): positive means the
// points go counter-clockwise.
function signedArea(ring) {
  let sum = 0
  for (let i = 0; i < ring.length - 1; i++) {
    sum += ring[i][0] * ring[i + 1][1] - ring[i + 1][0] * ring[i][1]
  }
  return sum / 2
}

// GeoJSON (and NWS) list a polygon's outer ring counter-clockwise, but the
// globe uses spherical geometry where that order means "the whole Earth
// EXCEPT this shape" — one warning would paint the entire globe. Flip outer
// rings to clockwise and holes to counter-clockwise.
function sphericalWinding(geometry) {
  const fix = (rings) =>
    rings.map((ring, i) => {
      const counterClockwise = signedArea(ring) > 0
      const isOuter = i === 0
      return isOuter === counterClockwise ? [...ring].reverse() : ring
    })
  if (geometry.type === 'Polygon') return { ...geometry, coordinates: fix(geometry.coordinates) }
  if (geometry.type === 'MultiPolygon') return { ...geometry, coordinates: geometry.coordinates.map(fix) }
  return null
}

// Turn the API's alert rows into polygons the globe can draw. The API only
// sends a polygon for Extreme/Severe alerts; a bad one is skipped, not fatal.
export function alertPolygons(alerts) {
  const out = []
  for (const alert of alerts) {
    if (!alert.geometry) continue
    try {
      const geometry = sphericalWinding(JSON.parse(alert.geometry))
      if (geometry) out.push({ ...alert, geometry })
    } catch {
      // ignore malformed geometry
    }
  }
  return out
}
