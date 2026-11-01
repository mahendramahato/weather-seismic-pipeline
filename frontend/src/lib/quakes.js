// Colour for an earthquake by magnitude, shared by the globe, the replay bars
// and the alerts so the same strength always has the same colour:
// gold (small) → orange → crimson → deep wine (M6+).
export function magnitudeColor(magnitude) {
  if (magnitude >= 6) return '#a61e4d'
  if (magnitude >= 4.5) return '#d63f3f'
  if (magnitude >= 2.5) return '#e0712f'
  return '#e8b33a'
}

// "#e0712f" + 0.4 -> "rgba(224, 113, 47, 0.4)", for fading rings on the globe.
export function withAlpha(hex, alpha) {
  const n = parseInt(hex.slice(1), 16)
  return `rgba(${(n >> 16) & 255}, ${(n >> 8) & 255}, ${n & 255}, ${alpha})`
}

// USGS groups depths into these bands; shallow quakes are felt the most.
export function depthClass(km) {
  if (km < 70) return 'shallow'
  if (km < 300) return 'intermediate'
  return 'deep'
}

// Newest significant (M4.5+) quake in a list the API returns newest first.
export function latestSignificant(quakes) {
  return quakes.find((q) => q.is_significant) ?? null
}

export function largest(quakes) {
  return quakes.reduce((best, q) => (!best || q.magnitude > best.magnitude ? q : best), null)
}
