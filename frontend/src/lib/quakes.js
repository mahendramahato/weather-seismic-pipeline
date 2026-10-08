// Colour for an earthquake by magnitude, shared by the globe and the alerts so
// the same strength always has the same colour.
export function magnitudeColor(magnitude) {
  if (magnitude >= 6) return '#b91c1c'
  if (magnitude >= 4.5) return '#ef4444'
  if (magnitude >= 2.5) return '#fb923c'
  return '#fcd34d'
}
