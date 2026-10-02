// Fetch JSON from our API; throw on an error status so callers can show it.
export async function getJson(path) {
  const response = await fetch(path)
  if (!response.ok) {
    throw new Error(`${path} returned ${response.status}`)
  }
  return response.json()
}

// "2026-10-02T14:05:00Z" -> "5 min ago" / "3 h ago" / "2 days ago".
export function timeAgo(isoTime) {
  if (!isoTime) return 'never'
  const minutes = Math.round((Date.now() - new Date(isoTime)) / 60000)
  if (minutes < 1) return 'just now'
  if (minutes < 60) return `${minutes} min ago`
  const hours = Math.round(minutes / 60)
  if (hours < 48) return `${hours} h ago`
  return `${Math.round(hours / 24)} days ago`
}

export function toFahrenheit(celsius) {
  return Math.round((celsius * 9) / 5 + 32)
}

// Text from external feeds (USGS place names) goes into globe tooltips as
// HTML, so escape it first.
export function escapeHtml(text) {
  return String(text)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
}
