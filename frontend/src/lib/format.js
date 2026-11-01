// Fetch JSON from our API; throw on an error status so callers can show it.
export async function getJson(path) {
  const response = await fetch(path)
  // 429 = the server's rate limit (Caddy) was hit; the page retries on its
  // regular refresh, so say that instead of showing a bare status code.
  if (response.status === 429) {
    throw new Error('too many requests right now — the page will retry in a minute')
  }
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

// UTC clock text for the replay and quake details, e.g. "Sun 1 Nov · 12:38 UTC".
// The pipeline runs on UTC, so times are shown in UTC everywhere.
const UTC_DAY = new Intl.DateTimeFormat('en-GB', { weekday: 'short', day: 'numeric', month: 'short', timeZone: 'UTC' })
const UTC_TIME = new Intl.DateTimeFormat('en-GB', { hour: '2-digit', minute: '2-digit', hour12: false, timeZone: 'UTC' })

export function formatUtc(time, { withDay = true } = {}) {
  const date = new Date(time)
  const clock = `${UTC_TIME.format(date)} UTC`
  return withDay ? `${UTC_DAY.format(date).replace(',', '')} · ${clock}` : clock
}

// "2026-10-31" -> "Oct 31".
export function formatDay(isoDay) {
  if (!isoDay) return '–'
  return new Date(`${String(isoDay).slice(0, 10)}T00:00:00Z`).toLocaleDateString('en-US', {
    month: 'short',
    day: 'numeric',
    timeZone: 'UTC',
  })
}
