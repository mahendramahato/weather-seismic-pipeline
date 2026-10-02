// Fetch JSON from our API; throw if the server answered with an error status
// so callers can show a message instead of silently drawing nothing.
export async function getJson(path) {
  const response = await fetch(path)
  if (!response.ok) {
    throw new Error(`${path} returned ${response.status}`)
  }
  return response.json()
}

// Turn an ISO UTC time ("2026-10-02T14:05:00Z") into "5 min ago" / "3 h ago".
export function timeAgo(isoTime) {
  if (!isoTime) return 'never'
  const minutes = Math.round((Date.now() - new Date(isoTime)) / 60000)
  if (minutes < 1) return 'just now'
  if (minutes < 60) return `${minutes} min ago`
  const hours = Math.round(minutes / 60)
  if (hours < 48) return `${hours} h ago`
  return `${Math.round(hours / 24)} days ago`
}
