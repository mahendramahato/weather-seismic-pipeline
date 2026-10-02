// Where on Earth the sun is directly overhead right now (the "subsolar point"),
// as [longitude, latitude] in degrees. The globe shader lights the hemisphere
// facing this point, so the day/night line on the globe matches reality.
export function subsolarPoint(date = new Date()) {
  const startOfDay = Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate())
  const dayOfYear = Math.round((startOfDay - Date.UTC(date.getUTCFullYear(), 0, 0)) / 864e5)
  const seasonAngle = ((2 * Math.PI) / 365) * (dayOfYear - 81)

  // Tilt of the Earth's axis toward the sun: about +23° in June, -23° in December.
  const latitude = 23.44 * Math.sin(seasonAngle)

  // The sun crosses 15° of longitude per hour (overhead at 0° at 12:00 UTC),
  // corrected by the "equation of time" — the few minutes the real sun runs
  // ahead of or behind clock time over the year.
  const equationOfTimeMinutes =
    9.87 * Math.sin(2 * seasonAngle) - 7.53 * Math.cos(seasonAngle) - 1.5 * Math.sin(seasonAngle)
  const longitude = ((startOfDay - date) / 864e5) * 360 - 180 - equationOfTimeMinutes / 4

  return [((((longitude + 180) % 360) + 360) % 360) - 180, latitude]
}
