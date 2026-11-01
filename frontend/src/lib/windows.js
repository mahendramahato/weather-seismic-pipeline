// The two time windows: how many hours the API is asked for, and how many
// bars the activity chart splits that into (1 h bars, or 6 h bars for a week).
export const WINDOWS = {
  day: { label: '24 h', hours: 24, buckets: 24 },
  week: { label: '7 days', hours: 168, buckets: 28 },
}
