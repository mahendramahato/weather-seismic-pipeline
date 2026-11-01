// Picks a condition from NWS/NOAA text like "Mostly Cloudy", "Chance Light
// Rain", "Patchy Fog". Order matters: the most notable weather wins.
export function conditionKind(text = '', isDay = true) {
  const s = (text || '').toLowerCase()
  if (/thunder|t-storm/.test(s)) return 'thunder'
  if (/snow|sleet|flurr|ice|wintry|freezing/.test(s)) return 'snow'
  if (/rain|shower|drizzle/.test(s)) return 'rain'
  if (/fog|haze|smoke|mist|dust/.test(s)) return 'fog'
  if (/overcast|^cloudy|mostly cloudy/.test(s)) return 'cloudy'
  if (/partly|mostly sunny|mostly clear|few clouds|scattered/.test(s)) return isDay ? 'partly-day' : 'partly-night'
  if (/wind|breez|blustery/.test(s)) return 'wind'
  return isDay ? 'clear-day' : 'clear-night'
}
