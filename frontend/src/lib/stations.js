// Display names for the NOAA station codes the producer polls.
export const STATION_NAMES = {
  KBOI: 'Boise',
  KDEN: 'Denver',
  KJFK: 'New York',
  KLAX: 'Los Angeles',
  KORD: 'Chicago',
}

// Status from the streaming detector -> label shown to visitors.
// "unscored" means the 24h baseline isn't complete enough to judge yet.
export const STATUS_LABELS = {
  normal: 'Normal',
  anomaly: 'Anomaly',
  unscored: 'Calibrating',
}
