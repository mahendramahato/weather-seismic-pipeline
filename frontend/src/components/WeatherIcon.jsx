// Small line icons for weather conditions and observation fields, drawn as
// inline SVG so they stay crisp and follow the theme. Condition icons use a
// few fixed accent colours (sun gold, rain teal...); field icons use the
// current text colour.
const SUN = '#e8b33a'
const RAIN = '#3f9fb3'
const SNOW = '#8fb8d8'
const BOLT = '#e0712f'

const CLOUD = 'M7 18.5h10a4 4 0 0 0 .6-7.96A6 6 0 0 0 6.2 9.9 4.3 4.3 0 0 0 7 18.5z'

function Sun({ cx = 12, cy = 12, r = 4, rays = 3 }) {
  return (
    <g stroke={SUN}>
      <circle cx={cx} cy={cy} r={r} fill={SUN} fillOpacity="0.25" />
      {[0, 45, 90, 135, 180, 225, 270, 315].map((a) => (
        <line
          key={a}
          x1={cx}
          y1={cy - r - 1.5}
          x2={cx}
          y2={cy - r - 1.5 - rays}
          transform={`rotate(${a} ${cx} ${cy})`}
        />
      ))}
    </g>
  )
}

function Moon({ dx = 0, dy = 0, scale = 1 }) {
  return (
    <path
      d="M20 14.5A8 8 0 0 1 9.5 4a8 8 0 1 0 10.5 10.5Z"
      transform={`translate(${dx} ${dy}) scale(${scale})`}
      stroke={SUN}
      fill={SUN}
      fillOpacity="0.2"
    />
  )
}

function Cloud({ dy = 0 }) {
  return <path d={CLOUD} transform={`translate(0 ${dy})`} fill="var(--panel-solid)" />
}

const CONDITIONS = {
  'clear-day': () => <Sun />,
  'clear-night': () => <Moon />,
  'partly-day': () => (
    <>
      <Sun cx={8.5} cy={8} r={3} rays={2} />
      <Cloud dy={1.5} />
    </>
  ),
  'partly-night': () => (
    <>
      <Moon dx={-1.5} dy={-1.5} scale={0.62} />
      <Cloud dy={1.5} />
    </>
  ),
  cloudy: () => <Cloud />,
  rain: () => (
    <>
      <Cloud dy={-3} />
      <g stroke={RAIN}>
        <line x1="9" y1="18" x2="8" y2="21" />
        <line x1="13" y1="18" x2="12" y2="21" />
        <line x1="17" y1="18" x2="16" y2="21" />
      </g>
    </>
  ),
  snow: () => (
    <>
      <Cloud dy={-3} />
      <g fill={SNOW} stroke="none">
        <circle cx="8.5" cy="19.5" r="1.2" />
        <circle cx="12.5" cy="21" r="1.2" />
        <circle cx="16.5" cy="19.5" r="1.2" />
      </g>
    </>
  ),
  thunder: () => (
    <>
      <Cloud dy={-3} />
      <path d="M12.5 15.5 10 19.5h3l-1.5 3.5" stroke={BOLT} />
    </>
  ),
  fog: () => (
    <>
      <Cloud dy={-4} />
      <line x1="5" y1="18" x2="19" y2="18" />
      <line x1="7" y1="21" x2="17" y2="21" />
    </>
  ),
  wind: () => (
    <>
      <path d="M3 9h11a3 3 0 1 0-3-3" />
      <path d="M3 13h15a3 3 0 1 1-3 3" />
      <path d="M3 17h7" />
    </>
  ),
}

export function WeatherIcon({ kind, size = 28, label }) {
  const draw = CONDITIONS[kind] ?? CONDITIONS.cloudy
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 24 24"
      fill="none"
      stroke="var(--muted)"
      strokeWidth="1.6"
      strokeLinecap="round"
      strokeLinejoin="round"
      role={label ? 'img' : undefined}
      aria-label={label}
      aria-hidden={label ? undefined : true}
      className="weather-icon"
    >
      {draw()}
    </svg>
  )
}

const FIELDS = {
  thermometer: <path d="M10 13.5V4a2 2 0 1 1 4 0v9.5a4 4 0 1 1-4 0z M12 9v7" />,
  humidity: <path d="M12 3.5s-6 6.6-6 10.5a6 6 0 0 0 12 0c0-3.9-6-10.5-6-10.5z" />,
  dewpoint: (
    <>
      <path d="M12 3.5s-6 6.6-6 10.5a6 6 0 0 0 12 0c0-3.9-6-10.5-6-10.5z" />
      <path d="M9 14.5a3 3 0 0 0 3 3" />
    </>
  ),
  wind: (
    <>
      <path d="M3 9h11a3 3 0 1 0-3-3" />
      <path d="M3 14h15a3 3 0 1 1-3 3" />
    </>
  ),
  gust: (
    <>
      <path d="M3 8h9a2.5 2.5 0 1 0-2.5-2.5" />
      <path d="M3 12h15a3 3 0 1 1-3 3" />
      <path d="M3 16h6" />
    </>
  ),
  pressure: (
    <>
      <path d="M4.5 16a8 8 0 1 1 15 0" />
      <path d="M12 12l3.5-3.5" />
      <circle cx="12" cy="12" r="1" />
    </>
  ),
  visibility: (
    <>
      <path d="M2.5 12S6 5.5 12 5.5 21.5 12 21.5 12 18 18.5 12 18.5 2.5 12 2.5 12z" />
      <circle cx="12" cy="12" r="3" />
    </>
  ),
  cloud: <path d={CLOUD} />,
  precip: (
    <>
      <path d="M3 12a9 9 0 0 1 18 0z" />
      <path d="M12 12v6.5a2 2 0 0 1-4 0" />
    </>
  ),
}

export function FieldIcon({ name, size = 18 }) {
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.6"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      {FIELDS[name]}
    </svg>
  )
}
