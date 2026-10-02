import { useEffect, useState } from 'react'

// Light theme from 06:00 to 18:00 in the visitor's own time zone, dark otherwise.
// "?theme=dark" or "?theme=light" in the URL pins one (handy for screenshots).
function themeForNow() {
  const pinned = new URLSearchParams(window.location.search).get('theme')
  if (pinned === 'light' || pinned === 'dark') return pinned
  const hour = new Date().getHours()
  return hour >= 6 && hour < 18 ? 'light' : 'dark'
}

export function useDayTheme() {
  const [theme, setTheme] = useState(themeForNow)

  // Re-check every minute so the page switches at 06:00 / 18:00 on its own.
  useEffect(() => {
    const timer = setInterval(() => setTheme(themeForNow()), 60_000)
    return () => clearInterval(timer)
  }, [])

  // The CSS colour variables are keyed on <html data-theme="...">.
  useEffect(() => {
    document.documentElement.dataset.theme = theme
  }, [theme])

  return theme
}
