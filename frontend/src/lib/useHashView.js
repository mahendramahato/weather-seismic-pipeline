import { useEffect, useState } from 'react'

// Which page is showing, from the URL: "#/pipeline" = the pipeline page,
// anything else = the globe. Using the hash keeps it a single static page
// (no router, no server config) while Back/Forward and links still work.
function viewFromHash() {
  return window.location.hash === '#/pipeline' ? 'pipeline' : 'globe'
}

export function useHashView() {
  const [view, setView] = useState(viewFromHash)

  useEffect(() => {
    const onChange = () => setView(viewFromHash())
    window.addEventListener('hashchange', onChange)
    return () => window.removeEventListener('hashchange', onChange)
  }, [])

  return view
}
