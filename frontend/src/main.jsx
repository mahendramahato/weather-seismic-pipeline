import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
// Leaflet's own CSS: without it map tiles and markers render broken
import 'leaflet/dist/leaflet.css'
import './styles.css'
import App from './App.jsx'

// Mount the App component into <div id="root"> in index.html.
// StrictMode adds extra checks during development (no effect in production).
createRoot(document.getElementById('root')).render(
  <StrictMode>
    <App />
  </StrictMode>,
)
