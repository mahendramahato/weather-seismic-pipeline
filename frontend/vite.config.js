import react from '@vitejs/plugin-react'
import { defineConfig } from 'vite'

export default defineConfig({
  plugins: [react()],
  // Dev only: forward /api/... to the FastAPI server on 8000, so the browser
  // talks to one address (no CORS). In production Caddy does this forwarding.
  server: {
    proxy: {
      '/api': 'http://localhost:8000',
    },
  },
  // The 3D globe chunk (mostly three.js) is ~2 MB / ~550 kB gzipped. It's
  // lazy-loaded separately from the page, so the default 500 kB warning
  // doesn't apply; warn only if it grows well beyond today's size.
  build: {
    chunkSizeWarningLimit: 2500,
  },
})
