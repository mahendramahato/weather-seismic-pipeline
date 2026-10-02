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
})
