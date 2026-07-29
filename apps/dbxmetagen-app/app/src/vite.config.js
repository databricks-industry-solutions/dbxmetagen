import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: {
    // Scope to '/api/' so the proxy doesn't swallow local source modules like
    // '/apiCache.js' during dev. All real backend calls use '/api/...'.
    proxy: { '/api/': 'http://localhost:8000' }
  },
  build: {
    outDir: 'dist',
    // emptyOutDir clears stale content-hashed files on each build so old
    // assets/index-<hash>.js don't accumulate in the committed dist/.
    emptyOutDir: true,
    // Use Vite's default content-hashed filenames (assets/index-<hash>.js) so
    // every deploy busts the browser cache automatically. index.html — served
    // fresh by FastAPI StaticFiles(html=True), not CDN-cached — always points at
    // the current hashes, so there is no stale-HTML/dead-hash mismatch risk.
  }
})
