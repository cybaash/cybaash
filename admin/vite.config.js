import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig(({ mode }) => ({
  plugins: [react()],
  // Use import.meta.env (Vite standard) — process.env is NOT available in client builds.
  // VITE_BASE_URL can be set in .env or as a CI/CD environment variable.
  base: process.env.VITE_BASE_URL ?? './',
  build: {
    outDir: 'dist',
    assetsInlineLimit: 4096,
    // Warn if any individual chunk exceeds 600 kB (App.jsx is large)
    chunkSizeWarningLimit: 600,
  },
}))
