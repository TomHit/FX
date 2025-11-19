import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import { fileURLToPath, URL } from 'node:url'

export default defineConfig({
  base: '/react/',
  plugins: [react()],
  resolve: {
    alias: { '@': fileURLToPath(new URL('./src', import.meta.url)) },
    dedupe: ['react', 'react-dom'],     // keep (prevents the original bug)
  },
  build: {
    sourcemap: false,                   // revert debug
    minify: true,                       // revert debug (default)
    // Keep this for stability (optional). Remove to re-enable vendor splitting.
    rollupOptions: { output: { manualChunks: undefined } },
  },
  optimizeDeps: {
    include: ['react', 'react-dom', 'react/jsx-runtime', 'react-dom/client'],
    exclude: [],
  },
})
