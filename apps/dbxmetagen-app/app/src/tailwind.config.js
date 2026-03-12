/** @type {import('tailwindcss').Config} */
export default {
  darkMode: 'class',
  content: ['./index.html', './main.jsx', './App.jsx', './components/**/*.{js,jsx}'],
  theme: {
    extend: {
      colors: {
        'dbx-lava': '#FF3621',
        'dbx-navy': '#0B2026',
        'dbx-oat': '#EEEDE9',
        'dbx-oat-light': '#F9F7F4',
        'dbx-oat-dark': '#E2E0D9',
      },
    },
  },
  plugins: [],
}
