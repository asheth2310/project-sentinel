/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{js,ts,jsx,tsx}'],
  theme: {
    extend: {
      colors: {
        sentinel: {
          50: '#f0f7ff',
          100: '#e0efff',
          200: '#baddff',
          300: '#7cc2ff',
          400: '#36a3ff',
          500: '#0c86f0',
          600: '#0068cd',
          700: '#0053a6',
          800: '#044889',
          900: '#0a3d71',
          950: '#07264b',
        }
      }
    },
  },
  plugins: [],
}
