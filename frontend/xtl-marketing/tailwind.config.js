/** @type {import('tailwindcss').Config} */
module.exports = {
  content: [
    "./pages/**/*.{js,ts,jsx,tsx}",
    "./components/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        bg: "#07060A",
        panel: "#0E0B12",
        gold: "#D4AF37",
        gold2: "#F5D77A",
      },
      boxShadow: {
        soft: "0 10px 30px rgba(0,0,0,0.45)",
        glow: "0 0 0 1px rgba(212,175,55,0.12), 0 20px 60px rgba(212,175,55,0.10)",
      },
    },
  },
  plugins: [],
};
