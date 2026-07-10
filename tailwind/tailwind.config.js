/** @type {import('tailwindcss').Config} */
// Build config for the vendored, offline Tailwind stylesheet (Thing 5C).
// Regenerate with: see tailwind/README.md
module.exports = {
  content: [
    "./finances/web/templates/**/*.html",
    "./finances/web/**/*.py",
  ],
  theme: { extend: {} },
  plugins: [],
};
