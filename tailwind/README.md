# Vendored Tailwind CSS (offline)

The viewer loads `finances/web/static/css/tailwind.css` locally so it stays
fully styled with no internet (Thing 5C — replaces the old Tailwind Play CDN).

`tailwind.css` is a build artifact: Tailwind v3 compiles only the utility
classes it finds in the templates. Regenerate it after adding/removing classes:

```bash
npx -y tailwindcss@3.4.17 \
  -c tailwind/tailwind.config.js \
  -i tailwind/input.css \
  -o finances/web/static/css/tailwind.css \
  --minify

# Strip Tailwind's /*! ... https://tailwindcss.com */ license banner so the
# vendored CSS carries no external URL at all (keeps the offline ethos).
python - <<'PY'
import re, pathlib
p = pathlib.Path("finances/web/static/css/tailwind.css")
p.write_text(re.sub(r"/\*!.*?\*/", "", p.read_text(), flags=re.S))
PY
```

`content` globs (in `tailwind.config.js`) cover `templates/**/*.html` and
`web/**/*.py`. If a class only appears in a newly added file outside those
globs, add the glob before rebuilding or it will be purged.
