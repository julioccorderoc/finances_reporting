#!/usr/bin/env bash
#
# finances.command — double-click this in Finder: the ONE entry point.
# (Finder hides the .command suffix and shows it as "finances"; the suffix
# is what makes macOS execute it on double-click.)
#
# What it does (deliberately dumb — plain bash, no launchd, no daemons):
#   1. cd into the repo (wherever this file lives),
#   2. start `finances serve` if the port is free, else just reuse the running one,
#   3. open the browser at the viewer right away — never wait on network syncs,
#   4. THEN run `finances update` as a background job in this terminal, so the
#      per-source summary prints while you're already browsing; if it fails
#      (offline, VPN off for Binance), the launcher keeps going,
#   5. on exit (Ctrl-C or closing the window), regenerate report.html so the
#      static file reflects any edits made this session.
#
# The server's own shutdown hook also regenerates report.html; the EXIT trap
# below is a harmless belt-and-suspenders for the cases where it can't (e.g. a
# hard window close before the server finishes teardown). `finances update`
# regenerates report.html too and is safe alongside the server (SQLite WAL).

set -euo pipefail

# Resolve the repo dir from this script's location and work from there.
REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
cd "$REPO_DIR"

# Finder launches .command with a bare PATH; add the usual spots so `uv` resolves.
export PATH="$HOME/.local/bin:/opt/homebrew/bin:/usr/local/bin:$PATH"

PORT="${FINANCES_PORT:-8765}"
URL="http://localhost:${PORT}/"

if ! command -v uv >/dev/null 2>&1; then
  echo "Error: 'uv' is not on PATH. Install uv (https://docs.astral.sh/uv/), then re-run." >&2
  read -r -p "Press Enter to close… " _ || true
  exit 1
fi

port_in_use() {
  lsof -nP -iTCP:"$PORT" -sTCP:LISTEN >/dev/null 2>&1
}

if port_in_use; then
  echo "Finances viewer already running at ${URL} — reusing it."
  open "$URL" || true
  exit 0
fi

# Refresh the static report on the way out (idempotent; read-only export).
regen_report() {
  echo "Refreshing report.html …"
  uv run finances html >/dev/null 2>&1 || true
}
trap regen_report EXIT

# Open the browser once the server has had a moment to bind.
( sleep 2; open "$URL" >/dev/null 2>&1 || true ) &

# Sync every source in the background AFTER the browser opens; the per-source
# summary prints into this terminal while you browse. A failure here (offline,
# VPN off) never kills the launcher — the summary itself says what went wrong.
(
  sleep 3
  echo ""
  echo "── Syncing sources (finances update) — summary prints here while you browse ──"
  uv run finances update || echo "finances update failed — the viewer still works with existing data."
) &

echo "Starting Finances viewer at ${URL}"
echo "Press Ctrl-C (or close this window) to stop — report.html is refreshed on exit."
uv run finances serve --port "$PORT"
