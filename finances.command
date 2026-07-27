#!/usr/bin/env bash
#
# finances.command — double-click this in Finder: the ONE entry point.
# (Finder hides the .command suffix and shows it as "finances"; the suffix
# is what makes macOS execute it on double-click.)
#
# What it does (deliberately dumb — plain bash, no launchd, no daemons):
#   1. cd into the repo (wherever this file lives),
#   2. if the port is already bound, reuse the running server: re-open the
#      browser and exit,
#   3. run `finances update` in the FOREGROUND so its per-source summary
#      (inserted counts, errors, VPN hint, needs-review total + triage URL)
#      is on screen before the viewer opens; if it fails (offline, VPN off
#      for Binance) the launch continues — the viewer opens on existing data
#      and the sync strip shows what's stale,
#   4. start `finances serve` and open the browser at the viewer,
#   5. on exit (Ctrl-C or closing the window), regenerate report.html so the
#      static file reflects any edits made this session.
#
# `finances serve` also regenerates report.html on the way out — from the
# lifespan hook normally, or from serve_cmd itself under the reload supervisor
# (where the child is SIGTERM'd on every source edit and must NOT export each
# time). The EXIT trap below is a harmless belt-and-suspenders for the cases
# where neither can (e.g. a hard window close before teardown finishes).
# `finances update` regenerates report.html too.

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

# "Running" now means answering, not merely bound. Under the reload
# supervisor (ADR-012 Amendment 2026-07-26) the parent holds the socket even
# when its child is dead on a broken edit — so a bare lsof check would report
# "already running", open the browser onto a hang, and exit 0. If the port is
# bound but /health does not answer, fall through to the start path and let it
# fail loudly on "Address already in use": a visible error beats a silent hang.
port_in_use() {
  lsof -nP -iTCP:"$PORT" -sTCP:LISTEN >/dev/null 2>&1 || return 1
  curl -fs --max-time 2 "${URL}health" >/dev/null 2>&1
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

# Sync every source BEFORE the viewer opens, in the foreground, so the
# summary (per-source counts, errors, needs-review total + triage URL) is
# the first thing on screen. The `|| echo` guard means a hard update failure
# never aborts the launch under `set -e` — the viewer still opens on
# existing data and its sync strip shows the staleness.
echo "── Syncing sources (finances update) ──"
uv run finances update || echo "finances update failed — opening the viewer on existing data."
echo ""

# Open the browser once the server has had a moment to bind.
( sleep 2; open "$URL" >/dev/null 2>&1 || true ) &

echo "Starting Finances viewer at ${URL}"
echo "Press Ctrl-C (or close this window) to stop — report.html is refreshed on exit."
uv run finances serve --port "$PORT"
