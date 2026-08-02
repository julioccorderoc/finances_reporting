#!/usr/bin/env bash
#
# finances.command — double-click this in Finder: the ONE entry point.
# (Finder hides the .command suffix and shows it as "finances"; the suffix
# is what makes macOS execute it on double-click.)
#
# What it does (deliberately dumb — plain bash, no launchd, no daemons):
#   1. cd into the repo (wherever this file lives) and check the few things
#      that make everything else impossible when they are missing,
#   2. work out what owns the port: a viewer already answering there is
#      reused (re-open the browser and exit), another app's server is
#      stepped around (scan up for a free port), one of ours that is bound
#      but silent is offered up to be killed,
#   3. run `finances update` in the FOREGROUND so its per-source summary
#      (inserted counts, errors, VPN hint, needs-review total + triage URL)
#      is on screen before the viewer opens; if it fails (offline, VPN off
#      for Binance) the launch continues — the viewer opens on existing data
#      and the sync strip shows what's stale,
#   4. start `finances serve`, and open the browser only once the viewer
#      actually answers /health,
#   5. on exit (Ctrl-C or closing the window), regenerate report.html so the
#      static file reflects any edits made this session.
#
# Every failure ends in die(), which prints WHAT failed, WHY (the evidence)
# and WHAT TO DO, then holds the window open. This matters because Terminal
# closes the window the moment the script exits: an error that only prints
# scrolls past and is gone before it can be read.
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
REQUESTED_PORT="$PORT"
URL="http://localhost:${PORT}/"
# How far past $PORT to look when something else owns it (see
# resolve_target_port). Twenty is well past any realistic pile-up and
# keeps the scan instant.
PORT_SCAN=20
# How many times to re-pick a port when one is stolen mid-startup.
START_ATTEMPTS=3

# ---------------------------------------------------------------------------
# Failure reporting
# ---------------------------------------------------------------------------

# die "<what failed>" "<detail line>"...
#
# The detail lines are for a human staring at a Terminal window at 11pm: name
# the evidence that was actually observed, then the command that fixes it.
die() {
  local what="$1"
  shift
  {
    echo ""
    echo "┌──────────────────────────────────────────────────────────────"
    echo "│ Finances launcher stopped: ${what}"
    echo "└──────────────────────────────────────────────────────────────"
    local line
    for line in "$@"; do
      echo "  ${line}"
    done
    echo ""
    echo "  Where: ${REPO_DIR}"
    echo "  Port:  ${PORT}   Time: $(date '+%Y-%m-%d %H:%M:%S')"
    echo ""
  } >&2
  read -r -p "Press Enter to close… " _ || true
  exit 1
}

# Fail on the things that make every later step impossible, while the
# message can still be specific about them.
preflight() {
  if ! command -v uv >/dev/null 2>&1; then
    die "'uv' was not found" \
      "Everything here runs through 'uv run', so nothing can start without it." \
      "PATH searched: ${PATH}" \
      "" \
      "Fix: install uv → https://docs.astral.sh/uv/getting-started/installation/" \
      "     then double-click this launcher again."
  fi
  if [ ! -f "${REPO_DIR}/pyproject.toml" ] || [ ! -d "${REPO_DIR}/finances" ]; then
    die "this launcher is not sitting in the finances repo" \
      "Looked in: ${REPO_DIR}" \
      "Expected pyproject.toml and a finances/ package next to this file." \
      "This is what a COPY of finances.command on the Desktop looks like." \
      "" \
      "Fix: launch the one inside the repo — or right-click it there and" \
      "     choose Make Alias, and keep the alias wherever you like."
  fi
}

# ---------------------------------------------------------------------------
# Ports
# ---------------------------------------------------------------------------

terminate_pids() {
  kill "$@" 2>/dev/null || true
}

port_bound() {
  lsof -nP -iTCP:"$1" -sTCP:LISTEN >/dev/null 2>&1
}

port_holder_pids() {
  lsof -nP -iTCP:"$1" -sTCP:LISTEN -t 2>/dev/null | tr '\n' ' ' | sed 's/ *$//'
}

# What is squatting the port, in words — so the message names the culprit
# instead of leaving a pid to look up by hand.
port_holder_command() {
  local pid
  for pid in $(port_holder_pids "$1"); do
    ps -o command= -p "$pid" 2>/dev/null | cut -c1-100
    return 0
  done
}

# "Running" means answering *as this viewer*, not merely bound. Under the
# reload supervisor (ADR-012 Amendment 2026-07-26) the parent holds the
# socket even when its child is dead on a broken edit — so a bare lsof check
# would report "already running", open the browser onto a hang, and exit 0.
# The body match matters too: 8765 is a popular dev-server port and plenty of
# other FastAPI apps expose /health, so a bare 200 would send the browser to
# somebody else's app.
port_in_use() {
  port_bound "$1" || return 1
  curl -fs --max-time 2 "http://localhost:${1}/health" 2>/dev/null |
    grep -q '{"status":"ok"}'
}

# Does a `finances serve` process hold this port? A foreign app is stepped
# around; our own wedged supervisor is not, because routing around it would
# leave a zombie on 8765 and move the viewer's URL on every later launch.
port_holder_is_ours() {
  local pid
  for pid in $(port_holder_pids "$1"); do
    if ps -o command= -p "$pid" 2>/dev/null | grep -q "finances serve"; then
      return 0
    fi
  done
  return 1
}

# Echo "<action> <port>":
#   reuse  — a Finances viewer is already answering there
#   start  — nothing is listening there
#   wedged — one of ours holds it but does not answer
# The scan is what fixes the 2026-08-02 collision (another project's uvicorn
# owned 8765 for days, so `finances serve` died on "Address already in use"
# after the sync had already run). Checking `reuse` before `start` on every
# candidate is what keeps the second double-click from launching a second
# viewer next to the one the first double-click put on the fallback port.
resolve_target_port() {
  local port
  for ((port = PORT; port <= PORT + PORT_SCAN; port++)); do
    if port_in_use "$port"; then
      echo "reuse $port"
      return 0
    fi
    if ! port_bound "$port"; then
      echo "start $port"
      return 0
    fi
    if port_holder_is_ours "$port"; then
      echo "wedged $port"
      return 0
    fi
  done
  return 1
}

# One of ours, bound but silent. Killing it is the right move and the owner
# is the one who gets to make it, so ask — the whole launch is one
# double-click and one keystroke, not a debugging session.
clear_wedged_port() {
  local port="$1" pids reply="" waited=0
  pids="$(port_holder_pids "$port")"
  echo ""
  echo "Port ${port} is held by a Finances server that is not answering /health."
  echo "  Holder pid(s): ${pids}"
  echo "  Most likely a reload supervisor whose child died on a broken edit:"
  echo "  the socket stays open while nothing behind it serves."
  read -r -p "  Kill it and start a fresh viewer? [Y/n] " reply || reply=""
  case "$reply" in
    [Nn]*)
      die "port ${port} left alone at your request" \
        "Nothing was killed — pid(s) ${pids} still hold the port." \
        "" \
        "Fix: kill ${pids}" \
        "     …then re-run this launcher." \
        "Or leave it be and use another port: FINANCES_PORT=8790 open this file."
      ;;
  esac
  # shellcheck disable=SC2086
  terminate_pids $pids
  while port_bound "$port"; do
    if [ "$waited" -ge 10 ]; then
      die "port ${port} is still held after asking those processes to stop" \
        "Sent SIGTERM to pid(s) ${pids}; five seconds later the port is still bound." \
        "Something is ignoring the signal." \
        "" \
        "Fix: kill -9 ${pids}" \
        "     …then re-run this launcher."
    fi
    sleep 0.5
    waited=$((waited + 1))
  done
  echo "  Cleared port ${port}."
  echo ""
}

# ---------------------------------------------------------------------------
# Launch
# ---------------------------------------------------------------------------

# Wait for the viewer to actually answer before opening the browser. The old
# `sleep 2; open` fired blind — during the 2026-08-02 collision that put the
# browser on another project's app while our server was failing to bind.
# Thirty seconds is generous: a cold start with migrations is ~2s.
open_when_ready() {
  local waited=0
  while [ "$waited" -lt 60 ]; do
    if port_in_use "$PORT"; then
      open "$URL" >/dev/null 2>&1 || true
      return 0
    fi
    sleep 0.5
    waited=$((waited + 1))
  done
  {
    echo ""
    echo "The viewer did not answer ${URL}health within 30s — not opening the browser."
    echo "Read the server output above for the reason; the server is still running,"
    echo "so ${URL} may still come up on its own."
  } >&2
  return 1
}

# Run the server, and survive losing the port race. Between
# resolve_target_port and uvicorn's bind, any other process can take the
# port — a small window, but taking 8765 is exactly what happened once
# already. A stolen port is retried elsewhere; anything else fails fast with
# the diagnosis, because retrying a genuinely broken server just hides it.
launch_server() {
  local attempt=1 status decision
  while :; do
    echo "Starting Finances viewer at ${URL}"
    echo "Press Ctrl-C (or close this window) to stop — report.html is refreshed on exit."
    open_when_ready &
    OPENER_PID=$!
    status=0
    uv run finances serve --port "$PORT" || status=$?
    terminate_pids "$OPENER_PID"

    case "$status" in
      0 | 130 | 143) return 0 ;; # clean exit, Ctrl-C, or SIGTERM
    esac

    if [ "$attempt" -lt "$START_ATTEMPTS" ] &&
      port_bound "$PORT" && ! port_holder_is_ours "$PORT"; then
      echo ""
      echo "Port ${PORT} was taken by another app while the viewer was starting:"
      echo "  $(port_holder_command "$PORT")"
      echo "retrying on the next free port…"
      attempt=$((attempt + 1))
      decision="$(resolve_target_port)" || die "no free port to retry on" \
        "Ports ${PORT}–$((PORT + PORT_SCAN)) are all taken by other apps." \
        "Fix: quit whatever is using them, or pick a quieter range:" \
        "     FINANCES_PORT=8900 open this file."
      PORT="${decision##* }"
      URL="http://localhost:${PORT}/"
      continue
    fi

    die "the viewer server exited with status ${status}" \
      "Command: uv run finances serve --port ${PORT}" \
      "The server's own output is above this box — its last lines name the cause." \
      "" \
      "Usual suspects:" \
      "  • a Python error in a file that was just edited (the reloader prints it)" \
      "  • database missing or unmigrated → uv run python -m finances.db.migrate" \
      "  • dependencies out of date → uv pip install -e ." \
      "  • the port was taken mid-start (already retried ${attempt}×)"
  done
}

# ---------------------------------------------------------------------------
# Go
# ---------------------------------------------------------------------------

preflight

DECISION=""
for _ in 1 2; do
  if ! DECISION="$(resolve_target_port)"; then
    die "every port from ${PORT} to $((PORT + PORT_SCAN)) is in use" \
      "Each one is held by some other program, so there is nowhere to start." \
      "Holder of ${PORT}: $(port_holder_command "$PORT")" \
      "" \
      "Fix: quit whatever is using them, or pick a quieter range:" \
      "     FINANCES_PORT=8900 open this file."
  fi
  [ "${DECISION%% *}" = "wedged" ] || break
  clear_wedged_port "${DECISION##* }"
  DECISION=""
done

if [ -z "$DECISION" ] || [ "${DECISION%% *}" = "wedged" ]; then
  die "port ${PORT} is still held by a Finances server that will not answer" \
    "It was killed once and something is back on the port already." \
    "Holder now: $(port_holder_command "$PORT")" \
    "" \
    "Fix: kill $(port_holder_pids "$PORT")" \
    "     …then re-run. Or use another port: FINANCES_PORT=8790 open this file."
fi

ACTION="${DECISION%% *}"
PORT="${DECISION##* }"
URL="http://localhost:${PORT}/"

if [ "$ACTION" = "reuse" ]; then
  echo "Finances viewer already running at ${URL} — reusing it."
  open "$URL" || true
  exit 0
fi

if [ "$PORT" != "$REQUESTED_PORT" ]; then
  echo "Port ${REQUESTED_PORT} is taken by another app — using ${PORT} instead."
  echo "  ${REQUESTED_PORT} is held by: $(port_holder_command "$REQUESTED_PORT")"
  echo ""
fi

# Refresh the static report on the way out (idempotent; read-only export).
regen_report() {
  echo "Refreshing report.html …"
  uv run finances html >/dev/null 2>&1 || true
}
trap regen_report EXIT

# Sync every source BEFORE the viewer opens, in the foreground, so the
# summary (per-source counts, errors, needs-review total + triage URL) is
# the first thing on screen. A failed sync is NOT fatal: the viewer opens on
# existing data and its sync strip shows the staleness — but say plainly what
# failed and what it costs, since stale data looks exactly like fresh data.
echo "── Syncing sources (finances update) ──"
UPDATE_STATUS=0
uv run finances update || UPDATE_STATUS=$?
if [ "$UPDATE_STATUS" -ne 0 ]; then
  echo ""
  echo "⚠ finances update exited ${UPDATE_STATUS} — opening the viewer on EXISTING data."
  echo "  Which source failed is in its output above. Usual suspects:"
  echo "    • Binance refuses without the VPN on (restricted location)"
  echo "    • no network → the BCV and P2P scrapes cannot run"
  echo "  Nothing was lost or half-written; the sync strip in the viewer shows"
  echo "  what is stale. Re-run this launcher once the connection is back."
fi
echo ""

launch_server
