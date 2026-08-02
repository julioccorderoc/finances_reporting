"""Coverage for ``finances.command`` — the double-click entry point.

This file is the ONE thing the owner launches, and until now nothing
tested it. That was tolerable while its port check was a single `lsof`
call. It stopped being tolerable under the reload supervisor (ADR-012
Amendment 2026-07-26), where the process holding port 8765 is the
supervisor and can outlive a child that died on a broken edit: a bound
port no longer means a working viewer, so the launcher must ask.

It stopped being enough again on 2026-08-02, when another project's
uvicorn was squatting 8765: the /health probe correctly said "not our
viewer", the launcher fell through to its start path, and uvicorn died on
``[Errno 48] Address already in use``. 8765 is a popular dev-server port,
so the launcher now distinguishes *who* holds it — a foreign app is
stepped around, our own wedged supervisor is reported.

Bash is tested the way bash is testable — the functions under test are
extracted from the real file and sourced into a subshell whose PATH holds
stubbed ``lsof``, ``curl`` and ``ps``. Extraction fails loudly if a
function is renamed or reshaped, which is the failure mode worth having: a
launcher test that silently stops testing the launcher is worse than none.
"""

from __future__ import annotations

import shutil
import subprocess
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
LAUNCHER = REPO_ROOT / "finances.command"

PORT = 8765

# The exact bytes ``GET /health`` returns. The launcher greps for this
# instead of trusting a bare 200, because a foreign dev server on 8765 may
# well answer /health too — and "reuse it" would then open the browser on
# somebody else's app and exit 0.
HEALTH_BODY = '{"status":"ok"}'

FUNCTIONS = (
    "die",
    "preflight",
    "terminate_pids",
    "port_bound",
    "port_in_use",
    "port_holder_is_ours",
    "port_holder_pids",
    "port_holder_command",
    "resolve_target_port",
    "clear_wedged_port",
    "open_when_ready",
    "launch_server",
)

# Stubs. Each fake keeps its universe in the environment: BOUND_PORTS is
# what lsof sees listening, HEALTHY_PORTS is what curl gets an answer
# from, OUR_PORTS is what ps reports as a `finances serve` process. A
# port's PID is the port number itself, which keeps the fakes joinable.
LSOF_STUB = """
port=""
want_pids=0
for arg in "$@"; do
  case "$arg" in
    -iTCP:*) port="${arg#-iTCP:}" ;;
    -t) want_pids=1 ;;
  esac
done
# A port the test has "killed" the holder of stops being bound.
if [ -f "$STUB_STATE/unbound.$port" ]; then exit 1; fi
for bound in $BOUND_PORTS; do
  if [ "$bound" = "$port" ]; then
    if [ "$want_pids" = 1 ]; then echo "$port"; fi
    exit 0
  fi
done
exit 1
"""

CURL_STUB = """
printf '%s\\n' "$*" >> "$CURL_LOG"
port=""
for arg in "$@"; do
  case "$arg" in
    http://localhost:*) port="${arg#http://localhost:}"; port="${port%%/*}" ;;
  esac
done
for healthy in $HEALTHY_PORTS; do
  if [ "$healthy" = "$port" ]; then
    printf '%s' 'HEALTH_BODY_PLACEHOLDER'
    exit 0
  fi
done
exit 22
""".replace("HEALTH_BODY_PLACEHOLDER", HEALTH_BODY)

# `ps -o command= -p <pid>` — the PID is the last argument.
PS_STUB = """
pid="${!#}"
for ours in $OUR_PORTS; do
  if [ "$ours" = "$pid" ]; then
    echo "/repo/.venv/bin/python /repo/.venv/bin/finances serve --port $pid"
    exit 0
  fi
done
echo "/other/.venv/bin/python /other/.venv/bin/uvicorn api.main:app --port $pid"
"""

# `uv run …` — records its argv and exits with the next code in
# UV_EXIT_CODES (the last one repeats), so a test can say "fail, then work".
UV_STUB = """
printf '%s\\n' "$*" >> "$UV_LOG"
count_file="$STUB_STATE/uv-count"
n=0
if [ -f "$count_file" ]; then n="$(cat "$count_file")"; fi
n=$((n + 1))
echo "$n" > "$count_file"
i=0
code=0
for code in ${UV_EXIT_CODES:-0}; do
  i=$((i + 1))
  if [ "$i" = "$n" ]; then exit "$code"; fi
done
exit "$code"
"""

OPEN_STUB = """
printf '%s\\n' "$*" >> "$OPEN_LOG"
"""

# Polling loops are the reason these tests would otherwise take 30s each.
SLEEP_STUB = "exit 0"


def _extract_function(name: str) -> str:
    """Pull one ``name() { ... }`` block out of the launcher verbatim.

    The launcher cannot simply be sourced: it is a top-to-bottom script
    that would cd, sync every source, and start a server.
    """
    lines = LAUNCHER.read_text().splitlines()
    try:
        start = next(
            i for i, line in enumerate(lines) if line.startswith(f"{name}() {{")
        )
    except StopIteration:  # pragma: no cover - the assert below reports it
        raise AssertionError(
            f"{name}() not found in {LAUNCHER.name} — if it was renamed or "
            "reshaped, update this test rather than deleting it"
        ) from None

    end = next(i for i, line in enumerate(lines[start:], start) if line == "}")
    return "\n".join(lines[start : end + 1])


def _stub(directory: Path, name: str, body: str) -> None:
    path = directory / name
    path.write_text(f"#!/usr/bin/env bash\n{body}\n")
    path.chmod(0o755)


@dataclass
class Run:
    """What a harness run produced, including the stub logs."""

    stdout: str
    stderr: str
    returncode: int
    curl_argv: str
    uv_argv: str
    open_argv: str


def _run_raw(
    tmp_path: Path,
    snippet: str,
    *,
    bound: Iterable[int] = (),
    healthy: Iterable[int] = (),
    ours: Iterable[int] = (),
    port: int = PORT,
    uv_exit_codes: str = "0",
    with_uv: bool = True,
    repo_dir: Path | None = None,
    stdin: str = "",
) -> Run:
    """Run ``snippet`` against the real launcher functions and stubbed tools."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    state = tmp_path / "state"
    state.mkdir()
    logs = {name: tmp_path / f"{name}-argv" for name in ("curl", "uv", "open")}
    for log in logs.values():
        log.touch()

    _stub(bin_dir, "lsof", LSOF_STUB)
    _stub(bin_dir, "curl", CURL_STUB)
    _stub(bin_dir, "ps", PS_STUB)
    _stub(bin_dir, "open", OPEN_STUB)
    _stub(bin_dir, "sleep", SLEEP_STUB)
    if with_uv:
        _stub(bin_dir, "uv", UV_STUB)

    script = tmp_path / "harness.sh"
    script.write_text(
        "\n".join(
            [
                "#!/usr/bin/env bash",
                # Same shell options the real launcher runs under, so an
                # errexit interaction would show up here too.
                "set -euo pipefail",
                f"PORT={port}",
                "PORT_SCAN=20",
                "START_ATTEMPTS=3",
                f'REPO_DIR="{repo_dir or tmp_path}"',
                f'URL="http://localhost:{port}/"',
                *(_extract_function(name) for name in FUNCTIONS),
                snippet,
            ]
        )
    )

    result = subprocess.run(
        ["bash", str(script)],
        capture_output=True,
        text=True,
        input=stdin,
        env={
            "PATH": f"{bin_dir}:/usr/bin:/bin",
            "HOME": str(tmp_path),
            "BOUND_PORTS": " ".join(str(p) for p in bound),
            "HEALTHY_PORTS": " ".join(str(p) for p in healthy),
            "OUR_PORTS": " ".join(str(p) for p in ours),
            "UV_EXIT_CODES": uv_exit_codes,
            "STUB_STATE": str(state),
            "CURL_LOG": str(logs["curl"]),
            "UV_LOG": str(logs["uv"]),
            "OPEN_LOG": str(logs["open"]),
        },
        check=False,
    )
    return Run(
        stdout=result.stdout.strip(),
        stderr=result.stderr.strip(),
        returncode=result.returncode,
        curl_argv=logs["curl"].read_text().strip(),
        uv_argv=logs["uv"].read_text().strip(),
        open_argv=logs["open"].read_text().strip(),
    )


def _run(tmp_path: Path, snippet: str, **kwargs: object) -> tuple[str, str]:
    """The port-probe tests only care about stdout and the curl argv."""
    result = _run_raw(tmp_path, snippet, **kwargs)  # type: ignore[arg-type]
    assert result.returncode == 0, result.stderr
    return result.stdout, result.curl_argv


PROBE = 'if port_in_use "$PORT"; then echo IN_USE; else echo FREE; fi'
RESOLVE = 'if decision="$(resolve_target_port)"; then echo "$decision"; else echo NONE; fi'


def test_free_port_reports_free(tmp_path: Path) -> None:
    """Nothing listening — the launcher must start a server."""
    verdict, curl_argv = _run(tmp_path, PROBE)

    assert verdict == "FREE"
    assert curl_argv == "", "curl must not run when the port is not bound"


def test_bound_and_answering_reports_in_use(tmp_path: Path) -> None:
    """A healthy viewer is reused, as it always was."""
    verdict, _ = _run(tmp_path, PROBE, bound=[PORT], healthy=[PORT], ours=[PORT])

    assert verdict == "IN_USE"


def test_bound_but_not_answering_reports_free(tmp_path: Path) -> None:
    """The regression this probe exists for.

    A supervisor whose child died on a broken edit still holds the socket.
    The old check said "already running", opened the browser onto a hang,
    and exited 0.
    """
    verdict, _ = _run(tmp_path, PROBE, bound=[PORT], ours=[PORT])

    assert verdict == "FREE"


def test_the_probe_asks_health_with_a_timeout(tmp_path: Path) -> None:
    """Bounded, and pointed at the endpoint that survives every bind mode.

    ``/health`` is exempt from BearerTokenMiddleware, so this probe works
    on a LAN bind too; an unbounded curl would hang exactly when the
    server is wedged, which is when it is asked.
    """
    _, curl_argv = _run(tmp_path, PROBE, bound=[PORT], healthy=[PORT])

    assert f"http://localhost:{PORT}/health" in curl_argv
    assert "--max-time" in curl_argv
    assert "-fs" in curl_argv


def test_a_foreign_answer_is_not_our_viewer(tmp_path: Path) -> None:
    """A 200 from somebody else's /health must not count as "running".

    8765 is a popular dev-server port and plenty of FastAPI apps expose
    /health. Trusting the status code alone would open the browser on the
    wrong app and exit 0.
    """
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    _stub(bin_dir, "lsof", LSOF_STUB)
    _stub(bin_dir, "curl", 'printf \'{"detail":"not us"}\'\nexit 0')
    _stub(bin_dir, "ps", PS_STUB)

    script = tmp_path / "harness.sh"
    script.write_text(
        "\n".join(
            [
                "#!/usr/bin/env bash",
                "set -euo pipefail",
                f"PORT={PORT}",
                _extract_function("port_bound"),
                _extract_function("port_in_use"),
                PROBE,
            ]
        )
    )
    result = subprocess.run(
        ["bash", str(script)],
        capture_output=True,
        text=True,
        env={
            "PATH": f"{bin_dir}:/usr/bin:/bin",
            "HOME": str(tmp_path),
            "BOUND_PORTS": str(PORT),
            "STUB_STATE": str(tmp_path),
        },
        check=True,
    )

    assert result.stdout.strip() == "FREE"


def test_healthy_viewer_on_the_default_port_is_reused(tmp_path: Path) -> None:
    verdict, _ = _run(tmp_path, RESOLVE, bound=[PORT], healthy=[PORT], ours=[PORT])

    assert verdict == f"reuse {PORT}"


def test_free_default_port_is_started(tmp_path: Path) -> None:
    verdict, _ = _run(tmp_path, RESOLVE)

    assert verdict == f"start {PORT}"


def test_foreign_app_on_the_default_port_steps_aside(tmp_path: Path) -> None:
    """2026-08-02: another project's uvicorn owned 8765 for days.

    The launcher used to hand the port straight to uvicorn, which died on
    ``Address already in use`` after `finances update` had already run.
    Nothing about the viewer needs 8765 specifically.
    """
    verdict, _ = _run(tmp_path, RESOLVE, bound=[PORT])

    assert verdict == f"start {PORT + 1}"


def test_our_wedged_server_is_reported_not_routed_around(tmp_path: Path) -> None:
    """Stepping aside from our OWN dead supervisor would hide it.

    A second viewer next to a zombie holding 8765 means every later launch
    lands somewhere new. Report it and let the owner kill it.
    """
    verdict, _ = _run(tmp_path, RESOLVE, bound=[PORT], ours=[PORT])

    assert verdict == f"wedged {PORT}"


def test_a_viewer_already_on_the_fallback_port_is_reused(tmp_path: Path) -> None:
    """The second double-click while a foreign app still squats 8765.

    Scanning for "the first free port" alone would skip past the viewer
    started by the first double-click and launch another one.
    """
    verdict, _ = _run(
        tmp_path,
        RESOLVE,
        bound=[PORT, PORT + 1],
        healthy=[PORT + 1],
        ours=[PORT + 1],
    )

    assert verdict == f"reuse {PORT + 1}"


def test_no_free_port_in_range_fails(tmp_path: Path) -> None:
    """Give up loudly rather than scan forever."""
    verdict, _ = _run(tmp_path, RESOLVE, bound=range(PORT, PORT + 30))

    assert verdict == "NONE"


def test_holder_pids_are_reported_for_the_wedged_message(tmp_path: Path) -> None:
    """The wedged branch tells the owner what to kill."""
    verdict, _ = _run(
        tmp_path, 'port_holder_pids "$PORT"', bound=[PORT], ours=[PORT]
    )

    assert verdict == str(PORT)


def test_health_body_matches_what_the_launcher_greps(tmp_path: Path) -> None:
    """Contract between the app and the launcher's probe.

    If ``/health`` ever changes shape, the launcher would stop recognising
    its own viewer and start a second one on every launch.
    """
    from fastapi.testclient import TestClient

    from finances.web.app import create_app
    from finances.web.settings import WebSettings

    app = create_app(WebSettings(host="127.0.0.1", db_path=tmp_path / "empty.db"))
    response = TestClient(app).get("/health")

    assert response.status_code == 200
    assert response.text == HEALTH_BODY
    assert f"grep -q '{HEALTH_BODY}'" in LAUNCHER.read_text(), (
        "the launcher must grep for the body this test pins"
    )


# --------------------------------------------------------------------------
# Failure reporting.
#
# The launcher runs in a Terminal window that closes the instant the script
# exits — an error that only prints is an error nobody gets to read. Every
# failure path therefore goes through die(), which holds the window open and
# says WHAT failed, WHY (the evidence it saw) and WHAT TO DO next.
# --------------------------------------------------------------------------


def test_die_reports_what_why_and_the_fix(tmp_path: Path) -> None:
    result = _run_raw(
        tmp_path,
        'die "the thing broke" "Evidence: pid 42 held it" "Fix: kill 42"',
    )

    assert result.returncode == 1
    assert "the thing broke" in result.stderr
    assert "Evidence: pid 42 held it" in result.stderr
    assert "Fix: kill 42" in result.stderr
    # Context the owner would otherwise have to be asked for.
    assert str(tmp_path) in result.stderr
    assert str(PORT) in result.stderr


def test_die_waits_before_the_window_closes(tmp_path: Path) -> None:
    """The prompt is the only thing keeping the diagnosis on screen."""
    assert 'read -r -p "Press Enter to close' in _extract_function("die")


def test_missing_uv_names_the_tool_and_the_path_it_searched(tmp_path: Path) -> None:
    result = _run_raw(tmp_path, "preflight", with_uv=False)

    assert result.returncode == 1
    assert "uv" in result.stderr
    assert "docs.astral.sh/uv" in result.stderr
    assert "PATH" in result.stderr, "say where it looked, not just that it failed"


def test_a_copy_outside_the_repo_says_so(tmp_path: Path) -> None:
    """Copying finances.command to the Desktop is the obvious wrong move.

    Without this the failure surfaces much later as an import error.
    """
    result = _run_raw(tmp_path, "preflight", repo_dir=tmp_path / "Desktop")

    assert result.returncode == 1
    assert "pyproject.toml" in result.stderr
    assert str(tmp_path / "Desktop") in result.stderr
    assert "alias" in result.stderr.lower(), "point at the fix, not just the fault"


def test_preflight_passes_in_the_real_repo(tmp_path: Path) -> None:
    result = _run_raw(tmp_path, "preflight", repo_dir=REPO_ROOT)

    assert result.returncode == 0, result.stderr


# --------------------------------------------------------------------------
# Clearing our own wedged server.
# --------------------------------------------------------------------------


def test_wedged_port_is_cleared_after_the_owner_agrees(tmp_path: Path) -> None:
    result = _run_raw(
        tmp_path,
        "\n".join(
            [
                # Stand in for `kill`, which is a builtin and cannot be
                # stubbed on PATH: record it, and free the port.
                'terminate_pids() { echo "KILLED $*"; touch '
                f'"$STUB_STATE/unbound.{PORT}"; }}',
                'clear_wedged_port "$PORT"',
                "echo CLEARED",
            ]
        ),
        bound=[PORT],
        ours=[PORT],
        stdin="y\n",
    )

    assert result.returncode == 0, result.stderr
    assert f"KILLED {PORT}" in result.stdout
    assert "CLEARED" in result.stdout


def test_declining_the_kill_explains_how_to_do_it_by_hand(tmp_path: Path) -> None:
    result = _run_raw(
        tmp_path,
        'clear_wedged_port "$PORT"',
        bound=[PORT],
        ours=[PORT],
        stdin="n\n",
    )

    assert result.returncode == 1
    assert f"kill {PORT}" in result.stderr, "the pid to kill, spelled out"
    assert "FINANCES_PORT" in result.stderr


def test_a_kill_that_does_not_free_the_port_is_reported(tmp_path: Path) -> None:
    """SIGTERM ignored — say so and give the harder command."""
    result = _run_raw(
        tmp_path,
        'terminate_pids() { :; }\nclear_wedged_port "$PORT"',
        bound=[PORT],
        ours=[PORT],
        stdin="y\n",
    )

    assert result.returncode == 1
    assert f"kill -9 {PORT}" in result.stderr


# --------------------------------------------------------------------------
# Opening the browser, and starting the server.
# --------------------------------------------------------------------------


def test_browser_opens_only_once_the_viewer_answers(tmp_path: Path) -> None:
    """`sleep 2; open` used to fire blind.

    On 2026-08-02 that meant the browser landed on another project's app
    while our server was still failing to bind.
    """
    result = _run_raw(
        tmp_path, "open_when_ready", bound=[PORT], healthy=[PORT], ours=[PORT]
    )

    assert result.returncode == 0, result.stderr
    assert f"http://localhost:{PORT}/" in result.open_argv


def test_a_viewer_that_never_answers_is_reported_not_opened(tmp_path: Path) -> None:
    result = _run_raw(tmp_path, "open_when_ready || true")

    assert result.open_argv == "", "no browser onto a server that is not there"
    assert "did not answer" in result.stderr
    assert "server output above" in result.stderr


def test_a_port_stolen_during_startup_is_retried_elsewhere(tmp_path: Path) -> None:
    """The gap between the port check and uvicorn's bind is a real race.

    Losing the race is exactly how 8765 was lost, so retry on a fresh port
    rather than dying — the owner double-clicked to see the viewer, not to
    referee a port dispute.
    """
    result = _run_raw(
        tmp_path,
        "launch_server",
        bound=[PORT],  # a foreign app owns it by the time we bind
        uv_exit_codes="1 0",
    )

    assert result.returncode == 0, result.stderr
    attempts = [line for line in result.uv_argv.splitlines() if "serve" in line]
    assert attempts == [
        f"run finances serve --port {PORT}",
        f"run finances serve --port {PORT + 1}",
    ]
    assert "retrying" in result.stdout + result.stderr


def test_a_server_that_dies_for_another_reason_explains_itself(
    tmp_path: Path,
) -> None:
    result = _run_raw(tmp_path, "launch_server", uv_exit_codes="3")

    assert result.returncode == 1
    assert "status 3" in result.stderr
    assert f"finances serve --port {PORT}" in result.stderr, "the exact command"
    assert "finances.db.migrate" in result.stderr, "the usual suspects, spelled out"
    assert "uv pip install -e ." in result.stderr
    assert len(
        [line for line in result.uv_argv.splitlines() if "serve" in line]
    ) == 1, "a broken server must fail fast, not burn every attempt"


def test_ctrl_c_is_not_an_error(tmp_path: Path) -> None:
    """SIGINT is how the owner is told to stop the viewer."""
    result = _run_raw(tmp_path, "launch_server", uv_exit_codes="130")

    assert result.returncode == 0
    assert "stopped" not in result.stderr.lower()


def test_launcher_is_syntactically_valid() -> None:
    subprocess.run(["bash", "-n", str(LAUNCHER)], check=True)


@pytest.mark.skipif(
    shutil.which("shellcheck") is None, reason="shellcheck not installed"
)
def test_launcher_passes_shellcheck() -> None:  # pragma: no cover - optional
    subprocess.run(["shellcheck", str(LAUNCHER)], check=True)
