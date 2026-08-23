"""The only test that proves the whole process tree, end to end.

Everything else here asserts a piece: the env round-trip, the watch set,
the call shape, the ppid signalling. None of them can catch the thing that
would actually hurt — a supervisor that watches the wrong files, drops the
socket between restarts, or cannot be stopped.

So this one really runs ``finances serve``, really touches a watched
template, and really checks that the boot id changed while the socket kept
answering. The socket-survives assertion is the load-bearing one: it is
what makes a restart cost the owner a sub-second pause rather than a
failed request in the middle of triage.

Marked ``integration`` — it spawns processes and waits on real timing, so
it stays out of the default unit run (rule-011).
"""

from __future__ import annotations

import os
import socket
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

REPO_ROOT = Path(__file__).resolve().parents[2]
# Touched to trigger a restart. Nothing fetches it during this test (the
# parked sheet is only rendered when it opens), and a bare touch changes no
# content, so the working tree stays clean.
#
# It must point at a template that EXISTS: ``Path.touch()`` creates a
# missing file, and this test quietly resurrected ``triage_empty.html`` as
# an empty orphan on every run for a while after the redesign deleted it.
WATCHED_TEMPLATE = (
    REPO_ROOT
    / "finances"
    / "web"
    / "templates"
    / "partials"
    / "triage_sheet_parked.html"
)
assert WATCHED_TEMPLATE.exists(), WATCHED_TEMPLATE


def _fingerprint(path: Path) -> tuple[bool, int, int]:
    """(exists, size, mtime_ns) — enough to catch a WAL header rewrite."""
    try:
        stat = path.stat()
    except FileNotFoundError:
        return (False, 0, 0)
    return (True, stat.st_size, stat.st_mtime_ns)


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _health(port: int, timeout: float = 2.0) -> tuple[int, str] | None:
    try:
        with urllib.request.urlopen(
            f"http://127.0.0.1:{port}/health", timeout=timeout
        ) as resp:
            return resp.status, resp.headers.get("X-Finances-Boot", "")
    except (urllib.error.URLError, TimeoutError, ConnectionError, OSError):
        return None


def _await(predicate, seconds: float = 25.0, step: float = 0.2):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        result = predicate()
        if result:
            return result
        time.sleep(step)
    return None


def _touch_and_wait(
    port: int, boot_before: str, seconds: float = 4.0
) -> tuple[int, str] | None:
    """Touch the watched template, then watch for a new boot id."""
    WATCHED_TEMPLATE.touch()
    return _await(
        lambda: (
            result
            if (result := _health(port)) and result[1] != boot_before
            else None
        ),
        seconds=seconds,
    )


def test_watch_restart_socket_survives_then_stop(tmp_path: Path) -> None:
    port = _free_port()
    # --db-path, and a path that is DELIBERATELY not migrated.
    #
    # This test spawns the real process tree, so nothing the suite patches
    # applies to it: for a long time it opened config.DB_PATH — the owner's
    # actual ledger — which bumped its mtime on every run and materialised a
    # 4096-byte stub in a fresh worktree. tests/conftest.py fails the session
    # on that now, and this is the flag that gives it somewhere else to go.
    #
    # Leaving the file unmigrated matters: on the way out the supervisor runs
    # _regen_report_on_shutdown, which writes config.REPORT_HTML_PATH — the
    # repo's real report.html — no matter which DB it read. Against an empty
    # file the export raises "no such table: accounts", the warn-only handler
    # swallows it, and report.html is left alone. The startup refresh fails
    # the same way, which also keeps this test off the network.
    scratch_db = tmp_path / "serve-smoke.db"
    ledger_before = _fingerprint(REPO_ROOT / "finances.db")
    report_before = _fingerprint(REPO_ROOT / "report.html")
    proc = subprocess.Popen(
        [
            "uv",
            "run",
            "finances",
            "serve",
            "--port",
            str(port),
            "--no-open",
            "--db-path",
            str(scratch_db),
        ],
        cwd=REPO_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    try:
        first = _await(lambda: _health(port))
        assert first is not None, "server never came up"
        status, boot_before = first
        assert status == 200
        assert boot_before

        # The touch is retried, not issued once. macOS delivers file events
        # through an FSEvents stream that is not live the instant the socket
        # starts answering, so a change made in that window is dropped
        # outright — reproduced: the first touch is silently lost and the
        # second lands. The claim under test is "a watched edit restarts the
        # server", not "the very first event after boot is delivered".
        after = _await(
            lambda: _touch_and_wait(port, boot_before), seconds=30, step=0.0
        )
        assert after is not None, (
            "touching a watched template did not restart the server"
        )
        _, boot_after = after

        # A new boot id proves the child was replaced.
        assert boot_after != boot_before

        # And this proves the socket holder was NOT: _health only ever
        # returns on success, so asserting 200 here would be
        # non-falsifiable — the poll simply waits until something answers.
        # The real continuity claim is that the supervisor, which owns the
        # listening socket, never exited across the swap. If a future
        # refactor re-execs the whole server instead of respawning a child,
        # the listener closes, in-flight requests are refused, and this is
        # what notices.
        assert proc.poll() is None, (
            "the process holding the listening socket exited during the "
            "restart — a request in flight would have been refused"
        )

        urllib.request.urlopen(
            urllib.request.Request(
                f"http://127.0.0.1:{port}/shutdown", method="POST"
            ),
            timeout=5,
        ).read()

        # Stopping must take down the SUPERVISOR, not just its child —
        # otherwise the port stays bound and the next edit respawns it.
        assert _await(lambda: proc.poll() is not None, seconds=20) is not None, (
            "the supervisor outlived POST /shutdown"
        )
        assert _health(port) is None, "the port is still answering after stop"

        # The whole tree ran against the scratch file, start to finish.
        assert _fingerprint(REPO_ROOT / "finances.db") == ledger_before, (
            "the spawned server opened the real ledger"
        )
        assert _fingerprint(REPO_ROOT / "report.html") == report_before, (
            "the shutdown regen rewrote the repo's report.html from the "
            "scratch DB"
        )
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=10)
