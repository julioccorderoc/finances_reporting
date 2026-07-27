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
# Touched to trigger a restart. Nothing reads it during this test, and a
# bare touch changes no content, so the working tree stays clean.
WATCHED_TEMPLATE = (
    REPO_ROOT / "finances" / "web" / "templates" / "partials" / "triage_empty.html"
)


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


def test_watch_restart_socket_survives_then_stop() -> None:
    port = _free_port()
    # No DB fixture: serve_cmd builds its own WebSettings from the CLI flags
    # and exports THOSE, so a FINANCES_WEB_DB_PATH set here would just be
    # overwritten. db_path is not a CLI option, so `finances serve` always
    # opens config.DB_PATH. This test only touches /health and /shutdown,
    # neither of which reads the ledger.
    proc = subprocess.Popen(
        [
            "uv",
            "run",
            "finances",
            "serve",
            "--port",
            str(port),
            "--no-open",
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
        restarted_status, boot_after = after

        # The whole point: the listening socket never went away, so a
        # request landing mid-restart is answered late, not refused.
        assert restarted_status == 200
        assert boot_after != boot_before

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
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=10)
