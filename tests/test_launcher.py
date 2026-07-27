"""Coverage for ``finances.command`` — the double-click entry point.

This file is the ONE thing the owner launches, and until now nothing
tested it. That was tolerable while its port check was a single `lsof`
call. It stopped being tolerable under the reload supervisor (ADR-012
Amendment 2026-07-26), where the process holding port 8765 is the
supervisor and can outlive a child that died on a broken edit: a bound
port no longer means a working viewer, so the launcher must ask.

Bash is tested the way bash is testable — the function under test is
extracted from the real file and sourced into a subshell whose PATH holds
stubbed ``lsof`` and ``curl``. Extraction fails loudly if the function is
renamed or reshaped, which is the failure mode worth having: a launcher
test that silently stops testing the launcher is worse than none.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
LAUNCHER = REPO_ROOT / "finances.command"

PORT = "8765"
URL = f"http://localhost:{PORT}/"


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


def _run_port_in_use(
    tmp_path: Path, *, lsof_rc: int, curl_rc: int
) -> tuple[str, str]:
    """Run the real ``port_in_use`` against stubbed lsof/curl.

    Returns ``(verdict, recorded curl argv)``.
    """
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    recorded = tmp_path / "curl-argv"

    _stub(bin_dir, "lsof", f"exit {lsof_rc}")
    _stub(bin_dir, "curl", f'printf "%s\\n" "$*" > {recorded}\nexit {curl_rc}')

    script = tmp_path / "harness.sh"
    script.write_text(
        "\n".join(
            [
                "#!/usr/bin/env bash",
                # Same shell options the real launcher runs under, so an
                # errexit interaction would show up here too.
                "set -euo pipefail",
                f'PORT="{PORT}"',
                f'URL="{URL}"',
                _extract_function("port_in_use"),
                "if port_in_use; then echo IN_USE; else echo FREE; fi",
            ]
        )
    )

    result = subprocess.run(
        ["bash", str(script)],
        capture_output=True,
        text=True,
        env={"PATH": f"{bin_dir}:/usr/bin:/bin", "HOME": str(tmp_path)},
        check=True,
    )
    argv = recorded.read_text().strip() if recorded.exists() else ""
    return result.stdout.strip(), argv


def test_free_port_reports_free(tmp_path: Path) -> None:
    """Nothing listening — the launcher must start a server."""
    verdict, curl_argv = _run_port_in_use(tmp_path, lsof_rc=1, curl_rc=0)

    assert verdict == "FREE"
    assert curl_argv == "", "curl must not run when the port is not bound"


def test_bound_and_answering_reports_in_use(tmp_path: Path) -> None:
    """A healthy viewer is reused, as it always was."""
    verdict, _ = _run_port_in_use(tmp_path, lsof_rc=0, curl_rc=0)

    assert verdict == "IN_USE"


def test_bound_but_not_answering_reports_free(tmp_path: Path) -> None:
    """The regression this probe exists for.

    A supervisor whose child died on a broken edit still holds the socket.
    The old check said "already running", opened the browser onto a hang,
    and exited 0. Reporting FREE sends the launcher down its start path,
    which fails loudly on "Address already in use" — a visible error beats
    a silent hang.
    """
    verdict, _ = _run_port_in_use(tmp_path, lsof_rc=0, curl_rc=7)

    assert verdict == "FREE"


def test_the_probe_asks_health_with_a_timeout(tmp_path: Path) -> None:
    """Bounded, and pointed at the endpoint that survives every bind mode.

    ``/health`` is exempt from BearerTokenMiddleware, so this probe works
    on a LAN bind too; an unbounded curl would hang exactly when the
    server is wedged, which is when it is asked.
    """
    _, curl_argv = _run_port_in_use(tmp_path, lsof_rc=0, curl_rc=0)

    assert f"{URL}health" in curl_argv
    assert "--max-time" in curl_argv
    assert "-fs" in curl_argv


def test_launcher_is_syntactically_valid() -> None:
    subprocess.run(["bash", "-n", str(LAUNCHER)], check=True)


@pytest.mark.skipif(
    shutil.which("shellcheck") is None, reason="shellcheck not installed"
)
def test_launcher_passes_shellcheck() -> None:  # pragma: no cover - optional
    subprocess.run(["shellcheck", str(LAUNCHER)], check=True)
