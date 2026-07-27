"""What the reload supervisor watches — asserted through uvicorn's own filter.

These tests build the REAL ``FileFilter`` uvicorn uses at runtime rather
than re-implementing its glob semantics, because the interesting cases all
live in its details: ``*.py`` is an implicit include, ``reload_excludes``
entries that name a directory become ``exclude_dirs``, and dotfiles are
excluded after the include matches, not before.

The last test in this file is the one that matters most. The watch set is
``finances/`` and NOT the repo root, and it can only include ``*.html``
because ``report.html`` and ``finances.db`` happen to live at the repo
root, outside it. If either ever moves under the package, the shutdown
regen writes a watched file, which restarts the server, which regenerates
on teardown — a restart loop with no way out but SIGKILL. That guarantee
is positional, so it is pinned here.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from uvicorn.config import Config
from uvicorn.supervisors.watchfilesreload import FileFilter

from finances import config as finances_config
from finances.web.app import (
    IMPORT_STRING,
    PACKAGE_DIR,
    RELOAD_DIRS,
    RELOAD_EXCLUDES,
    RELOAD_INCLUDES,
)


@pytest.fixture
def watch_filter() -> FileFilter:
    return FileFilter(
        Config(
            IMPORT_STRING,
            factory=True,
            reload=True,
            reload_dirs=list(RELOAD_DIRS),
            reload_includes=list(RELOAD_INCLUDES),
            reload_excludes=list(RELOAD_EXCLUDES),
        )
    )


def test_the_two_files_from_the_incident_trigger_a_restart(
    watch_filter: FileFilter,
) -> None:
    """Regression for 2026-07-26.

    A router and its template were edited together; the running server
    picked up only the template. Both halves must restart the process, so
    the pair can never be split again. The template half is also what
    proves uvicorn's default ``["*.py"]`` include was actually widened.
    """
    package = Path(PACKAGE_DIR)

    assert watch_filter(package / "web" / "routers" / "partials.py")
    assert watch_filter(
        package
        / "web"
        / "templates"
        / "partials"
        / "modal_transaction_triage.html"
    )


def test_static_assets_do_not_restart(watch_filter: FileFilter) -> None:
    """tailwind.css is rebuilt after template class changes (MEMORY.md).

    StaticFiles reads from disk per request, so static assets are never
    stale — bouncing a live triage session for a CSS rebuild would be a
    restart with no reader.
    """
    static = Path(PACKAGE_DIR) / "web" / "static"

    assert not watch_filter(static / "css" / "tailwind.css")
    assert not watch_filter(static / "vendor" / "htmx.min.js")


def test_editor_and_bytecode_noise_does_not_restart(
    watch_filter: FileFilter,
) -> None:
    package = Path(PACKAGE_DIR)

    assert not watch_filter(package / "web" / "templates" / ".base.html")
    assert not watch_filter(package / "web" / "__pycache__" / "app.pyc")


def test_generated_files_lie_outside_every_watch_dir() -> None:
    """The infinite-restart guard. Do not relax this test.

    ``report.html`` matches the ``*.html`` include; it is safe only
    because it is written outside the watch set.
    """
    watched = [Path(directory).resolve() for directory in RELOAD_DIRS]

    for directory in watched:
        assert directory not in finances_config.REPORT_HTML_PATH.resolve().parents
        assert directory not in finances_config.DB_PATH.resolve().parents


def test_the_watch_set_is_the_package_not_the_repo_root() -> None:
    assert RELOAD_DIRS == (str(PACKAGE_DIR),)
    assert Path(PACKAGE_DIR).name == "finances"
