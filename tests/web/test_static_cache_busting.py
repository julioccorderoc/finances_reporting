"""A stylesheet edit must reach the browser without a hard refresh.

The three-row filter panel (2026-09-07) was never a layout bug: the server
had shipped ``repeat(5, …)`` for ``.flow-filter-groups`` hours earlier, and
the browser was still painting the ``repeat(4, …)`` it had cached — five
dropdowns wrapping to a second line under the first row of fields.

Nothing in the suite could see it. Every server-side test reads the CSS off
disk, so disk and assertion agreed while the only surface that mattered
disagreed with both. The fix is a version stamp on every asset URL, keyed to
the file's **mtime** — deliberately not to ``app.state.boot_id``, because
``RELOAD_EXCLUDES`` keeps static/ out of the watcher and a stylesheet edit
therefore does not respawn the child. A per-process stamp would have been as
stale as no stamp; mtime moves when the bytes move, and only then.

These tests own the two halves: base.html routes every asset through the
``asset()`` global, and what it renders tracks the file on disk.
"""

from __future__ import annotations

import os
import re
import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[2]
BASE_HTML = ROOT / "finances" / "web" / "templates" / "base.html"

#: An href/src pointing straight at /static — the shape that caches forever.
BARE_STATIC = re.compile(r'(?:href|src)="/static/')


def test_base_html_stamps_every_asset_url() -> None:
    """No bare /static href survives in the head."""
    head = BASE_HTML.read_text(encoding="utf-8")

    offenders = BARE_STATIC.findall(head)

    assert not offenders, (
        f"base.html links {len(offenders)} asset(s) without a version stamp; "
        "route them through asset('css/x.css') so an edit is not cached away"
    )


def test_base_html_still_links_every_sheet_it_did() -> None:
    """The stamp is a rewrite of the URLs, not a chance to drop one."""
    head = BASE_HTML.read_text(encoding="utf-8")

    for path in (
        "favicon.svg",
        "css/tailwind.css",
        "css/app.css",
        "css/fonts.css",
        "css/signal.css",
        "css/shell.css",
        "css/triage.css",
        "css/today.css",
        "css/flow.css",
        "css/reports.css",
        "css/placeholders.css",
        "js/triage.js",
        "vendor/htmx.min.js",
        "vendor/alpine.min.js",
        "vendor/chart.umd.min.js",
    ):
        assert f"asset('{path}')" in head, f"base.html no longer links {path}"


def test_every_rendered_asset_url_carries_its_files_mtime(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    """Every asset URL on a real page is stamped with the file it serves."""
    client: TestClient = web_client_factory()
    html = client.get("/transactions").text

    urls = re.findall(r'(?:href|src)="/static/([^"?]+)\?v=([^"]+)"', html)
    assert urls, "the page linked no stamped static asset at all"

    static = ROOT / "finances" / "web" / "static"
    wrong = [
        (rel, stamp)
        for rel, stamp in urls
        if stamp != str(int((static / rel).stat().st_mtime))
    ]
    assert not wrong, f"assets stamped with something other than their mtime: {wrong}"

    unstamped = re.findall(r'(?:href|src)="(/static/[^"?]+)"', html)
    assert not unstamped, f"assets served with no version at all: {unstamped}"


def test_the_stamped_url_still_resolves(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    """A query string must not turn a served file into a 404."""
    client: TestClient = web_client_factory()
    html = client.get("/transactions").text
    flow_css = re.search(r'href="(/static/css/flow\.css\?v=[0-9]+)"', html)
    assert flow_css, "flow.css is no longer linked from the page"

    resp = client.get(flow_css.group(1))

    assert resp.status_code == 200
    assert "flow-filter-groups" in resp.text


def test_editing_a_stylesheet_changes_its_url(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The whole point: new bytes on disk, new URL, no restart required.

    static/ is in RELOAD_EXCLUDES, so this is the case a boot-id stamp
    would have got wrong — the child that served the stale sheet is still
    the child serving the fixed one.
    """
    from finances.web import app as app_module

    sheet = tmp_path / "css" / "flow.css"
    sheet.parent.mkdir()
    sheet.write_text(".flow-filter-groups { grid-template-columns: repeat(4, 1fr); }")
    monkeypatch.setattr(app_module, "STATIC_DIR", tmp_path)
    asset = app_module._asset_url("bootid")

    before = asset("css/flow.css")
    os.utime(sheet, (0, 0))  # an edit, two seconds of wall clock ago or ten
    after = asset("css/flow.css")

    assert before != after, (
        "a stylesheet edit did not change its URL — the browser would go on "
        "painting the version it already holds"
    )


def test_an_asset_that_does_not_exist_still_renders(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A typo'd asset name is a dead link, never a 500 mid-page."""
    from finances.web import app as app_module

    monkeypatch.setattr(app_module, "STATIC_DIR", tmp_path)

    url = app_module._asset_url("bootid")("css/nope.css")

    assert url == "/static/css/nope.css?v=bootid"
