"""A stylesheet edit must reach the browser without a hard refresh.

The three-row filter panel (2026-09-07) was never a layout bug: the server
had shipped ``repeat(5, …)`` for ``.flow-filter-groups`` hours earlier, and
the browser was still painting the ``repeat(4, …)`` it had cached — five
dropdowns wrapping to a second line under the first row of fields.

Nothing in the suite could see it. Every server-side test reads the CSS off
disk, so disk and assertion agreed while the only surface that mattered
disagreed with both. The fix is a version stamp on every asset URL, keyed to
``app.state.boot_id`` — the identity of the process that rendered the page,
already minted for the restart banner. A restart is exactly when the CSS on
disk changes (watchfiles respawns the child on every edit), so a new boot_id
is a new URL is a fresh fetch.

These tests own the two halves: base.html routes every asset through the
``asset()`` global, and what it renders carries this process's stamp.
"""

from __future__ import annotations

import re
import sqlite3
from pathlib import Path

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


def test_rendered_page_stamps_assets_with_this_process_boot_id(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    """Every asset URL on a real page carries the rendering process's id."""
    client: TestClient = web_client_factory()
    html = client.get("/transactions").text

    boot_id = re.search(r'name="finances-boot" content="([0-9a-f]+)"', html)
    assert boot_id, "the page no longer declares its boot id"
    stamp = f"?v={boot_id.group(1)}"

    urls = re.findall(r'(?:href|src)="(/static/[^"]+)"', html)
    assert urls, "the page linked no static asset at all"

    unstamped = [u for u in urls if not u.endswith(stamp)]
    assert not unstamped, f"assets served without this boot's stamp: {unstamped}"


def test_the_stamped_url_still_resolves(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    """A query string must not turn a served file into a 404."""
    client: TestClient = web_client_factory()
    html = client.get("/transactions").text
    flow_css = re.search(r'href="(/static/css/flow\.css\?v=[0-9a-f]+)"', html)
    assert flow_css, "flow.css is no longer linked from the page"

    resp = client.get(flow_css.group(1))

    assert resp.status_code == 200
    assert "flow-filter-groups" in resp.text


def test_two_processes_stamp_differently(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    """The point of the stamp: a restart invalidates what the browser holds.

    Building a second app stands in for the respawn that watchfiles performs
    on every edit.
    """
    pattern = r'href="/static/css/flow\.css\?v=([0-9a-f]+)"'
    one: TestClient = web_client_factory()
    two: TestClient = web_client_factory()

    first = re.findall(pattern, one.get("/transactions").text)
    second = re.findall(pattern, two.get("/transactions").text)

    assert first and second
    assert first[0] != second[0], (
        "two processes stamped the same version — a restart would not "
        "invalidate the browser's cached stylesheet"
    )
