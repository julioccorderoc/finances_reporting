"""Phase 1 foundation tests for the local web viewer (EPIC-022).

Per rule-011 (TDD discipline) these tests are committed before the
implementation under ``finances/web/`` lands. They cover the foundation
contract from docs/plans/web-viewer-v1.md Phase 1:

* the FastAPI app boots from ``WebSettings`` defaults,
* vendored static assets are served,
* the bearer-token middleware is a no-op on ``127.0.0.1``,
* ``WebSettings`` rejects a LAN bind without a token,
* LAN binds reject unauthenticated traffic and accept bearer tokens,
* the placeholder dashboard renders on Phase 1,
* the ``finances serve`` Typer command refuses ``--host 0.0.0.0``
  without a token.
"""

from __future__ import annotations

from typer.testing import CliRunner

from fastapi import FastAPI
from fastapi.testclient import TestClient


# ---------------------------------------------------------------------------
# App boot / health.
# ---------------------------------------------------------------------------


def test_app_boots_with_default_settings() -> None:
    from finances.web.app import create_app
    from finances.web.settings import WebSettings

    app = create_app(WebSettings())
    assert isinstance(app, FastAPI)

    client = TestClient(app)
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


# ---------------------------------------------------------------------------
# Static assets.
# ---------------------------------------------------------------------------


def test_static_vendor_files_served() -> None:
    from finances.web.app import create_app
    from finances.web.settings import WebSettings

    app = create_app(WebSettings())
    client = TestClient(app)

    resp = client.get("/static/vendor/htmx.min.js")
    assert resp.status_code == 200
    ctype = resp.headers.get("content-type", "")
    assert ctype.startswith("application/javascript") or ctype.startswith(
        "text/javascript"
    ), f"unexpected content-type: {ctype!r}"
    assert len(resp.content) > 0


# ---------------------------------------------------------------------------
# Auth — localhost is no-auth.
# ---------------------------------------------------------------------------


def test_localhost_no_auth_required() -> None:
    from finances.web.app import create_app
    from finances.web.settings import WebSettings

    app = create_app(WebSettings(host="127.0.0.1", token=None))
    client = TestClient(app)

    resp = client.get("/health")
    assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Auth — settings-level guard for LAN binds.
# ---------------------------------------------------------------------------


def test_lan_bind_requires_token_at_settings_construction() -> None:
    import pytest

    from finances.web.settings import WebSettings

    with pytest.raises(ValueError, match="LAN bind requires --token or FINANCES_WEB_TOKEN"):
        WebSettings(host="0.0.0.0", token=None)


# ---------------------------------------------------------------------------
# Auth — middleware behaviour on a LAN bind.
# ---------------------------------------------------------------------------


def test_lan_bind_rejects_unauthenticated_request() -> None:
    from finances.web.app import create_app
    from finances.web.settings import WebSettings

    app = create_app(WebSettings(host="0.0.0.0", token="secret"))
    client = TestClient(app)

    resp = client.get("/", follow_redirects=False)
    assert resp.status_code == 401


def _tmp_migrated_db_path(tmp_path) -> "object":
    """Create a tmp sqlite DB with migrations applied; return its path.

    Phase 2a wired the / route to query the DB, so middleware tests that
    hit / now need a real database path.
    """
    import sqlite3

    from finances.db.connection import _register_decimal_adapters
    from finances.db.migrate import apply_migrations

    db_path = tmp_path / "web.db"
    _register_decimal_adapters()
    conn = sqlite3.connect(
        str(db_path),
        detect_types=sqlite3.PARSE_DECLTYPES,
        isolation_level=None,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    apply_migrations(conn)
    conn.close()
    return db_path


def test_lan_bind_accepts_bearer_token(tmp_path) -> None:
    from finances.web.app import create_app
    from finances.web.settings import WebSettings

    db_path = _tmp_migrated_db_path(tmp_path)
    app = create_app(WebSettings(host="0.0.0.0", token="secret", db_path=db_path))
    client = TestClient(app)

    resp = client.get("/", headers={"Authorization": "Bearer secret"})
    assert resp.status_code == 200


def test_lan_bind_accepts_token_query_string_and_sets_cookie(tmp_path) -> None:
    from finances.web.app import create_app
    from finances.web.settings import WebSettings

    db_path = _tmp_migrated_db_path(tmp_path)
    app = create_app(WebSettings(host="0.0.0.0", token="secret", db_path=db_path))
    client = TestClient(app)

    # Follow redirects so the implementation can choose either pattern:
    # set-cookie + 200, or set-cookie + 302 -> stripped query.
    resp = client.get("/", params={"token": "secret"}, follow_redirects=True)
    assert resp.status_code == 200
    # The session cookie should now be present on the client jar.
    assert client.cookies.get("finances_web_token") == "secret"


def test_lan_bind_serves_static_without_auth() -> None:
    from finances.web.app import create_app
    from finances.web.settings import WebSettings

    app = create_app(WebSettings(host="0.0.0.0", token="secret"))
    client = TestClient(app)

    resp = client.get("/static/vendor/htmx.min.js")
    assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Dashboard route — Phase 1 placeholder replaced by Phase 2a real handler.
# ---------------------------------------------------------------------------


def test_dashboard_route_renders(tmp_path) -> None:
    """Phase 2a replaces the Phase 1 placeholder with the real dashboard.

    We assert the route boots and contains a stable dashboard marker
    rather than the placeholder copy that no longer ships.
    """
    from finances.web.app import create_app
    from finances.web.settings import WebSettings

    db_path = _tmp_migrated_db_path(tmp_path)
    app = create_app(WebSettings(host="127.0.0.1", db_path=db_path))
    client = TestClient(app)

    resp = client.get("/")
    assert resp.status_code == 200
    # Phase 2a contract: the dashboard exposes the four KPI labels.
    assert "Net worth" in resp.text
    assert "Recent activity" in resp.text


# ---------------------------------------------------------------------------
# CLI guard.
# ---------------------------------------------------------------------------


def test_serve_cli_command_validates_lan_token() -> None:
    from finances.cli.main import app as cli_app

    runner = CliRunner()
    result = runner.invoke(
        cli_app,
        ["serve", "--host", "0.0.0.0", "--port", "18765"],
        env={"FINANCES_WEB_TOKEN": ""},
    )
    assert result.exit_code != 0
    combined = result.output or ""
    assert "token" in combined.lower()
