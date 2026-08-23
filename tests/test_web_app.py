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

import os
from types import SimpleNamespace

import pytest
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


def _seed_db_with_txn(tmp_path) -> "object":
    """Migrated tmp DB holding one transaction so the export has data."""
    from datetime import UTC, datetime
    from decimal import Decimal

    from finances.db.connection import get_connection
    from finances.db.migrate import apply_migrations
    from finances.db.repos import accounts as accounts_repo
    from finances.db.repos import transactions as transactions_repo
    from finances.domain.models import (
        Account,
        AccountKind,
        Transaction,
        TransactionKind,
    )

    db_path = tmp_path / "shutdown.db"
    conn = get_connection(db_path)
    apply_migrations(conn)
    try:
        acct = accounts_repo.insert(
            conn, Account(name="Cash USD", kind=AccountKind.CASH, currency="USD")
        )
        transactions_repo.insert(
            conn,
            Transaction(
                account_id=acct.id,
                occurred_at=datetime.now(tz=UTC),
                kind=TransactionKind.EXPENSE,
                amount=Decimal("4.20"),
                currency="USD",
                description="shutdown seed",
                source="cash_cli",
                source_ref="shutdown-seed-1",
            ),
        )
    finally:
        conn.close()
    return db_path


def test_server_shutdown_regenerates_report(tmp_path, monkeypatch) -> None:
    """On server shutdown the static report.html is regenerated (Thing 5B).

    The .command launcher relies on this so the double-clickable static file
    reflects any edits made during the serve session.
    """
    from finances import config
    from finances.web.app import create_app
    from finances.web.settings import WebSettings

    db_path = _seed_db_with_txn(tmp_path)
    report = tmp_path / "report.html"
    monkeypatch.setattr(config, "REPORT_HTML_PATH", report)

    app = create_app(
        WebSettings(
            host="127.0.0.1",
            db_path=db_path,
            regen_report_on_shutdown=True,
        )
    )
    assert not report.exists()

    # Entering/exiting the TestClient context fires FastAPI startup/shutdown.
    with TestClient(app) as client:
        assert client.get("/health").status_code == 200
        assert not report.exists()  # nothing written mid-session

    # Only after the context exits (shutdown) does the report appear.
    assert report.exists() and report.stat().st_size > 0


def test_the_shutdown_regen_is_opt_in(tmp_path, monkeypatch) -> None:
    """A settings object built without thinking must not write the repo.

    Production always says which it wants — ``serve_cmd`` sets the flag on
    both paths, and the reload child rebuilds it from a required env key.
    Tests do not, and while the default was ``True`` every
    ``with TestClient(create_app(...))`` in this suite rewrote the owner's
    real ``report.html`` with fixture rows on the way out.
    """
    from finances import config
    from finances.web.app import create_app
    from finances.web.settings import WebSettings

    db_path = _seed_db_with_txn(tmp_path)
    report = tmp_path / "report.html"
    monkeypatch.setattr(config, "REPORT_HTML_PATH", report)

    assert WebSettings().regen_report_on_shutdown is False

    app = create_app(WebSettings(host="127.0.0.1", db_path=db_path))
    with TestClient(app) as client:
        assert client.get("/health").status_code == 200

    assert not report.exists()


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


# ---------------------------------------------------------------------------
# serve_cmd under the reload supervisor (ADR-012 Amendment 2026-07-26).
#
# The reloader cannot be handed an app object — it re-imports the app in a
# spawn()'ed child — so serve_cmd's call shape changes, and with it the way
# every setting reaches the server. These tests pin that shape, because a
# regression here is invisible until it is a wrong ledger or an open socket.
# ---------------------------------------------------------------------------


@pytest.fixture
def uvicorn_recorder(monkeypatch):
    """Capture the uvicorn.run call and the environment it was made in.

    Also neutralises the shutdown regen: under reload the CLI owns it, and
    an unpatched run would rewrite the repo's real report.html from the
    repo's real ledger during the test suite.
    """
    import uvicorn

    from finances.web import app as web_app

    calls: list[dict] = []
    regen_calls: list[object] = []

    def _fake_run(*args, **kwargs) -> None:
        calls.append(
            {"args": args, "kwargs": kwargs, "environ": dict(os.environ)}
        )

    monkeypatch.setattr(uvicorn, "run", _fake_run)
    monkeypatch.setattr(
        web_app,
        "_regen_report_on_shutdown",
        lambda settings: regen_calls.append(settings),
    )
    monkeypatch.setattr(os, "environ", dict(os.environ))

    return SimpleNamespace(calls=calls, regen_calls=regen_calls)


def _invoke_serve(*argv: str):
    from finances.cli.main import app as cli_app

    return CliRunner().invoke(cli_app, ["serve", *argv])


def test_serve_defaults_to_an_import_string_and_factory(
    uvicorn_recorder,
) -> None:
    from finances.web.app import (
        IMPORT_STRING,
        RELOAD_DIRS,
        RELOAD_EXCLUDES,
        RELOAD_INCLUDES,
    )

    result = _invoke_serve("--port", "18765")
    assert result.exit_code == 0, result.output

    (call,) = uvicorn_recorder.calls
    assert call["args"][0] == IMPORT_STRING
    assert call["kwargs"]["factory"] is True
    assert call["kwargs"]["reload"] is True
    assert call["kwargs"]["reload_dirs"] == list(RELOAD_DIRS)
    assert call["kwargs"]["reload_includes"] == list(RELOAD_INCLUDES)
    assert call["kwargs"]["reload_excludes"] == list(RELOAD_EXCLUDES)
    assert call["kwargs"]["timeout_graceful_shutdown"] == 10
    assert call["kwargs"]["host"] == "127.0.0.1"
    assert call["kwargs"]["port"] == 18765


def test_serve_no_reload_passes_the_app_object(uvicorn_recorder) -> None:
    """The pre-supervisor path must stay exactly as it was."""
    result = _invoke_serve("--no-reload", "--port", "18765")
    assert result.exit_code == 0, result.output

    (call,) = uvicorn_recorder.calls
    assert isinstance(call["args"][0], FastAPI)
    assert "reload" not in call["kwargs"]
    assert "factory" not in call["kwargs"]


def test_serve_exports_full_settings_before_uvicorn_run(
    uvicorn_recorder,
) -> None:
    """The child inherits only os.environ, so everything must be in it.

    --token is the sharp case: Typer consumes it into a local and nothing
    propagates it, so a child would otherwise rebuild settings with
    token=None.
    """
    from finances.web.settings import (
        ENV_DB_PATH,
        ENV_HOST,
        ENV_PORT,
        ENV_REGEN,
        ENV_RELOAD_CHILD,
        ENV_TOKEN,
    )

    result = _invoke_serve("--port", "18765", "--token", "from-argv")
    assert result.exit_code == 0, result.output

    env = uvicorn_recorder.calls[0]["environ"]
    assert env[ENV_RELOAD_CHILD] == "1"
    assert env[ENV_HOST] == "127.0.0.1"
    assert env[ENV_PORT] == "18765"
    assert env[ENV_TOKEN] == "from-argv"
    assert env[ENV_DB_PATH]
    assert env[ENV_REGEN] == "0"


def test_reload_moves_regen_to_the_supervisor(uvicorn_recorder) -> None:
    from finances.web.settings import ENV_REGEN

    result = _invoke_serve("--port", "18765")
    assert result.exit_code == 0, result.output

    assert uvicorn_recorder.calls[0]["environ"][ENV_REGEN] == "0"
    assert len(uvicorn_recorder.regen_calls) == 1


def test_no_reload_leaves_regen_with_the_lifespan_hook(
    uvicorn_recorder,
) -> None:
    result = _invoke_serve("--no-reload", "--port", "18765")
    assert result.exit_code == 0, result.output

    app_object = uvicorn_recorder.calls[0]["args"][0]
    assert app_object.state.settings.regen_report_on_shutdown is True
    assert uvicorn_recorder.regen_calls == []


def test_lan_bind_refuses_reload(uvicorn_recorder) -> None:
    """A reload child rebuilds settings from env and is localhost-only.

    Allowing --reload on a LAN bind would spawn a child that defaults host
    back to 127.0.0.1 — which is the value the bearer middleware
    short-circuits on — while the parent holds a LAN socket.
    """
    result = _invoke_serve(
        "--host", "0.0.0.0", "--token", "t", "--port", "18765", "--reload"
    )

    assert result.exit_code == 2
    assert "localhost" in result.output.lower()
    assert uvicorn_recorder.calls == []


def test_lan_bind_defaults_to_no_reload(uvicorn_recorder) -> None:
    result = _invoke_serve(
        "--host", "0.0.0.0", "--token", "t", "--port", "18765"
    )
    assert result.exit_code == 0, result.output

    (call,) = uvicorn_recorder.calls
    assert isinstance(call["args"][0], FastAPI)
    assert "reload" not in call["kwargs"]


def test_serve_defaults_to_the_configured_ledger(uvicorn_recorder) -> None:
    from finances import config

    result = _invoke_serve("--no-reload", "--port", "18765")
    assert result.exit_code == 0, result.output

    app_object = uvicorn_recorder.calls[0]["args"][0]
    assert app_object.state.settings.db_path == config.DB_PATH


def test_serve_db_path_points_the_whole_process_tree_elsewhere(
    uvicorn_recorder, tmp_path
) -> None:
    """``--db-path`` is what lets a test (or a scratch session) run serve.

    ``db_path`` is a WebSettings field with an env key on the wire already,
    but the CLI had no way to set it, so ``finances serve`` always opened
    ``config.DB_PATH`` — which is how the test suite ended up touching the
    real ledger. All three consumers must see the override: the exported
    environment (the reload child), the settings object (the app), and the
    shutdown regen the supervisor runs on its way out.
    """
    from finances.web.settings import ENV_DB_PATH

    scratch = tmp_path / "scratch.db"

    result = _invoke_serve("--port", "18765", "--db-path", str(scratch))
    assert result.exit_code == 0, result.output

    assert uvicorn_recorder.calls[0]["environ"][ENV_DB_PATH] == str(scratch)
    (regen_settings,) = uvicorn_recorder.regen_calls
    assert regen_settings.db_path == scratch


def test_serve_db_path_reaches_the_no_reload_app(
    uvicorn_recorder, tmp_path
) -> None:
    scratch = tmp_path / "scratch.db"

    result = _invoke_serve(
        "--no-reload", "--port", "18765", "--db-path", str(scratch)
    )
    assert result.exit_code == 0, result.output

    app_object = uvicorn_recorder.calls[0]["args"][0]
    assert app_object.state.settings.db_path == scratch


def test_serve_db_path_can_come_from_the_environment(
    uvicorn_recorder, tmp_path, monkeypatch
) -> None:
    """Same env key the reload child reads, so one name covers both hops."""
    from finances.web.settings import ENV_DB_PATH

    scratch = tmp_path / "from-env.db"
    monkeypatch.setenv(ENV_DB_PATH, str(scratch))

    result = _invoke_serve("--no-reload", "--port", "18765")
    assert result.exit_code == 0, result.output

    app_object = uvicorn_recorder.calls[0]["args"][0]
    assert app_object.state.settings.db_path == scratch


def test_lifespan_skips_regen_when_the_caller_owns_it(
    tmp_path, monkeypatch
) -> None:
    from finances import config
    from finances.web.app import create_app
    from finances.web.settings import WebSettings

    db_path = _seed_db_with_txn(tmp_path)
    report = tmp_path / "report.html"
    monkeypatch.setattr(config, "REPORT_HTML_PATH", report)

    app = create_app(
        WebSettings(db_path=db_path, regen_report_on_shutdown=False)
    )
    with TestClient(app) as client:
        assert client.get("/health").status_code == 200

    assert not report.exists()
