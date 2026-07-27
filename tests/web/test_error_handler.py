"""A 500 has to say what broke, because the owner is mid-data-entry.

During the 2026-07-26 outage the entire user-visible signal was a red
toast reading "Error 500" that auto-dismissed after four seconds, into a
screen someone was typing amounts into. That is what a bare Starlette
``ServerErrorMiddleware`` response produces: ``Internal Server Error`` as
plain text, no ``detail`` key for base.html's toast handler to unwrap.

Two shapes, chosen by who is asking. An htmx request gets JSON with a
``detail`` — 5xx makes htmx skip the swap, so the half-typed modal
survives and the existing listener turns ``detail`` into a toast with no
new JS. A browser navigation gets a self-contained page: the server may be
the broken thing, so it cannot reference ``/static`` (the same invariant
the goodbye page already holds).

On a LAN bind the exception text is redacted. ADR-012 accepts plaintext on
the home LAN, but the token is a single shared secret and an unauthorised
reader should not be handed tracebacks.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient

from finances.web.app import create_app
from finances.web.settings import WebSettings

INCIDENT_MESSAGE = "Object of type Undefined is not JSON serializable"


def _app_with_a_broken_route(**overrides: object) -> FastAPI:
    settings_kwargs: dict[str, object] = {"host": "127.0.0.1"}
    settings_kwargs.update(overrides)
    app = create_app(WebSettings(**settings_kwargs))  # type: ignore[arg-type]

    @app.get("/_boom", include_in_schema=False)
    def _boom() -> None:
        raise TypeError(INCIDENT_MESSAGE)

    return app


def _client(app: FastAPI) -> TestClient:
    # raise_server_exceptions=True (the default) re-raises instead of
    # returning the handler's response, which would hide what we assert on.
    return TestClient(app, raise_server_exceptions=False)


def test_hx_request_500_returns_json_detail(tmp_path: Path) -> None:
    app = _app_with_a_broken_route(db_path=tmp_path / "x.db")

    with _client(app) as client:
        resp = client.get("/_boom", headers={"HX-Request": "true"})

    assert resp.status_code == 500
    detail = resp.json()["detail"]
    assert INCIDENT_MESSAGE in detail
    assert "reload" in detail.lower()
    assert "saved edits are safe" in detail.lower()


def test_browser_navigation_500_returns_a_self_contained_page(
    tmp_path: Path,
) -> None:
    app = _app_with_a_broken_route(db_path=tmp_path / "x.db")

    with _client(app) as client:
        resp = client.get("/_boom")

    assert resp.status_code == 500
    assert resp.headers["content-type"].startswith("text/html")
    assert "/static/" not in resp.text
    assert "reload" in resp.text.lower()
    assert INCIDENT_MESSAGE in resp.text


def test_lan_bind_redacts_the_exception_detail(tmp_path: Path) -> None:
    app = _app_with_a_broken_route(
        host="0.0.0.0", token="t", db_path=tmp_path / "x.db"
    )

    with _client(app) as client:
        resp = client.get(
            "/_boom",
            headers={
                "HX-Request": "true",
                "Authorization": "Bearer t",
            },
        )

    assert resp.status_code == 500
    assert INCIDENT_MESSAGE not in resp.text
    assert "TypeError" not in resp.text


def test_the_error_page_escapes_the_exception_text(tmp_path: Path) -> None:
    app = create_app(WebSettings(db_path=tmp_path / "x.db"))

    @app.get("/_boom_html", include_in_schema=False)
    def _boom_html() -> None:
        raise ValueError("<script>alert(1)</script>")

    with _client(app) as client:
        resp = client.get("/_boom_html")

    assert "<script>alert(1)</script>" not in resp.text
    assert "&lt;script&gt;" in resp.text


def test_the_boot_id_still_rides_along_on_a_500(tmp_path: Path) -> None:
    """The banner must still learn about a restart that ended in an error."""
    app = _app_with_a_broken_route(db_path=tmp_path / "x.db")

    with _client(app) as client:
        resp = client.get("/_boom", headers={"HX-Request": "true"})

    assert resp.headers["X-Finances-Boot"] == app.state.boot_id


@pytest.mark.parametrize("hx_header", ["true", "TRUE", "True"])
def test_the_hx_header_match_is_case_insensitive(
    tmp_path: Path, hx_header: str
) -> None:
    app = _app_with_a_broken_route(db_path=tmp_path / "x.db")

    with _client(app) as client:
        resp = client.get("/_boom", headers={"HX-Request": hx_header})

    assert resp.headers["content-type"].startswith("application/json")
