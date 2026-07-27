"""Boot identity — the half of the problem a reloader structurally cannot fix.

Reload guarantees that the PROCESS matches the disk. It can never
guarantee that the HTML already sitting in the owner's browser matches the
process: a triage modal rendered at 14:02 is still on screen at 14:20,
six restarts later. Every response therefore carries the id of the process
that produced it, so the page can notice it outlived its server.

The id is seeded into ``base.html`` from ``request.app.state`` rather than
a template context key on purpose — Starlette injects ``request`` into
every context, so no ``TemplateResponse`` call site has to change, and the
template-context contract test stays clean.
"""

from __future__ import annotations

import re
from typing import Callable

import pytest
from starlette.testclient import TestClient

from finances.web.app import create_app
from finances.web.settings import WebSettings


def test_every_response_carries_the_boot_header(
    web_client_factory: Callable[[], TestClient],
) -> None:
    with web_client_factory() as client:
        health = client.get("/health")
        page = client.get("/")

    assert health.headers["X-Finances-Boot"]
    assert page.headers["X-Finances-Boot"]


def test_two_apps_get_different_boot_ids(
    web_client_factory: Callable[[], TestClient],
) -> None:
    with web_client_factory() as first, web_client_factory() as second:
        one = first.get("/health").headers["X-Finances-Boot"]
        two = second.get("/health").headers["X-Finances-Boot"]

    assert one != two


def test_base_html_embeds_the_boot_id(
    web_client_factory: Callable[[], TestClient],
) -> None:
    with web_client_factory() as client:
        page = client.get("/")

    match = re.search(
        r'<meta name="finances-boot" content="([^"]+)"', page.text
    )
    assert match is not None
    assert match.group(1) == page.headers["X-Finances-Boot"]


def test_bearer_middleware_still_outermost(tmp_path) -> None:
    """``add_middleware`` prepends, so the boot stamp must not displace auth.

    ADR-012 §4 / rule-012: the token is enforced before any route handler
    runs. A middleware added after ``BearerTokenMiddleware`` would wrap
    OUTSIDE it and start stamping — and therefore answering — requests
    that were never authenticated.
    """
    app = create_app(
        WebSettings(host="0.0.0.0", token="t", db_path=tmp_path / "x.db")
    )

    with TestClient(app) as client:
        assert client.get("/").status_code == 401


@pytest.mark.parametrize("path", ["/health", "/triage"])
def test_boot_header_survives_the_auth_middleware(
    web_client_factory: Callable[[], TestClient],
    path: str,
) -> None:
    with web_client_factory() as client:
        assert client.get(path).headers.get("X-Finances-Boot")
