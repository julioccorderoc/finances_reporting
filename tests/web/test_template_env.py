"""The template environment must not hot-reload behind running code.

Jinja re-reads a changed template on the next request, but the Python that
builds that template's context is frozen at process start. An edit that
touches both halves therefore renders NEW markup against an OLD context.
That is how a live triage session hit ``TypeError: Object of type
Undefined is not JSON serializable`` on 2026-07-26: a concurrent session
added the modal's prev/next arrows, the running server picked up the new
template but not the router that supplies ``prev_url``.

Freezing the loader makes the running server one coherent snapshot — a
restart picks up both halves or neither.
"""

from __future__ import annotations

from typing import Callable

from starlette.testclient import TestClient


def test_template_env_does_not_auto_reload(
    web_client_factory: Callable[[], TestClient],
) -> None:
    with web_client_factory() as client:
        env = client.app.state.templates.env

    assert env.auto_reload is False
