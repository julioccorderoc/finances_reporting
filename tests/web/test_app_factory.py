"""The ASGI factory uvicorn's reloader calls in the spawned child.

``uvicorn.run`` can take an app OBJECT, which is what ``finances serve``
did before the reload supervisor — but the reloader cannot: it re-imports
the app in a fresh interpreter, so it needs an import string and a
zero-argument factory. Nothing else in the codebase pins that string, so a
rename of ``create_app_from_env`` would break the viewer only at runtime,
in a child process, where the traceback goes to a terminal nobody reads.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from starlette.testclient import TestClient

from finances.web.app import IMPORT_STRING, create_app_from_env
from finances.web.settings import ENV_RELOAD_CHILD, WebSettings


def test_create_app_from_env_builds_the_configured_app(
    monkeypatch: pytest.MonkeyPatch,
    web_db_path: Path,
) -> None:
    for key, value in WebSettings(db_path=web_db_path).to_env().items():
        monkeypatch.setenv(key, value)

    app = create_app_from_env()

    assert app.state.settings.db_path == web_db_path
    with TestClient(app) as client:
        assert client.get("/health").status_code == 200


def test_create_app_from_env_refuses_without_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(ENV_RELOAD_CHILD, raising=False)

    with pytest.raises(RuntimeError):
        create_app_from_env()


def test_import_string_resolves() -> None:
    """The string the CLI hands uvicorn must actually import."""
    from uvicorn.importer import import_from_string

    assert callable(import_from_string(IMPORT_STRING))


def test_reload_child_regen_flag_reaches_the_app(
    monkeypatch: pytest.MonkeyPatch,
    web_db_path: Path,
) -> None:
    """The child must not own report.html regen — the supervisor does."""
    settings = WebSettings(db_path=web_db_path, regen_report_on_shutdown=False)
    for key, value in settings.to_env().items():
        monkeypatch.setenv(key, value)

    app = create_app_from_env()

    assert app.state.settings.regen_report_on_shutdown is False
