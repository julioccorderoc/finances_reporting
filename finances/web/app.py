"""FastAPI application factory for the local web viewer (EPIC-022 / ADR-012).

The factory pattern (``create_app(settings) -> FastAPI``) keeps tests
hermetic — each test constructs an app with its own ``WebSettings`` —
and matches the standard FastAPI getting-started recipe.

Foundation contract:
    * mount ``/static`` from ``finances/web/static``,
    * configure Jinja2 templates pointed at ``finances/web/templates``
      and stash on ``app.state.templates``,
    * include the (empty in Phase 1) routers under
      :mod:`finances.web.routers`,
    * register :class:`BearerTokenMiddleware`,
    * expose ``GET /health`` (always public, even on LAN).
"""

from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from finances.web.auth import BearerTokenMiddleware
from finances.web.routers import api as api_router
from finances.web.routers import pages as pages_router
from finances.web.routers import partials as partials_router
from finances.web.settings import WebSettings

WEB_PACKAGE_DIR = Path(__file__).resolve().parent
STATIC_DIR = WEB_PACKAGE_DIR / "static"
TEMPLATES_DIR = WEB_PACKAGE_DIR / "templates"


def create_app(settings: WebSettings) -> FastAPI:
    """Construct a FastAPI app wired with templates, static, routers, auth."""
    app = FastAPI(title="finances viewer", version="0.1.0", docs_url=None, redoc_url=None)
    app.state.settings = settings

    app.state.templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

    app.mount(
        "/static",
        StaticFiles(directory=str(STATIC_DIR)),
        name="static",
    )

    @app.get("/health", include_in_schema=False)
    def health() -> dict[str, str]:  # pragma: no cover - exercised by tests
        return {"status": "ok"}

    app.include_router(pages_router.router)
    app.include_router(partials_router.router)
    app.include_router(api_router.router)

    # Middleware is added last so it wraps everything (StaticFiles included);
    # the auth class itself exempts /static and /health.
    app.add_middleware(BearerTokenMiddleware)

    return app
