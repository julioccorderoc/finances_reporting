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

import sys
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from finances.format import fmt_date, fmt_money, fmt_month, fmt_number
from finances.web.auth import BearerTokenMiddleware
from finances.web.routers import api as api_router
from finances.web.routers import pages as pages_router
from finances.web.routers import partials as partials_router
from finances.web.settings import WebSettings

WEB_PACKAGE_DIR = Path(__file__).resolve().parent
STATIC_DIR = WEB_PACKAGE_DIR / "static"
TEMPLATES_DIR = WEB_PACKAGE_DIR / "templates"


def _regen_report_on_shutdown(settings: WebSettings) -> None:
    """Regenerate the static ``report.html`` from the current DB (Thing 5B).

    Best-effort: the double-click launcher (``finances.command``) relies on
    the static file reflecting any write-back edits made during the serve
    session, but a regen failure must never crash server teardown. Read-only —
    :func:`export_html` issues SELECTs only.
    """
    from finances import config
    from finances.db.connection import get_connection
    from finances.reports.html_export import export_html

    try:
        conn = get_connection(settings.db_path)
        try:
            export_html(conn, config.REPORT_HTML_PATH)
        finally:
            conn.close()
    except Exception as exc:  # noqa: BLE001 - warn-only by design
        print(
            f"warning: report.html regen on shutdown failed: {exc}",
            file=sys.stderr,
        )


def create_app(settings: WebSettings) -> FastAPI:
    """Construct a FastAPI app wired with templates, static, routers, auth."""

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        yield
        # Shutdown: refresh the static report so it mirrors this session's edits.
        _regen_report_on_shutdown(settings)

    app = FastAPI(
        title="finances viewer",
        version="0.1.0",
        docs_url=None,
        redoc_url=None,
        lifespan=lifespan,
    )
    app.state.settings = settings

    app.state.templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
    # Shared display filters (UX overhaul WP1) — the SAME four names are
    # the cross-plan contract; templates use them directly and via macros.
    app.state.templates.env.filters.update(
        {
            "fmt_number": fmt_number,
            "fmt_money": fmt_money,
            "fmt_date": fmt_date,
            "fmt_month": fmt_month,
        }
    )

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
