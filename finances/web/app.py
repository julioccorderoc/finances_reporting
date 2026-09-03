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
from uuid import uuid4

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from finances.format import (
    fmt_date,
    fmt_date_short,
    fmt_money,
    fmt_month,
    fmt_native,
    fmt_number,
    fmt_usd,
)
from finances.web.auth import BearerTokenMiddleware
from finances.web.errors import install_exception_handlers
from finances.web.routers import api as api_router
from finances.web.routers import pages as pages_router
from finances.web.routers import partials as partials_router
from finances.web.settings import WebSettings
from finances.web.services.rail import build_rail
from finances.web.services.triage_view import prov_chip
from finances.web.urls import modal_url_for

WEB_PACKAGE_DIR = Path(__file__).resolve().parent
STATIC_DIR = WEB_PACKAGE_DIR / "static"
TEMPLATES_DIR = WEB_PACKAGE_DIR / "templates"
PACKAGE_DIR = WEB_PACKAGE_DIR.parent

# The reload supervisor's configuration (ADR-012 Amendment 2026-07-26),
# kept here so the CLI and the tests read the same values.
#
# RELOAD_DIRS is the PACKAGE, deliberately not the repo root: ``*.html``
# has to be watched (the 2026-07-26 outage was a template/router pair
# splitting), and the shutdown regen writes ``report.html`` at the repo
# root. Watching the root would make the server restart itself forever.
IMPORT_STRING = "finances.web.app:create_app_from_env"
RELOAD_DIRS = (str(PACKAGE_DIR),)
RELOAD_INCLUDES = ("*.html", "*.j2")  # uvicorn adds "*.py" implicitly
RELOAD_EXCLUDES = (str(STATIC_DIR),)


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
        # Startup: top up stale rate sources in the background. ADR-016
        # expires the P2P median at 14 days, so a viewer that is only opened
        # occasionally would otherwise price spending off BCV. Dispatch is
        # non-blocking and swallows every failure — Binance is 451-blocked
        # from Venezuela without a VPN, and that must not stop the server
        # from coming up. Guarded by a setting so only `finances serve`
        # reaches the network.
        if settings.refresh_on_start:
            from finances.db.connection import get_connection
            from finances.web.services import refresh as refresh_svc

            refresh_svc.maybe_refresh(
                lambda: get_connection(settings.db_path)
            )
        yield
        # Shutdown: refresh the static report so it mirrors this session's
        # edits — unless the caller owns that. Under the reload supervisor
        # this process is SIGTERM'd on every source edit, and a full
        # non-atomic export per edit would leave report.html truncated far
        # more often than correct; the supervisor runs it once, at the end.
        if settings.regen_report_on_shutdown:
            _regen_report_on_shutdown(settings)

    app = FastAPI(
        title="finances viewer",
        version="0.1.0",
        docs_url=None,
        redoc_url=None,
        lifespan=lifespan,
    )
    app.state.settings = settings
    # Identity of THIS process. Reload keeps the process matching the disk;
    # nothing can keep already-rendered HTML matching the process, so the
    # page compares this against every response and says so when it drifts.
    app.state.boot_id = uuid4().hex[:12]

    app.state.templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
    # Templates are frozen at boot, like the Python around them. Jinja's
    # default is to re-read a changed template on the next request, which
    # leaves a long-running viewer serving NEW markup from an OLD context
    # builder whenever an edit lands mid-session — the new half reads a
    # variable the old half never puts in the context, and the render
    # 500s. Freezing makes the process one coherent snapshot: a restart
    # picks up both halves, or neither. This matters MORE under the reload
    # supervisor, not less: watchfiles debounces and the child costs a
    # fraction of a second to spawn, so there is always a brief window
    # where the outgoing child is still answering requests with the new
    # template already on disk.
    app.state.templates.env.auto_reload = False
    # Shared display filters (UX overhaul WP1) — the SAME four names are
    # the cross-plan contract; templates use them directly and via macros.
    app.state.templates.env.filters.update(
        {
            "fmt_number": fmt_number,
            "fmt_money": fmt_money,
            "fmt_date": fmt_date,
            "fmt_month": fmt_month,
            # Triage redesign (Wave 2). fmt_usd/fmt_native render SIGNAL's
            # money pair — U+2212, sign before symbol, an explicit "+" on a
            # credit — and fmt_date_short the queue's 64px date column.
            "fmt_date_short": fmt_date_short,
            "fmt_native": fmt_native,
            "fmt_usd": fmt_usd,
        }
    )
    # Globals rather than filters: both take keyword arguments and read
    # more like the components they stand for. ``prov_chip`` is the one
    # place that decides how a rate tier is drawn (D2/D3); ``modal_url_for``
    # is how a row knows which dialog opens it; ``rail_state`` is what the
    # shell's rail reads — a global rather than a context processor so the
    # blocking count is computed only when a page renders the rail, not on
    # every partial swap.
    app.state.templates.env.globals.update(
        {
            "prov_chip": prov_chip,
            "modal_url_for": modal_url_for,
            "rail_state": build_rail,
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

    install_exception_handlers(app)

    @app.middleware("http")
    async def _stamp_boot_id(request, call_next):  # type: ignore[no-untyped-def]
        response = await call_next(request)
        response.headers["X-Finances-Boot"] = request.app.state.boot_id
        return response

    # Middleware is added last so it wraps everything (StaticFiles included);
    # the auth class itself exempts /static and /health. add_middleware
    # PREPENDS, so anything registered after this would sit outside auth and
    # answer unauthenticated requests — keep BearerTokenMiddleware last.
    app.add_middleware(BearerTokenMiddleware)

    return app


def create_app_from_env() -> FastAPI:
    """ASGI factory for uvicorn's reloader (``factory=True``).

    The reload child is a ``spawn()``-ed interpreter: nothing ``serve_cmd``
    constructed in the parent survives into it, so the settings arrive
    through the environment — and :meth:`WebSettings.from_env` refuses to
    fall back to defaults rather than quietly serving the wrong ledger, or
    a LAN socket with auth disabled.
    """
    return create_app(WebSettings.from_env())
