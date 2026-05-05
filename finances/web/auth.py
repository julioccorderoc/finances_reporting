"""Bearer-token middleware for LAN-bound web viewer instances (EPIC-022 / ADR-012).

Behaviour matrix (``settings.host``):

* ``127.0.0.1``  → middleware is a pass-through. No auth.
* otherwise      → request must carry a valid token via one of:
                       - ``Authorization: Bearer <token>``
                       - ``?token=<token>`` query string (first hop only;
                         middleware sets a ``finances_web_token`` cookie
                         and redirects to the same path without the
                         query string)
                       - ``finances_web_token=<token>`` cookie (subsequent
                         hops)
                   Static asset paths (``/static/*``) are exempt so the
                   browser can fetch the vendored JS even before auth
                   completes.

Failure mode:
    JSON requests → 401 ``{"error": "unauthorized"}``.
    HTML requests → 401 with a minimal HTML page.

Token comparison uses :func:`secrets.compare_digest` to keep timing
attacks off the table.
"""

from __future__ import annotations

import secrets

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse, RedirectResponse, Response

COOKIE_NAME = "finances_web_token"

_HTML_401 = (
    "<!doctype html><html><head><meta charset=\"utf-8\"><title>Unauthorized</title>"
    "</head><body><h1>401 Unauthorized</h1>"
    "<p>This finances viewer requires a token. Append <code>?token=...</code> "
    "to the URL or send <code>Authorization: Bearer ...</code>.</p>"
    "</body></html>"
)


def _wants_html(request: Request) -> bool:
    accept = request.headers.get("accept", "")
    return "text/html" in accept.lower()


def _unauthorized(request: Request) -> Response:
    if _wants_html(request):
        return HTMLResponse(_HTML_401, status_code=401)
    return JSONResponse({"error": "unauthorized"}, status_code=401)


def _token_from_request(request: Request) -> str | None:
    auth_header = request.headers.get("authorization")
    if auth_header:
        scheme, _, value = auth_header.partition(" ")
        if scheme.lower() == "bearer" and value:
            return value.strip()
    qp_token = request.query_params.get("token")
    if qp_token:
        return qp_token
    cookie_token = request.cookies.get(COOKIE_NAME)
    if cookie_token:
        return cookie_token
    return None


class BearerTokenMiddleware(BaseHTTPMiddleware):
    """Enforces a static bearer token on non-localhost binds.

    The middleware reads the active :class:`WebSettings` from
    ``request.app.state.settings`` so that test fixtures and the CLI
    can swap settings per-instance.
    """

    async def dispatch(self, request: Request, call_next):  # type: ignore[override]
        settings = getattr(request.app.state, "settings", None)
        if settings is None:
            # If for some reason the app was constructed without settings,
            # fail closed rather than open.
            return _unauthorized(request)

        # Localhost is the trusted-by-default mode (ADR-012).
        if settings.host == "127.0.0.1":
            return await call_next(request)

        path = request.url.path
        # Static assets are served unauthenticated so the page can load
        # JS even before the cookie/redirect handshake completes.
        if path.startswith("/static/"):
            return await call_next(request)
        # Health is universally public for debuggability.
        if path == "/health":
            return await call_next(request)

        expected = (settings.token or "").strip()
        if not expected:
            # Misconfigured app — should have been caught at WebSettings
            # construction. Fail closed.
            return _unauthorized(request)

        provided = _token_from_request(request)
        if provided is None or not secrets.compare_digest(provided, expected):
            return _unauthorized(request)

        # If the token came in via a query string, normalise: drop the
        # query, set a cookie, redirect.
        if request.query_params.get("token"):
            new_query_pairs = [
                (k, v) for k, v in request.query_params.multi_items() if k != "token"
            ]
            from urllib.parse import urlencode

            new_query = urlencode(new_query_pairs)
            target = request.url.path + (f"?{new_query}" if new_query else "")
            redirect = RedirectResponse(url=target, status_code=303)
            redirect.set_cookie(
                COOKIE_NAME,
                expected,
                httponly=True,
                samesite="lax",
            )
            return redirect

        response = await call_next(request)
        # Refresh the cookie on each authenticated request (idempotent).
        if not request.cookies.get(COOKIE_NAME):
            response.set_cookie(
                COOKIE_NAME,
                expected,
                httponly=True,
                samesite="lax",
            )
        return response
