"""What the viewer says when a request fails outright (ADR-012 Amendment).

Without a handler, an unhandled exception reaches Starlette's
``ServerErrorMiddleware`` and comes back as the string ``Internal Server
Error`` with no body structure at all. On 2026-07-26 that meant a triage
session whose every modal was 500ing showed nothing but a red "Error 500"
toast that vanished after four seconds — while the owner kept typing.

Two shapes, one per caller:

* **htmx** gets JSON with a ``detail``. htmx classifies 5xx as
  ``{swap: false, error: true}``, so the response cannot clobber
  ``#tx-modal-host`` and a half-typed modal survives; base.html's existing
  ``htmx:response-error`` listener already unwraps ``detail`` into a
  toast, so this needs no new JavaScript.
* **a browser navigation** gets a self-contained page. The server may be
  the broken thing, so the page inlines its styles and references no
  ``/static`` asset — the same invariant the goodbye page holds.

The exception text itself is shown only on the localhost bind. That is the
same predicate :class:`BearerTokenMiddleware` uses to decide auth is
unnecessary; on a LAN bind the token is a single shared secret, and
whoever is on the other side of a failed request does not need tracebacks.
"""

from __future__ import annotations

import html

from fastapi import FastAPI
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse, Response

_HX_MESSAGE = (
    "Server error — this view may be out of date. Reload (Cmd-R). "
    "Your saved edits are safe."
)

_ERROR_PAGE_HTML = """<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <title>Finances — error</title>
    <style>
      body {{ font-family: -apple-system, system-ui, sans-serif; background: #f8fafc;
             color: #0f172a; display: grid; place-items: center; min-height: 100vh; margin: 0; }}
      main {{ text-align: center; max-width: 34rem; padding: 1.5rem; }}
      p {{ color: #475569; }}
      code {{ display: block; margin-top: 1rem; padding: 0.75rem; text-align: left;
             background: #f1f5f9; border-radius: 0.375rem; color: #b91c1c;
             font-size: 0.8125rem; overflow-x: auto; }}
      button {{ margin-top: 1.25rem; padding: 0.5rem 1rem; border-radius: 0.375rem;
               border: 1px solid #cbd5e1; background: #fff; cursor: pointer; }}
    </style>
  </head>
  <body>
    <main>
      <h1>Something broke</h1>
      <p>This page could not be rendered. Everything already saved is in the
         database — nothing was lost.</p>
      <p>If the server was just restarted by an edit, reloading is usually
         enough. Otherwise the launcher terminal has the full traceback.</p>
      <code>{detail}</code>
      <button onclick="location.reload()">Reload</button>
    </main>
  </body>
</html>"""


async def unhandled_exception_handler(
    request: Request, exc: Exception
) -> Response:
    """Turn any unhandled exception into something the owner can act on."""
    settings = request.app.state.settings
    is_local = settings.host == "127.0.0.1"
    detail = f"{type(exc).__name__}: {exc}" if is_local else "Internal server error."

    if request.headers.get("HX-Request", "").lower() == "true":
        response: Response = JSONResponse(
            {"detail": f"{_HX_MESSAGE} {detail}"}, status_code=500
        )
    else:
        response = HTMLResponse(
            _ERROR_PAGE_HTML.format(detail=html.escape(detail)),
            status_code=500,
        )

    # Stamped here rather than by the boot-id middleware: an Exception
    # handler is invoked by ServerErrorMiddleware, which sits OUTSIDE the
    # whole middleware stack, so nothing downstream ever sees this response.
    # The restart banner has to learn about a restart that ended in a 500
    # exactly as much as one that ended in a 200.
    response.headers["X-Finances-Boot"] = request.app.state.boot_id
    return response


def install_exception_handlers(app: FastAPI) -> None:
    app.add_exception_handler(Exception, unhandled_exception_handler)
