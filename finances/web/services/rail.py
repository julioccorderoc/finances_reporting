"""The rail's state — the one thing the shell needs from the ledger.

Every full page renders ``partials/rail.html``, and the rail has exactly
one live number on it: how many rows block in Triage. It is read through
a Jinja global (``rail_state(request)``, registered in ``app.py`` beside
``modal_url_for``) rather than a context processor so the work happens
only when a page actually renders the rail — a context processor runs for
every ``TemplateResponse``, partial swaps included, and the modal run
would pay for a badge it never draws.
"""

from __future__ import annotations

from fastapi import Request
from pydantic import BaseModel, ConfigDict

from finances.web.deps import dismissed_pairs, open_conn
from finances.web.services.triage import count_blocking


class RailState(BaseModel):
    """What ``partials/rail.html`` reads."""

    model_config = ConfigDict(frozen=True)

    path: str
    blocking_count: int

    def is_current(self, href: str) -> bool:
        """Whether ``href`` is the destination this page belongs to.

        ``/`` is exact; everything else matches by prefix so a filtered
        ``/transactions?accounts=…`` still lights Flow.
        """
        if href == "/":
            return self.path == "/"
        return self.path == href or self.path.startswith(href + "/")


def build_rail(request: Request) -> RailState:
    """Assemble the rail's state for this request."""
    conn = open_conn(request.app.state.settings)
    try:
        blocking = count_blocking(conn, dismissed=dismissed_pairs(request))
    finally:
        conn.close()
    return RailState(path=request.url.path, blocking_count=blocking)


__all__ = ["RailState", "build_rail"]
