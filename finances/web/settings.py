"""Pydantic v2 settings for the local web viewer (EPIC-022 / ADR-012).

The web viewer's runtime knobs are intentionally minimal. They are
constructed by the ``finances serve`` Typer command and threaded
through ``create_app`` onto ``app.state.settings``.

LAN binds (anything other than ``127.0.0.1``) require an explicit
bearer token — enforced here rather than at the route layer so the
``finances serve`` CLI fails fast with a clear error instead of
booting an open server.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field

from finances import config as _config


class WebSettings(BaseModel):
    """Runtime configuration for the FastAPI viewer.

    Fields:
        host: Address to bind. ``127.0.0.1`` (default) means localhost-only
            and disables the auth middleware. Anything else is treated as a
            LAN bind and requires ``token``.
        port: TCP port. Default ``8765``.
        token: Static bearer token for LAN access. Required when ``host``
            is not ``127.0.0.1``.
        db_path: SQLite database path. Defaults to ``config.DB_PATH``.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    host: str = "127.0.0.1"
    port: int = 8765
    token: Optional[str] = None
    db_path: Path = Field(default_factory=lambda: _config.DB_PATH)

    def model_post_init(self, __context: object) -> None:
        if self.host != "127.0.0.1" and not (self.token or "").strip():
            raise ValueError(
                "LAN bind requires --token or FINANCES_WEB_TOKEN "
                f"(host={self.host!r})."
            )
