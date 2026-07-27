"""Pydantic v2 settings for the local web viewer (EPIC-022 / ADR-012).

The web viewer's runtime knobs are intentionally minimal. They are
constructed by the ``finances serve`` Typer command and threaded
through ``create_app`` onto ``app.state.settings``.

LAN binds (anything other than ``127.0.0.1``) require an explicit
bearer token — enforced here rather than at the route layer so the
``finances serve`` CLI fails fast with a clear error instead of
booting an open server.

Under the reload supervisor (ADR-012 Amendment 2026-07-26) these
settings also have to cross a process boundary: uvicorn's reloader
``spawn()``s a fresh interpreter, and the only thing it inherits is
``os.environ``. ``to_env``/``from_env`` are that channel, and they are
deliberately total and fail-closed — see ``from_env``.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field

from finances import config as _config

# The wire contract between ``finances serve`` and its reload child.
# ENV_TOKEN is already the Typer envvar behind ``--token``; the rest exist
# only for this hop.
ENV_RELOAD_CHILD = "FINANCES_WEB_RELOAD_CHILD"
ENV_HOST = "FINANCES_WEB_HOST"
ENV_PORT = "FINANCES_WEB_PORT"
ENV_TOKEN = "FINANCES_WEB_TOKEN"
ENV_DB_PATH = "FINANCES_WEB_DB_PATH"
ENV_REGEN = "FINANCES_WEB_REGEN_ON_SHUTDOWN"

# Every WebSettings field maps to exactly one env key. A test asserts this
# covers ``model_fields`` — a field added without an entry here would not
# raise in the child, it would silently serve a default.
_FIELD_TO_ENV = {
    "host": ENV_HOST,
    "port": ENV_PORT,
    "token": ENV_TOKEN,
    "db_path": ENV_DB_PATH,
    "regen_report_on_shutdown": ENV_REGEN,
}


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
        regen_report_on_shutdown: Who rewrites ``report.html`` on teardown.
            ``True`` (default) is the lifespan hook — today's behaviour and
            the ``--no-reload`` path. ``False`` hands ownership to the
            caller: under the reload supervisor the child is SIGTERM'd on
            every source edit, and a 665-query non-atomic export per edit
            would truncate ``report.html`` dozens of times an hour while
            the supervisor blocks on ``join()``.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    host: str = "127.0.0.1"
    port: int = 8765
    token: Optional[str] = None
    db_path: Path = Field(default_factory=lambda: _config.DB_PATH)
    regen_report_on_shutdown: bool = True

    def model_post_init(self, __context: object) -> None:
        if self.host != "127.0.0.1" and not (self.token or "").strip():
            raise ValueError(
                "LAN bind requires --token or FINANCES_WEB_TOKEN "
                f"(host={self.host!r})."
            )

    def to_env(self) -> dict[str, str]:
        """Serialise every field for the reload child's environment."""
        return {
            ENV_RELOAD_CHILD: "1",
            ENV_HOST: self.host,
            ENV_PORT: str(self.port),
            ENV_TOKEN: self.token or "",
            ENV_DB_PATH: str(self.db_path),
            ENV_REGEN: "1" if self.regen_report_on_shutdown else "0",
        }

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> WebSettings:
        """Rebuild the parent's settings inside a reload child.

        Fails closed, and never ``.get(key, default)`` on a required key:
        a value dropped in transit must be an exception here, because the
        silent alternative is a child that reverts ``host`` to
        ``127.0.0.1`` and serves an unauthenticated viewer on a socket its
        parent bound to the LAN.
        """
        env = os.environ if environ is None else environ

        if env.get(ENV_RELOAD_CHILD) != "1":
            raise RuntimeError(
                f"{ENV_RELOAD_CHILD}=1 is required; WebSettings.from_env "
                "refuses to fall back to defaults (host would default to "
                "127.0.0.1 and disable the bearer middleware)."
            )

        missing = [
            key
            for key in (ENV_HOST, ENV_PORT, ENV_DB_PATH, ENV_REGEN)
            if key not in env
        ]
        if missing:
            raise RuntimeError(
                "missing required env for the reload child: "
                + ", ".join(sorted(missing))
            )

        host = env[ENV_HOST]
        if host != "127.0.0.1":
            raise RuntimeError(
                f"the reload child is localhost-only; refusing host={host!r}"
            )

        return cls(
            host=host,
            port=int(env[ENV_PORT]),
            token=(env.get(ENV_TOKEN) or "").strip() or None,
            db_path=Path(env[ENV_DB_PATH]),
            regen_report_on_shutdown=env[ENV_REGEN] == "1",
        )
