"""Refresh stale rate sources when the viewer starts (WP2).

ADR-016 capped the ``binance_p2p_median`` tier at 14 days. Past the cap the
resolver falls through to BCV instead of carrying a months-old snapshot —
which makes refresh frequency a correctness requirement, not a nicety. The
viewer is the only thing the owner opens regularly, so it is where the
refresh belongs.

Design constraints:

* **No forked ingest logic.** The work is
  :func:`finances.reports.update.run_rate_refresh`, the same step functions
  ``finances update`` calls (rule-004). This module only decides *whether*
  and *when*.
* **Never blocks the page.** On the last real run bcv took ~2s, p2p ~2s and
  binance ~6s. The refresh is dispatched to a worker thread; the lifespan
  returns immediately.
* **Never raises into the lifespan.** An exception at startup would take the
  server down. Binance failing without a VPN is routine here, not fatal.
* **Its own connection.** sqlite connections are not safe to share across
  threads, so the worker opens one from ``connect`` and closes it.
"""

from __future__ import annotations

import sqlite3
import threading
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

#: Refresh when the newest successful run for a source is older than this.
DEFAULT_MAX_AGE_HOURS = 12

#: The sources this refresh covers, named as ``import_runs.source`` actually
#: records them. Note ``binance_p2p_median``: ``ingest.p2p_rates.SOURCE`` is
#: the headline rate name, not "p2p_rates". Provincial is absent on purpose —
#: statements arrive by browser drop, and polling for them would mean
#: re-reading ``inputs/`` on every boot.
REFRESH_SOURCES: tuple[str, ...] = ("bcv", "binance_p2p_median", "binance")


class RefreshGate:
    """Admits one refresh at a time.

    A class rather than module-level globals so tests get a fresh gate per
    case instead of a reset hook in production code. One instance,
    :data:`_GATE`, backs the real server.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._in_flight = False

    def acquire(self) -> bool:
        """Claim the gate. ``False`` when a refresh is already running."""
        with self._lock:
            if self._in_flight:
                return False
            self._in_flight = True
            return True

    def release(self) -> None:
        with self._lock:
            self._in_flight = False


_GATE = RefreshGate()


def _last_success_at(
    conn: sqlite3.Connection, source: str
) -> datetime | None:
    """When ``source`` last completed successfully, or ``None`` if never.

    Only ``status='success'`` counts. An error run is not freshness — the
    whole point of retrying is that the last attempt failed.
    """
    row = conn.execute(
        """
        SELECT finished_at
        FROM import_runs
        WHERE source = ? AND status = 'success' AND finished_at IS NOT NULL
        ORDER BY finished_at DESC, id DESC
        LIMIT 1
        """,
        (source,),
    ).fetchone()
    if row is None:
        return None
    value = row["finished_at"] if isinstance(row, sqlite3.Row) else row[0]
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("T", " "))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def stale_sources(
    conn: sqlite3.Connection,
    *,
    now: datetime | None = None,
    max_age_hours: int = DEFAULT_MAX_AGE_HOURS,
) -> list[str]:
    """Return the :data:`REFRESH_SOURCES` due for a refresh.

    A source is due when it has never succeeded, or its newest success is
    strictly older than ``max_age_hours``. The boundary is inclusive: a run
    exactly ``max_age_hours`` old still counts as fresh.
    """
    now = now or datetime.now(tz=UTC)
    cutoff = now - timedelta(hours=max_age_hours)
    due: list[str] = []
    for source in REFRESH_SOURCES:
        last = _last_success_at(conn, source)
        if last is None or last < cutoff:
            due.append(source)
    return due


def _production_runner(conn: sqlite3.Connection) -> None:
    """Run the real rate steps. Imported lazily to keep startup cheap."""
    from finances.cli.main import _make_binance_client
    from finances.reports.update import run_rate_refresh

    run_rate_refresh(conn, make_binance_client=_make_binance_client)


def maybe_refresh(
    connect: Callable[[], sqlite3.Connection],
    *,
    now: datetime | None = None,
    max_age_hours: int = DEFAULT_MAX_AGE_HOURS,
    runner: Callable[[sqlite3.Connection], Any] | None = None,
    gate: RefreshGate | None = None,
    background: bool = True,
) -> bool:
    """Refresh stale rate sources. Returns whether a refresh was dispatched.

    Args:
        connect: Opens a connection. Called once to read staleness, and again
            inside the worker so the two threads never share a handle.
        now: Injectable clock for tests.
        max_age_hours: Staleness threshold.
        runner: Receives an open connection and does the work. Defaults to
            the production rate steps.
        gate: Concurrency gate. Defaults to the process-wide one — a page
            reload, or a watchfiles supervisor restart, must not start a
            second concurrent refresh.
        background: ``False`` runs inline, for tests.

    Never raises: every failure path is swallowed and reported through
    ``import_runs`` (each step already records its own run row), because this
    is called from the ASGI lifespan where an exception aborts startup.
    """
    runner = runner or _production_runner
    gate = gate or _GATE

    try:
        conn = connect()
        try:
            due = stale_sources(conn, now=now, max_age_hours=max_age_hours)
        finally:
            if background:
                conn.close()
    except Exception:  # noqa: BLE001 - startup must survive a bad DB read
        return False

    if not due:
        return False
    if not gate.acquire():
        return False

    def _work() -> None:
        try:
            work_conn = connect() if background else conn
            try:
                runner(work_conn)
            finally:
                if background:
                    work_conn.close()
        except Exception:  # noqa: BLE001 - a failed refresh is not fatal
            pass
        finally:
            gate.release()

    if background:
        threading.Thread(
            target=_work, name="finances-rate-refresh", daemon=True
        ).start()
    else:
        _work()
    return True


__all__ = [
    "DEFAULT_MAX_AGE_HOURS",
    "REFRESH_SOURCES",
    "RefreshGate",
    "maybe_refresh",
    "stale_sources",
]
