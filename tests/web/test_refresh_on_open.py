"""Background rate refresh when the viewer starts (WP2).

ADR-016 capped the P2P median tier at 14 days, which turns refresh frequency
into a correctness requirement: past the cap the chain drops to BCV rather
than carrying a stale median. The viewer is the only thing the owner opens
regularly, so it is where the refresh belongs.

The contract these tests pin:

* Staleness is decided from the newest *successful* ``import_runs`` row per
  source. An error run is not freshness.
* The gate dispatches at most one refresh at a time, and releases even when
  the run raises.
* One source failing (Binance without a VPN is routine here) must not stop
  the others, and must never raise into the lifespan.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta

import pytest

from finances.web.services import refresh as refresh_svc

NOW = datetime(2026, 8, 3, 12, 0, tzinfo=UTC)


def _run(
    conn: sqlite3.Connection,
    source: str,
    *,
    finished_at: datetime | None,
    status: str = "success",
) -> None:
    conn.execute(
        "INSERT INTO import_runs (source, started_at, finished_at, status) "
        "VALUES (?, ?, ?, ?)",
        (
            source,
            (finished_at or NOW).strftime("%Y-%m-%d %H:%M:%S"),
            finished_at.strftime("%Y-%m-%d %H:%M:%S") if finished_at else None,
            status,
        ),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Staleness gate.
# ---------------------------------------------------------------------------


def test_source_never_run_is_stale(web_db: sqlite3.Connection) -> None:
    assert "bcv" in refresh_svc.stale_sources(web_db, now=NOW)


def test_source_refreshed_an_hour_ago_is_not_stale(
    web_db: sqlite3.Connection,
) -> None:
    _run(web_db, "bcv", finished_at=NOW - timedelta(hours=1))

    assert "bcv" not in refresh_svc.stale_sources(web_db, now=NOW)


def test_source_past_the_threshold_is_stale(web_db: sqlite3.Connection) -> None:
    _run(web_db, "bcv", finished_at=NOW - timedelta(hours=13))

    assert "bcv" in refresh_svc.stale_sources(
        web_db, now=NOW, max_age_hours=12
    )


def test_threshold_boundary_is_inclusive(web_db: sqlite3.Connection) -> None:
    """Exactly at the threshold still counts as fresh."""
    _run(web_db, "bcv", finished_at=NOW - timedelta(hours=12))

    assert "bcv" not in refresh_svc.stale_sources(
        web_db, now=NOW, max_age_hours=12
    )


def test_a_failed_run_is_not_freshness(web_db: sqlite3.Connection) -> None:
    """An error run must not suppress the next attempt."""
    _run(web_db, "bcv", finished_at=NOW - timedelta(minutes=5), status="error")

    assert "bcv" in refresh_svc.stale_sources(web_db, now=NOW)


def test_p2p_staleness_reads_the_source_ingest_actually_writes(
    web_db: sqlite3.Connection,
) -> None:
    """``p2p_rates.SOURCE`` is 'binance_p2p_median', not 'p2p_rates'."""
    _run(web_db, "binance_p2p_median", finished_at=NOW - timedelta(hours=1))

    assert "binance_p2p_median" not in refresh_svc.stale_sources(
        web_db, now=NOW
    )


def test_all_three_rate_sources_are_watched(web_db: sqlite3.Connection) -> None:
    assert set(refresh_svc.REFRESH_SOURCES) == {
        "bcv",
        "binance_p2p_median",
        "binance",
    }


# ---------------------------------------------------------------------------
# Dispatch gate.
# ---------------------------------------------------------------------------


def test_dispatches_when_a_source_is_stale(web_db: sqlite3.Connection) -> None:
    calls: list[str] = []

    dispatched = refresh_svc.maybe_refresh(
        lambda: web_db,
        now=NOW,
        runner=lambda conn: calls.append("ran"),
        gate=refresh_svc.RefreshGate(),
        background=False,
    )

    assert dispatched is True
    assert calls == ["ran"]


def test_does_not_dispatch_when_everything_is_fresh(
    web_db: sqlite3.Connection,
) -> None:
    for source in ("bcv", "binance_p2p_median", "binance"):
        _run(web_db, source, finished_at=NOW - timedelta(hours=1))
    calls: list[str] = []

    dispatched = refresh_svc.maybe_refresh(
        lambda: web_db,
        now=NOW,
        runner=lambda conn: calls.append("ran"),
        gate=refresh_svc.RefreshGate(),
        background=False,
    )

    assert dispatched is False
    assert calls == []


def test_second_call_while_one_is_in_flight_does_not_dispatch(
    web_db: sqlite3.Connection,
) -> None:
    """A page reload must not start a second concurrent refresh."""
    gate = refresh_svc.RefreshGate()
    inner: list[bool] = []

    def runner(conn: sqlite3.Connection) -> None:
        inner.append(
            refresh_svc.maybe_refresh(
                lambda: web_db,
                now=NOW,
                runner=lambda c: None,
                gate=gate,
                background=False,
            )
        )

    refresh_svc.maybe_refresh(
        lambda: web_db, now=NOW, runner=runner, gate=gate, background=False
    )

    assert inner == [False]


def test_gate_releases_after_the_run_raises(
    web_db: sqlite3.Connection,
) -> None:
    """A crashed refresh must not wedge the gate shut for the process life."""
    gate = refresh_svc.RefreshGate()

    def boom(conn: sqlite3.Connection) -> None:
        raise RuntimeError("network down")

    refresh_svc.maybe_refresh(
        lambda: web_db, now=NOW, runner=boom, gate=gate, background=False
    )

    calls: list[str] = []
    assert (
        refresh_svc.maybe_refresh(
            lambda: web_db,
            now=NOW,
            runner=lambda conn: calls.append("ran"),
            gate=gate,
            background=False,
        )
        is True
    )
    assert calls == ["ran"]


def test_a_raising_runner_never_propagates(web_db: sqlite3.Connection) -> None:
    """It runs inside the lifespan; an exception there kills startup."""

    def boom(conn: sqlite3.Connection) -> None:
        raise RuntimeError("network down")

    refresh_svc.maybe_refresh(
        lambda: web_db,
        now=NOW,
        runner=boom,
        gate=refresh_svc.RefreshGate(),
        background=False,
    )


# ---------------------------------------------------------------------------
# Step isolation — reuses reports.update, no forked ingest logic (rule-004).
# ---------------------------------------------------------------------------


def test_binance_failure_does_not_stop_bcv_or_p2p(
    web_db: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    from finances.reports import update as update_mod

    monkeypatch.setattr(
        update_mod.bcv_ingest, "ingest_bcv", lambda conn, dry_run=False: 2
    )
    monkeypatch.setattr(
        update_mod.p2p_ingest,
        "ingest_p2p_rates",
        lambda conn, **kw: {"rows_written": [1, 2, 3]},
    )

    def geo_blocked() -> object:
        raise RuntimeError("451 unavailable for legal reasons")

    outcomes = update_mod.run_rate_refresh(
        web_db, make_binance_client=geo_blocked
    )

    by_source = {o.source: o for o in outcomes}
    assert by_source["bcv"].status == "ok"
    assert by_source["p2p_rates"].status == "ok"
    assert by_source["binance"].status == "error"
    assert by_source["binance"].hint == update_mod.VPN_HINT


def test_rate_refresh_skips_provincial_and_report_regen(
    web_db: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Provincial is the browser-drop path (WP3), not a startup fetch."""
    from finances.reports import update as update_mod

    monkeypatch.setattr(
        update_mod.bcv_ingest, "ingest_bcv", lambda conn, dry_run=False: 0
    )
    monkeypatch.setattr(
        update_mod.p2p_ingest,
        "ingest_p2p_rates",
        lambda conn, **kw: {"rows_written": []},
    )
    monkeypatch.setattr(
        update_mod.binance_ingest,
        "sync_binance",
        lambda conn, **kw: {"rows_inserted": 0, "rows_updated": 0},
    )

    outcomes = update_mod.run_rate_refresh(
        web_db, make_binance_client=lambda: object()
    )

    assert [o.source for o in outcomes] == ["bcv", "p2p_rates", "binance"]
