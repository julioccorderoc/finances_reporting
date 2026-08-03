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


# ---------------------------------------------------------------------------
# Wiring: opt-in setting, lifespan hook, and the chip that reports the result.
# ---------------------------------------------------------------------------


def test_refresh_on_start_defaults_off(tmp_path) -> None:
    """Off by default so tests and ad-hoc app builds never hit the network."""
    from finances.web.settings import WebSettings

    assert WebSettings(host="127.0.0.1", db_path=tmp_path / "x.db").refresh_on_start is False


def test_refresh_on_start_survives_the_reload_child_hop(tmp_path) -> None:
    """WebSettings crosses a process boundary through os.environ only."""
    from finances.web.settings import WebSettings

    original = WebSettings(
        host="127.0.0.1", db_path=tmp_path / "x.db", refresh_on_start=True
    )

    restored = WebSettings.from_env(original.to_env())

    assert restored.refresh_on_start is True


def test_lifespan_does_not_refresh_when_the_setting_is_off(
    web_db: sqlite3.Connection, web_client_factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[str] = []
    monkeypatch.setattr(
        refresh_svc, "maybe_refresh", lambda *a, **k: calls.append("x")
    )

    with web_client_factory():
        pass

    assert calls == []


def test_lifespan_refreshes_when_the_setting_is_on(
    web_db_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from fastapi.testclient import TestClient

    from finances.web.app import create_app
    from finances.web.settings import WebSettings

    calls: list[str] = []
    monkeypatch.setattr(
        refresh_svc, "maybe_refresh", lambda *a, **k: calls.append("x")
    )

    app = create_app(
        WebSettings(
            host="127.0.0.1", db_path=web_db_path, refresh_on_start=True
        )
    )
    with TestClient(app):
        pass

    assert calls == ["x"]


def test_p2p_sync_chip_reads_the_source_ingest_writes(
    web_db: sqlite3.Connection,
) -> None:
    """The chip queried 'p2p_rates', which ingest has never written."""
    from finances.web.services.dashboard import build_sync_status

    _run(web_db, "binance_p2p_median", finished_at=NOW)

    chip = next(
        c for c in build_sync_status(web_db) if c.source == "p2p_rates"
    )

    assert chip.last_status == "success"


def test_sync_strip_polls_once_shortly_after_load(
    web_db: sqlite3.Connection, web_client_factory
) -> None:
    """The startup refresh lands in ~10s; a 60s-only poll shows stale chips."""
    client = web_client_factory()

    html = client.get("/").text

    strip = html.split('id="sync-status-strip"', 1)[1].split(">", 1)[0]
    assert "load delay:" in strip
    assert "every 60s" in strip
