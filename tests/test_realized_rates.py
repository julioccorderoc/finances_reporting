"""Tests for finances.domain.realized_rates (ADR-013).

Derives the per-day *realized* VES rate — the rate the owner actually got
when selling USDT on Binance P2P — from the ``user_rate`` already stored on
every P2P fill. Same-day sells are volume-weighted (VWAP).

Selection deliberately keys off ``source_ref LIKE 'p2p:%'`` + a negative
amount rather than ``kind``: bank-anchored pairing (EPIC-006) promotes these
rows to ``kind='transfer'`` after the fact, so ``kind`` is not stable.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pytest
from typer.testing import CliRunner

from finances.db.repos import accounts as accounts_repo
from finances.db.repos import rates as rates_repo
from finances.db.repos import transactions as txn_repo
from finances.domain import rates as rates_engine
from finances.domain import realized_rates
from finances.domain.models import Rate, Transaction, TransactionKind

REALIZED_SOURCE = "binance_p2p_realized"


def _account_id(conn: sqlite3.Connection, name: str) -> int:
    for account in accounts_repo.list_all(conn):
        if account.name == name:
            assert account.id is not None
            return account.id
    raise AssertionError(f"seeded_db should provide a {name!r} account")


def _binance_account_id(conn: sqlite3.Connection) -> int:
    return _account_id(conn, "Binance Spot")


def _ves_account_id(conn: sqlite3.Connection) -> int:
    return _account_id(conn, "Provincial Bolivares")


def _p2p_fill(
    conn: sqlite3.Connection,
    *,
    account_id: int,
    day: date,
    usdt: str,
    rate: str,
    order: str,
    fiat: str = "VES",
    trade_type: str = "SELL",
    kind: TransactionKind = TransactionKind.EXPENSE,
) -> Transaction:
    """Insert one P2P fill shaped exactly like finances.ingest.binance emits it."""
    signed = -Decimal(usdt) if trade_type == "SELL" else Decimal(usdt)
    return txn_repo.insert(
        conn,
        Transaction(
            account_id=account_id,
            occurred_at=datetime(day.year, day.month, day.day, 12, 0, tzinfo=UTC),
            kind=kind,
            amount=signed,
            currency="USDT",
            description=f"P2P {trade_type} USDT @ {rate} {fiat} (order {order})",
            user_rate=Decimal(rate),
            source="binance",
            source_ref=f"p2p:{order}",
        ),
    )


def _rates_by_day(conn: sqlite3.Connection) -> dict[date, Decimal]:
    rows = conn.execute(
        "SELECT as_of_date, rate FROM rates WHERE source = ? ORDER BY as_of_date",
        (REALIZED_SOURCE,),
    ).fetchall()
    return {
        (
            row["as_of_date"]
            if isinstance(row["as_of_date"], date)
            else date.fromisoformat(row["as_of_date"])
        ): Decimal(str(row["rate"]))
        for row in rows
    }


# The `rate DECIMAL` column carries NUMERIC affinity, so sqlite coerces the
# stored decimal text to a float64 — a pre-existing property of every rate in
# this schema (bcv, binance_p2p_median included), not of this feature. The
# derivation itself stays exact in Decimal; only the round-trip quantizes, by
# ~1e-14 relative. Storage assertions therefore compare within tolerance while
# `compute_realized_rates` is asserted exactly.
_STORAGE_TOLERANCE = Decimal("1e-9")


def _assert_stored(actual: dict[date, Decimal], expected: dict[date, Decimal]) -> None:
    assert actual.keys() == expected.keys()
    for day, want in expected.items():
        assert abs(actual[day] - want) < _STORAGE_TOLERANCE, (
            f"{day}: stored {actual[day]}, expected ~{want}"
        )


# ---------------------------------------------------------------------------
# VWAP derivation
# ---------------------------------------------------------------------------


def test_single_sell_day_uses_its_own_rate(seeded_db: sqlite3.Connection) -> None:
    account_id = _binance_account_id(seeded_db)
    _p2p_fill(
        seeded_db, account_id=account_id, day=date(2025, 7, 1),
        usdt="100", rate="40", order="1",
    )

    results = realized_rates.compute_realized_rates(seeded_db)

    assert len(results) == 1
    assert results[0].as_of_date == date(2025, 7, 1)
    assert results[0].rate == Decimal("40")
    assert results[0].base == "USDT"
    assert results[0].quote == "VES"
    assert results[0].source == REALIZED_SOURCE


def test_same_day_sells_are_volume_weighted(seeded_db: sqlite3.Connection) -> None:
    account_id = _binance_account_id(seeded_db)
    day = date(2025, 7, 1)
    # 100 USDT @ 40 + 50 USDT @ 44 -> 6200 VES for 150 USDT -> 41.333...
    _p2p_fill(seeded_db, account_id=account_id, day=day, usdt="100", rate="40", order="1")
    _p2p_fill(seeded_db, account_id=account_id, day=day, usdt="50", rate="44", order="2")

    results = realized_rates.compute_realized_rates(seeded_db)

    assert len(results) == 1
    assert results[0].rate == Decimal("6200") / Decimal("150")


def test_separate_days_produce_separate_rates(seeded_db: sqlite3.Connection) -> None:
    account_id = _binance_account_id(seeded_db)
    _p2p_fill(
        seeded_db, account_id=account_id, day=date(2025, 7, 1),
        usdt="100", rate="40", order="1",
    )
    _p2p_fill(
        seeded_db, account_id=account_id, day=date(2025, 7, 8),
        usdt="100", rate="50", order="2",
    )

    results = realized_rates.compute_realized_rates(seeded_db)

    assert [(r.as_of_date, r.rate) for r in results] == [
        (date(2025, 7, 1), Decimal("40")),
        (date(2025, 7, 8), Decimal("50")),
    ]


# ---------------------------------------------------------------------------
# Selection rules
# ---------------------------------------------------------------------------


def test_p2p_buys_are_excluded(seeded_db: sqlite3.Connection) -> None:
    """A BUY spends VES to get USDT — a disposal, not an acquisition."""
    account_id = _binance_account_id(seeded_db)
    _p2p_fill(
        seeded_db, account_id=account_id, day=date(2025, 7, 1),
        usdt="100", rate="99", order="1",
        trade_type="BUY", kind=TransactionKind.INCOME,
    )

    assert realized_rates.compute_realized_rates(seeded_db) == []


def test_sell_promoted_to_transfer_is_still_included(
    seeded_db: sqlite3.Connection,
) -> None:
    """Bank-anchored pairing rewrites kind to 'transfer'; selection must survive it."""
    account_id = _binance_account_id(seeded_db)
    _p2p_fill(
        seeded_db, account_id=account_id, day=date(2025, 7, 1),
        usdt="100", rate="40", order="1", kind=TransactionKind.TRANSFER,
    )

    results = realized_rates.compute_realized_rates(seeded_db)

    assert len(results) == 1
    assert results[0].rate == Decimal("40")


def test_non_p2p_rows_are_excluded(seeded_db: sqlite3.Connection) -> None:
    """A plain expense carrying a manual user_rate is not an acquisition."""
    account_id = _binance_account_id(seeded_db)
    txn_repo.insert(
        seeded_db,
        Transaction(
            account_id=account_id,
            occurred_at=datetime(2025, 7, 1, 12, 0, tzinfo=UTC),
            kind=TransactionKind.EXPENSE,
            amount=Decimal("-500"),
            currency="VES",
            description="groceries",
            user_rate=Decimal("77"),
            source="provincial",
            source_ref="row:1",
        ),
    )

    assert realized_rates.compute_realized_rates(seeded_db) == []


def test_sell_without_user_rate_is_excluded(seeded_db: sqlite3.Connection) -> None:
    account_id = _binance_account_id(seeded_db)
    txn_repo.insert(
        seeded_db,
        Transaction(
            account_id=account_id,
            occurred_at=datetime(2025, 7, 1, 12, 0, tzinfo=UTC),
            kind=TransactionKind.EXPENSE,
            amount=Decimal("-100"),
            currency="USDT",
            description="P2P SELL USDT @ ? VES (order 9)",
            user_rate=None,
            source="binance",
            source_ref="p2p:9",
        ),
    )

    assert realized_rates.compute_realized_rates(seeded_db) == []


def test_non_ves_fiat_sell_is_skipped(seeded_db: sqlite3.Connection) -> None:
    """A COP-denominated sell must never be folded into the VES VWAP."""
    account_id = _binance_account_id(seeded_db)
    day = date(2025, 7, 1)
    _p2p_fill(
        seeded_db, account_id=account_id, day=day,
        usdt="100", rate="40", order="1", fiat="VES",
    )
    _p2p_fill(
        seeded_db, account_id=account_id, day=day,
        usdt="100", rate="4000", order="2", fiat="COP",
    )

    results = realized_rates.compute_realized_rates(seeded_db)

    assert len(results) == 1
    assert results[0].rate == Decimal("40")


# ---------------------------------------------------------------------------
# rebuild()
# ---------------------------------------------------------------------------


def test_rebuild_writes_rates_rows(seeded_db: sqlite3.Connection) -> None:
    account_id = _binance_account_id(seeded_db)
    _p2p_fill(
        seeded_db, account_id=account_id, day=date(2025, 7, 1),
        usdt="100", rate="40", order="1",
    )

    written = realized_rates.rebuild(seeded_db)

    assert written == 1
    _assert_stored(_rates_by_day(seeded_db), {date(2025, 7, 1): Decimal("40")})


def test_rebuild_is_idempotent(seeded_db: sqlite3.Connection) -> None:
    account_id = _binance_account_id(seeded_db)
    _p2p_fill(
        seeded_db, account_id=account_id, day=date(2025, 7, 1),
        usdt="100", rate="40", order="1",
    )

    realized_rates.rebuild(seeded_db)
    realized_rates.rebuild(seeded_db)

    _assert_stored(_rates_by_day(seeded_db), {date(2025, 7, 1): Decimal("40")})


def test_rebuild_reflects_new_fills_on_rerun(seeded_db: sqlite3.Connection) -> None:
    account_id = _binance_account_id(seeded_db)
    day = date(2025, 7, 1)
    _p2p_fill(seeded_db, account_id=account_id, day=day, usdt="100", rate="40", order="1")
    realized_rates.rebuild(seeded_db)

    _p2p_fill(seeded_db, account_id=account_id, day=day, usdt="50", rate="44", order="2")
    realized_rates.rebuild(seeded_db)

    _assert_stored(
        _rates_by_day(seeded_db),
        {date(2025, 7, 1): Decimal("6200") / Decimal("150")},
    )


def test_rebuild_with_no_p2p_history_writes_nothing(
    seeded_db: sqlite3.Connection,
) -> None:
    assert realized_rates.rebuild(seeded_db) == 0
    assert _rates_by_day(seeded_db) == {}


# ---------------------------------------------------------------------------
# Pruning: the materialised set mirrors the fills, it does not accumulate.
#
# rebuild() used to only upsert, so a day that stopped qualifying kept the
# rate row it was last given. The resolver would then price bolivar spending
# off a fill that no longer exists — and because the realized tier outranks
# the market median, a stale row wins over a correct one.
# ---------------------------------------------------------------------------


def test_rebuild_drops_a_day_whose_last_fill_lost_its_rate(
    seeded_db: sqlite3.Connection,
) -> None:
    """Clearing the only fill of a day must remove that day's rate."""
    account_id = _binance_account_id(seeded_db)
    fill = _p2p_fill(
        seeded_db, account_id=account_id, day=date(2025, 7, 1),
        usdt="100", rate="40", order="1",
    )
    realized_rates.rebuild(seeded_db)
    assert _rates_by_day(seeded_db) != {}

    txn_repo.update(seeded_db, id=fill.id, user_rate=None)

    assert realized_rates.rebuild(seeded_db) == 0
    assert _rates_by_day(seeded_db) == {}


def test_rebuild_keeps_days_that_still_qualify(
    seeded_db: sqlite3.Connection,
) -> None:
    """Pruning is surgical: only the day that stopped qualifying goes."""
    account_id = _binance_account_id(seeded_db)
    gone = _p2p_fill(
        seeded_db, account_id=account_id, day=date(2025, 7, 1),
        usdt="100", rate="40", order="1",
    )
    _p2p_fill(
        seeded_db, account_id=account_id, day=date(2025, 7, 2),
        usdt="100", rate="44", order="2",
    )
    realized_rates.rebuild(seeded_db)

    txn_repo.update(seeded_db, id=gone.id, user_rate=None)
    realized_rates.rebuild(seeded_db)

    _assert_stored(_rates_by_day(seeded_db), {date(2025, 7, 2): Decimal("44")})


def test_rebuild_drops_a_day_whose_fill_was_deleted(
    seeded_db: sqlite3.Connection,
) -> None:
    """The other drift ADR-013 §2 names: a P2P transaction deleted."""
    account_id = _binance_account_id(seeded_db)
    fill = _p2p_fill(
        seeded_db, account_id=account_id, day=date(2025, 7, 1),
        usdt="100", rate="40", order="1",
    )
    realized_rates.rebuild(seeded_db)

    seeded_db.execute("DELETE FROM transactions WHERE id = ?", (fill.id,))
    realized_rates.rebuild(seeded_db)

    assert _rates_by_day(seeded_db) == {}


def test_rebuild_leaves_other_sources_alone(
    seeded_db: sqlite3.Connection,
) -> None:
    """Pruning is scoped to the source this module owns.

    ``binance_p2p_median`` and ``bcv`` rows on the same day belong to the
    ingesters, and rule-005 keeps them as independent tiers.
    """
    account_id = _binance_account_id(seeded_db)
    fill = _p2p_fill(
        seeded_db, account_id=account_id, day=date(2025, 7, 1),
        usdt="100", rate="40", order="1",
    )
    for source in ("binance_p2p_median", "bcv"):
        rates_repo.upsert(
            seeded_db,
            Rate(
                as_of_date=date(2025, 7, 1),
                base="USDT" if source != "bcv" else "USD",
                quote="VES",
                rate=Decimal("41"),
                source=source,
            ),
        )
    realized_rates.rebuild(seeded_db)

    txn_repo.update(seeded_db, id=fill.id, user_rate=None)
    realized_rates.rebuild(seeded_db)

    survivors = seeded_db.execute(
        "SELECT source FROM rates ORDER BY source"
    ).fetchall()
    assert [row["source"] for row in survivors] == ["bcv", "binance_p2p_median"]


def test_pruning_a_stale_day_restores_the_market_tier(
    seeded_db: sqlite3.Connection,
) -> None:
    """Why it matters: the realized tier outranks the market median.

    A stale realized row does not merely linger — it keeps winning the
    resolver chain over the market rate that is now the honest answer.
    """
    account_id = _binance_account_id(seeded_db)
    day = date(2025, 7, 1)
    fill = _p2p_fill(
        seeded_db, account_id=account_id, day=day,
        usdt="100", rate="40", order="1",
    )
    rates_repo.upsert(
        seeded_db,
        Rate(
            as_of_date=day, base="USDT", quote="VES",
            rate=Decimal("55"), source="binance_p2p_median",
        ),
    )
    realized_rates.rebuild(seeded_db)

    spend = txn_repo.insert(
        seeded_db,
        Transaction(
            account_id=_ves_account_id(seeded_db),
            occurred_at=datetime(2025, 7, 1, 15, 0, tzinfo=UTC),
            kind=TransactionKind.EXPENSE,
            amount=Decimal("-4000.00"),
            currency="VES",
            description="COM.PAGO bodega",
            source="provincial",
            source_ref="prune-spend",
        ),
    )
    assert rates_engine.resolve(seeded_db, spend)[1] == REALIZED_SOURCE

    txn_repo.update(seeded_db, id=fill.id, user_rate=None)
    realized_rates.rebuild(seeded_db)

    rate, source = rates_engine.resolve(seeded_db, spend)
    assert source == "binance_p2p_median"
    assert rate == Decimal("55")


# ---------------------------------------------------------------------------
# CLI: finances rates rebuild-realized
#
# The manual recovery path — used for the one-time backfill over existing P2P
# history, and whenever a P2P transaction is edited or deleted and the derived
# rates need to catch up.
# ---------------------------------------------------------------------------


@pytest.fixture
def cli_db(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    from finances import config
    from finances.db.connection import get_connection
    from finances.db.migrate import apply_migrations
    from finances.domain.models import Account, AccountKind

    db_file = tmp_path / "cli-realized.db"
    conn = get_connection(db_file)
    apply_migrations(conn)
    account = accounts_repo.insert(
        conn,
        Account(
            name="Binance Spot",
            kind=AccountKind.CRYPTO_SPOT,
            currency="USDT",
            institution="Binance",
        ),
    )
    assert account.id is not None
    _p2p_fill(
        conn, account_id=account.id, day=date(2025, 7, 1),
        usdt="100", rate="40", order="1",
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(config, "DB_PATH", db_file)
    return db_file


def test_cli_rebuild_realized_materializes_rates(cli_db: Path) -> None:
    from finances.cli.main import app
    from finances.db.connection import get_connection

    result = CliRunner().invoke(app, ["rates", "rebuild-realized"])

    assert result.exit_code == 0, result.output
    conn = get_connection(cli_db)
    try:
        assert _rates_by_day(conn) == {date(2025, 7, 1): Decimal("40")}
    finally:
        conn.close()


def test_cli_rebuild_realized_reports_count(cli_db: Path) -> None:
    from finances.cli.main import app

    result = CliRunner().invoke(app, ["rates", "rebuild-realized"])

    assert result.exit_code == 0, result.output
    assert "1" in result.output


def test_fill_with_unparseable_description_is_treated_as_ves(
    seeded_db: sqlite3.Connection,
) -> None:
    """ADR-013 §5: an unparseable description falls back to VES, not skipped.

    The ledger is VES-only in practice, so a row whose text does not match the
    ingester's shape is far more likely to be VES than a second currency.
    """
    account_id = _binance_account_id(seeded_db)
    txn_repo.insert(
        seeded_db,
        Transaction(
            account_id=account_id,
            occurred_at=datetime(2025, 7, 1, 12, 0, tzinfo=UTC),
            kind=TransactionKind.EXPENSE,
            amount=Decimal("-100"),
            currency="USDT",
            description="hand-edited note with no fiat token",
            user_rate=Decimal("40"),
            source="binance",
            source_ref="p2p:edited",
        ),
    )

    results = realized_rates.compute_realized_rates(seeded_db)

    assert len(results) == 1
    assert results[0].rate == Decimal("40")


def test_fill_with_null_description_is_treated_as_ves(
    seeded_db: sqlite3.Connection,
) -> None:
    account_id = _binance_account_id(seeded_db)
    txn_repo.insert(
        seeded_db,
        Transaction(
            account_id=account_id,
            occurred_at=datetime(2025, 7, 1, 12, 0, tzinfo=UTC),
            kind=TransactionKind.EXPENSE,
            amount=Decimal("-100"),
            currency="USDT",
            description=None,
            user_rate=Decimal("40"),
            source="binance",
            source_ref="p2p:nodesc",
        ),
    )

    results = realized_rates.compute_realized_rates(seeded_db)

    assert len(results) == 1
    assert results[0].rate == Decimal("40")
