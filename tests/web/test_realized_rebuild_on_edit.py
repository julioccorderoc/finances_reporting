"""A web rate edit rebuilds the realized cost basis (ADR-013 Amendment 2026-07-26).

ADR-013 §2 materialises the ``binance_p2p_realized`` tier and buys off the
resulting drift with ``realized_rates.rebuild()`` -- "called at the end of
every Binance ingest and exposed as ``finances rates rebuild-realized``".
The ADR-012 viewer write path landed later and never joined that bargain.

``SQL_P2P_SELLS`` derives the basis from ``source_ref LIKE 'p2p:%' AND
amount < 0 AND user_rate IS NOT NULL``, so saving a ``user_rate`` on a P2P
sell through the viewer edits an *input* to a materialised tier without
recomputing it: every bolivar row keeps its old USD value until the next
Binance ingest re-prices all of them at once.

The hook goes in ``apply_edit`` -- the single choke point for every web
``user_rate`` write (triage modal, /transactions modal, PATCH /api) -- and
fires on a narrow predicate: the request set ``user_rate``, the row has the
``SQL_P2P_SELLS`` shape, and the value actually changed.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient

from finances.db.repos import accounts as accounts_repo
from finances.db.repos import categories as categories_repo
from finances.db.repos import rates as rates_repo
from finances.db.repos import transactions as transactions_repo
from finances.domain import rates as rates_engine
from finances.domain import realized_rates
from finances.domain.models import (
    Account,
    AccountKind,
    Transaction,
    TransactionKind,
)
from finances.domain.realized_rates import REALIZED_SOURCE
from finances.web.services.transactions_write import (
    TransactionEditRequest,
    apply_edit,
)


FILL_DAY = datetime(2026, 5, 10, tzinfo=UTC)
SPEND_DAY = datetime(2026, 5, 12, tzinfo=UTC)


# ---------------------------------------------------------------------------
# Fixtures.
# ---------------------------------------------------------------------------


@pytest.fixture
def fills_db(web_db: sqlite3.Connection) -> sqlite3.Connection:
    """Two same-day P2P fills plus one bolivar expense priced off them.

    Fills are USDT-denominated with negative amounts and the ingester's
    description shape, matching the live ledger exactly -- the derivation
    keys off ``source_ref``/sign, not ``kind``, because bank-anchored
    pairing later promotes these rows to ``kind='transfer'``.

    Fill A carries a rate; fill B does not (the live ledger has three such
    rows, ingested with ``unitPrice`` 0). The basis is materialised once
    at the end, standing in for the rebuild an ingest would have run.
    """
    binance = accounts_repo.insert(
        web_db,
        Account(
            name="Binance Spot",
            kind=AccountKind.CRYPTO_SPOT,
            currency="USDT",
            institution="Binance",
        ),
    )
    provincial = accounts_repo.insert(
        web_db,
        Account(
            name="Provincial",
            kind=AccountKind.BANK,
            currency="VES",
            institution="Provincial",
        ),
    )
    groceries = categories_repo.get_by_name(
        web_db, TransactionKind.EXPENSE, "Groceries"
    )
    assert groceries is not None

    def _fill(ref: str, amount: str, rate: str | None, order: str) -> int:
        shown = rate if rate is not None else "0"
        row = transactions_repo.insert(
            web_db,
            Transaction(
                account_id=binance.id,
                occurred_at=FILL_DAY,
                kind=TransactionKind.EXPENSE,
                amount=Decimal(amount),
                currency="USDT",
                description=f"P2P SELL USDT @ {shown} VES (order {order})",
                category_id=groceries.id,
                user_rate=Decimal(rate) if rate is not None else None,
                source="binance",
                source_ref=ref,
            ),
        )
        return row.id

    _fill("p2p:a", "-100.00", "50", "1001")
    _fill("p2p:b", "-100.00", None, "1002")

    transactions_repo.insert(
        web_db,
        Transaction(
            account_id=provincial.id,
            occurred_at=SPEND_DAY,
            kind=TransactionKind.EXPENSE,
            amount=Decimal("-7500.00"),
            currency="VES",
            description="COM.PAGO bodega",
            category_id=groceries.id,
            source="provincial",
            source_ref="prov-spend",
        ),
    )

    realized_rates.rebuild(web_db)
    return web_db


@pytest.fixture
def rebuild_spy(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Record calls to ``rebuild`` while still performing them.

    Patched on the owning module: both existing callers reach it as
    ``realized_rates.rebuild(conn)`` (``ingest/binance.py:794``,
    ``cli/main.py:502``) and the new hook must do the same.
    """
    calls: list[str] = []
    real = realized_rates.rebuild

    def _spy(conn: sqlite3.Connection) -> int:
        calls.append("rebuild")
        return real(conn)

    monkeypatch.setattr(realized_rates, "rebuild", _spy)
    return calls


def _txn_id(conn: sqlite3.Connection, ref: str) -> int:
    row = conn.execute(
        "SELECT id FROM transactions WHERE source_ref = ?", (ref,)
    ).fetchone()
    assert row is not None
    return int(row["id"])


def _basis(conn: sqlite3.Connection) -> Decimal | None:
    rate = rates_repo.latest_on_or_before(
        conn,
        as_of_date=FILL_DAY.date(),
        base="USDT",
        quote="VES",
        source=REALIZED_SOURCE,
    )
    return None if rate is None else rate.rate


def _spend_rate(conn: sqlite3.Connection) -> tuple[Decimal | None, str]:
    txn = transactions_repo.get_by_id(conn, _txn_id(conn, "prov-spend"))
    assert txn is not None
    return rates_engine.resolve(conn, txn)


def _rate_edit(value: str | None) -> TransactionEditRequest:
    return TransactionEditRequest(
        set_user_rate=True,
        user_rate=None if value is None else Decimal(value),
    )


# ---------------------------------------------------------------------------
# The basis moves when a fill's rate is edited.
# ---------------------------------------------------------------------------


def test_the_fixture_starts_from_one_priced_fill(
    fills_db: sqlite3.Connection,
) -> None:
    """Guard: only fill A is in the basis, so the day's VWAP is its rate."""
    assert _basis(fills_db) == Decimal("50")


def test_saving_a_rate_on_a_p2p_sell_rebuilds_the_basis(
    fills_db: sqlite3.Connection,
) -> None:
    """Fill B joins the average: VWAP(100@50, 100@100) = 75."""
    apply_edit(fills_db, txn_id=_txn_id(fills_db, "p2p:b"), req=_rate_edit("100"))

    assert _basis(fills_db) == Decimal("75")


def test_the_edit_reprices_bolivar_spending_immediately(
    fills_db: sqlite3.Connection,
) -> None:
    """The point of the fix: no waiting for the next ingest.

    7500 VES at the old 50 basis is 150 USD; at the rebuilt 75 it is 100.

    The spend is two days after the fill, so the resolver reports the
    carried-forward label (ADR-013 §3) rather than the bare source.
    """
    carried = REALIZED_SOURCE + "_carry"

    before_rate, before_source = _spend_rate(fills_db)
    assert before_source == carried
    assert before_rate == Decimal("50")

    apply_edit(fills_db, txn_id=_txn_id(fills_db, "p2p:b"), req=_rate_edit("100"))

    after_rate, after_source = _spend_rate(fills_db)
    assert after_source == carried
    assert after_rate == Decimal("75")


def test_changing_an_existing_rate_rebuilds(
    fills_db: sqlite3.Connection,
) -> None:
    apply_edit(fills_db, txn_id=_txn_id(fills_db, "p2p:a"), req=_rate_edit("80"))

    assert _basis(fills_db) == Decimal("80")


def test_clearing_a_rate_rebuilds(
    fills_db: sqlite3.Connection, rebuild_spy: list[str]
) -> None:
    """Clearing removes a fill from the average, so it must re-derive.

    Fill B is given a rate first, so the day still has a priced fill after
    A is cleared -- ``rebuild()`` upserts and never deletes, so a day that
    loses its last fill keeps its stale row (noted in the amendment's
    consequences, pre-existing and shared with the ingest caller).
    """
    apply_edit(fills_db, txn_id=_txn_id(fills_db, "p2p:b"), req=_rate_edit("100"))
    assert _basis(fills_db) == Decimal("75")

    apply_edit(fills_db, txn_id=_txn_id(fills_db, "p2p:a"), req=_rate_edit(None))

    assert _basis(fills_db) == Decimal("100")
    assert len(rebuild_spy) == 2


# ---------------------------------------------------------------------------
# The trigger is narrow: only a real rate change on a real fill.
# ---------------------------------------------------------------------------


def test_a_category_only_save_does_not_rebuild(
    fills_db: sqlite3.Connection, rebuild_spy: list[str]
) -> None:
    dating = categories_repo.get_by_name(fills_db, TransactionKind.EXPENSE, "Dating")
    assert dating is not None

    apply_edit(
        fills_db,
        txn_id=_txn_id(fills_db, "p2p:a"),
        req=TransactionEditRequest(set_category=True, category_id=dating.id),
    )

    assert rebuild_spy == []


def test_a_notes_only_save_does_not_rebuild(
    fills_db: sqlite3.Connection, rebuild_spy: list[str]
) -> None:
    apply_edit(
        fills_db,
        txn_id=_txn_id(fills_db, "p2p:a"),
        req=TransactionEditRequest(set_notes=True, notes="checked against the app"),
    )

    assert rebuild_spy == []


def test_resaving_the_same_rate_does_not_rebuild(
    fills_db: sqlite3.Connection, rebuild_spy: list[str]
) -> None:
    """A no-op save is a keystroke, not a change of basis."""
    apply_edit(fills_db, txn_id=_txn_id(fills_db, "p2p:a"), req=_rate_edit("50"))

    assert rebuild_spy == []


def test_a_rate_edit_on_a_non_p2p_row_does_not_rebuild(
    fills_db: sqlite3.Connection, rebuild_spy: list[str]
) -> None:
    """A manual override on a bolivar expense is tier 1, not an input."""
    apply_edit(
        fills_db, txn_id=_txn_id(fills_db, "prov-spend"), req=_rate_edit("42")
    )

    assert rebuild_spy == []


def test_a_rate_edit_on_a_p2p_buy_does_not_rebuild(
    fills_db: sqlite3.Connection, rebuild_spy: list[str]
) -> None:
    """The basis is acquisition-side only: ``amount < 0`` in SQL_P2P_SELLS."""
    buy = transactions_repo.insert(
        fills_db,
        Transaction(
            account_id=_txn_account(fills_db, "p2p:a"),
            occurred_at=FILL_DAY,
            kind=TransactionKind.INCOME,
            amount=Decimal("100.00"),
            currency="USDT",
            description="P2P BUY USDT @ 50 VES (order 1003)",
            source="binance",
            source_ref="p2p:buy",
        ),
    )

    apply_edit(fills_db, txn_id=buy.id, req=_rate_edit("60"))

    assert rebuild_spy == []


def _txn_account(conn: sqlite3.Connection, ref: str) -> int:
    txn = transactions_repo.get_by_id(conn, _txn_id(conn, ref))
    assert txn is not None
    return txn.account_id


# ---------------------------------------------------------------------------
# Ordering: nothing in the response is derived from an invalidated basis.
# ---------------------------------------------------------------------------


def test_rebuild_runs_before_the_post_write_resolve(
    fills_db: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Defensive, not load-bearing -- and cheap enough to keep either way.

    Every fill in the live ledger is USDT, so the edited row resolves
    natively and its own ``needs_review`` cannot depend on the basis. The
    ordering holds the invariant for a fill that is one day not USDT.
    """
    order: list[str] = []
    real_rebuild = realized_rates.rebuild
    real_resolve = rates_engine.resolve

    def _rebuild(conn: sqlite3.Connection) -> int:
        order.append("rebuild")
        return real_rebuild(conn)

    def _resolve(conn, txn):
        order.append("resolve")
        return real_resolve(conn, txn)

    monkeypatch.setattr(realized_rates, "rebuild", _rebuild)
    monkeypatch.setattr(rates_engine, "resolve", _resolve)

    apply_edit(fills_db, txn_id=_txn_id(fills_db, "p2p:b"), req=_rate_edit("100"))

    assert order.index("rebuild") < order.index("resolve")


# ---------------------------------------------------------------------------
# Every web write path, not just triage.
# ---------------------------------------------------------------------------


def _form(rate: str) -> dict[str, str]:
    return {
        "set_category": "false",
        "category_id": "",
        "set_user_rate": "true",
        "user_rate": rate,
        "set_notes": "false",
        "notes": "",
    }


def test_the_triage_modal_save_rebuilds(
    fills_db: sqlite3.Connection, web_client_factory, rebuild_spy: list[str]
) -> None:
    client: TestClient = web_client_factory()

    resp = client.post(
        f"/_partial/triage/{_txn_id(fills_db, 'p2p:b')}/edit", data=_form("100")
    )

    assert resp.status_code == 200, resp.text
    assert rebuild_spy == ["rebuild"]
    assert _basis(fills_db) == Decimal("75")


def test_the_transactions_modal_save_rebuilds(
    fills_db: sqlite3.Connection, web_client_factory, rebuild_spy: list[str]
) -> None:
    """Same bug, different screen -- the hook is shared, not triage-local."""
    client: TestClient = web_client_factory()

    resp = client.post(
        f"/_partial/transactions/{_txn_id(fills_db, 'p2p:b')}/edit",
        data=_form("100"),
    )

    assert resp.status_code == 200, resp.text
    assert rebuild_spy == ["rebuild"]
    assert _basis(fills_db) == Decimal("75")


def test_the_json_api_patch_rebuilds(
    fills_db: sqlite3.Connection, web_client_factory, rebuild_spy: list[str]
) -> None:
    """The EPIC-016 surface writes through the same service function."""
    client: TestClient = web_client_factory()

    resp = client.patch(
        f"/api/transactions/{_txn_id(fills_db, 'p2p:b')}",
        json={"set_user_rate": True, "user_rate": "100"},
    )

    assert resp.status_code == 200, resp.text
    assert rebuild_spy == ["rebuild"]
    assert _basis(fills_db) == Decimal("75")
