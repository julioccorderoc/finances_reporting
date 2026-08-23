"""Confirming a P2P pairing rebuilds the realized cost basis (criterion H5).

ACCEPTANCE-CRITERIA H5: *confirming a P2P sell pairing makes it the realized
cost basis for VES rows in the following 14 days -- visible in later rows'
provenance.*

``apply_edit`` already keeps that bargain for the ``/transactions`` and triage
rate edits (ADR-013 Amendment 2026-07-26, ``test_realized_rebuild_on_edit``):
saving a ``user_rate`` on a P2P sell recomputes the materialised
``binance_p2p_realized`` tier before anything reads it.

``confirm_pair`` never joined it. A pair confirmation promotes both legs to
``kind='transfer'`` and shares a ``transfer_id``; it writes no rate, and
``SQL_P2P_SELLS`` is keyed off ``source_ref``/sign rather than ``kind``, so
the *set* of fills is unchanged by the write. What is not guaranteed is that
the set was ever **materialised**: the basis is only as fresh as the last
ingest, backfill or ``finances rates rebuild-realized``. A P2P sell that
reached the ledger by any other path -- a legacy backfill row, a hand-entered
fill, a restored snapshot -- carries its ``user_rate`` and contributes
nothing, and every bolivar row in the following fortnight prices off the
market median or BCV instead of what those bolivars actually cost.

Pair confirmation is the moment the owner asserts "these bolivars came from
that sell", which is exactly when the assertion should reach the tier that
encodes it. The rebuild is idempotent and derived wholly from
``transactions``, so the hook is safe to fire on every confirmation.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from finances.db.repos import accounts as accounts_repo
from finances.db.repos import categories as categories_repo
from finances.db.repos import rates as rates_repo
from finances.db.repos import transactions as transactions_repo
from finances.domain import rates as rates_engine
from finances.domain import realized_rates
from finances.domain.models import (
    Account,
    AccountKind,
    Rate,
    Transaction,
    TransactionKind,
)
from finances.domain.realized_rates import REALIZED_SOURCE
from finances.web.services.triage import confirm_pair


FILL_DAY = datetime(2026, 5, 10, 14, 0, tzinfo=UTC)
SPEND_DAY = datetime(2026, 5, 13, 9, 0, tzinfo=UTC)

# The fill's own price, and the market median standing in for it until the
# basis is materialised. Deliberately far apart so the provenance flip is
# also a visible change of number.
FILL_RATE = Decimal("60")
MEDIAN_RATE = Decimal("50")


@pytest.fixture
def pair_db(web_db: sqlite3.Connection) -> dict[str, int]:
    """One confirmable pair, one later bolivar spend, no realized basis.

    The ``rates`` table holds a market median only: the state of any ledger
    whose P2P history arrived without a rebuild behind it.
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

    sell = transactions_repo.insert(
        web_db,
        Transaction(
            account_id=binance.id,
            occurred_at=FILL_DAY,
            kind=TransactionKind.EXPENSE,
            amount=Decimal("-100.00"),
            currency="USDT",
            description=f"P2P SELL USDT @ {FILL_RATE} VES (order 90210)",
            user_rate=FILL_RATE,
            source="binance",
            source_ref="p2p:h5",
        ),
    )
    deposit = transactions_repo.insert(
        web_db,
        Transaction(
            account_id=provincial.id,
            occurred_at=FILL_DAY,
            kind=TransactionKind.INCOME,
            amount=Decimal("6000.00"),
            currency="VES",
            description="TRANSFERENCIA RECIBIDA",
            source="provincial",
            source_ref="prov:h5-deposit",
        ),
    )
    spend = transactions_repo.insert(
        web_db,
        Transaction(
            account_id=provincial.id,
            occurred_at=SPEND_DAY,
            kind=TransactionKind.EXPENSE,
            amount=Decimal("-600.00"),
            currency="VES",
            description="COM.PAGO bodega",
            category_id=groceries.id,
            source="provincial",
            source_ref="prov:h5-spend",
        ),
    )

    rates_repo.upsert(
        web_db,
        Rate(
            as_of_date=date(2026, 5, 10),
            base="USDT",
            quote="VES",
            rate=MEDIAN_RATE,
            source=rates_engine.BINANCE_P2P_SOURCE,
        ),
    )

    return {"sell": sell.id, "deposit": deposit.id, "spend": spend.id}


def _resolve_spend(conn: sqlite3.Connection, spend_id: int):
    txn = transactions_repo.get_by_id(conn, spend_id)
    assert txn is not None
    return rates_engine.resolve_detail(conn, txn)


def test_later_ves_row_prices_off_the_median_before_the_pair_is_confirmed(
    web_db: sqlite3.Connection, pair_db: dict[str, int]
) -> None:
    """The precondition H5 measures against: no realized tier yet."""
    before = _resolve_spend(web_db, pair_db["spend"])

    assert before.source.startswith(rates_engine.BINANCE_P2P_SOURCE)
    assert not before.source.startswith(REALIZED_SOURCE)
    assert before.rate == MEDIAN_RATE


def test_confirming_the_pair_makes_the_fill_the_realized_cost_basis(
    web_db: sqlite3.Connection, pair_db: dict[str, int]
) -> None:
    """H5 end to end: the later row's provenance flips to realized."""
    confirm_pair(
        web_db, deposit_id=pair_db["deposit"], sell_id=pair_db["sell"]
    )

    after = _resolve_spend(web_db, pair_db["spend"])

    assert after.source.startswith(REALIZED_SOURCE)
    assert after.rate == FILL_RATE
    # Three days later is well inside the tier's fortnight, so this is a
    # carry, not an approximation.
    assert after.approximate is False
    assert after.age_days == 3


def test_the_confirmation_materialises_the_fill_day_itself(
    web_db: sqlite3.Connection, pair_db: dict[str, int]
) -> None:
    """The realized row lands on the fill's own day, at its own rate."""
    assert (
        rates_repo.latest_on_or_before(
            web_db,
            as_of_date=date(2026, 5, 10),
            base="USDT",
            quote="VES",
            source=REALIZED_SOURCE,
        )
        is None
    )

    confirm_pair(
        web_db, deposit_id=pair_db["deposit"], sell_id=pair_db["sell"]
    )

    materialised = rates_repo.latest_on_or_before(
        web_db,
        as_of_date=date(2026, 5, 10),
        base="USDT",
        quote="VES",
        source=REALIZED_SOURCE,
    )
    assert materialised is not None
    assert materialised.as_of_date == date(2026, 5, 10)
    assert materialised.rate == FILL_RATE


def test_confirm_pair_calls_rebuild_once(
    web_db: sqlite3.Connection,
    pair_db: dict[str, int],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Patched on the owning module, like every other caller.

    ``ingest/binance.py`` and ``cli/main.py`` both reach it as
    ``realized_rates.rebuild(conn)``; the new hook must do the same or the
    recovery command and the write path drift apart.
    """
    real = realized_rates.rebuild
    calls: list[int] = []

    def _spy(conn: sqlite3.Connection) -> int:
        calls.append(1)
        return real(conn)

    monkeypatch.setattr(realized_rates, "rebuild", _spy)

    confirm_pair(
        web_db, deposit_id=pair_db["deposit"], sell_id=pair_db["sell"]
    )

    assert len(calls) == 1


def test_a_refused_pair_leaves_the_basis_untouched(
    web_db: sqlite3.Connection,
    pair_db: dict[str, int],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The rebuild sits after the guards, so a 400 writes nothing at all."""
    calls: list[int] = []
    monkeypatch.setattr(
        realized_rates, "rebuild", lambda conn: calls.append(1) or 0
    )

    # A deposit seven weeks from the fill: past MANUAL_PAIR_MAX_DAYS, so it
    # trips the same guard the modal's disabled button renders.
    far = transactions_repo.get_by_id(web_db, pair_db["deposit"])
    assert far is not None
    stale = transactions_repo.insert(
        web_db,
        Transaction(
            account_id=far.account_id,
            occurred_at=datetime(2026, 6, 30, tzinfo=UTC),
            kind=TransactionKind.INCOME,
            amount=Decimal("6000.00"),
            currency="VES",
            description="TRANSFERENCIA RECIBIDA",
            source="provincial",
            source_ref="prov:h5-far",
        ),
    )

    with pytest.raises(ValueError):
        confirm_pair(web_db, deposit_id=stale.id, sell_id=pair_db["sell"])

    assert calls == []
    assert (
        rates_repo.latest_on_or_before(
            web_db,
            as_of_date=date(2026, 5, 10),
            base="USDT",
            quote="VES",
            source=REALIZED_SOURCE,
        )
        is None
    )
