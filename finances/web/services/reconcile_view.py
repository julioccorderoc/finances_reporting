"""Set balance: the viewer's surface for ADR-018 reconciliation adjustments.

``finances.domain.reconciliation_adjustments`` has been able to close a
position to a custodian's figure since 2026-08-04, and only ``finances
reconcile balances`` ever called it. This module is the read-only half of
the same act, plus the one argument a CLI flag cannot make.

**Why a preview exists at all.** A plug absorbs whatever is wrong upstream
without distinguishing missing history from a bug — that is ADR-020 §1.2,
written after three adjustments were sized against balances a duplicate
sync had corrupted, and it happened again on 2026-09-03 when ten Binance
Pay twins were found double-counting 2,260.72 USDT. Both times the ledger
looked exactly like a ledger missing history. So before the viewer will
write anything it lists, for the last 60 days on that account, the four
shapes that produce a false gap:

* **unpaired** — a transfer leg or a P2P leg with no counterpart. Money
  that moved and was recorded once (rule-002).
* **twins** — same day, same amount, same currency, more than one row. The
  Sitting A shape.
* **uncategorised** — a row nobody has looked at yet.
* **approximate** — priced from a nearest rate (ADR-021 ``*_nearest``), so
  its USD figure is an estimate.

Each row links to its modal, because the answer to "that is a duplicate" is
to open it and delete it, not to plug around it.

Read-only apart from :func:`write_adjustment`, which delegates every write
to the domain function (rule-012: nothing else may create an adjustment).
"""

from __future__ import annotations

import sqlite3
from datetime import date, datetime, timedelta
from decimal import Decimal

from pydantic import BaseModel, ConfigDict

from finances.db.repos import accounts as accounts_repo
from finances.domain import money
from finances.domain.reconciliation_adjustments import (
    NEGLIGIBLE,
    AdjustmentResult,
    position_balance,
    record_adjustment,
)
from finances.web.services.transactions_query import (
    TXN_QUERY_BASE,
    _project_card,
    _row_to_transaction,
)

#: How far back the panel looks for an explanation. Long enough to cover a
#: monthly statement cycle and the pairing window either side of it, short
#: enough that the list stays readable — the point is evidence the owner
#: can actually walk, not every row the account has ever held.
LOOKBACK_DAYS = 60


class ReconcileRow(BaseModel):
    """One row the owner should look at before plugging."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    transaction_id: int
    occurred_at: datetime
    description: str
    amount_native: Decimal
    currency: str
    modal_url: str


class ReconcileReason(BaseModel):
    """One shape of false gap, and the rows that have it."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    key: str
    label: str
    hint: str
    rows: list[ReconcileRow]


class ReconcilePreview(BaseModel):
    """The difference, and what might explain it."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    account_id: int
    account_name: str
    currency: str
    ledger_balance: Decimal
    ledger_plain: str
    actual_balance: Decimal
    actual_plain: str
    delta: Decimal
    matches: bool
    window_days: int
    since: date
    reasons: list[ReconcileReason]


def plain(value: Decimal) -> str:
    """Render a Decimal for an ``<input value>``: no exponent, at least 2dp.

    Two places is a floor, never a ceiling. A crypto position carries eight
    decimals, and rounding the pre-filled figure to cents would make an
    *unchanged* submission a different number from the ledger — so leaving
    the field alone and pressing the button would write a dust adjustment
    for the rounding. ``NEGLIGIBLE`` is 1e-8, not a cent.

    The floor is for the common case: ``Decimal("15")`` renders as ``"15"``
    and a balance field that sometimes shows cents and sometimes does not
    reads like two different controls.
    """
    normalised = value.normalize()
    _sign, _digits, exponent = normalised.as_tuple()
    if not isinstance(exponent, int) or exponent > -2:
        normalised = normalised.quantize(Decimal("0.01"))
    return format(normalised, "f")


# ---------------------------------------------------------------------------
# The four explanations
# ---------------------------------------------------------------------------

# Every clause is scoped to one account and one window. None is scoped to
# the reconciled *currency*: mistaking one asset for another on the same
# account is itself a way to misread a gap (v_account_balances folds
# Binance Spot's USDC into its USDT figure), so a USDC twin stays visible
# while a USDT position is being reconciled. Each row names its own
# currency and the owner decides.
_WINDOW = """
      AND t.account_id = :account_id
      AND t.occurred_at >= :since
"""

_SQL_UNPAIRED = (
    TXN_QUERY_BASE
    + """
    WHERE t.transfer_id IS NULL
      AND (t.kind = 'transfer' OR t.source_ref LIKE 'p2p:%')
"""
    + _WINDOW
    + " ORDER BY t.occurred_at DESC, t.id DESC"
)

# ``amount`` is TEXT (ADR-009 keeps money out of float), so ``=`` here is a
# string comparison: "-113.04" matches "-113.04" and would not match
# "-113.0400". That is the right trade for this list. A twin is two rows the
# same ingest wrote for one event, so they carry byte-identical amounts;
# casting to REAL to be thorough would put the whole check back on the
# float arithmetic every other query in this project avoids.
_SQL_TWINS = (
    TXN_QUERY_BASE
    + """
    WHERE EXISTS (
            SELECT 1 FROM transactions AS other
             WHERE other.id <> t.id
               AND other.account_id = t.account_id
               AND other.currency = t.currency
               AND other.amount = t.amount
               AND substr(other.occurred_at, 1, 10) = substr(t.occurred_at, 1, 10)
          )
"""
    + _WINDOW
    + " ORDER BY t.occurred_at DESC, t.amount, t.id"
)

_SQL_UNCATEGORISED = (
    TXN_QUERY_BASE
    + """
    WHERE t.category_id IS NULL
      AND t.kind NOT IN ('transfer', 'adjustment')
"""
    + _WINDOW
    + " ORDER BY t.occurred_at DESC, t.id DESC"
)


def _to_row(conn: sqlite3.Connection, row: sqlite3.Row) -> ReconcileRow:
    txn = _row_to_transaction(row)
    assert txn.id is not None
    return ReconcileRow(
        transaction_id=txn.id,
        occurred_at=txn.occurred_at,
        description=txn.description or "(no description)",
        amount_native=txn.amount,
        currency=txn.currency,
        modal_url=f"/_partial/transactions/{txn.id}/modal",
    )


def _approximate_rows(
    conn: sqlite3.Connection, *, account_id: int, since: str
) -> list[ReconcileRow]:
    """Rows the resolver could only price from a nearest rate (ADR-021).

    Candidates are narrowed in SQL the way the triage queue narrows them: a
    native-USD row is 1:1 and a row carrying ``user_rate`` is priced by
    that, so neither can ever be approximate. Everything else is asked of
    the resolver, through ``_project_card`` — the same projection every
    other surface reads ``approximate`` off, never a second chain
    (rule-005).
    """
    natives = sorted(money.NATIVE_USD_CURRENCIES)
    placeholders = ",".join("?" * len(natives))
    rows = conn.execute(
        TXN_QUERY_BASE
        + f"""
        WHERE t.account_id = ?
          AND t.occurred_at >= ?
          AND t.user_rate IS NULL
          AND t.currency NOT IN ({placeholders})
        ORDER BY t.occurred_at DESC, t.id DESC
        """,  # noqa: S608 - placeholders only, values are bound
        (account_id, since, *natives),
    ).fetchall()

    out: list[ReconcileRow] = []
    for row in rows:
        card = _project_card(
            conn,
            _row_to_transaction(row),
            account_name=row["account_name"],
            category_name=row["category_name"],
        )
        if card.approximate:
            out.append(_to_row(conn, row))
    return out


_REASON_TEXT: dict[str, tuple[str, str]] = {
    "unpaired": (
        "Unpaired legs",
        "Money that moved with nothing recording where it landed. Pair it "
        "and the position moves on its own.",
    ),
    "twins": (
        "Same-day, same-amount twins",
        "Two rows that could be one event ingested twice. Ten of these "
        "double-counted 2,260.72 USDT before anyone noticed.",
    ),
    "uncategorised": (
        "Nobody has looked at these",
        "Uncategorised rows in the window. A miscategorised transfer is a "
        "gap in both directions.",
    ),
    "approximate": (
        "Priced from a distant rate",
        "ADR-021 approximate pricing: the USD figure is an estimate, so a "
        "USD-denominated difference here may be arithmetic, not history.",
    ),
}


def build_preview(
    conn: sqlite3.Connection,
    *,
    account_id: int,
    actual: Decimal,
    today: date,
) -> ReconcilePreview:
    """The gap for one position, and the rows that might explain it.

    ``LookupError`` when the account does not exist. Read-only.

    The ledger figure is the **position** — one account, one asset, summed
    by the same function ``record_adjustment`` compares against. The card
    grid's figure comes from ``v_account_balances``, which sums an account
    across currencies; reconciling against that would size the plug by the
    wrong arithmetic on every Binance account.
    """
    account = accounts_repo.get_by_id(conn, account_id)
    if account is None or account.id is None:
        raise LookupError(f"account id={account_id} not found")

    currency = account.currency.upper()
    ledger = position_balance(conn, account_id=account.id, currency=currency)
    delta = actual - ledger

    since_date = today - timedelta(days=LOOKBACK_DAYS)
    since = since_date.isoformat()
    params = {"account_id": account.id, "since": since}

    grouped: list[tuple[str, list[ReconcileRow]]] = [
        ("unpaired", [_to_row(conn, r) for r in conn.execute(_SQL_UNPAIRED, params)]),
        ("twins", [_to_row(conn, r) for r in conn.execute(_SQL_TWINS, params)]),
        (
            "uncategorised",
            [_to_row(conn, r) for r in conn.execute(_SQL_UNCATEGORISED, params)],
        ),
        ("approximate", _approximate_rows(conn, account_id=account.id, since=since)),
    ]

    reasons = [
        ReconcileReason(
            key=key,
            label=_REASON_TEXT[key][0],
            hint=_REASON_TEXT[key][1],
            rows=rows,
        )
        for key, rows in grouped
        if rows
    ]

    return ReconcilePreview(
        account_id=account.id,
        account_name=account.name,
        currency=currency,
        ledger_balance=ledger,
        ledger_plain=plain(ledger),
        actual_balance=actual,
        actual_plain=plain(actual),
        delta=delta,
        matches=abs(delta) < NEGLIGIBLE,
        window_days=LOOKBACK_DAYS,
        since=since_date,
        reasons=reasons,
    )




def write_adjustment(
    conn: sqlite3.Connection,
    *,
    account_id: int,
    actual: Decimal,
    note: str,
    now: datetime,
) -> AdjustmentResult | None:
    """Write the plug. ``None`` when the position already agrees.

    The note is mandatory *here* and optional in the domain function: the
    CLI states its reason in a shell history and a commit message, a click
    states it nowhere. ADR-018 §2 asks that a plug stay explicable, and the
    only moment the owner knows why is the moment they write it.

    ``LookupError`` for an unknown account, ``ValueError`` for a blank note.
    """
    reason = note.strip()
    if not reason:
        raise ValueError("a note is required: say why this gap cannot be explained")

    account = accounts_repo.get_by_id(conn, account_id)
    if account is None or account.id is None:
        raise LookupError(f"account id={account_id} not found")

    return record_adjustment(
        conn,
        account_id=account.id,
        currency=account.currency,
        actual=actual,
        occurred_at=now,
        note=reason,
    )


__all__ = [
    "LOOKBACK_DAYS",
    "ReconcilePreview",
    "ReconcileReason",
    "ReconcileRow",
    "build_preview",
    "plain",
    "write_adjustment",
]
