from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Final

from finances.domain.models import Tombstone, Transaction, TransactionKind

# The cash CLI's ``source``. It lives here, not in ``ingest.cash_cli``,
# because the repo has to know it (a cash delete leaves no tombstone,
# ADR-022 §2.2) and ``ingest`` already depends on ``repos`` — the other
# direction would be a cycle. ``cash_cli`` re-exports it.
CASH_CLI_SOURCE: Final[str] = "cash_cli"

# Sources whose rows are the ledger's own corrections (ADR-018, ADR-020).
# They are restated by the module that wrote them, never deleted by hand.
_UNDELETABLE_SOURCES: Final[frozenset[str]] = frozenset(
    {"reconciliation", "opening_balance"}
)


class _Unset:
    """Sentinel for ``update`` arguments that callers want left untouched.

    A bare ``None`` is a *meaningful* value at the SQL boundary
    (e.g. clear ``user_rate``); we need a third state for "do not touch".
    """

    _instance: "_Unset | None" = None

    def __new__(cls) -> "_Unset":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self) -> str:  # pragma: no cover - debug aid
        return "_UNSET"

    def __bool__(self) -> bool:
        return False


_UNSET: Final[_Unset] = _Unset()


def _to_text(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format(value, "f")


def _int_text(value: int | None) -> str | None:
    if value is None:
        return None
    return str(value)


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    return value.isoformat() if hasattr(value, "isoformat") else str(value)


def _row_to_transaction(row: sqlite3.Row) -> Transaction:
    return Transaction(
        id=row["id"],
        account_id=row["account_id"],
        occurred_at=row["occurred_at"],
        kind=TransactionKind(row["kind"]),
        amount=row["amount"] if isinstance(row["amount"], Decimal) else Decimal(str(row["amount"])),
        currency=row["currency"],
        description=row["description"],
        category_id=row["category_id"],
        transfer_id=row["transfer_id"],
        user_rate=(
            None
            if row["user_rate"] is None
            else (
                row["user_rate"]
                if isinstance(row["user_rate"], Decimal)
                else Decimal(str(row["user_rate"]))
            )
        ),
        source=row["source"],
        source_ref=row["source_ref"],
        needs_review=bool(row["needs_review"]),
        parked=bool(row["parked"]),
        notes=row["notes"],
    )


def insert(conn: sqlite3.Connection, txn: Transaction) -> Transaction:
    cur = conn.execute(
        """
        INSERT INTO transactions (
            account_id, occurred_at, kind, amount, currency, description,
            category_id, transfer_id, user_rate, source, source_ref,
            needs_review, parked, notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            txn.account_id,
            _iso(txn.occurred_at),
            txn.kind.value,
            _to_text(txn.amount),
            txn.currency,
            txn.description,
            txn.category_id,
            txn.transfer_id,
            _to_text(txn.user_rate),
            txn.source,
            txn.source_ref,
            1 if txn.needs_review else 0,
            1 if txn.parked else 0,
            txn.notes,
        ),
    )
    return txn.model_copy(update={"id": cur.lastrowid})


def get_by_id(conn: sqlite3.Connection, transaction_id: int) -> Transaction | None:
    row = conn.execute(
        """
        SELECT id, account_id, occurred_at, kind, amount, currency, description,
               category_id, transfer_id, user_rate, source, source_ref,
               needs_review, parked, notes
        FROM transactions WHERE id = ?
        """,
        (transaction_id,),
    ).fetchone()
    return _row_to_transaction(row) if row else None


def get_by_source_ref(
    conn: sqlite3.Connection, source: str, source_ref: str
) -> Transaction | None:
    row = conn.execute(
        """
        SELECT id, account_id, occurred_at, kind, amount, currency, description,
               category_id, transfer_id, user_rate, source, source_ref,
               needs_review, parked, notes
        FROM transactions WHERE source = ? AND source_ref = ?
        """,
        (source, source_ref),
    ).fetchone()
    return _row_to_transaction(row) if row else None


def is_tombstoned(conn: sqlite3.Connection, source: str, source_ref: str) -> bool:
    """Has the owner deleted this ``(source, source_ref)`` (ADR-022)?"""
    row = conn.execute(
        "SELECT 1 FROM deleted_transactions WHERE source = ? AND source_ref = ?",
        (source, source_ref),
    ).fetchone()
    return row is not None


def upsert_by_source_ref(conn: sqlite3.Connection, txn: Transaction) -> dict[str, Any]:
    """Insert-or-update on (source, source_ref) per ADR-010.

    Returns {"rows_inserted": 0|1, "rows_updated": 0|1,
    "rows_skipped_deleted": 0|1, "id": int|None}. A second identical call
    returns rows_inserted=0.

    A ``(source, source_ref)`` the owner deleted (ADR-022) is *retired*:
    the row is skipped, ``rows_skipped_deleted`` is 1 and ``id`` is None.
    This is the one place the rule needs to live — every importer and the
    backfill (rule-004) enter the ledger through here, so a delete holds
    against a re-import without any query elsewhere having to know.

    The UPDATE branch overwrites statement-sourced fields (amount,
    description, dates...) but PRESERVES enrichment the statement cannot
    know: category_id, transfer_id, user_rate, notes, a resolved needs_review, and
    the kind of a paired row. Re-ingesting raw data must never undo triage
    or pairing work.
    """
    if txn.source_ref is None:
        raise ValueError("upsert_by_source_ref requires a non-null source_ref")

    if is_tombstoned(conn, txn.source, txn.source_ref):
        return {
            "rows_inserted": 0,
            "rows_updated": 0,
            "rows_skipped_deleted": 1,
            "id": None,
        }

    existing = get_by_source_ref(conn, txn.source, txn.source_ref)
    params = (
        txn.account_id,
        _iso(txn.occurred_at),
        txn.kind.value,
        _to_text(txn.amount),
        txn.currency,
        txn.description,
        txn.category_id,
        txn.transfer_id,
        _to_text(txn.user_rate),
        txn.source,
        txn.source_ref,
        1 if txn.needs_review else 0,
        1 if txn.parked else 0,
        txn.notes,
    )
    conn.execute(
        """
        INSERT INTO transactions (
            account_id, occurred_at, kind, amount, currency, description,
            category_id, transfer_id, user_rate, source, source_ref,
            needs_review, parked, notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(source, source_ref) DO UPDATE SET
            account_id   = excluded.account_id,
            occurred_at  = excluded.occurred_at,
            -- A paired row's kind came from the pairing pass; a raw
            -- statement re-ingest must not flip it back.
            kind         = CASE WHEN transactions.transfer_id IS NOT NULL
                                THEN transactions.kind ELSE excluded.kind END,
            amount       = excluded.amount,
            currency     = excluded.currency,
            description  = excluded.description,
            -- Enrichment (triage category, pairing, user rate) lives on the
            -- row but is not sourced from raw statements: keep existing
            -- values unless the incoming row actually carries one.
            -- Category: an already-set value (hand triage or an earlier rule)
            -- always wins over what re-ingest suggests, and a row pairing
            -- turned into a transfer never gains one (currency movement is
            -- not spending).
            category_id  = CASE
                               WHEN transactions.transfer_id IS NOT NULL
                                   THEN transactions.category_id
                               ELSE COALESCE(transactions.category_id,
                                             excluded.category_id)
                           END,
            transfer_id  = COALESCE(excluded.transfer_id, transactions.transfer_id),
            user_rate    = COALESCE(excluded.user_rate, transactions.user_rate),
            -- Notes are manual-only enrichment (viewer edit modal). Stronger
            -- than the category/rate pattern: an EXISTING note always wins;
            -- an incoming note only fills a row that has none (00-design §4).
            notes        = COALESCE(transactions.notes, excluded.notes),
            needs_review = CASE WHEN transactions.needs_review = 0 THEN 0
                                ELSE excluded.needs_review END,
            updated_at   = CURRENT_TIMESTAMP
        """,
        params,
    )

    row = conn.execute(
        "SELECT id FROM transactions WHERE source = ? AND source_ref = ?",
        (txn.source, txn.source_ref),
    ).fetchone()
    row_id = int(row["id"])

    return {
        "rows_inserted": 0 if existing else 1,
        "rows_updated": 1 if existing else 0,
        "rows_skipped_deleted": 0,
        "id": row_id,
    }


def delete(
    conn: sqlite3.Connection, transaction_id: int, *, reason: str | None = None
) -> Tombstone:
    """Remove a row and retire its ``(source, source_ref)`` (ADR-022).

    A plain ``DELETE`` is not enough for anything an importer wrote: dedup
    is keyed on the pair (rule-010), so the next ``finances update`` or
    statement drop would insert the row again. The tombstone is what makes
    "re-ingest same day = 0 new rows" survive a delete —
    :func:`upsert_by_source_ref` skips a tombstoned pair.

    Refused (ADR-022 §2.3):

    * a **paired** row (``transfer_id`` set) — deleting one leg leaves an
      orphan and breaks rule-002's sum-to-zero. The pair has to be broken
      first, and no surface does that yet;
    * rows the reconciliation engine wrote (``reconciliation``,
      ``opening_balance``) — they are the ledger's own corrections, and
      removing one by hand re-opens what it closed. Restate them through
      their own module instead (ADR-018, ADR-020).

    ``cash_cli`` rows are deleted **without** a tombstone (§2.2): nothing
    re-ingests them, and two legitimately identical cash entries hash to
    the same ref, so a tombstone would block the second one.

    Returns the :class:`Tombstone` either way, so the caller can name what
    it removed. The write is one savepoint — a savepoint, not ``BEGIN``,
    because ingest callers are already inside a transaction and a nested
    ``BEGIN`` would raise.
    """
    txn = get_by_id(conn, transaction_id)
    if txn is None:
        raise LookupError(f"transaction id={transaction_id} not found")

    if txn.transfer_id is not None:
        raise ValueError(
            "This row is one half of a transfer — the pair has to be broken "
            "first"
        )
    if txn.source in _UNDELETABLE_SOURCES:
        raise ValueError(
            f"a '{txn.source}' row is the ledger's own correction, not an "
            "import: restate it through the module that wrote it "
            "(ADR-018 / ADR-020), never by deleting it"
        )

    snapshot = txn.model_dump(mode="json")
    deleted_at = datetime.now(tz=UTC)
    # Cash carries no tombstone; neither does a row with no ref, which
    # nothing could re-insert anyway (upsert_by_source_ref refuses a null).
    tombstoned = txn.source != CASH_CLI_SOURCE and txn.source_ref is not None

    conn.execute("SAVEPOINT delete_transaction")
    try:
        if tombstoned:
            conn.execute(
                """
                INSERT INTO deleted_transactions
                    (source, source_ref, deleted_at, reason, snapshot)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(source, source_ref) DO UPDATE SET
                    deleted_at = excluded.deleted_at,
                    reason     = excluded.reason,
                    snapshot   = excluded.snapshot
                """,
                (
                    txn.source,
                    txn.source_ref,
                    deleted_at.isoformat(),
                    reason,
                    json.dumps(snapshot),
                ),
            )
        # transaction_edits cascades (migration 009): the edit history of a
        # deleted row goes with it.
        conn.execute("DELETE FROM transactions WHERE id = ?", (transaction_id,))
    except Exception:
        conn.execute("ROLLBACK TO delete_transaction")
        conn.execute("RELEASE delete_transaction")
        raise
    conn.execute("RELEASE delete_transaction")

    return Tombstone(
        source=txn.source,
        source_ref=txn.source_ref,
        deleted_at=deleted_at,
        reason=reason,
        snapshot=snapshot,
    )


def list_by_account(
    conn: sqlite3.Connection, account_id: int, *, limit: int | None = None
) -> list[Transaction]:
    sql = """
        SELECT id, account_id, occurred_at, kind, amount, currency, description,
               category_id, transfer_id, user_rate, source, source_ref,
               needs_review, parked, notes
        FROM transactions WHERE account_id = ?
        ORDER BY occurred_at DESC, id DESC
    """
    params: tuple[Any, ...] = (account_id,)
    if limit is not None:
        sql += " LIMIT ?"
        params = (account_id, limit)
    rows = conn.execute(sql, params).fetchall()
    return [_row_to_transaction(r) for r in rows]


def count(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT COUNT(*) AS c FROM transactions").fetchone()
    return int(row["c"])


def update(
    conn: sqlite3.Connection,
    *,
    id: int,
    category_id: int | None | _Unset = _UNSET,
    user_rate: Decimal | None | _Unset = _UNSET,
    needs_review: bool | _Unset = _UNSET,
    parked: bool | _Unset = _UNSET,
    notes: str | None | _Unset = _UNSET,
) -> Transaction:
    """Patch a single transaction's mutable fields.

    Only fields explicitly passed (not the ``_UNSET`` sentinel) are
    touched; ``None`` is a real value meaning "clear this column". The
    function always sets ``updated_at`` to the current UTC instant
    regardless of which fields were provided.

    Per ADR-009 / rule-009, the return value is a Pydantic ``Transaction``
    re-fetched from the row's authoritative state.

    Per rule-012, this is the only allowed entry point for the web
    viewer's transaction-edit modal — the viewer must not run its own
    UPDATE statements.
    """
    # Read current state first so we can record what actually CHANGED into
    # transaction_edits (Wave 2 Thing 3). This is the single sanctioned write
    # path (rule-012), so the audit trail covers the modal, triage, PATCH API
    # and bulk-edit with no endpoint changes. needs_review is deliberately not
    # recorded — it is resolver-derived (ADR-005), not a manual field.
    current = get_by_id(conn, id)
    if current is None:
        raise LookupError(f"transaction id={id} not found")

    sets: list[str] = []
    params: list[Any] = []
    # (field, old_value_text, new_value_text) for each recordable change.
    edits: list[tuple[str, str | None, str | None]] = []

    if not isinstance(category_id, _Unset):
        sets.append("category_id = ?")
        params.append(category_id)
        if category_id != current.category_id:
            edits.append(
                ("category_id", _int_text(current.category_id), _int_text(category_id))
            )

    if not isinstance(user_rate, _Unset):
        sets.append("user_rate = ?")
        params.append(_to_text(user_rate))
        # Decimal equality is value-based, so 36.0 == 36.00 records nothing.
        if user_rate != current.user_rate:
            edits.append(
                ("user_rate", _to_text(current.user_rate), _to_text(user_rate))
            )

    if not isinstance(needs_review, _Unset):
        sets.append("needs_review = ?")
        params.append(1 if needs_review else 0)

    if not isinstance(parked, _Unset):
        sets.append("parked = ?")
        params.append(1 if parked else 0)
        # Deliberately not recorded in transaction_edits: migration 009's
        # CHECK (field IN ('category_id','user_rate','notes')) would reject
        # a 'parked' row, and parking is a workflow flag, not a ledger
        # correction (rule-012).

    if not isinstance(notes, _Unset):
        sets.append("notes = ?")
        params.append(notes)
        if notes != current.notes:
            edits.append(("notes", current.notes, notes))

    sets.append("updated_at = ?")
    params.append(datetime.now(tz=UTC).isoformat())

    params.append(id)
    sql = f"UPDATE transactions SET {', '.join(sets)} WHERE id = ?"
    conn.execute(sql, params)

    for field, old_value, new_value in edits:
        conn.execute(
            """
            INSERT INTO transaction_edits (transaction_id, field, old_value, new_value)
            VALUES (?, ?, ?, ?)
            """,
            (id, field, old_value, new_value),
        )

    refreshed = get_by_id(conn, id)
    if refreshed is None:  # pragma: no cover - defensive; UPDATE just succeeded
        raise LookupError(f"transaction id={id} disappeared after update")
    return refreshed
