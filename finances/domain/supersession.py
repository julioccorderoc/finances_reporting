"""Retire backfill rows that a later native-id sync re-imported.

The backfill has no external id to work with — the legacy CSV carries
none — so per ADR-010 it falls back to ``<prefix>:hash:<sha>``. The live
sync has the real thing and mints ``<prefix>:<native-id>``. Both are valid
``source_ref`` values for the same ledger event, and dedup is keyed on
``(source, source_ref)`` (rule-010), so ``UNIQUE`` cannot see that they
are the same deposit. Two rows survive.

This stayed harmless for as long as every live sync started after the
backfill's last day. On 2026-08-04 one ran with ``--since`` reaching back
to 2025-10-03 — straight through the backfilled window — and re-imported
105 events that were already on the books.

Matching is by shape, because there is no key to join on. Same source_ref
prefix and leg, same currency, same sign, within ``max_day_gap`` days, and
amounts agreeing within ``tolerance_ratio`` — the legacy CSV rounds to two
decimals and P2P fees move the last cents, so exact equality would miss
most real twins. Tightest fit is claimed first and each row is consumed
once, which asserts that N legacy rows were superseded by N native ones,
not that any individual pairing is provably the true one. Anything that
does not match is left exactly as it is: 187 legacy rows are the only
record of their event, and sweeping them would destroy data.

The native row survives. It carries the authoritative external id, so a
future sync of the same window dedups against it instead of duplicating
again. Whatever the legacy row held and the native one lacks — a category
Julio picked, a ``user_rate``, notes — moves across first.

Where the legacy row anchored a transfer (a P2P sell paired to its bank
deposit under ADR-002), deleting it would orphan the counterpart, so the
pairing moves to the survivor. Where the legacy row's whole transfer group
is superseded — both halves of a convert — nothing needs re-pointing and
the group simply goes. Where the survivor is already pledged to a
different group, the pass refuses: that is two competing readings of one
event, and a human picks.
"""
from __future__ import annotations

import sqlite3
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, NamedTuple

from finances.domain.reconciliation import MatchProposal

LEGACY_MARKER = ":hash:"
DEFAULT_SOURCE = "binance"
DEFAULT_MAX_DAY_GAP = 1
DEFAULT_TOLERANCE_RATIO = Decimal("0.01")

_LEG_SUFFIXES = frozenset({"from", "to"})

SQL_ROWS_FOR_SOURCE = """
    SELECT id, account_id, occurred_at, kind, amount, currency,
           category_id, user_rate, notes, transfer_id, source_ref
      FROM transactions
     WHERE source = ? AND source_ref IS NOT NULL
     ORDER BY occurred_at, id
"""

SQL_GROUP_MEMBERS = """
    SELECT id FROM transactions WHERE transfer_id = ?
"""


class _Row(NamedTuple):
    """One candidate row, normalised for shape matching."""

    id: int
    day: date
    amount: Decimal
    currency: str
    prefix: str
    leg: str
    kind: str
    transfer_id: str | None


def _to_decimal(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal(0)


def _parse_day(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.fromisoformat(str(value)).date()


def _split_ref(source_ref: str) -> tuple[str, str]:
    """Return ``(prefix, leg)`` — leg is ``''`` for single-row events."""
    prefix = source_ref.split(":", 1)[0]
    tail = source_ref.rsplit(":", 1)[-1]
    return prefix, tail if tail in _LEG_SUFFIXES else ""


def is_legacy_ref(source_ref: str | None) -> bool:
    """True for a backfill hash fallback, false for a native external id."""
    return LEGACY_MARKER in (source_ref or "")


class LegacyRefSupersession:
    """Strategy: retire a backfill row whose event was re-imported natively."""

    name: str = "legacy_ref_supersession"

    def __init__(
        self,
        conn: sqlite3.Connection,
        *,
        source: str = DEFAULT_SOURCE,
        max_day_gap: int = DEFAULT_MAX_DAY_GAP,
        tolerance_ratio: Decimal = DEFAULT_TOLERANCE_RATIO,
    ) -> None:
        self._conn = conn
        self._source = source
        self._max_day_gap = max_day_gap
        self._tolerance_ratio = tolerance_ratio

    # -- matching ----------------------------------------------------------

    def match(self) -> list[MatchProposal]:
        legacy, native = self._load()
        pairs = self._assign(legacy, native)
        if not pairs:
            return []

        superseded = {legacy_row.id for legacy_row, _ in pairs}
        return [
            MatchProposal(
                strategy=self.name,
                details={
                    "legacy_id": legacy_row.id,
                    "native_id": native_row.id,
                    # A group losing only some of its members would strand
                    # the rest, so the survivor takes over the legacy row's
                    # place in it.
                    "repoint": self._group_outlives(legacy_row, superseded),
                },
            )
            for legacy_row, native_row in pairs
        ]

    def _load(self) -> tuple[list[_Row], list[_Row]]:
        cursor = self._conn.execute(SQL_ROWS_FOR_SOURCE, (self._source,))
        legacy: list[_Row] = []
        native: list[_Row] = []
        for raw in cursor.fetchall():
            source_ref = str(raw["source_ref"])
            prefix, leg = _split_ref(source_ref)
            row = _Row(
                id=int(raw["id"]),
                day=_parse_day(raw["occurred_at"]),
                amount=_to_decimal(raw["amount"]),
                currency=str(raw["currency"]).upper(),
                prefix=prefix,
                leg=leg,
                kind=str(raw["kind"]),
                transfer_id=raw["transfer_id"],
            )
            (legacy if is_legacy_ref(source_ref) else native).append(row)
        return legacy, native

    def _assign(
        self, legacy: list[_Row], native: list[_Row]
    ) -> list[tuple[_Row, _Row]]:
        """Greedy 1:1, tightest amount fit first, then smallest date gap."""
        scored: list[tuple[Decimal, int, int, int]] = []
        for legacy_row in legacy:
            for native_row in native:
                drift = self._drift(legacy_row, native_row)
                if drift is None:
                    continue
                gap = abs((legacy_row.day - native_row.day).days)
                scored.append((drift, gap, legacy_row.id, native_row.id))

        # Total order, so the same ledger always yields the same assignment.
        scored.sort()

        by_id = {row.id: row for row in legacy + native}
        claimed_legacy: set[int] = set()
        claimed_native: set[int] = set()
        pairs: list[tuple[_Row, _Row]] = []
        for _drift, _gap, legacy_id, native_id in scored:
            if legacy_id in claimed_legacy or native_id in claimed_native:
                continue
            claimed_legacy.add(legacy_id)
            claimed_native.add(native_id)
            pairs.append((by_id[legacy_id], by_id[native_id]))

        pairs.sort(key=lambda pair: pair[0].id)
        return pairs

    def _drift(self, legacy_row: _Row, native_row: _Row) -> Decimal | None:
        """Relative amount difference, or None when the two cannot be twins.

        Account and kind are deliberately not compared: the backfill booked
        P2P sells on Funding as transfer legs where the live sync books them
        on Spot as expenses. Same event, different bookkeeping — the shape
        that identifies it is the money, the day and the ref prefix.
        """
        if legacy_row.prefix != native_row.prefix:
            return None
        if legacy_row.leg != native_row.leg:
            return None
        if legacy_row.currency != native_row.currency:
            return None
        if (legacy_row.amount > 0) != (native_row.amount > 0):
            return None
        if abs((legacy_row.day - native_row.day).days) > self._max_day_gap:
            return None

        scale = max(abs(legacy_row.amount), abs(native_row.amount))
        if scale == 0:
            return None
        drift = abs(abs(legacy_row.amount) - abs(native_row.amount)) / scale
        return drift if drift <= self._tolerance_ratio else None

    def _group_outlives(self, legacy_row: _Row, superseded: set[int]) -> bool:
        """True when the legacy row's transfer group keeps a member."""
        if legacy_row.transfer_id is None:
            return False
        members = {
            int(raw["id"])
            for raw in self._conn.execute(
                SQL_GROUP_MEMBERS, (legacy_row.transfer_id,)
            ).fetchall()
        }
        return bool(members - superseded)

    # -- applying ----------------------------------------------------------

    def apply(self, proposal: MatchProposal) -> None:
        legacy_id = int(proposal.details["legacy_id"])
        native_id = int(proposal.details["native_id"])
        repoint = bool(proposal.details["repoint"])

        legacy = self._fetch(legacy_id)
        native = self._fetch(native_id)
        if legacy is None or native is None:
            raise LookupError(
                f"supersession {legacy_id}->{native_id}: row disappeared"
            )

        group = legacy["transfer_id"]
        held = native["transfer_id"]
        # Refuse before writing anything: the runner keeps going after a
        # failed proposal, so a half-applied one would leave the ledger
        # carrying an edit for a row it never retired.
        if repoint and held is not None and held != group:
            raise ValueError(
                f"transaction {native_id} is already paired into "
                f"{held!r}; superseding {legacy_id} would move it to "
                f"{group!r}. Two readings of one event — resolve by hand."
            )

        self._carry_fields(legacy, native, native_id)

        if repoint:
            if held is None:
                # A transfer leg must read as one, or rule-002's leg checks
                # and every transfer-aware report skip it.
                self._conn.execute(
                    "UPDATE transactions"
                    "   SET transfer_id = ?, kind = ?, updated_at = ?"
                    " WHERE id = ?",
                    (group, legacy["kind"], _now(), native_id),
                )

        self._conn.execute("DELETE FROM transactions WHERE id = ?", (legacy_id,))

    def _fetch(self, transaction_id: int) -> sqlite3.Row | None:
        return self._conn.execute(
            "SELECT * FROM transactions WHERE id = ?", (transaction_id,)
        ).fetchone()

    def _carry_fields(
        self, legacy: sqlite3.Row, native: sqlite3.Row, native_id: int
    ) -> None:
        """Move what the legacy row holds and the native row is missing."""
        sets: list[str] = []
        params: list[Any] = []
        for field in ("category_id", "user_rate", "notes"):
            legacy_value = legacy[field]
            if legacy_value is None or legacy_value == "":
                continue
            native_value = native[field]
            if native_value is not None and native_value != "":
                continue
            sets.append(f"{field} = ?")
            params.append(legacy_value)

        if not sets:
            return

        # A row that now has a category is no longer waiting on a human.
        if "category_id = ?" in sets:
            sets.append("needs_review = 0")
        sets.append("updated_at = ?")
        params.append(_now())
        params.append(native_id)
        self._conn.execute(
            f"UPDATE transactions SET {', '.join(sets)} WHERE id = ?", params
        )


def _now() -> str:
    from datetime import UTC

    return datetime.now(tz=UTC).isoformat()


__all__ = [
    "DEFAULT_MAX_DAY_GAP",
    "DEFAULT_SOURCE",
    "DEFAULT_TOLERANCE_RATIO",
    "LegacyRefSupersession",
    "is_legacy_ref",
]
