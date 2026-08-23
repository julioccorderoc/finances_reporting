"""The queue screen's layout, assembled server-side.

`services/triage.py` answers *what is in the queue*. This module answers
*how the screen reads*, so a template never computes any of it:

* **The three groups** — their exact labels, hints and order come from
  `design_handoff_triage/README.md` §"Queue screen"; a group with no rows
  is absent rather than empty-headed (A6, A7).
* **The integrity banner** — the design names the real account, date and
  amount of the orphan leg rather than printing ``transfer_id=…`` at the
  owner (A12).
* **The parked panel** — live count, a capped sample, the oldest row the
  cutoff could reach, and the cutoff itself (A13, F4, F8).
* **The category picker** — one payload for every picker on the screen
  (the modal's and the bulk sheet's), built once per render (K1).

Read-only module: SELECTs only, and every one of them goes through an
existing service or repo.
"""

from __future__ import annotations

import sqlite3
from datetime import date, datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict

from finances import config
from finances.domain import transfers as transfers_domain
from finances.format import fmt_date_short, fmt_number
from finances.web.services.categories_view import PickerPayload, picker_payload
from finances.web.services.triage import (
    TriageItem,
    TriageQueue,
    build_queue,
)

#: How many parked rows the sheet shows. "A few of them" — 266 rows is
#: not a sample, and the sheet is 520px tall.
PARKED_SAMPLE_SIZE = 5


class TriageGroup(BaseModel):
    """One heading in the queue, and the rows under it.

    ``collapsed`` is the *default* state only. Collapse is a reading
    convenience and never removes an entry from the run (B2), so the
    walk list is built from the queue rather than from what is open.
    """

    model_config = ConfigDict(extra="forbid")

    bucket: int
    label: str
    hint: str
    collapsed: bool
    items: tuple[TriageItem, ...] = ()


#: The three groups, in the README's order. Bucket numbers are the
#: server-assigned ones from ``triage._bucket_for`` (K5) — 0 needs a
#: category, 1 pair proposals, 2 priced roughly. *Priced roughly* starts
#: collapsed because an approximate rate never blocks a sitting (A6/D6).
GROUPS: tuple[TriageGroup, ...] = (
    TriageGroup(
        bucket=0,
        label="Needs a category",
        hint="One decision each",
        collapsed=False,
    ),
    TriageGroup(
        bucket=1,
        label="Proposed pairs",
        hint="Two rows that look like one transfer",
        collapsed=False,
    ),
    TriageGroup(
        bucket=2,
        label="Priced roughly",
        hint="No rate within 14 days — Ledger used the nearest one",
        collapsed=True,
    ),
)


class IntegrityBanner(BaseModel):
    """The warning above the groups, in the design's words (A12)."""

    model_config = ConfigDict(extra="forbid")

    title: str
    body: str


class ParkedPanel(BaseModel):
    """Everything the parked strip and the parked sheet render."""

    model_config = ConfigDict(extra="forbid")

    count: int
    sample: tuple[TriageItem, ...]
    oldest: date | None
    """The oldest row the cutoff could still reach, or ``None``.

    "The oldest one is Mon, Nov 3, 2024" is a floor, so it is measured
    over every uncategorised income/expense row — parked or not — which
    is exactly the set ``park_before`` scopes itself to."""
    cutoff: date
    note: str


class TriageScreen(BaseModel):
    """The whole queue screen, ready to render."""

    model_config = ConfigDict(extra="forbid")

    queue: TriageQueue
    groups: tuple[TriageGroup, ...]
    banner: IntegrityBanner | None
    parked: ParkedPanel
    picker: PickerPayload
    walk: tuple[TriageItem, ...]
    """The run, in group order regardless of what is collapsed (B2)."""


PARKED_NOTE = (
    "They keep their money in every balance and report, and they are "
    "still here when you want them."
)

_OLDEST_OUTSTANDING_SQL = """
    SELECT MIN(occurred_at) AS oldest
    FROM transactions
    WHERE category_id IS NULL
      AND kind IN ('income', 'expense')
"""

_ORPHAN_LEG_SQL = """
    SELECT t.occurred_at AS occurred_at,
           t.amount      AS amount,
           t.currency    AS currency,
           a.name        AS account_name
    FROM transactions t
    LEFT JOIN accounts a ON a.id = t.account_id
    WHERE t.id = ?
"""


def _oldest_outstanding(conn: sqlite3.Connection) -> date | None:
    """Oldest uncategorised income/expense row, parked or not.

    Same predicate as :func:`finances.domain.triage_admin.park_before`
    minus its ``parked = 0`` term, so the hint under the cutoff field
    names a date the cutoff can actually reach.
    """
    row = conn.execute(_OLDEST_OUTSTANDING_SQL).fetchone()
    if row is None or row["oldest"] is None:
        return None
    return date.fromisoformat(str(row["oldest"])[:10])


def _leg_sentence(conn: sqlite3.Connection, txn_id: int, today: date) -> str | None:
    """``Binance Funding, Jun 29 — 96.40 USDT out`` for one orphan leg."""
    row = conn.execute(_ORPHAN_LEG_SQL, (txn_id,)).fetchone()
    if row is None:
        return None
    amount = row["amount"]
    if not isinstance(amount, Decimal):
        amount = Decimal(str(amount))
    direction = "out" if amount < 0 else "in"
    when = fmt_date_short(row["occurred_at"], today=today)
    return (
        f"{row['account_name'] or 'Unknown account'}, {when} — "
        f"{fmt_number(abs(amount))} {row['currency']} {direction}"
    )


def _build_banner(
    conn: sqlite3.Connection, today: date
) -> IntegrityBanner | None:
    """Read ``v_unreconciled_transfers`` and say it in the design's words.

    The payload's own ``integrity_warnings`` are engineer-facing strings
    (``transfer_id=x has only 1 leg(s)``); criterion A12 wants the
    account, date and amount of the leg that is actually missing a
    partner. Both read the same view — this one just resolves the ids.
    """
    rows = transfers_domain.find_unreconciled(conn)
    if not rows:
        return None

    sentences: list[str] = []
    for row in rows:
        ids = str(row.get("transaction_ids") or "")
        for raw_id in ids.split(","):
            if not raw_id.strip():
                continue
            sentence = _leg_sentence(conn, int(raw_id), today)
            if sentence is not None:
                sentences.append(sentence)

    if not sentences:
        return None

    sentences.sort()
    count = len(rows)
    title = (
        "One transfer has a single leg"
        if count == 1
        else f"{count} transfers have a single leg"
    )
    body = (
        "; ".join(sentences)
        + " with nothing on the other side. Pair it, or say it was not a "
        "transfer."
    )
    return IntegrityBanner(title=title, body=body)


def _group_items(queue: TriageQueue) -> tuple[TriageGroup, ...]:
    """Partition the queue into its groups, dropping the empty ones (A7)."""
    filled: list[TriageGroup] = []
    for group in GROUPS:
        items = tuple(item for item in queue.items if item.bucket == group.bucket)
        if items:
            filled.append(group.model_copy(update={"items": items}))
    return tuple(filled)


def build_screen(
    conn: sqlite3.Connection,
    *,
    today: date | None = None,
) -> TriageScreen:
    """Assemble the whole queue screen from one queue build.

    ``today`` is injectable for tests and anchors both the picker's
    twelve-month usage window and every date the screen renders, so a
    single render cannot straddle midnight.
    """
    if today is None:
        today = datetime.now(tz=config.CARACAS_TZ).date()

    queue = build_queue(conn)
    parked_items = tuple(queue.parked_items)

    return TriageScreen(
        queue=queue,
        groups=_group_items(queue),
        banner=_build_banner(conn, today),
        parked=ParkedPanel(
            count=queue.parked_count,
            sample=parked_items[:PARKED_SAMPLE_SIZE],
            oldest=_oldest_outstanding(conn),
            cutoff=date(today.year, 1, 1),
            note=PARKED_NOTE,
        ),
        picker=picker_payload(conn, today=today),
        walk=tuple(queue.items),
    )


__all__ = [
    "GROUPS",
    "PARKED_NOTE",
    "PARKED_SAMPLE_SIZE",
    "IntegrityBanner",
    "ParkedPanel",
    "TriageGroup",
    "TriageScreen",
    "build_screen",
]
