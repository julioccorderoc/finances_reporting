"""Everything ``CatPicker`` needs, assembled server-side.

Triage redesign, Wave 1.2 (criteria E1-E9, K7). The picker asks three
things of the data and this module answers all three in one payload so a
template never has to compute any of them:

* **The eight chips** — ranked over twelve months of real usage
  (criterion E3), numbered ``1``-``8`` in the order the keyboard
  shortcuts use. Fees and the system-written categories are out of the
  ranking; see :func:`finances.db.repos.categories.list_pickable`.
* **The full pickable list**, grouped ``EXPENSE`` / ``INCOME`` for the
  *"The other N"* disclosure (criterion E7).
* **Label and test on every entry**, because search matches either one
  (criterion E4) and the test strip must have a sentence to show at the
  moment of choosing (criterion E5). The sentences come from
  ``docs/architecture/category-definitions.md`` (criterion K7) — never
  retyped here.

Search itself stays client-side; this module supplies both haystacks.

Read-only module — SELECTs only.
"""

from __future__ import annotations

import sqlite3
from datetime import date

from pydantic import BaseModel, ConfigDict

from finances.db.repos import categories as categories_repo
from finances.domain.models import TransactionKind
from finances.domain.category_definitions import definition_for
from finances.domain.money import MOVEMENT_CATEGORY_KIND
from finances.web.services.category_stats import top_categories

#: Group order in the expanded list, and the eyebrow each group wears.
#: The transfer group is last and is worded as what it means to the owner:
#: a transfer-kind category on an income or expense row says the money
#: moved rather than being earned or spent (``finances.domain.money``).
#: Pickable since migration 022; never on a chip. Adjustment kinds are
#: ``auto_only`` and never reach the picker.
GROUP_ORDER = ("expense", "income", "transfer")
GROUP_LABELS = {
    "expense": "EXPENSE",
    "income": "INCOME",
    "transfer": "MOVED, NOT SPENT",
}

CHIP_COUNT = 8
USAGE_MONTHS = 12


class PickerCategory(BaseModel):
    """One choosable category, with everything needed to draw and find it."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    id: int
    label: str
    kind: str
    icon: str | None
    test: str
    auto_only: bool
    chip_eligible: bool


class PickerChip(BaseModel):
    """A category on a numbered chip. ``number`` is its keyboard shortcut."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    number: int
    category: PickerCategory


class PickerGroup(BaseModel):
    """The pickable list, split by kind for the expanded disclosure."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    kind: str
    label: str
    categories: tuple[PickerCategory, ...]


class PickerPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    chips: tuple[PickerChip, ...]
    groups: tuple[PickerGroup, ...]
    categories: tuple[PickerCategory, ...]
    pickable_count: int
    #: How many pickable categories are not on a chip — the *"The other N"* count.
    other_count: int


def _in_scope(category, kind: TransactionKind | None) -> bool:
    """Whether ``category`` may be offered on a row of ``kind``.

    The same predicate as ``transactions_write.category_fits``, so the
    picker never offers what the save would refuse: the row's own kind,
    or a movement (transfer-kind) category, which fits any income or
    expense row. ``None`` (the bulk sheet) takes everything pickable.
    """
    if kind is None:
        return True
    return category.kind is kind or category.kind is MOVEMENT_CATEGORY_KIND


def _to_picker_category(category) -> PickerCategory:
    assert category.id is not None
    return PickerCategory(
        id=category.id,
        label=category.name,
        kind=category.kind.value,
        icon=category.icon,
        test=definition_for(category.name) or "",
        auto_only=category.auto_only,
        chip_eligible=category.chip_eligible,
    )


def picker_payload(
    conn: sqlite3.Connection,
    *,
    kind: TransactionKind | None = None,
    today: date | None = None,
    chip_count: int = CHIP_COUNT,
    months: int = USAGE_MONTHS,
) -> PickerPayload:
    """Build the whole picker from the database and the definitions doc.

    ``kind`` scopes the picker to one transaction kind. The modal passes
    the row's own, because ``transactions_write.apply_edit`` refuses a
    category whose kind contradicts it — the guard that exists because
    the ledger had accumulated 65 such contradictions, six of them income
    rows filed under ``Fees``. An unscoped picker on an expense row would
    put ``Salary`` on keyboard shortcut 2: a 422 waiting for a keystroke.
    The scope mirrors that guard exactly (:func:`_in_scope`): the row's
    own kind, plus the transfer-kind categories it accepts on any row.
    The bulk sheet passes ``None``, having no single row to scope to.

    Every count in the payload describes the SCOPED set, so "Search 17
    categories" and "The other 9" are both true of what is on screen.

    ``today`` anchors the usage window (defaults to the wall clock);
    ``chip_count`` and ``months`` exist so the sheet variant and future
    tuning do not need a second function.
    """
    pickable = [
        _to_picker_category(c)
        for c in categories_repo.list_pickable(conn)
        if _in_scope(c, kind)
    ]
    by_id = {c.id: c for c in pickable}

    # Chips stay scoped to the row's own kind: ``chips_only`` also drops
    # everything with ``chip_eligible = 0``, which is how a movement
    # category never ranks onto a number key.
    ranked = top_categories(
        conn, kind=kind, limit=chip_count, months=months, chips_only=True, today=today
    )
    chips = tuple(
        PickerChip(number=index, category=by_id[category.id])
        for index, category in enumerate(ranked, start=1)
        if category.id in by_id
    )

    groups = tuple(
        PickerGroup(
            kind=group_kind,
            label=GROUP_LABELS[group_kind],
            categories=tuple(c for c in pickable if c.kind == group_kind),
        )
        for group_kind in GROUP_ORDER
    )

    return PickerPayload(
        chips=chips,
        groups=groups,
        categories=tuple(pickable),
        pickable_count=len(pickable),
        other_count=len(pickable) - len(chips),
    )


__all__ = [
    "CHIP_COUNT",
    "GROUP_ORDER",
    "USAGE_MONTHS",
    "PickerCategory",
    "PickerChip",
    "PickerGroup",
    "PickerPayload",
    "picker_payload",
]
