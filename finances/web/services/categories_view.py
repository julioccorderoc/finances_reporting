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
from finances.domain.category_definitions import definition_for
from finances.web.services.category_stats import top_categories

#: Group order in the expanded list. Transfer and adjustment kinds are
#: ``auto_only`` and never reach the picker, so two groups is the whole set.
GROUP_ORDER = ("expense", "income")

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
    today: date | None = None,
    chip_count: int = CHIP_COUNT,
    months: int = USAGE_MONTHS,
) -> PickerPayload:
    """Build the whole picker from the database and the definitions doc.

    ``today`` anchors the usage window (defaults to the wall clock);
    ``chip_count`` and ``months`` exist so the sheet variant and future
    tuning do not need a second function.
    """
    pickable = [_to_picker_category(c) for c in categories_repo.list_pickable(conn)]
    by_id = {c.id: c for c in pickable}

    ranked = top_categories(
        conn, kind=None, limit=chip_count, months=months, chips_only=True, today=today
    )
    chips = tuple(
        PickerChip(number=index, category=by_id[category.id])
        for index, category in enumerate(ranked, start=1)
        if category.id in by_id
    )

    groups = tuple(
        PickerGroup(
            kind=kind,
            label=kind.upper(),
            categories=tuple(c for c in pickable if c.kind == kind),
        )
        for kind in GROUP_ORDER
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
