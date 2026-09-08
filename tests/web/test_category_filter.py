"""Filtering /transactions by category, and reading the notes while you do.

Two asks from the owner on 2026-09-07, in one sitting because they are
the same screen and the same scan:

* "I want to be able to filter by the categories" — the fifth dropdown,
  beside Accounts / Kinds / Currencies / Sources. The *query* half has
  been there since EPIC-023 (``TransactionsFilter.categories`` and its
  ``c.name IN (…)`` branch); only the control was missing, so a category
  filter was reachable by hand-editing the URL and by nothing else.

* "I want to see the notes … this will help me understand what's going
  on" — the note has always rendered under the description, at 10.5px in
  placeholder ink and cut at 60 characters, which is a note you can see
  is *there* rather than one you can read. 475 live rows carry one.

Two decisions the owner made here (2026-09-07), pinned as tests because
either could be "tidied" away by a later consistency pass:

1. The list offers **only categories that are actually on a row**. Five
   of the thirty exist but have never been used (Lifestyle, Tools,
   Clothing, Reconciliation, FX Diff); offering them means offering a
   pick that always answers nothing.
2. **Uncategorized is an option.** 240 live rows have no category at
   all, and ``c.name IN (…)`` can never reach them — SQL NULL is not a
   name. It is a sentinel value, not a category name, so it needs its
   own branch in the WHERE builder and its own label in the menu.

Tests precede the implementation per rule-011.
"""

from __future__ import annotations

import re
import sqlite3
from pathlib import Path

from fastapi.testclient import TestClient

from finances.web.services.transactions_query import (
    UNCATEGORIZED,
    TransactionsFilter,
    category_options,
    count_matching,
)

FLOW_CSS = (
    Path(__file__).resolve().parents[2]
    / "finances"
    / "web"
    / "static"
    / "css"
    / "flow.css"
)


def _group(body: str, name: str) -> str:
    m = re.search(
        rf'<details\s+class="flow-dd"[^>]*data-filter-group="{name}"[^>]*>.*?</details>',
        body,
        re.S,
    )
    assert m, f"no dropdown for {name}"
    return m.group(0)


def _summary_text(group_html: str) -> str:
    m = re.search(r'<span class="flow-dd-value"[^>]*>(.*?)</span>', group_html, re.S)
    assert m, "no summary value"
    return m.group(1).strip()


def _row_ids(body: str) -> list[str]:
    return re.findall(r'data-tx-id="(\d+)"', body)


# ---------------------------------------------------------------------------
# The option list.
# ---------------------------------------------------------------------------


def test_category_options_lists_only_categories_in_use(
    seeded_web_db: sqlite3.Connection,
) -> None:
    """Thirty categories exist; two are on a row. Two are offered.

    Transport and Rent are seeded by migrations 002 and 011 and carry no
    row in this fixture — exactly the shape of the five dead ones in the
    live ledger.
    """
    options = category_options(seeded_web_db)

    names = [label for _value, label in options]
    assert "Groceries" in names
    assert "Salary" in names
    assert "Transport" not in names
    assert "Rent" not in names


def test_uncategorized_leads_the_list_and_is_a_sentinel_not_a_name(
    seeded_web_db: sqlite3.Connection,
) -> None:
    """It is the absence of a category, so it sorts above the names.

    Its *value* can never collide with a category name — that is the
    whole point of the sentinel — while its *label* reads as English.
    """
    options = category_options(seeded_web_db)

    assert options[0] == (UNCATEGORIZED, "Uncategorized")
    assert UNCATEGORIZED not in [name for _v, name in options[1:]]
    # A real category name would be a plausible thing to type; this is not.
    assert not UNCATEGORIZED[0].isalnum()


def test_uncategorized_is_absent_when_every_row_has_a_category(
    seeded_web_db: sqlite3.Connection,
) -> None:
    """Same rule as the names: never offer a pick that answers nothing."""
    seeded_web_db.execute(
        "UPDATE transactions SET category_id = ("
        "  SELECT id FROM categories WHERE kind='expense' AND name='Groceries'"
        ") WHERE category_id IS NULL"
    )

    options = category_options(seeded_web_db)

    assert UNCATEGORIZED not in [value for value, _label in options]


# ---------------------------------------------------------------------------
# The WHERE branch.
# ---------------------------------------------------------------------------


def test_uncategorized_matches_the_rows_no_name_can_reach(
    seeded_web_db: sqlite3.Connection,
) -> None:
    """The fixture's three rows with ``category_id IS NULL``."""
    n = count_matching(
        seeded_web_db, TransactionsFilter(categories=[UNCATEGORIZED])
    )

    assert n == 3


def test_a_category_name_still_matches_by_name(
    seeded_web_db: sqlite3.Connection,
) -> None:
    """The pre-existing branch is untouched by the sentinel's arrival."""
    n = count_matching(seeded_web_db, TransactionsFilter(categories=["Groceries"]))

    assert n == 3


def test_uncategorized_composes_with_a_name_rather_than_replacing_it(
    seeded_web_db: sqlite3.Connection,
) -> None:
    """Picking both is a union, the way picking two accounts is.

    The tempting bug is an AND — a row is either named Groceries or has
    no category, never both, so an AND would answer zero and look like
    an empty ledger.
    """
    n = count_matching(
        seeded_web_db,
        TransactionsFilter(categories=["Groceries", UNCATEGORIZED]),
    )

    assert n == 6


def test_the_category_branch_still_ands_with_the_other_filters(
    seeded_web_db: sqlite3.Connection,
) -> None:
    """Its internal OR must not leak into the rest of the WHERE.

    An unparenthesised ``a OR b AND c`` binds the AND tighter and would
    quietly widen every other filter on the form.
    """
    both = TransactionsFilter(categories=["Groceries", UNCATEGORIZED])
    narrowed = both.model_copy(update={"kinds": ["income"]})

    assert count_matching(seeded_web_db, both) == 6
    # Of those six, exactly one is income (the uncategorised Earn payout);
    # the three Groceries rows and two of the uncategorised are expenses.
    assert count_matching(seeded_web_db, narrowed) == 1


# ---------------------------------------------------------------------------
# The control.
# ---------------------------------------------------------------------------


def test_transactions_page_renders_a_categories_dropdown(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()

    body = client.get("/transactions").text

    group = _group(body, "categories")
    assert '<legend class="teyebrow">Categories</legend>' in body
    assert (
        '<input type="checkbox" class="tcheck" name="categories" value="Groceries">'
        in group
    )
    assert 'value="Transport"' not in group
    assert _summary_text(group) == "Any"


def test_the_sentinel_option_carries_its_label_for_the_summary(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    """``data-label`` is what keeps "__none__" off the screen.

    Alpine rebuilds the summary text from the checked boxes after every
    change, reading ``value`` — which for this one option is a sentinel
    the owner should never see. The attribute is written only where the
    label differs from the value, so the other four dropdowns' markup is
    byte-identical to what test_flow_filter_dropdowns pins.
    """
    client: TestClient = web_client_factory()

    body = client.get("/transactions").text
    group = _group(body, "categories")

    assert (
        f'<input type="checkbox" class="tcheck" name="categories" '
        f'value="{UNCATEGORIZED}" data-label="Uncategorized">' in group
    )
    assert "<span>Uncategorized</span>" in group
    # The named options are plain: no attribute where there is nothing to say.
    assert 'value="Groceries" data-label' not in group
    assert "dataset.label" in group


def test_selecting_the_sentinel_reads_as_uncategorized_not_as_the_value(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    """Server-rendered summary, before Alpine has run at all."""
    client: TestClient = web_client_factory()

    body = client.get(
        "/transactions", params=[("categories", UNCATEGORIZED)]
    ).text
    group = _group(body, "categories")

    assert _summary_text(group) == "Uncategorized"
    assert UNCATEGORIZED not in _summary_text(group)
    assert 'class="flow-input flow-dd-summary is-set"' in group
    assert (
        f'<input type="checkbox" class="tcheck" name="categories" '
        f'value="{UNCATEGORIZED}" data-label="Uncategorized" checked>' in group
    )


def test_the_dropdown_narrows_the_list_it_sits_above(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    """End to end: the URL the form produces returns the rows it claims."""
    client: TestClient = web_client_factory()

    groceries = client.get("/transactions", params=[("categories", "Groceries")]).text
    assert len(_row_ids(groceries)) == 3
    assert "3 matches" in groceries

    none = client.get("/transactions", params=[("categories", UNCATEGORIZED)]).text
    assert len(_row_ids(none)) == 3
    assert "LEGACY needs review" in none
    assert "COM.PAGO bodega" not in none


def test_the_partial_swap_and_the_json_api_take_the_same_param(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    """One parser (``filter_from_query``) feeds all three routers."""
    client: TestClient = web_client_factory()

    partial = client.get(
        "/_partial/transactions/list", params=[("categories", UNCATEGORIZED)]
    )
    assert partial.status_code == 200
    assert len(_row_ids(partial.text)) == 3

    api = client.get("/api/transactions", params=[("categories", "Groceries")])
    assert api.status_code == 200
    assert api.json()["total"] == 3


def test_five_dropdowns_get_five_columns() -> None:
    """The row was built for exactly four; a fifth would have wrapped."""
    css = FLOW_CSS.read_text(encoding="utf-8")

    groups = css[css.index(".flow-filter-groups {") :]
    groups = groups[: groups.index("}")]
    assert "repeat(5, minmax(0, 1fr))" in groups
