"""The four multi-select filters on Flow are dropdowns (owner, 2026-09-03).

The screenshot annotation said "make them all dropdowns" over the
Accounts / Kinds / Currencies / Sources chip groups. They stay
multi-select — the param contract (repeated ``accounts=`` etc.) is what
``test_filters_polish`` pins and is untouched — but each group is now a
native ``<details>`` whose summary reads like the Needs review / Paired
selects beside it, and whose menu holds the same checkboxes the chips
were. No ``<select multiple>``: that is a listbox, not a dropdown.

Tests precede the implementation per rule-011.
"""

from __future__ import annotations

import re
import sqlite3
from pathlib import Path

from fastapi.testclient import TestClient

FLOW_CSS = (
    Path(__file__).resolve().parents[2] / "finances" / "web" / "static" / "css" / "flow.css"
)

GROUPS = ("accounts", "kinds", "currencies", "sources")


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


def test_each_group_is_a_details_dropdown_with_the_same_checkboxes(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()

    body = client.get("/transactions").text

    assert "choice-chip" not in body
    for name in GROUPS:
        group = _group(body, name)
        # The eyebrow label survives as the fieldset's legend.
        assert '<summary class="flow-input flow-dd-summary' in group
        assert '<div class="flow-dd-menu"' in group
        assert re.search(
            rf'<label class="flow-dd-option"><input type="checkbox" class="tcheck" name="{name}" value="[^"]+"><span>[^<]+</span></label>',
            group,
        ), name
        # Closed by default; closes on Escape and on a click elsewhere.
        assert not re.search(r'<details\s+class="flow-dd"[^>]*\sopen[\s>]', group)
        assert "@click.outside" in group
        assert "@keydown.escape" in group
        # The summary text is kept live by Alpine after a change.
        assert '@change="sync()"' in group
        assert 'x-text="text"' in group
    assert '<legend class="teyebrow">Accounts</legend>' in body


def test_summary_reads_any_the_one_value_or_a_count(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()

    none = client.get("/transactions").text
    for name in GROUPS:
        group = _group(none, name)
        assert _summary_text(group) == "Any"
        assert '<summary class="flow-input flow-dd-summary"' in group

    one = client.get("/transactions", params=[("accounts", "Provincial")]).text
    group = _group(one, "accounts")
    assert _summary_text(group) == "Provincial"
    assert 'class="flow-input flow-dd-summary is-set"' in group
    assert '<input type="checkbox" class="tcheck" name="accounts" value="Provincial" checked>' in group
    assert _summary_text(_group(one, "kinds")) == "Any"

    two = client.get(
        "/transactions", params=[("accounts", "Provincial"), ("accounts", "Cash USD")]
    ).text
    assert _summary_text(_group(two, "accounts")) == "2 selected"


def test_dropdowns_sit_in_one_row_of_four_and_flow_css_owns_them() -> None:
    css = FLOW_CSS.read_text(encoding="utf-8")

    assert "choice-chip" not in css
    for selector in (".flow-dd {", ".flow-dd-summary", ".flow-dd-menu {", ".flow-dd-option"):
        assert selector in css, selector

    groups = css[css.index(".flow-filter-groups {") :]
    groups = groups[: groups.index("}")]
    assert "repeat(4, minmax(0, 1fr))" in groups

    menu = css[css.index(".flow-dd-menu {") :]
    menu = menu[: menu.index("}")]
    assert "position: absolute" in menu
    assert "var(--surface-raised)" in menu
    assert "var(--shadow-lg)" in menu

    # Selection is a state, never the accent: the summary's set look is ink.
    is_set = css[css.index(".flow-dd-summary.is-set") :]
    is_set = is_set[: is_set.index("}")]
    assert "red" not in is_set
