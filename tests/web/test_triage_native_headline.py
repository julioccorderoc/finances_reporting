"""In triage the big number is the money that actually moved (2026-09-05).

Julio: "the currency that needs to be shown must be the original one — if
it's a bank movement the big number must be in bolívares, if it's a
binance movement it must be in USDT".

Triaging is recognising a row: the figure on the statement, in the
currency the statement is in, is what he is matching against. The dollar
conversion is context, so it swaps to the small line and takes the
provenance chip with it — the chip explains the derived number, and after
the flip the derived number is the small one.

Flow keeps the USD headline. That is deliberate and was his call: the
ledger's own list answers "how much did this cost me", where one currency
across every row is the point. So ``money()`` takes a ``headline``, and
the two surfaces pass different values.

Classes are POSITIONAL — ``tmoney-lead`` / ``tmoney-trail`` — because
after this the big figure is USD on one page and bolívares on another,
and a class called ``tmoney-usd`` that sizes the bolívar figure would be
a lie in the stylesheet.
"""

from __future__ import annotations

import re
import sqlite3
from collections.abc import Callable

from starlette.testclient import TestClient

#: fmt_native glues the ticker to the number with U+00A0, so "Bs. 365.00"
#: typed with an ordinary space matches nothing.
NBSP = "\u00a0"


def _money_blocks(html: str) -> list[str]:
    """Every ``<span class="tmoney …">…</span>``, balanced.

    A regex cannot do this: the block nests two levels on one branch and
    three on the other, so any non-greedy pattern closes early on one of
    them. Counting the tags is shorter than being clever about it.
    """
    blocks: list[str] = []
    for start in (m.start() for m in re.finditer(r'<span class="tmoney[ "]', html)):
        depth = 0
        for tag in re.finditer(r"<span\b|</span>", html[start:]):
            depth += 1 if tag.group(0).startswith("<span") else -1
            if depth == 0:
                blocks.append(html[start : start + tag.end()])
                break
    return blocks


def _lead(block: str) -> str:
    match = re.search(r'<span class="tmoney-lead"[^>]*>([^<]*)</span>', block)
    assert match is not None, f"no lead figure in {block!r}"
    return match.group(1)


def _trail(block: str) -> str | None:
    match = re.search(r'<span class="tmoney-trail"[^>]*>([^<]*)</span>', block)
    return None if match is None else match.group(1)


def _block_for(html: str, native: str) -> str:
    for block in _money_blocks(html):
        if native in block:
            return block
    raise AssertionError(f"no money block carrying {native!r}")


# ---------------------------------------------------------------------------
# Triage — the original currency leads.
# ---------------------------------------------------------------------------


def test_the_triage_modal_leads_with_the_bolivares(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    with web_client_factory() as client:
        block = _block_for(
            client.get("/_partial/triage/1/modal").text, f"16,000.00"
        )

    assert _lead(block) == f"−Bs.{NBSP}16,000.00"
    assert _trail(block) == "−$100.00"


def test_the_triage_queue_leads_with_the_bolivares(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    with web_client_factory() as client:
        block = _block_for(client.get("/triage").text, "24,000.00")

    assert _lead(block) == f"−Bs.{NBSP}24,000.00"
    assert _trail(block) == "−$154.84"


def test_the_chip_rides_the_derived_figure(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    """The chip explains the conversion, so it follows the dollars."""
    with web_client_factory() as client:
        block = _block_for(
            client.get("/_partial/triage/1/modal").text, f"16,000.00"
        )

    trail_line = re.search(
        r'<span class="tmoney-trail-line">.*?</span>\s*</span>', block, re.S
    )
    assert trail_line is not None
    assert 'class="prov' in trail_line.group(0)
    assert "−$100.00" in trail_line.group(0)


def test_a_usdt_row_says_it_once(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    """A dollar priced at one dollar has no second line to draw (D3)."""
    with web_client_factory() as client:
        block = _block_for(client.get("/triage").text, "USDT")

    assert _lead(block) == f"−200.00{NBSP}USDT"
    assert _trail(block) is None


# ---------------------------------------------------------------------------
# Flow — unchanged, on purpose.
# ---------------------------------------------------------------------------


def test_flow_still_leads_with_the_dollars(
    seeded_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    with web_client_factory() as client:
        block = _block_for(client.get("/transactions").text, f"Bs.{NBSP}365.00")

    assert _lead(block) == "−$10.00"
    assert _trail(block) == f"−Bs.{NBSP}365.00"


def test_the_flow_modal_still_leads_with_the_dollars(
    seeded_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    with web_client_factory() as client:
        rows = client.get("/transactions").text
        txn_id = re.search(r'data-tx-id="(\d+)"', rows).group(1)
        block = _money_blocks(
            client.get(f"/_partial/transactions/{txn_id}/modal").text
        )[0]

    assert _lead(block).startswith(("−$", "+$", "$"))


# ---------------------------------------------------------------------------
# The stylesheet has to size the new names.
# ---------------------------------------------------------------------------


def test_the_stylesheet_sizes_by_position_not_by_currency() -> None:
    import pathlib

    css = (
        pathlib.Path(__file__).resolve().parents[2]
        / "finances"
        / "web"
        / "static"
        / "css"
        / "triage.css"
    ).read_text(encoding="utf-8")

    assert ".tmoney-lead {" in css
    assert ".tmoney-trail {" in css
    assert ".tmoney-usd" not in css, "the old currency-named rule survived"
    assert ".tmoney-native" not in css, "the old currency-named rule survived"
