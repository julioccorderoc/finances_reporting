"""Money arriving gets a colour; money leaving keeps the ink (2026-09-05).

Julio: "we should add subtle colors to the UI to indicate if it's money in
or money out (subtle, but noticeable)". He also said he feels a little lost
while triaging, which is what this is really for — the sign is easy to miss
at 22px and it is the first thing you need to know about a row.

Only INFLOW is tinted. Outflow is most of the ledger, so colouring it would
colour the whole page; leaving it in ink means one coloured number per
screenful and "coloured = arriving" is a rule you learn in one sitting. The
hue is a muted forest green, never red — red already means error, danger
and BCV everywhere else in SIGNAL.

Transfers are excluded. A paired transfer's positive leg is the same money
as its negative one, so tinting it would claim the money arrived twice.

This overrides signal.css's "money is ink with a sign, never a colour",
which is why that comment is amended rather than deleted: it was a real
decision and this is a real reversal of it.
"""

from __future__ import annotations

import pathlib
import re
import sqlite3
from collections.abc import Callable
from decimal import Decimal

from starlette.testclient import TestClient

CSS = pathlib.Path(__file__).resolve().parents[2] / "finances" / "web" / "static" / "css"

INFLOW = "tmoney-in"


def _lead_span(html: str, figure: str) -> str:
    match = re.search(
        r'<span class="tmoney-lead[^"]*"[^>]*>' + re.escape(figure) + "</span>", html
    )
    assert match is not None, f"no lead figure {figure!r} in this render"
    return match.group(0)


# ---------------------------------------------------------------------------
# The markup.
# ---------------------------------------------------------------------------


def test_money_arriving_is_tinted(
    seeded_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    """The seeded ABONO nomina income row."""
    with web_client_factory() as client:
        html = client.get("/transactions", params={"kinds": "income"}).text

    span = _lead_span(html, "+$1,000.00")
    assert INFLOW in span


def test_money_leaving_keeps_the_ink(
    seeded_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    with web_client_factory() as client:
        html = client.get("/transactions", params={"kinds": "expense"}).text

    for span in re.findall(r'<span class="tmoney-lead[^"]*"', html):
        assert INFLOW not in span, span


def test_a_transfer_leg_is_never_tinted(
    web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    """Its positive leg is the negative one's money, not a second arrival."""
    from datetime import UTC, datetime

    from finances.db.repos import accounts as accounts_repo
    from finances.db.repos import transactions as transactions_repo
    from finances.domain.models import (
        Account,
        AccountKind,
        Transaction,
        TransactionKind,
    )

    cash = accounts_repo.insert(
        web_db, Account(name="Cash USD", kind=AccountKind.CASH, currency="USD")
    )
    transactions_repo.insert(
        web_db,
        Transaction(
            account_id=cash.id,
            occurred_at=datetime(2026, 5, 1, tzinfo=UTC),
            kind=TransactionKind.TRANSFER,
            amount=Decimal("250.00"),
            currency="USD",
            description="MOVED IN",
            source="cash_cli",
            source_ref="xfer-in",
        ),
    )

    with web_client_factory() as client:
        html = client.get("/transactions").text

    assert INFLOW not in _lead_span(html, "+$250.00")


def test_triage_tints_the_native_headline(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    """The colour rides whichever figure leads, so it works after the flip."""
    with web_client_factory() as client:
        html = client.get("/triage").text

    span = _lead_span(html, "Bs. 50,000.00")
    assert INFLOW in span


# ---------------------------------------------------------------------------
# The stylesheet.
# ---------------------------------------------------------------------------


def test_the_positive_token_is_no_longer_ink() -> None:
    """signal.css already had the axis; it just pointed both ends at ink."""
    signal = (CSS / "signal.css").read_text(encoding="utf-8")

    positive = re.search(r"--text-positive:\s*([^;]+);", signal)
    negative = re.search(r"--text-negative:\s*([^;]+);", signal)
    assert positive is not None and negative is not None
    assert positive.group(1).strip() != "var(--ink-900)"
    assert negative.group(1).strip() == "var(--ink-900)", "outflow stays ink"


def test_the_reversal_is_written_down() -> None:
    """The old rule was a decision, so overriding it is documented in place."""
    signal = (CSS / "signal.css").read_text(encoding="utf-8")

    assert "green never enters this system at all" not in signal, (
        "the superseded claim is still asserted as current"
    )
    assert "2026-09-05" in signal


def test_the_inflow_rule_uses_the_token() -> None:
    triage = (CSS / "triage.css").read_text(encoding="utf-8")

    rule = re.search(r"\.tmoney-in\s*\{[^}]*\}", triage, re.S)
    assert rule is not None, "no .tmoney-in rule in triage.css"
    assert "var(--text-positive)" in rule.group(0)
