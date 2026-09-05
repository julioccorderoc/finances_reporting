"""Searching must not lose rows to a date window nobody chose (2026-09-05).

Julio searched "Hemirla Milexa Pena Ba", got nothing, and there are five
such rows in the ledger. They are all older than thirty days, and
/transactions used to invent a last-thirty-days window when the URL named
no dates — so the honest answer to a filter he never set was zero.

Two changes, both here:

1. No date filter is the default. A bare /transactions is the whole
   ledger, and the date pickers start empty.
2. When dates ARE set and hide every match, the empty state counts what
   is outside them and offers one click to drop them. The filter stays
   honest; it just stops being silent.
"""

from __future__ import annotations

import re
import sqlite3

from fastapi.testclient import TestClient

#: Seeded by ``seeded_web_db``, dated 2010-01-01 — far outside any window
#: a "recent" default would draw.
ANCIENT = "LEGACY needs review"


# ---------------------------------------------------------------------------
# 1 — the default is every date.
# ---------------------------------------------------------------------------


def test_bare_transactions_page_shows_rows_of_every_age(
    seeded_web_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    """A 2010 row is on the first page of an unfiltered list."""
    client: TestClient = web_client_factory()
    resp = client.get("/transactions")
    assert resp.status_code == 200
    assert ANCIENT in resp.text


def test_bare_transactions_page_leaves_the_date_pickers_empty(
    seeded_web_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    """Nothing is pre-filled, because nothing is being filtered."""
    client: TestClient = web_client_factory()
    body = client.get("/transactions").text

    for field in ("date_from", "date_to"):
        match = re.search(
            rf'name="{field}"\s+value="([^"]*)"', body
        )
        assert match is not None, f"no {field} input rendered"
        assert match.group(1) == "", f"{field} pre-filled with {match.group(1)!r}"


def test_bare_transactions_header_states_no_window(
    seeded_web_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    """No dates, no "Aug 6 - Sep 5" line under the row count."""
    client: TestClient = web_client_factory()
    assert 'class="flow-window"' not in client.get("/transactions").text


def test_a_search_alone_reaches_the_whole_ledger(
    seeded_web_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    """The reported bug: q with no dates finds the old row."""
    client: TestClient = web_client_factory()
    resp = client.get("/transactions", params={"q": "LEGACY"})
    assert resp.status_code == 200
    assert ANCIENT in resp.text
    assert "No rows match these filters" not in resp.text


# ---------------------------------------------------------------------------
# 2 — a window the owner DID set says what it is hiding.
# ---------------------------------------------------------------------------

#: A range containing none of the seeded rows.
_EMPTY_RANGE = {"date_from": "2019-01-01", "date_to": "2019-12-31"}


def test_empty_state_counts_the_matches_outside_the_range(
    seeded_web_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    client: TestClient = web_client_factory()
    body = client.get(
        "/transactions", params={"q": "LEGACY", **_EMPTY_RANGE}
    ).text

    assert "No rows match these filters" in body
    assert "1 row matches outside" in body
    assert "Tue, Jan 1, 2019 – Tue, Dec 31, 2019" in body


def test_empty_state_offers_the_same_search_over_every_date(
    seeded_web_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    """One click drops the dates and keeps everything else."""
    client: TestClient = web_client_factory()
    body = client.get(
        "/transactions", params={"q": "LEGACY", **_EMPTY_RANGE}
    ).text

    assert "data-search-all-dates" in body
    assert 'href="/transactions?q=LEGACY"' in body


def test_the_outside_count_respects_the_other_filters(
    seeded_web_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    """It counts what the range hides, not what the whole ledger holds."""
    client: TestClient = web_client_factory()
    body = client.get(
        "/transactions",
        params={"q": "no such description anywhere", **_EMPTY_RANGE},
    ).text

    assert "No rows match these filters" in body
    assert "flow-empty-outside" not in body
    assert "data-search-all-dates" not in body


def test_no_dates_means_no_outside_offer(
    seeded_web_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    """With no window there is no "elsewhere" to point at."""
    client: TestClient = web_client_factory()
    body = client.get(
        "/transactions", params={"q": "no such description anywhere"}
    ).text

    assert "No rows match these filters" in body
    assert "data-search-all-dates" not in body


def test_plural_when_the_range_hides_several(
    seeded_web_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    client: TestClient = web_client_factory()
    body = client.get(
        "/transactions", params={"q": "COM.PAGO", **_EMPTY_RANGE}
    ).text

    assert "3 rows match outside" in body
