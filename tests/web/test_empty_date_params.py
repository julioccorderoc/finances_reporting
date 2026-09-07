"""An empty date box is "no date", not a malformed one.

Found in a browser on 2026-09-07 while hand-walking the new category
dropdown: ticking any option fired

    GET /_partial/transactions/list?date_from=&date_to=&q=&…

and htmx logged a 422. Not the category filter's doing — *every* control
on the form was dead the same way, and had been since 2026-09-05.

How it got in, and why ~2,240 tests all passed over it: the filter form
has always serialised every field it owns, empty ones included, so it has
always sent ``date_from=``. Until 2026-09-05 that string was never empty,
because ``resolve_defaults`` invented a last-30-days window and the
template rendered it into the two date inputs. Removing that window (the
right call — it made an unfiltered search silently local) left the boxes
blank, and blank is what FastAPI hands Pydantic as ``""`` for a
``date | None``, which is not a date and not None.

Nothing in the suite ever sent it. Tests build params as lists of pairs
and simply omit what they are not exercising; the browser has no such
option. This file sends what the form sends.

``_monthly_filter_dep`` already had the coercion (``value == ""`` → None)
and is the shape this follows.

Tests precede the fix per rule-011.
"""

from __future__ import annotations

import sqlite3

from fastapi.testclient import TestClient

#: Exactly what the filter form serialises with nothing filled in — the
#: string off the failing request in the browser console, order included.
UNTOUCHED_FORM = (
    "date_from=&date_to=&q=&needs_review=any&paired=any"
    "&sort=occurred_at&direction=desc&page=1&page_size=50"
)


def test_the_untouched_filter_form_does_not_422(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    """The regression itself: submit the form as rendered, change nothing."""
    client: TestClient = web_client_factory()

    resp = client.get(f"/_partial/transactions/list?{UNTOUCHED_FORM}")

    assert resp.status_code == 200


def test_ticking_one_dropdown_on_the_untouched_form_works(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    """What the owner actually did. Any group; categories is the new one."""
    client: TestClient = web_client_factory()

    resp = client.get(
        f"/_partial/transactions/list?{UNTOUCHED_FORM}&categories=Groceries"
    )

    assert resp.status_code == 200
    assert resp.text.count('data-tx-id="') == 3


def test_an_empty_date_means_no_date_not_a_bad_one(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    """``date_from=`` must land as ``None``, i.e. the whole ledger."""
    client: TestClient = web_client_factory()

    empty = client.get("/api/transactions?date_from=&date_to=")
    absent = client.get("/api/transactions")

    assert empty.status_code == 200
    assert empty.json()["filter"]["date_from"] is None
    assert empty.json()["filter"]["date_to"] is None
    assert empty.json()["total"] == absent.json()["total"]


def test_the_full_page_takes_the_empty_dates_too(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    """hx-push-url means the browser's address bar carries them back."""
    client: TestClient = web_client_factory()

    resp = client.get(f"/transactions?{UNTOUCHED_FORM}")

    assert resp.status_code == 200
    assert 'value=""' in resp.text  # the date inputs, still empty


def test_a_genuinely_malformed_date_is_still_rejected(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    """Coercing "" is not coercing everything — a typo stays a 422.

    Silently reading "yesterday" as "no filter" would answer the wrong
    question with a full page of rows and no way to tell.
    """
    client: TestClient = web_client_factory()

    assert client.get("/api/transactions?date_from=yesterday").status_code == 422
    assert client.get("/api/transactions?date_from=2026-13-01").status_code == 422


def test_a_real_date_still_narrows(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    """Guard against a coercion that swallows the value it was given."""
    client: TestClient = web_client_factory()

    whole = client.get("/api/transactions?date_from=&date_to=").json()
    windowed = client.get("/api/transactions?date_from=2011-01-01").json()

    assert windowed["filter"]["date_from"] == "2011-01-01"
    # The fixture's 2010 row falls outside; everything else is recent.
    assert windowed["total"] == whole["total"] - 1


def test_blank_space_counts_as_blank(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    """A hand-edited URL with a stray space is the same intent."""
    client: TestClient = web_client_factory()

    resp = client.get("/api/transactions", params={"date_from": "  "})

    assert resp.status_code == 200
    assert resp.json()["filter"]["date_from"] is None
