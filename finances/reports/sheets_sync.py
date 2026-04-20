"""Google Sheets read-only mirror (EPIC-014).

Mirrors four pre-existing reports (``Transactions``, ``Balances``,
``Monthly``, ``Needs Review``) out to a Google Sheets spreadsheet as a
read-only projection. SQLite remains the source of truth per rule-001 /
ADR-001 — this module never reads from the Sheet, only writes to it.

Design
------

* **Destructive per tab.** Every tab is ``.clear()``-ed then ``.update()``-d
  with the freshly computed rows. No row-level merge — the mirror is fully
  derived state. This keeps the semantics predictable: delete a transaction
  in SQLite, re-run ``finances sync sheets``, it disappears from the tab.
* **Sentinel row.** Row 1 of every tab carries a short read-only warning
  and is frozen via ``worksheet.freeze(rows=1)`` so it stays pinned when a
  viewer scrolls. This is the only vertical signal we rely on to keep
  humans from editing the mirror in place.
* **Injectable gspread client.** ``sync_to_sheets`` accepts an optional
  ``client`` argument; tests pass a ``MagicMock`` shaped like
  ``gspread.Client``, production lets the function call :func:`_open_client`
  to build one from the service-account credentials in ``.env``. This is
  the same boundary-mocking pattern used by the Binance ingest (rule-011).

The tab builders are pure (conn → ``TabContent``) so they can be tested
without ever touching gspread.
"""

from __future__ import annotations

import sqlite3
import time
from typing import Any

from pydantic import BaseModel, ConfigDict

from finances.reports import balances as balances_report
from finances.reports import consolidated_usd as consolidated_report
from finances.reports import monthly as monthly_report
from finances.reports import needs_review as needs_review_report


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------


SENTINEL_TEXT = "⚠ READ-ONLY MIRROR — edit finances.db, not this sheet"

TRANSACTIONS_TAB = "Transactions"
BALANCES_TAB = "Balances"
MONTHLY_TAB = "Monthly"
NEEDS_REVIEW_TAB = "Needs Review"

_SCOPES = (
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
)


# ---------------------------------------------------------------------------
# Pydantic boundary
# ---------------------------------------------------------------------------


class TabContent(BaseModel):
    """One tab's rows, ready to hand to the writer.

    ``rows`` is a list-of-strings-of-strings so the gspread ``update``
    payload is trivially serializable and so the shape is obvious to
    anyone reading a test failure.
    """

    model_config = ConfigDict(strict=False, extra="forbid")

    name: str
    headers: list[str]
    rows: list[list[str]]


class SyncReport(BaseModel):
    """Summary returned by :func:`sync_to_sheets`."""

    model_config = ConfigDict(strict=False, extra="forbid")

    spreadsheet_id: str
    tabs: list[str]
    rows_written: dict[str, int]
    duration_s: float


# ---------------------------------------------------------------------------
# Tab builders — pure functions of (conn,)
# ---------------------------------------------------------------------------


_TRANSACTIONS_HEADERS: list[str] = [
    "transaction_id",
    "occurred_at",
    "account_id",
    "kind",
    "currency",
    "amount_native",
    "amount_usd",
    "rate_source",
    "description",
    "is_bcv_fallback",
]


def build_transactions_tab(conn: sqlite3.Connection) -> TabContent:
    """Consolidated USD view of every non-transfer transaction.

    Mirrors ``consolidated_usd.build_report(strict=False)`` so the
    headline/fallback split from ADR-005 is preserved — BCV-sourced rows
    still show up, flagged via ``is_bcv_fallback``.
    """
    report = consolidated_report.build_report(conn, strict=False)
    rows: list[list[str]] = []
    for r in report.rows:
        rows.append(
            [
                str(r.transaction_id),
                r.occurred_at.isoformat(),
                str(r.account_id),
                r.kind,
                r.currency,
                str(r.amount_native),
                "" if r.amount_usd is None else str(r.amount_usd),
                r.rate_source,
                r.description or "",
                "true" if r.is_bcv_fallback else "false",
            ]
        )
    return TabContent(
        name=TRANSACTIONS_TAB, headers=list(_TRANSACTIONS_HEADERS), rows=rows
    )


_BALANCES_HEADERS: list[str] = [
    "account_id",
    "account_name",
    "currency",
    "balance_native",
]


def build_balances_tab(conn: sqlite3.Connection) -> TabContent:
    """Per-account native-currency balances."""
    rows: list[list[str]] = []
    for b in balances_report.get_balances(conn):
        rows.append(
            [
                str(b.account_id),
                b.account_name,
                b.currency,
                str(b.balance_native),
            ]
        )
    return TabContent(
        name=BALANCES_TAB, headers=list(_BALANCES_HEADERS), rows=rows
    )


_MONTHLY_HEADERS: list[str] = [
    "month",
    "account_id",
    "account_name",
    "category_id",
    "category_name",
    "kind",
    "tx_count",
    "total_native",
    "currency",
    "total_usd",
    "fallback_usd",
    "needs_review_count",
]


def build_monthly_tab(conn: sqlite3.Connection) -> TabContent:
    """Monthly aggregate per (account, category, kind)."""
    report = monthly_report.build_report(conn)
    rows: list[list[str]] = []
    for r in report.rows:
        rows.append(
            [
                r.month,
                str(r.account_id),
                r.account_name,
                "" if r.category_id is None else str(r.category_id),
                r.category_name or "",
                r.kind,
                str(r.tx_count),
                str(r.total_native),
                r.currency,
                str(r.total_usd),
                str(r.fallback_usd),
                str(r.needs_review_count),
            ]
        )
    return TabContent(
        name=MONTHLY_TAB, headers=list(_MONTHLY_HEADERS), rows=rows
    )


_NEEDS_REVIEW_HEADERS: list[str] = [
    "transaction_id",
    "occurred_at",
    "account_id",
    "kind",
    "amount",
    "currency",
    "description",
    "source",
]


def build_needs_review_tab(conn: sqlite3.Connection) -> TabContent:
    """Every ``needs_review=1`` row — the triage queue."""
    rows: list[list[str]] = []
    for r in needs_review_report.get_needs_review(conn):
        rows.append(
            [
                str(r.transaction_id),
                r.occurred_at.isoformat(),
                str(r.account_id),
                r.kind,
                str(r.amount),
                r.currency,
                r.description or "",
                r.source,
            ]
        )
    return TabContent(
        name=NEEDS_REVIEW_TAB, headers=list(_NEEDS_REVIEW_HEADERS), rows=rows
    )


# ---------------------------------------------------------------------------
# Writer
# ---------------------------------------------------------------------------


def _get_or_create_worksheet(
    spreadsheet: Any, tab_name: str, *, columns: int
) -> Any:
    """Return the worksheet named ``tab_name``, creating it when absent."""
    # Lazy-import so tests that never touch the real gspread can still
    # import this module without the package installed.
    from gspread.exceptions import WorksheetNotFound

    try:
        return spreadsheet.worksheet(tab_name)
    except WorksheetNotFound:
        return spreadsheet.add_worksheet(
            title=tab_name, rows=100, cols=max(columns, 1)
        )


def _tab_values(tab: TabContent) -> list[list[str]]:
    """Build the full 2D payload: sentinel row + headers + data rows."""
    width = max(len(tab.headers), 1)
    sentinel = [SENTINEL_TEXT] + [""] * (width - 1)
    values: list[list[str]] = [sentinel, list(tab.headers)]
    for r in tab.rows:
        values.append(list(r))
    return values


def _write_tab(spreadsheet: Any, tab: TabContent) -> int:
    """Destructively clear + write ``tab``, then freeze the sentinel row.

    Returns the number of data rows written (excluding sentinel + header).
    """
    worksheet = _get_or_create_worksheet(
        spreadsheet, tab.name, columns=len(tab.headers)
    )
    worksheet.clear()
    worksheet.update(values=_tab_values(tab))
    worksheet.freeze(rows=1)
    return len(tab.rows)


def sync_to_sheets(
    conn: sqlite3.Connection,
    *,
    spreadsheet_id: str,
    client: Any | None = None,
) -> SyncReport:
    """Mirror all four tabs to ``spreadsheet_id``.

    Parameters
    ----------
    conn:
        Read-only SQLite connection to the ledger. All four builders run
        queries via ``conn``; no writes ever happen here.
    spreadsheet_id:
        Google Sheets ID (the bit between ``/d/`` and ``/edit`` in the
        URL). Must be a sheet the service account has been shared on as
        an editor.
    client:
        Optional ``gspread.Client``. When ``None`` the function builds one
        from :func:`_open_client`. Injected in tests to avoid live auth.
    """
    if client is None:
        client = _open_client()

    spreadsheet = client.open_by_key(spreadsheet_id)

    tabs: list[TabContent] = [
        build_transactions_tab(conn),
        build_balances_tab(conn),
        build_monthly_tab(conn),
        build_needs_review_tab(conn),
    ]

    started = time.monotonic()
    rows_written: dict[str, int] = {}
    names: list[str] = []
    for tab in tabs:
        rows_written[tab.name] = _write_tab(spreadsheet, tab)
        names.append(tab.name)
    duration = time.monotonic() - started

    return SyncReport(
        spreadsheet_id=spreadsheet_id,
        tabs=names,
        rows_written=rows_written,
        duration_s=duration,
    )


# ---------------------------------------------------------------------------
# Auth (production path; tests inject a MagicMock instead)
# ---------------------------------------------------------------------------


def _open_client() -> Any:
    """Build an authenticated gspread client from the service-account env.

    Imported lazily so test suites that stub this function out via
    ``monkeypatch.setattr`` never require ``google-auth`` at import time.
    """
    import gspread
    from google.oauth2.service_account import Credentials

    from finances.config import google_service_account

    creds_info = google_service_account()
    credentials = Credentials.from_service_account_info(
        creds_info, scopes=list(_SCOPES)
    )
    return gspread.authorize(credentials)


__all__ = [
    "BALANCES_TAB",
    "MONTHLY_TAB",
    "NEEDS_REVIEW_TAB",
    "SENTINEL_TEXT",
    "SyncReport",
    "TRANSACTIONS_TAB",
    "TabContent",
    "build_balances_tab",
    "build_monthly_tab",
    "build_needs_review_tab",
    "build_transactions_tab",
    "sync_to_sheets",
]
