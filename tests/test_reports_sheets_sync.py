"""Tests for the Sheets read-only mirror (EPIC-014).

Every ``gspread`` surface is mocked via ``pytest-mock`` / ``MagicMock`` per
rule-011 — no live Sheets writes happen in the suite. Tests target the
three public-function buckets the epic ships:

1. Tab builders — ``build_{transactions,balances,monthly,needs_review}_tab``.
   Happy path + empty-DB failure-mode for each.
2. ``sync_to_sheets`` — destructive-per-tab semantics, sentinel ordering,
   freeze, gspread-error propagation, return-shape.
3. ``finances.config.google_service_account`` — both env vectors and the
   failure-mode when neither is set.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from gspread.exceptions import WorksheetNotFound
from typer.testing import CliRunner

from finances.db.repos import rates as rates_repo
from finances.db.repos import transactions as transactions_repo
from finances.domain.models import (
    Rate,
    Transaction,
    TransactionKind,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build_mock_gspread_client() -> tuple[MagicMock, dict[str, MagicMock]]:
    """Return a MagicMock client plus a dict of worksheet-name → MagicMock.

    The spreadsheet's ``.worksheet(name)`` raises ``WorksheetNotFound`` until
    ``.add_worksheet(title=name, ...)`` has been called for that name — the
    same contract as real gspread. This lets tests exercise both the
    create-new-tab branch and the reuse-existing branch without touching the
    network.
    """
    client = MagicMock(name="gspread.Client")
    spreadsheet = client.open_by_key.return_value
    worksheets: dict[str, MagicMock] = {}

    def _add_worksheet(*, title: str, rows: int, cols: int) -> MagicMock:
        ws = MagicMock(name=f"worksheet[{title}]")
        ws.title = title
        worksheets[title] = ws
        return ws

    def _worksheet(name: str) -> MagicMock:
        if name in worksheets:
            return worksheets[name]
        raise WorksheetNotFound(name)

    spreadsheet.worksheet.side_effect = _worksheet
    spreadsheet.add_worksheet.side_effect = _add_worksheet
    return client, worksheets


def _seed_rate(
    conn: sqlite3.Connection,
    *,
    source: str,
    rate: Decimal,
    base: str = "USDT",
    quote: str = "VES",
) -> None:
    """Add a rate row for rate resolution.

    BCV publishes ``USD/VES``; Binance P2P publishes ``USDT/VES`` — the
    rate resolver discriminates by source, so callers that want to
    exercise the BCV branch must pass ``base="USD"``.
    """
    rates_repo.insert(
        conn,
        Rate(
            base=base,
            quote=quote,
            rate=rate,
            as_of_date=datetime(2026, 3, 15, tzinfo=UTC).date(),
            source=source,
        ),
    )


def _insert_ves_expense(
    conn: sqlite3.Connection,
    *,
    amount: Decimal,
    description: str,
    source_ref: str,
    source: str = "provincial",
    user_rate: Decimal | None = None,
) -> int:
    """Insert a VES-denominated expense row (the shape consolidated_usd sees)."""
    account_id = conn.execute(
        "SELECT id FROM accounts WHERE name = 'Provincial Bolivares'"
    ).fetchone()[0]
    result = transactions_repo.upsert_by_source_ref(
        conn,
        Transaction(
            account_id=account_id,
            occurred_at=datetime(2026, 3, 15, 12, 0, tzinfo=UTC),
            kind=TransactionKind.EXPENSE,
            amount=amount,
            currency="VES",
            description=description,
            source=source,
            source_ref=source_ref,
            user_rate=user_rate,
        ),
    )
    assert result["rows_inserted"] == 1
    return int(result["id"])


def _insert_needs_review(conn: sqlite3.Connection, *, source_ref: str) -> int:
    account_id = conn.execute(
        "SELECT id FROM accounts WHERE name = 'Provincial Bolivares'"
    ).fetchone()[0]
    result = transactions_repo.upsert_by_source_ref(
        conn,
        Transaction(
            account_id=account_id,
            occurred_at=datetime(2026, 3, 15, 12, 0, tzinfo=UTC),
            kind=TransactionKind.EXPENSE,
            amount=Decimal("500.00"),
            currency="VES",
            description="unclear",
            source="provincial",
            source_ref=source_ref,
            needs_review=True,
        ),
    )
    return int(result["id"])


# ---------------------------------------------------------------------------
# build_balances_tab — happy + failure-mode
# ---------------------------------------------------------------------------


class TestBuildBalancesTab:
    def test_empty_db_returns_header_only(self, seeded_db: sqlite3.Connection) -> None:
        from finances.reports import sheets_sync

        tab = sheets_sync.build_balances_tab(seeded_db)
        assert tab.name == sheets_sync.BALANCES_TAB
        assert tab.headers == [
            "account_id",
            "account_name",
            "currency",
            "balance_native",
        ]
        # seeded_db has 5 accounts with zero balance.
        assert len(tab.rows) == 5
        for row in tab.rows:
            assert len(row) == len(tab.headers)

    def test_reflects_transaction_amounts(
        self, seeded_db: sqlite3.Connection
    ) -> None:
        from finances.reports import sheets_sync

        _insert_ves_expense(
            seeded_db,
            amount=Decimal("-1000.00"),
            description="bodega",
            source_ref="balbuilder-1",
        )
        tab = sheets_sync.build_balances_tab(seeded_db)
        provincial = next(r for r in tab.rows if r[1] == "Provincial Bolivares")
        # Column order: account_id, account_name, currency, balance_native.
        # The v_account_balances view casts to REAL, so exact-string equality
        # would drift on trailing-zero formatting — compare numerically.
        assert Decimal(provincial[3]) == Decimal("-1000.00")


# ---------------------------------------------------------------------------
# build_transactions_tab — happy + failure-mode
# ---------------------------------------------------------------------------


class TestBuildTransactionsTab:
    def test_empty_db_returns_header_only(self, seeded_db: sqlite3.Connection) -> None:
        from finances.reports import sheets_sync

        tab = sheets_sync.build_transactions_tab(seeded_db)
        assert tab.name == sheets_sync.TRANSACTIONS_TAB
        assert tab.headers == [
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
        assert tab.rows == []

    def test_bcv_fallback_flag_surfaced_as_string(
        self, seeded_db: sqlite3.Connection
    ) -> None:
        from finances.reports import sheets_sync

        # BCV-only resolver path: no user_rate, no P2P median, just a BCV row.
        # BCV publishes USD/VES (not USDT/VES), which is why base="USD" here.
        _seed_rate(seeded_db, source="bcv", rate=Decimal("36.50"), base="USD")
        _insert_ves_expense(
            seeded_db,
            amount=Decimal("-1000.00"),
            description="bcv row",
            source_ref="txntab-1",
        )
        tab = sheets_sync.build_transactions_tab(seeded_db)
        assert len(tab.rows) == 1
        row = tab.rows[0]
        # is_bcv_fallback is the last column; rendered as lowercase boolean.
        assert row[-1] == "true"
        # rate_source for a BCV-only resolver is "bcv".
        assert "bcv" in row[7]

    def test_native_usd_headline_row_marked_false(
        self, seeded_db: sqlite3.Connection
    ) -> None:
        from finances.reports import sheets_sync

        account_id = seeded_db.execute(
            "SELECT id FROM accounts WHERE name = 'Cash USD'"
        ).fetchone()[0]
        transactions_repo.upsert_by_source_ref(
            seeded_db,
            Transaction(
                account_id=account_id,
                occurred_at=datetime(2026, 3, 15, 12, 0, tzinfo=UTC),
                kind=TransactionKind.EXPENSE,
                amount=Decimal("-12.00"),
                currency="USD",
                description="lunch",
                source="cash_cli",
                source_ref="usd-1",
            ),
        )
        tab = sheets_sync.build_transactions_tab(seeded_db)
        row = tab.rows[0]
        assert row[-1] == "false"


# ---------------------------------------------------------------------------
# build_monthly_tab — happy + failure-mode
# ---------------------------------------------------------------------------


class TestBuildMonthlyTab:
    def test_empty_db_returns_header_only(self, seeded_db: sqlite3.Connection) -> None:
        from finances.reports import sheets_sync

        tab = sheets_sync.build_monthly_tab(seeded_db)
        assert tab.name == sheets_sync.MONTHLY_TAB
        assert "month" in tab.headers
        assert "total_usd" in tab.headers
        assert "fallback_usd" in tab.headers
        assert "needs_review_count" in tab.headers
        assert tab.rows == []

    def test_aggregates_rows_into_month_bucket(
        self, seeded_db: sqlite3.Connection
    ) -> None:
        from finances.reports import sheets_sync

        _seed_rate(seeded_db, source="binance_p2p_median", rate=Decimal("36.00"))
        _insert_ves_expense(
            seeded_db,
            amount=Decimal("-1000.00"),
            description="a",
            source_ref="mo-1",
        )
        _insert_ves_expense(
            seeded_db,
            amount=Decimal("-500.00"),
            description="b",
            source_ref="mo-2",
        )
        tab = sheets_sync.build_monthly_tab(seeded_db)
        assert len(tab.rows) >= 1
        first = tab.rows[0]
        month_col = tab.headers.index("month")
        tx_count_col = tab.headers.index("tx_count")
        assert first[month_col] == "2026-03"
        assert first[tx_count_col] == "2"


# ---------------------------------------------------------------------------
# build_needs_review_tab — happy + failure-mode
# ---------------------------------------------------------------------------


class TestBuildNeedsReviewTab:
    def test_empty_db_returns_header_only(self, seeded_db: sqlite3.Connection) -> None:
        from finances.reports import sheets_sync

        tab = sheets_sync.build_needs_review_tab(seeded_db)
        assert tab.name == sheets_sync.NEEDS_REVIEW_TAB
        assert tab.headers == [
            "transaction_id",
            "occurred_at",
            "account_id",
            "kind",
            "amount",
            "currency",
            "description",
            "source",
        ]
        assert tab.rows == []

    def test_surfaces_only_needs_review_rows(
        self, seeded_db: sqlite3.Connection
    ) -> None:
        from finances.reports import sheets_sync

        _insert_ves_expense(
            seeded_db,
            amount=Decimal("-100.00"),
            description="clean",
            source_ref="nr-clean",
        )
        _insert_needs_review(seeded_db, source_ref="nr-dirty")
        tab = sheets_sync.build_needs_review_tab(seeded_db)
        assert len(tab.rows) == 1
        source_refs_in_tab = [r[7] for r in tab.rows]  # "source" column last
        assert source_refs_in_tab == ["provincial"]

    def test_null_description_renders_as_empty_string(
        self, seeded_db: sqlite3.Connection
    ) -> None:
        from finances.reports import sheets_sync

        account_id = seeded_db.execute(
            "SELECT id FROM accounts WHERE name = 'Provincial Bolivares'"
        ).fetchone()[0]
        transactions_repo.upsert_by_source_ref(
            seeded_db,
            Transaction(
                account_id=account_id,
                occurred_at=datetime(2026, 3, 15, 12, 0, tzinfo=UTC),
                kind=TransactionKind.EXPENSE,
                amount=Decimal("-42.00"),
                currency="VES",
                description=None,
                source="provincial",
                source_ref="nr-nodesc",
                needs_review=True,
            ),
        )
        tab = sheets_sync.build_needs_review_tab(seeded_db)
        desc_col = tab.headers.index("description")
        assert tab.rows[0][desc_col] == ""


# ---------------------------------------------------------------------------
# sync_to_sheets — destructive semantics, sentinel order, freeze, errors
# ---------------------------------------------------------------------------


class TestSyncToSheets:
    def test_writes_all_four_tabs_in_order(
        self, seeded_db: sqlite3.Connection
    ) -> None:
        from finances.reports import sheets_sync

        client, worksheets = _build_mock_gspread_client()
        report = sheets_sync.sync_to_sheets(
            seeded_db, spreadsheet_id="sheet-xyz", client=client
        )

        assert report.spreadsheet_id == "sheet-xyz"
        assert report.tabs == [
            sheets_sync.TRANSACTIONS_TAB,
            sheets_sync.BALANCES_TAB,
            sheets_sync.MONTHLY_TAB,
            sheets_sync.NEEDS_REVIEW_TAB,
        ]
        client.open_by_key.assert_called_once_with("sheet-xyz")
        # Each tab had exactly one .clear() call and exactly one values write.
        for name in report.tabs:
            ws = worksheets[name]
            assert ws.clear.call_count == 1
            assert ws.update.call_count == 1

    def test_sentinel_row_written_first_on_each_tab(
        self, seeded_db: sqlite3.Connection
    ) -> None:
        """Per EPIC-014 verification: sentinel is row 1 on every tab."""
        from finances.reports import sheets_sync

        client, worksheets = _build_mock_gspread_client()
        sheets_sync.sync_to_sheets(
            seeded_db, spreadsheet_id="sheet-xyz", client=client
        )
        for name, ws in worksheets.items():
            values = ws.update.call_args.kwargs.get("values")
            if values is None:
                # Fallback: some gspread versions take values positionally.
                values = ws.update.call_args.args[0]
            assert values[0][0] == sheets_sync.SENTINEL_TEXT, (
                f"tab {name!r} did not start with the sentinel row"
            )

    def test_destructive_per_tab_clear_precedes_write(
        self, seeded_db: sqlite3.Connection
    ) -> None:
        """Per EPIC-014 verification: destructive-per-tab (clear → write)."""
        from finances.reports import sheets_sync

        client, worksheets = _build_mock_gspread_client()
        # Pre-populate the spreadsheet so .worksheet() returns existing tabs
        # instead of forcing .add_worksheet (exercise the non-create branch).
        spreadsheet = client.open_by_key.return_value
        for name in (
            sheets_sync.TRANSACTIONS_TAB,
            sheets_sync.BALANCES_TAB,
            sheets_sync.MONTHLY_TAB,
            sheets_sync.NEEDS_REVIEW_TAB,
        ):
            spreadsheet.add_worksheet(title=name, rows=10, cols=10)

        sheets_sync.sync_to_sheets(
            seeded_db, spreadsheet_id="sheet-xyz", client=client
        )
        for name, ws in worksheets.items():
            mock_calls = [c[0] for c in ws.method_calls]
            clear_idx = mock_calls.index("clear")
            update_idx = mock_calls.index("update")
            assert clear_idx < update_idx, (
                f"tab {name!r}: .clear() must run before .update()"
            )

    def test_sentinel_row_is_frozen(
        self, seeded_db: sqlite3.Connection
    ) -> None:
        from finances.reports import sheets_sync

        client, worksheets = _build_mock_gspread_client()
        sheets_sync.sync_to_sheets(
            seeded_db, spreadsheet_id="sheet-xyz", client=client
        )
        for name, ws in worksheets.items():
            ws.freeze.assert_called_once()
            kwargs = ws.freeze.call_args.kwargs
            args = ws.freeze.call_args.args
            frozen_rows = kwargs.get("rows") if kwargs else (args[0] if args else None)
            assert frozen_rows == 1, f"tab {name!r}: expected 1 frozen row"

    def test_creates_missing_worksheet_when_not_present(
        self, seeded_db: sqlite3.Connection
    ) -> None:
        from finances.reports import sheets_sync

        client, worksheets = _build_mock_gspread_client()
        sheets_sync.sync_to_sheets(
            seeded_db, spreadsheet_id="sheet-xyz", client=client
        )
        # All four tabs were missing → all four add_worksheet calls happened.
        spreadsheet = client.open_by_key.return_value
        titles = [
            c.kwargs["title"] for c in spreadsheet.add_worksheet.call_args_list
        ]
        assert sorted(titles) == sorted(
            [
                sheets_sync.TRANSACTIONS_TAB,
                sheets_sync.BALANCES_TAB,
                sheets_sync.MONTHLY_TAB,
                sheets_sync.NEEDS_REVIEW_TAB,
            ]
        )

    def test_gspread_api_error_propagates(
        self, seeded_db: sqlite3.Connection
    ) -> None:
        from finances.reports import sheets_sync

        client, _ = _build_mock_gspread_client()
        client.open_by_key.side_effect = RuntimeError("boom")
        with pytest.raises(RuntimeError, match="boom"):
            sheets_sync.sync_to_sheets(
                seeded_db, spreadsheet_id="sheet-xyz", client=client
            )

    def test_sync_report_rows_written_matches_source_counts(
        self, seeded_db: sqlite3.Connection
    ) -> None:
        from finances.reports import sheets_sync

        _seed_rate(seeded_db, source="binance_p2p_median", rate=Decimal("36.00"))
        _insert_ves_expense(
            seeded_db,
            amount=Decimal("-1000.00"),
            description="a",
            source_ref="sr-1",
        )
        _insert_needs_review(seeded_db, source_ref="sr-nr")

        client, _ = _build_mock_gspread_client()
        report = sheets_sync.sync_to_sheets(
            seeded_db, spreadsheet_id="sheet-xyz", client=client
        )
        # 2 non-transfer transactions → 2 transactions rows (nr row is included).
        assert report.rows_written[sheets_sync.TRANSACTIONS_TAB] == 2
        assert report.rows_written[sheets_sync.BALANCES_TAB] == 5
        assert report.rows_written[sheets_sync.NEEDS_REVIEW_TAB] == 1


# ---------------------------------------------------------------------------
# finances.config.google_service_account — both env paths + failure-mode
# ---------------------------------------------------------------------------


class TestGoogleServiceAccountConfig:
    def test_reads_from_file_path_env(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from finances import config

        creds = {"type": "service_account", "project_id": "demo"}
        keyfile = tmp_path / "sa.json"
        keyfile.write_text(json.dumps(creds))

        monkeypatch.setattr(config, "_env_loaded", True)
        monkeypatch.setenv("GOOGLE_SERVICE_ACCOUNT_FILE", str(keyfile))
        monkeypatch.delenv("GOOGLE_SERVICE_ACCOUNT_JSON", raising=False)

        assert config.google_service_account() == creds

    def test_reads_from_inline_json_env(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from finances import config

        creds = {"type": "service_account", "project_id": "inline"}
        monkeypatch.setattr(config, "_env_loaded", True)
        monkeypatch.delenv("GOOGLE_SERVICE_ACCOUNT_FILE", raising=False)
        monkeypatch.setenv("GOOGLE_SERVICE_ACCOUNT_JSON", json.dumps(creds))

        assert config.google_service_account() == creds

    def test_raises_when_neither_env_set(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from finances import config

        monkeypatch.setattr(config, "_env_loaded", True)
        monkeypatch.delenv("GOOGLE_SERVICE_ACCOUNT_FILE", raising=False)
        monkeypatch.delenv("GOOGLE_SERVICE_ACCOUNT_JSON", raising=False)

        with pytest.raises(RuntimeError, match="GOOGLE_SERVICE_ACCOUNT"):
            config.google_service_account()

    def test_raises_when_file_missing(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from finances import config

        bogus = tmp_path / "nope.json"
        monkeypatch.setattr(config, "_env_loaded", True)
        monkeypatch.setenv("GOOGLE_SERVICE_ACCOUNT_FILE", str(bogus))
        monkeypatch.delenv("GOOGLE_SERVICE_ACCOUNT_JSON", raising=False)

        with pytest.raises(RuntimeError, match="does not exist"):
            config.google_service_account()


# ---------------------------------------------------------------------------
# CLI wiring — `finances sync sheets --spreadsheet-id <id>`
# ---------------------------------------------------------------------------


class TestSyncSheetsCLI:
    def test_exits_zero_and_reports_tab_counts(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        from finances.cli import main as cli_main
        from finances.reports import sheets_sync

        # Point the CLI at a freshly-migrated throwaway DB. Don't reuse
        # seeded_db because it's :memory: and the CLI reopens the file path.
        db_path = tmp_path / "cli.db"
        monkeypatch.setattr(cli_main, "DB_PATH", db_path)

        # Inject a stub client so the CLI does not touch the real gspread auth.
        stub_client, _ = _build_mock_gspread_client()

        def _fake_open_client() -> Any:
            return stub_client

        monkeypatch.setattr(sheets_sync, "_open_client", _fake_open_client)

        runner = CliRunner()
        result = runner.invoke(
            cli_main.app,
            ["sync", "sheets", "--spreadsheet-id", "sheet-xyz"],
        )
        assert result.exit_code == 0, result.output
        assert "sheets sync" in result.output
        assert "sheet-xyz" in result.output

    def test_missing_spreadsheet_id_exits_nonzero(self) -> None:
        from finances.cli import main as cli_main

        runner = CliRunner()
        result = runner.invoke(cli_main.app, ["sync", "sheets"])
        assert result.exit_code != 0
