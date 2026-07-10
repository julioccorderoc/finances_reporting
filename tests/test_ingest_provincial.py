"""Tests for ``finances/ingest/provincial.py`` — Provincial bank CSV ingest.

EPIC-008 / ADR-001 / ADR-002 (amendment) / ADR-009 / ADR-010.

Covers:

- ``RawProvincialRow`` Pydantic parsing of the Venezuelan Bs. format + fecha
  validation.
- ``compute_source_ref`` deterministic hash per ADR-010.
- ``ingest_csv`` happy path (inserts, kind mapping, Caracas-TZ, description
  preserved, ``needs_review=True`` when no categorizer, currency inherited
  from the Provincial account).
- Re-ingest idempotency (``rows_inserted=0`` on second run per ADR-010).
- Categorizer hook: matched rows clear ``needs_review``; unmatched rows
  preserve it.
- Bank-anchored P2P pairing: after running ``ingest_csv`` over a CSV with a
  paired Binance P2P sell seeded, both legs share a ``transfer_id`` and
  ``v_unreconciled_transfers`` returns zero rows.

Per rule-011: filesystem I/O is contained to ``tmp_path`` fixtures; no live
network or DB calls. Test commits precede implementation commits.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from pydantic import ValidationError

from finances.config import CARACAS_TZ
from finances.db.repos import accounts as accounts_repo
from finances.db.repos import transactions as txn_repo
from finances.domain.models import Account, AccountKind, Transaction, TransactionKind


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_csv(tmp_path: Path, lines: list[str], *, name: str = "provincial.csv") -> Path:
    """Write a semicolon-delimited fixture CSV and return its path."""
    target = tmp_path / name
    # Legacy CSV drops are UTF-8 (BOM optional); the parser must handle both.
    target.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return target


_HEADER = "Fecha;Descripción;Monto;Saldo"


# ---------------------------------------------------------------------------
# RawProvincialRow
# ---------------------------------------------------------------------------

class TestRawProvincialRow:
    """Pydantic model that parses one raw CSV row."""

    def test_parses_plain_decimal_monto(self) -> None:
        from finances.ingest.provincial import RawProvincialRow

        row = RawProvincialRow(
            fecha="19/04/2026",
            descripcion="COM. PAGO MOVIL",
            monto="-14,4",
        )
        assert row.monto == Decimal("-14.4")

    def test_parses_thousands_and_decimal_separator(self) -> None:
        from finances.ingest.provincial import RawProvincialRow

        row = RawProvincialRow(
            fecha="11/04/2026",
            descripcion="VAMOS PA QUE MENCHO",
            monto="-11.040,00",
        )
        assert row.monto == Decimal("-11040.00")

    def test_parses_positive_amount(self) -> None:
        from finances.ingest.provincial import RawProvincialRow

        row = RawProvincialRow(
            fecha="11/04/2026",
            descripcion="TRAV0028502265000009387",
            monto="30.000,00",
        )
        assert row.monto == Decimal("30000.00")

    def test_parses_saldo_when_present(self) -> None:
        from finances.ingest.provincial import RawProvincialRow

        row = RawProvincialRow(
            fecha="11/04/2026",
            descripcion="TRAV0028502265000009387",
            monto="30.000,00",
            saldo="38.509,81",
        )
        assert row.saldo == Decimal("38509.81")

    def test_accepts_blank_saldo_as_none(self) -> None:
        from finances.ingest.provincial import RawProvincialRow

        row = RawProvincialRow(
            fecha="11/04/2026",
            descripcion="X",
            monto="-10,00",
            saldo="",
        )
        assert row.saldo is None

    def test_accepts_bs_prefix(self) -> None:
        from finances.ingest.provincial import RawProvincialRow

        row = RawProvincialRow(
            fecha="19/04/2026",
            descripcion="X",
            monto="Bs. -900,00",
        )
        assert row.monto == Decimal("-900.00")

    def test_rejects_malformed_monto(self) -> None:
        from finances.ingest.provincial import RawProvincialRow

        with pytest.raises(ValidationError):
            RawProvincialRow(
                fecha="19/04/2026",
                descripcion="BAD",
                monto="totally not a number",
            )

    def test_rejects_malformed_fecha(self) -> None:
        from finances.ingest.provincial import RawProvincialRow

        with pytest.raises(ValidationError):
            RawProvincialRow(
                fecha="2026-04-19",  # ISO not accepted; bank CSV uses dd/mm/yyyy
                descripcion="X",
                monto="-10,00",
            )

    def test_rejects_float_monto(self) -> None:
        """ADR-009: float monetary inputs are forbidden."""
        from finances.ingest.provincial import RawProvincialRow

        with pytest.raises(ValidationError):
            RawProvincialRow(
                fecha="19/04/2026",
                descripcion="X",
                monto=-10.0,  # type: ignore[arg-type]
            )

    def test_to_datetime_returns_caracas_aware(self) -> None:
        from finances.ingest.provincial import RawProvincialRow

        row = RawProvincialRow(
            fecha="19/04/2026",
            descripcion="X",
            monto="-10,00",
        )
        dt = row.to_datetime()
        assert dt.year == 2026
        assert dt.month == 4
        assert dt.day == 19
        assert dt.tzinfo is not None
        assert dt.tzinfo.utcoffset(dt) is not None
        assert dt.tzinfo == CARACAS_TZ

    def test_kind_expense_for_negative_amount(self) -> None:
        from finances.ingest.provincial import RawProvincialRow

        row = RawProvincialRow(
            fecha="19/04/2026",
            descripcion="X",
            monto="-10,00",
        )
        assert row.kind is TransactionKind.EXPENSE

    def test_kind_income_for_positive_amount(self) -> None:
        from finances.ingest.provincial import RawProvincialRow

        row = RawProvincialRow(
            fecha="19/04/2026",
            descripcion="X",
            monto="100,00",
        )
        assert row.kind is TransactionKind.INCOME


# ---------------------------------------------------------------------------
# compute_source_ref
# ---------------------------------------------------------------------------

class TestComputeSourceRef:
    """Deterministic per-row ``source_ref`` per ADR-010."""

    def test_deterministic_for_identical_inputs(self) -> None:
        from finances.ingest.provincial import compute_source_ref

        dt = datetime(2026, 4, 19, 0, 0, tzinfo=CARACAS_TZ)
        a = compute_source_ref(
            occurred_at=dt, amount=Decimal("-14.40"), description="COM. PAGO MOVIL"
        )
        b = compute_source_ref(
            occurred_at=dt, amount=Decimal("-14.40"), description="COM. PAGO MOVIL"
        )
        assert a == b
        assert a.startswith("hash:")
        assert len(a) == len("hash:") + 16  # sha256 truncated to 16 hex chars

    def test_differs_when_amount_differs(self) -> None:
        from finances.ingest.provincial import compute_source_ref

        dt = datetime(2026, 4, 19, 0, 0, tzinfo=CARACAS_TZ)
        a = compute_source_ref(
            occurred_at=dt, amount=Decimal("-14.40"), description="COM. PAGO MOVIL"
        )
        b = compute_source_ref(
            occurred_at=dt, amount=Decimal("-14.41"), description="COM. PAGO MOVIL"
        )
        assert a != b

    def test_differs_when_description_differs(self) -> None:
        from finances.ingest.provincial import compute_source_ref

        dt = datetime(2026, 4, 19, 0, 0, tzinfo=CARACAS_TZ)
        a = compute_source_ref(
            occurred_at=dt, amount=Decimal("-14.40"), description="COM. PAGO MOVIL"
        )
        b = compute_source_ref(
            occurred_at=dt, amount=Decimal("-14.40"), description="COM. PAGO MOV PB"
        )
        assert a != b

    def test_differs_when_date_differs(self) -> None:
        from finances.ingest.provincial import compute_source_ref

        d1 = datetime(2026, 4, 19, 0, 0, tzinfo=CARACAS_TZ)
        d2 = datetime(2026, 4, 20, 0, 0, tzinfo=CARACAS_TZ)
        a = compute_source_ref(occurred_at=d1, amount=Decimal("-10"), description="X")
        b = compute_source_ref(occurred_at=d2, amount=Decimal("-10"), description="X")
        assert a != b


# ---------------------------------------------------------------------------
# ingest_csv — happy path
# ---------------------------------------------------------------------------

class TestIngestCsvHappyPath:
    """End-to-end ingest over a small synthetic CSV against ``seeded_db``."""

    def test_inserts_expected_row_count(
        self, tmp_path: Path, seeded_db: sqlite3.Connection
    ) -> None:
        from finances.ingest.provincial import ingest_csv

        csv_path = _write_csv(
            tmp_path,
            [
                _HEADER,
                "19/04/2026;COM. PAGO MOVIL;-14,4;8.240,23",
                "18/04/2026;TRAV0031264379000118698;20.000,00;20.115,09",
                "17/04/2026;DR OB V07372929 191NAC.C;-4.800,00;8.254,63",
            ],
        )

        report = ingest_csv(seeded_db, csv_path)

        assert report.rows_seen == 3
        assert report.rows_inserted == 3
        assert report.rows_updated == 0
        assert txn_repo.count(seeded_db) == 3

    def test_maps_sign_to_transaction_kind(
        self, tmp_path: Path, seeded_db: sqlite3.Connection
    ) -> None:
        from finances.ingest.provincial import ingest_csv

        csv_path = _write_csv(
            tmp_path,
            [
                _HEADER,
                "18/04/2026;TRAV0031264379000118698;20.000,00;20.115,09",
                "17/04/2026;DR OB V07372929 191NAC.C;-4.800,00;8.254,63",
            ],
        )

        ingest_csv(seeded_db, csv_path)

        account = accounts_repo.get_by_name(seeded_db, "Provincial Bolivares")
        assert account is not None and account.id is not None
        txns = txn_repo.list_by_account(seeded_db, account.id)
        kinds = {t.kind for t in txns}
        assert TransactionKind.INCOME in kinds
        assert TransactionKind.EXPENSE in kinds

    def test_writes_caracas_tz_on_occurred_at(
        self, tmp_path: Path, seeded_db: sqlite3.Connection
    ) -> None:
        from finances.ingest.provincial import ingest_csv

        csv_path = _write_csv(
            tmp_path,
            [_HEADER, "19/04/2026;COM. PAGO MOVIL;-14,4;8.240,23"],
        )

        ingest_csv(seeded_db, csv_path)

        account = accounts_repo.get_by_name(seeded_db, "Provincial Bolivares")
        assert account is not None and account.id is not None
        [txn] = txn_repo.list_by_account(seeded_db, account.id)
        assert txn.occurred_at.tzinfo is not None
        # Caracas is UTC-04:00 year-round.
        assert txn.occurred_at.utcoffset() == timedelta(hours=-4)

    def test_source_is_provincial(
        self, tmp_path: Path, seeded_db: sqlite3.Connection
    ) -> None:
        from finances.ingest.provincial import ingest_csv

        csv_path = _write_csv(
            tmp_path,
            [_HEADER, "19/04/2026;COM. PAGO MOVIL;-14,4;8.240,23"],
        )

        ingest_csv(seeded_db, csv_path)

        account = accounts_repo.get_by_name(seeded_db, "Provincial Bolivares")
        assert account is not None and account.id is not None
        [txn] = txn_repo.list_by_account(seeded_db, account.id)
        assert txn.source == "provincial"

    def test_source_ref_is_hash_prefixed(
        self, tmp_path: Path, seeded_db: sqlite3.Connection
    ) -> None:
        from finances.ingest.provincial import ingest_csv

        csv_path = _write_csv(
            tmp_path,
            [_HEADER, "19/04/2026;COM. PAGO MOVIL;-14,4;8.240,23"],
        )

        ingest_csv(seeded_db, csv_path)

        account = accounts_repo.get_by_name(seeded_db, "Provincial Bolivares")
        assert account is not None and account.id is not None
        [txn] = txn_repo.list_by_account(seeded_db, account.id)
        assert txn.source_ref is not None
        assert txn.source_ref.startswith("hash:")

    def test_description_preserved(
        self, tmp_path: Path, seeded_db: sqlite3.Connection
    ) -> None:
        from finances.ingest.provincial import ingest_csv

        csv_path = _write_csv(
            tmp_path,
            [_HEADER, "19/04/2026;COM. PAGO MOVIL;-14,4;8.240,23"],
        )

        ingest_csv(seeded_db, csv_path)

        account = accounts_repo.get_by_name(seeded_db, "Provincial Bolivares")
        assert account is not None and account.id is not None
        [txn] = txn_repo.list_by_account(seeded_db, account.id)
        assert txn.description == "COM. PAGO MOVIL"

    def test_currency_inherited_from_account(
        self, tmp_path: Path, seeded_db: sqlite3.Connection
    ) -> None:
        """Provincial account is VES; ingested rows must follow."""
        from finances.ingest.provincial import ingest_csv

        csv_path = _write_csv(
            tmp_path,
            [_HEADER, "19/04/2026;COM. PAGO MOVIL;-14,4;8.240,23"],
        )

        ingest_csv(seeded_db, csv_path)

        account = accounts_repo.get_by_name(seeded_db, "Provincial Bolivares")
        assert account is not None and account.id is not None
        [txn] = txn_repo.list_by_account(seeded_db, account.id)
        assert txn.currency == "VES"

    def test_needs_review_true_when_no_categorizer(
        self, tmp_path: Path, seeded_db: sqlite3.Connection
    ) -> None:
        from finances.ingest.provincial import ingest_csv

        csv_path = _write_csv(
            tmp_path,
            [_HEADER, "19/04/2026;COM. PAGO MOVIL;-14,4;8.240,23"],
        )

        ingest_csv(seeded_db, csv_path)

        account = accounts_repo.get_by_name(seeded_db, "Provincial Bolivares")
        assert account is not None and account.id is not None
        [txn] = txn_repo.list_by_account(seeded_db, account.id)
        assert txn.needs_review is True
        assert txn.category_id is None


# ---------------------------------------------------------------------------
# ingest_csv — idempotency (ADR-010)
# ---------------------------------------------------------------------------

def test_reingest_same_csv_inserts_zero_new_rows(
    tmp_path: Path, seeded_db: sqlite3.Connection
) -> None:
    """Per ADR-010: identical input → second run inserts 0 rows."""
    from finances.ingest.provincial import ingest_csv

    csv_path = _write_csv(
        tmp_path,
        [
            _HEADER,
            "19/04/2026;COM. PAGO MOVIL;-14,4;8.240,23",
            "18/04/2026;TRAV0031264379000118698;20.000,00;20.115,09",
            "17/04/2026;DR OB V07372929 191NAC.C;-4.800,00;8.254,63",
        ],
    )

    first = ingest_csv(seeded_db, csv_path)
    assert first.rows_inserted == 3

    second = ingest_csv(seeded_db, csv_path)
    assert second.rows_seen == 3
    assert second.rows_inserted == 0
    assert second.rows_updated == 3  # upsert touches updated_at even when unchanged
    assert txn_repo.count(seeded_db) == 3


# ---------------------------------------------------------------------------
# Categorizer hook
# ---------------------------------------------------------------------------

class TestCategorizerHook:
    """EPIC-008 calls an optional categorizer; EPIC-004 plugs in the real one."""

    def test_matched_description_sets_category_and_clears_review(
        self, tmp_path: Path, seeded_db: sqlite3.Connection
    ) -> None:
        from finances.db.repos import categories as categories_repo
        from finances.ingest.provincial import ingest_csv

        fees = categories_repo.get_by_name(seeded_db, TransactionKind.EXPENSE, "Fees")
        assert fees is not None and fees.id is not None

        def categorize(description: str) -> int | None:
            return fees.id if description.startswith("COM. PAGO") else None

        csv_path = _write_csv(
            tmp_path,
            [
                _HEADER,
                "19/04/2026;COM. PAGO MOVIL;-14,4;8.240,23",
                "17/04/2026;DR OB V07372929 191NAC.C;-4.800,00;8.254,63",
            ],
        )

        ingest_csv(seeded_db, csv_path, categorizer=categorize)

        account = accounts_repo.get_by_name(seeded_db, "Provincial Bolivares")
        assert account is not None and account.id is not None
        txns = {t.description: t for t in txn_repo.list_by_account(seeded_db, account.id)}

        matched = txns["COM. PAGO MOVIL"]
        assert matched.category_id == fees.id
        assert matched.needs_review is False

        unmatched = txns["DR OB V07372929 191NAC.C"]
        assert unmatched.category_id is None
        assert unmatched.needs_review is True


# ---------------------------------------------------------------------------
# Bank-anchored P2P pairing (rule-002, ADR-002 amendment)
# ---------------------------------------------------------------------------

def _seed_binance_spot(conn: sqlite3.Connection) -> int:
    """Return the id of the pre-seeded Binance Spot account from ``seeded_db``."""
    account = accounts_repo.get_by_name(conn, "Binance Spot")
    assert account is not None and account.id is not None
    return account.id


def _insert_binance_p2p_sell(
    conn: sqlite3.Connection,
    *,
    account_id: int,
    occurred_at: datetime,
    usdt_amount: Decimal,
    user_rate: Decimal,
    source_ref: str,
) -> Transaction:
    """Seed a Binance-side P2P sell leg that the strategy should match."""
    txn = Transaction(
        account_id=account_id,
        occurred_at=occurred_at,
        kind=TransactionKind.EXPENSE,
        amount=-usdt_amount,
        currency="USDT",
        description="P2P sell",
        user_rate=user_rate,
        source="binance",
        source_ref=source_ref,
    )
    return txn_repo.insert(conn, txn)


class TestBankAnchoredP2pPairing:
    """The ingest must run a reconciliation pass using BankAnchoredP2pPairing."""

    def test_matching_bank_deposit_gets_paired_with_binance_sell(
        self, tmp_path: Path, seeded_db: sqlite3.Connection
    ) -> None:
        from finances.ingest.provincial import ingest_csv

        binance_spot_id = _seed_binance_spot(seeded_db)
        # +20.000 VES on 2026-04-18 pairs with -500 USDT × 40 VES/USDT = 20000.
        _insert_binance_p2p_sell(
            seeded_db,
            account_id=binance_spot_id,
            occurred_at=datetime(2026, 4, 18, 14, 0, tzinfo=CARACAS_TZ),
            usdt_amount=Decimal("500.00"),
            user_rate=Decimal("40.0"),
            source_ref="p2p:test-001",
        )

        csv_path = _write_csv(
            tmp_path,
            [
                _HEADER,
                "18/04/2026;TRAV0031264379000118698;20.000,00;20.115,09",
            ],
        )

        report = ingest_csv(seeded_db, csv_path, pairing_window_days=2)

        # The reconciliation pass ran, found and applied exactly one pair.
        assert report.reconciliation is not None
        assert report.reconciliation.proposals_found == 1
        assert report.reconciliation.proposals_applied == 1
        assert report.reconciliation.errors == []

        # Both legs now share a transfer_id and are kind='transfer'.
        bank = txn_repo.get_by_source_ref(
            seeded_db, "binance", "p2p:test-001"
        )
        assert bank is not None
        assert bank.kind is TransactionKind.TRANSFER
        assert bank.transfer_id is not None

        account = accounts_repo.get_by_name(seeded_db, "Provincial Bolivares")
        assert account is not None and account.id is not None
        [bank_leg] = txn_repo.list_by_account(seeded_db, account.id)
        assert bank_leg.kind is TransactionKind.TRANSFER
        assert bank_leg.transfer_id == bank.transfer_id

    def test_after_pairing_no_unreconciled_rows(
        self, tmp_path: Path, seeded_db: sqlite3.Connection
    ) -> None:
        """Verification criterion: no kind='transfer' rows with transfer_id IS NULL."""
        from finances.ingest.provincial import ingest_csv

        binance_spot_id = _seed_binance_spot(seeded_db)
        _insert_binance_p2p_sell(
            seeded_db,
            account_id=binance_spot_id,
            occurred_at=datetime(2026, 4, 18, 14, 0, tzinfo=CARACAS_TZ),
            usdt_amount=Decimal("500.00"),
            user_rate=Decimal("40.0"),
            source_ref="p2p:test-002",
        )

        csv_path = _write_csv(
            tmp_path,
            [_HEADER, "18/04/2026;TRAV0031264379000118698;20.000,00;20.115,09"],
        )
        ingest_csv(seeded_db, csv_path)

        row = seeded_db.execute(
            "SELECT COUNT(*) AS c FROM transactions "
            "WHERE kind = 'transfer' AND transfer_id IS NULL"
        ).fetchone()
        assert int(row["c"]) == 0

    def test_pairing_respects_window_days_bound(
        self, tmp_path: Path, seeded_db: sqlite3.Connection
    ) -> None:
        """A bank row 5 days away from the Binance sell is NOT paired at window=2."""
        from finances.ingest.provincial import ingest_csv

        binance_spot_id = _seed_binance_spot(seeded_db)
        _insert_binance_p2p_sell(
            seeded_db,
            account_id=binance_spot_id,
            occurred_at=datetime(2026, 4, 10, 14, 0, tzinfo=CARACAS_TZ),
            usdt_amount=Decimal("500.00"),
            user_rate=Decimal("40.0"),
            source_ref="p2p:test-003",
        )

        csv_path = _write_csv(
            tmp_path,
            [_HEADER, "18/04/2026;TRAV0031264379000118698;20.000,00;20.115,09"],
        )
        report = ingest_csv(seeded_db, csv_path, pairing_window_days=2)

        assert report.reconciliation is not None
        assert report.reconciliation.proposals_found == 0

    def test_pairing_disabled_skips_reconciliation(
        self, tmp_path: Path, seeded_db: sqlite3.Connection
    ) -> None:
        """``run_pairing=False`` leaves the reconciliation field unset."""
        from finances.ingest.provincial import ingest_csv

        csv_path = _write_csv(
            tmp_path,
            [_HEADER, "19/04/2026;COM. PAGO MOVIL;-14,4;8.240,23"],
        )
        report = ingest_csv(seeded_db, csv_path, run_pairing=False)
        assert report.reconciliation is None


# ---------------------------------------------------------------------------
# Error + edge cases
# ---------------------------------------------------------------------------

def test_ingest_raises_when_account_missing(
    tmp_path: Path, in_memory_db: sqlite3.Connection
) -> None:
    """A fresh DB has no Provincial account; ingest must surface that clearly."""
    from finances.ingest.provincial import ingest_csv

    csv_path = _write_csv(
        tmp_path,
        [_HEADER, "19/04/2026;X;-10,00;100,00"],
    )

    with pytest.raises(ValueError, match="Provincial Bolivares"):
        ingest_csv(in_memory_db, csv_path)


def test_ingest_accepts_explicit_account_id(
    tmp_path: Path, in_memory_db: sqlite3.Connection
) -> None:
    """Callers can override the default account lookup."""
    from finances.ingest.provincial import ingest_csv

    account = accounts_repo.insert(
        in_memory_db,
        Account(
            name="Alt Provincial",
            kind=AccountKind.BANK,
            currency="VES",
            institution="Provincial",
        ),
    )
    assert account.id is not None

    csv_path = _write_csv(
        tmp_path,
        [_HEADER, "19/04/2026;X;-10,00;100,00"],
    )

    report = ingest_csv(in_memory_db, csv_path, account_id=account.id)
    assert report.rows_inserted == 1
    [txn] = txn_repo.list_by_account(in_memory_db, account.id)
    assert txn.currency == "VES"


def test_ingest_skips_blank_rows(
    tmp_path: Path, seeded_db: sqlite3.Connection
) -> None:
    """Trailing blank lines are tolerated (Excel likes to add them)."""
    from finances.ingest.provincial import ingest_csv

    csv_path = _write_csv(
        tmp_path,
        [
            _HEADER,
            "19/04/2026;COM. PAGO MOVIL;-14,4;8.240,23",
            ";;;",
            "",
        ],
    )
    report = ingest_csv(seeded_db, csv_path)
    assert report.rows_seen == 1
    assert report.rows_inserted == 1


def test_ingest_raises_on_malformed_row(
    tmp_path: Path, seeded_db: sqlite3.Connection
) -> None:
    """A malformed ``Monto`` surfaces as ``ValidationError`` (ADR-009)."""
    from finances.ingest.provincial import ingest_csv

    csv_path = _write_csv(
        tmp_path,
        [
            _HEADER,
            "19/04/2026;BAD;not-a-number;0",
        ],
    )
    with pytest.raises(ValidationError):
        ingest_csv(seeded_db, csv_path)


# ---------------------------------------------------------------------------
# Dry-run (mirrors the BCV pattern — see finances/ingest/bcv.py:155-220)
# ---------------------------------------------------------------------------


def test_provincial_ingest_dry_run_persists_nothing(
    tmp_path: Path, seeded_db: sqlite3.Connection
) -> None:
    """``dry_run=True`` parses + computes without persisting any side effects.

    The bank-anchored P2P pairing pass also runs end-to-end (so callers can
    preview pairings), but every write — transaction upsert, transfer
    promotion, ``import_runs`` row, ``import_state`` cursor — is rolled back.
    """
    from finances.ingest.provincial import ingest_csv

    binance_spot = accounts_repo.get_by_name(seeded_db, "Binance Spot")
    assert binance_spot is not None and binance_spot.id is not None
    _insert_binance_p2p_sell(
        seeded_db,
        account_id=binance_spot.id,
        occurred_at=datetime(2026, 4, 18, 14, 0, tzinfo=CARACAS_TZ),
        usdt_amount=Decimal("500.00"),
        user_rate=Decimal("40.0"),
        source_ref="p2p:dry-run-001",
    )
    txns_before = txn_repo.count(seeded_db)

    csv_path = _write_csv(
        tmp_path,
        [
            _HEADER,
            "19/04/2026;COM. PAGO MOVIL;-14,4;8.240,23",
            "18/04/2026;TRAV0031264379000118698;20.000,00;20.115,09",
            "17/04/2026;DR OB V07372929 191NAC.C;-4.800,00;8.254,63",
        ],
    )

    report = ingest_csv(seeded_db, csv_path, dry_run=True)

    assert report.rows_seen == 3
    assert report.rows_inserted == 3, "dry-run reports would-be inserts"

    # No new transaction rows (the seeded Binance leg should still be the
    # only one); pairing-side promotions must also have been rolled back.
    assert txn_repo.count(seeded_db) == txns_before, (
        "dry-run must not persist transactions or pairing promotions"
    )
    seeded_binance = txn_repo.get_by_source_ref(
        seeded_db, "binance", "p2p:dry-run-001"
    )
    assert seeded_binance is not None
    assert seeded_binance.kind is TransactionKind.EXPENSE
    assert seeded_binance.transfer_id is None, (
        "pairing promotion must be rolled back in dry-run"
    )

    runs_count = seeded_db.execute(
        "SELECT COUNT(*) FROM import_runs WHERE source = 'provincial'"
    ).fetchone()[0]
    assert runs_count == 0, "dry-run must not persist import_runs rows"

    state_count = seeded_db.execute(
        "SELECT COUNT(*) FROM import_state WHERE source = 'provincial'"
    ).fetchone()[0]
    assert state_count == 0, "dry-run must not persist import_state rows"


def test_provincial_ingest_dry_run_after_real_run_does_not_double_count(
    tmp_path: Path, seeded_db: sqlite3.Connection
) -> None:
    """A real run followed by a dry-run leaves real data intact and reports
    zero would-be inserts on the same input (ADR-010 idempotency)."""
    from finances.ingest.provincial import ingest_csv

    csv_path = _write_csv(
        tmp_path,
        [
            _HEADER,
            "19/04/2026;COM. PAGO MOVIL;-14,4;8.240,23",
            "18/04/2026;TRAV0031264379000118698;20.000,00;20.115,09",
            "17/04/2026;DR OB V07372929 191NAC.C;-4.800,00;8.254,63",
        ],
    )

    real = ingest_csv(seeded_db, csv_path)
    assert real.rows_inserted == 3
    real_count = txn_repo.count(seeded_db)

    real_runs = seeded_db.execute(
        "SELECT COUNT(*) FROM import_runs WHERE source = 'provincial'"
    ).fetchone()[0]
    assert real_runs == 1

    dry = ingest_csv(seeded_db, csv_path, dry_run=True)
    assert dry.rows_seen == 3
    assert dry.rows_inserted == 0, (
        "second pass against same CSV is fully duplicate; dry-run reports 0"
    )

    assert txn_repo.count(seeded_db) == real_count, (
        "dry-run must not corrupt real-run data"
    )
    runs_after = seeded_db.execute(
        "SELECT COUNT(*) FROM import_runs WHERE source = 'provincial'"
    ).fetchone()[0]
    assert runs_after == real_runs, "dry-run must not add an import_runs row"


# ---------------------------------------------------------------------------
# Bank HTML "XLS" exports (the bank's web export is an HTML table with an
# Excel MIME hint, not CSV and not real Excel)
# ---------------------------------------------------------------------------

_HTML_EXPORT = (
    '<html xmlns:o="urn:schemas-microsoft-com:office:office" '
    'xmlns:x="urn:schemas-microsoft-com:office:excel" '
    'xmlns="http://www.w3.org/TR/REC-html40">'
    '<meta http-equiv="content-type" '
    'content="application/vnd.ms-excel; charset=UTF-8">'
    "<head></head><body><table>"
    "<tr><td>Fecha</td><td>Descripción</td><td>Monto</td><td>Saldo</td></tr>"
    "<tr><td>09/07/2026</td><td>MI SUPER CA</td><td>-700,22</td><td>14.803,64</td></tr>"
    "<tr><td>08/07/2026</td><td>PAGO RECIBIDO</td><td>1.250,00</td><td>15.503,86</td></tr>"
    "<tr><td></td><td></td><td></td><td></td></tr>"
    "</table></body></html>"
)


def _write_html_export(
    tmp_path: Path, *, name: str = "provincial-july.xls", html: str | None = None
) -> Path:
    target = tmp_path / name
    # Real exports are one giant line, no terminators — keep it that way.
    target.write_text(html if html is not None else _HTML_EXPORT, encoding="utf-8")
    return target


class TestHtmlExportReader:
    """iter_raw_rows must accept the bank's fake-XLS HTML container and
    yield the exact same RawProvincialRow stream the CSV path yields."""

    def test_parses_bank_html_xls(self, tmp_path: Path) -> None:
        from finances.ingest.provincial import iter_raw_rows

        path = _write_html_export(tmp_path)
        rows = list(iter_raw_rows(path))
        assert len(rows) == 2  # blank spacer row skipped
        assert rows[0].fecha == "09/07/2026"
        assert rows[0].descripcion == "MI SUPER CA"
        assert rows[0].monto == Decimal("-700.22")
        assert rows[0].saldo == Decimal("14803.64")
        assert rows[1].monto == Decimal("1250.00")

    def test_missing_required_column_raises(self, tmp_path: Path) -> None:
        html = (
            "<html><body><table>"
            "<tr><td>Fecha</td><td>Descripción</td><td>Saldo</td></tr>"
            "<tr><td>09/07/2026</td><td>X</td><td>1,00</td></tr>"
            "</table></body></html>"
        )
        from finances.ingest.provincial import iter_raw_rows

        path = _write_html_export(tmp_path, html=html)
        with pytest.raises(ValueError, match="missing required columns"):
            list(iter_raw_rows(path))

    def test_html_without_table_raises(self, tmp_path: Path) -> None:
        from finances.ingest.provincial import iter_raw_rows

        path = _write_html_export(tmp_path, html="<html><body>nada</body></html>")
        with pytest.raises(ValueError, match="no <table>"):
            list(iter_raw_rows(path))

    def test_ingest_html_export_end_to_end(
        self, tmp_path: Path, seeded_db: sqlite3.Connection
    ) -> None:
        """Same pipeline as CSV: validation, dedup source_ref, import_runs."""
        from finances.ingest.provincial import ingest_csv

        path = _write_html_export(tmp_path)
        report = ingest_csv(seeded_db, path)
        assert report.rows_seen == 2
        assert report.rows_inserted == 2

        again = ingest_csv(seeded_db, path)
        assert again.rows_inserted == 0, "re-ingest must dedup to zero"

    def test_source_ref_identical_across_containers(self, tmp_path: Path) -> None:
        """The same transaction must hash to the same source_ref whether it
        arrived via CSV or via the HTML export — overlap dedup depends on it."""
        from finances.ingest.provincial import compute_source_ref, iter_raw_rows

        html_rows = list(iter_raw_rows(_write_html_export(tmp_path)))
        csv_path = _write_csv(
            tmp_path,
            [
                _HEADER,
                "09/07/2026;MI SUPER CA;-700,22;14.803,64",
            ],
        )
        csv_rows = list(iter_raw_rows(csv_path))
        h, c = html_rows[0], csv_rows[0]
        assert compute_source_ref(
            occurred_at=h.to_datetime(), amount=h.monto, description=h.descripcion
        ) == compute_source_ref(
            occurred_at=c.to_datetime(), amount=c.monto, description=c.descripcion
        )


class TestSameDayDuplicateTransactions:
    """Two genuinely distinct transactions with identical (date, description,
    amount) — e.g. the same transfer split in two — must BOTH survive ingest.
    Real case: inputs 2026-05-20 had two identical -30.207,81 transfers."""

    def test_occurrence_disambiguates_hash(self) -> None:
        from finances.ingest.provincial import compute_source_ref

        base = dict(
            occurred_at=datetime(2026, 5, 20, tzinfo=CARACAS_TZ),
            amount=Decimal("-30207.81"),
            description="DR OB V23807457 172BANCA",
        )
        first = compute_source_ref(**base)
        again = compute_source_ref(**base, occurrence=0)
        second = compute_source_ref(**base, occurrence=1)
        assert first == again, "occurrence=0 must equal the legacy hash"
        assert first != second

    def test_ingest_keeps_both_twins_and_stays_idempotent(
        self, tmp_path: Path, seeded_db: sqlite3.Connection
    ) -> None:
        from finances.ingest.provincial import ingest_csv

        csv_path = _write_csv(
            tmp_path,
            [
                _HEADER,
                "20/05/2026;DR OB V23807457 172BANCA;-30.207,81;50.000,00",
                "20/05/2026;DR OB V23807457 172BANCA;-30.207,81;19.792,19",
            ],
        )
        report = ingest_csv(seeded_db, csv_path)
        assert report.rows_inserted == 2, "twin transactions must both insert"

        again = ingest_csv(seeded_db, csv_path)
        assert again.rows_inserted == 0, "re-ingest must still dedup to zero"
        assert again.rows_updated == 2
