"""Provincial bank CSV ingest (EPIC-008).

Implements :class:`RawProvincialRow` (Pydantic v2 validator around the
Venezuelan ``Bs.`` / ``dd/mm/yyyy`` format), :func:`compute_source_ref`
(deterministic hash per ADR-010), and :func:`ingest_csv` (the production
entry point that parses a semicolon-delimited statement, upserts the
canonical :class:`~finances.domain.models.Transaction` rows, and then
triggers :class:`~finances.domain.transfers.BankAnchoredP2pPairing` so
that provincial deposits anchor their Binance-side P2P sells per the
ADR-002 amendment.

Design notes:

* ``RawProvincialRow`` owns all string → ``Decimal`` / string → aware
  ``datetime`` coercion — downstream code never touches raw CSV strings.
* The ingest keeps the CSV reader honest (``extra='forbid'`` equivalent
  via strict Pydantic), but tolerates blank trailing lines that
  spreadsheet exports frequently produce.
* Categorization is passed in as a callable so EPIC-008 can ship ahead
  of EPIC-004 without cross-importing; when the real categorizer lands
  (or a caller supplies one), matched rows drop their ``needs_review``
  flag and pick up a ``category_id``.
"""
from __future__ import annotations

import csv
import hashlib
import re
import sqlite3
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, field_validator

from finances.config import CARACAS_TZ
from finances.db.repos import accounts as accounts_repo
from finances.db.repos import transactions as txn_repo
from finances.domain.models import Transaction, TransactionKind
from finances.domain.reconciliation import (
    ReconciliationReport,
    run_reconciliation_pass,
)
from finances.domain.transfers import BankAnchoredP2pPairing

# ---------------------------------------------------------------------------
# Public constants
# ---------------------------------------------------------------------------

SOURCE: str = "provincial"
DEFAULT_ACCOUNT_NAME: str = "Provincial Bolivares"

# Provincial CSV exports use dd/mm/yyyy (European order).
_FECHA_RE = re.compile(r"^(\d{1,2})/(\d{1,2})/(\d{4})$")

# "Bs.", "Bs" or "BS" optionally prefix monetary cells.
_BS_PREFIX_RE = re.compile(r"(?i)^\s*bs\.?\s*")


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------

def _parse_venezuelan_amount(raw: Any) -> Decimal:
    """Coerce a Provincial ``Monto``/``Saldo`` cell into a ``Decimal``.

    Handles the Venezuelan format where ``.`` is the thousands separator
    and ``,`` is the decimal separator, an optional ``"Bs."`` / ``"Bs"``
    prefix, leading signs, and surrounding whitespace. ``float`` inputs
    are rejected to honour ADR-009.
    """
    if isinstance(raw, Decimal):
        return raw
    if isinstance(raw, bool):  # bool is an int subclass; reject explicitly.
        raise ValueError("bool is not a valid monetary value")
    if isinstance(raw, float):
        raise ValueError("float monetary inputs are forbidden; use Decimal or str")
    if isinstance(raw, int):
        return Decimal(raw)
    if not isinstance(raw, str):
        raise ValueError(f"cannot coerce {type(raw).__name__} to Decimal")

    text = _BS_PREFIX_RE.sub("", raw.strip())
    if not text:
        raise ValueError("empty amount string")

    # Venezuelan format only uses ``,`` as decimal separator. When a comma
    # is present, any ``.`` characters are thousands markers and must be
    # stripped before the comma→dot swap. When no comma is present, the
    # cell is already an integer-or-plain-decimal and we leave it alone.
    if "," in text:
        text = text.replace(".", "").replace(",", ".")

    try:
        return Decimal(text)
    except InvalidOperation as exc:
        raise ValueError(f"invalid amount format: {raw!r}") from exc


def compute_source_ref(
    *,
    occurred_at: datetime,
    amount: Decimal,
    description: str,
) -> str:
    """Return a deterministic ``"hash:<16-hex>"`` source_ref per ADR-010.

    The Provincial CSV does not expose a stable per-row reference, so we
    hash the tuple ``(occurred_at, amount, description)`` to produce a
    source_ref that survives re-ingest. Collisions within the same day
    on identical amount + description are indistinguishable on the bank
    statement and must be resolved upstream if encountered in practice.
    """
    payload = f"{occurred_at.isoformat()}|{format(amount, 'f')}|{description}"
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    return f"hash:{digest[:16]}"


# ---------------------------------------------------------------------------
# Pydantic row model
# ---------------------------------------------------------------------------

class RawProvincialRow(BaseModel):
    """One parsed row of a Provincial CSV statement (ADR-009 boundary)."""

    model_config = ConfigDict(strict=False, extra="forbid")

    fecha: str
    descripcion: str
    monto: Decimal
    saldo: Decimal | None = None

    @field_validator("monto", mode="before")
    @classmethod
    def _parse_monto(cls, v: Any) -> Decimal:
        return _parse_venezuelan_amount(v)

    @field_validator("saldo", mode="before")
    @classmethod
    def _parse_saldo(cls, v: Any) -> Decimal | None:
        if v is None:
            return None
        if isinstance(v, str) and not v.strip():
            return None
        return _parse_venezuelan_amount(v)

    @field_validator("fecha")
    @classmethod
    def _validate_fecha(cls, v: str) -> str:
        if not _FECHA_RE.match(v):
            raise ValueError(f"fecha must be dd/mm/yyyy; got {v!r}")
        return v

    def to_datetime(self, tz: Any = CARACAS_TZ) -> datetime:
        """Return a timezone-aware ``datetime`` at midnight Caracas time."""
        match = _FECHA_RE.match(self.fecha)
        assert match is not None  # validated at construction.
        day, month, year = match.groups()
        return datetime(int(year), int(month), int(day), tzinfo=tz)

    @property
    def kind(self) -> TransactionKind:
        """Positive amount → income, non-positive → expense."""
        return (
            TransactionKind.INCOME if self.monto > 0 else TransactionKind.EXPENSE
        )


# ---------------------------------------------------------------------------
# CSV reader
# ---------------------------------------------------------------------------

# Provincial exports use semicolon delimiters. The header carries accented
# column names; we accept both the accented and ASCII variants because
# different BBVA export paths (Excel save-as-CSV vs. direct CSV) emit them
# differently.
_CSV_DELIMITER = ";"


def _normalize_header(name: str) -> str:
    return name.strip().lower().replace("ó", "o").replace("é", "e")


def iter_raw_rows(csv_path: Path) -> Iterator[RawProvincialRow]:
    """Yield :class:`RawProvincialRow` for each data row of ``csv_path``.

    Blank trailing lines and rows where every mapped column is empty are
    silently skipped.
    """
    # ``utf-8-sig`` strips a leading BOM when banking exports include one.
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle, delimiter=_CSV_DELIMITER)
        try:
            header = next(reader)
        except StopIteration:
            return
        norm = [_normalize_header(h) for h in header]

        try:
            fecha_idx = norm.index("fecha")
            desc_idx = norm.index("descripcion")
            monto_idx = norm.index("monto")
        except ValueError as exc:
            raise ValueError(
                f"Provincial CSV missing required columns; got {header!r}"
            ) from exc

        saldo_idx: int | None
        try:
            saldo_idx = norm.index("saldo")
        except ValueError:
            saldo_idx = None

        for row in reader:
            if not row:
                continue

            def _cell(idx: int | None) -> str:
                if idx is None or idx >= len(row):
                    return ""
                return row[idx].strip()

            fecha = _cell(fecha_idx)
            desc = _cell(desc_idx)
            monto = _cell(monto_idx)
            saldo = _cell(saldo_idx)

            if not fecha and not desc and not monto:
                continue  # blank spacer row

            yield RawProvincialRow(
                fecha=fecha,
                descripcion=desc,
                monto=monto,
                saldo=saldo or None,
            )


# ---------------------------------------------------------------------------
# Ingest entry point
# ---------------------------------------------------------------------------

Categorizer = Callable[[str], int | None]


@dataclass
class IngestReport:
    """Summary of a single :func:`ingest_csv` run."""

    rows_seen: int = 0
    rows_inserted: int = 0
    rows_updated: int = 0
    reconciliation: ReconciliationReport | None = None


def ingest_csv(
    conn: sqlite3.Connection,
    csv_path: Path,
    *,
    account_id: int | None = None,
    categorizer: Categorizer | None = None,
    pairing_window_days: int = 2,
    run_pairing: bool = True,
) -> IngestReport:
    """Ingest a Provincial statement CSV into ``transactions``.

    When ``account_id`` is ``None`` the default-named ``"Provincial
    Bolivares"`` account is looked up; a missing account raises so the
    caller can create it before retrying.

    ``categorizer`` is an optional callable ``(description) -> category_id
    | None``; rows that don't match are inserted with ``needs_review=True``
    (rule-006 fall-through). The real rules engine from EPIC-004 plugs in
    here without requiring a new ADR.

    After the row loop, the bank-anchored P2P pairing strategy runs a
    single reconciliation pass (ADR-002 amendment). Callers who want the
    rows inserted without the pairing side-effect (e.g. the one-time
    backfill that orders runs across sources) can pass
    ``run_pairing=False``.
    """
    resolved_account_id, currency = _resolve_account(conn, account_id)

    report = IngestReport()

    for raw in iter_raw_rows(csv_path):
        report.rows_seen += 1

        occurred_at = raw.to_datetime()
        category_id: int | None = None
        if categorizer is not None:
            category_id = categorizer(raw.descripcion)
        needs_review = category_id is None

        source_ref = compute_source_ref(
            occurred_at=occurred_at,
            amount=raw.monto,
            description=raw.descripcion,
        )

        txn = Transaction(
            account_id=resolved_account_id,
            occurred_at=occurred_at,
            kind=raw.kind,
            amount=raw.monto,
            currency=currency,
            description=raw.descripcion,
            category_id=category_id,
            source=SOURCE,
            source_ref=source_ref,
            needs_review=needs_review,
        )

        result = txn_repo.upsert_by_source_ref(conn, txn)
        report.rows_inserted += result["rows_inserted"]
        report.rows_updated += result["rows_updated"]

    if run_pairing:
        strategy = BankAnchoredP2pPairing(
            conn,
            window_days=pairing_window_days,
            bank_source=SOURCE,
        )
        report.reconciliation = run_reconciliation_pass(strategy)

    return report


def _resolve_account(
    conn: sqlite3.Connection, account_id: int | None
) -> tuple[int, str]:
    """Return ``(account_id, currency)`` for the Provincial account."""
    if account_id is None:
        account = accounts_repo.get_by_name(conn, DEFAULT_ACCOUNT_NAME)
        if account is None or account.id is None:
            raise ValueError(
                f"account {DEFAULT_ACCOUNT_NAME!r} not found; seed it before "
                "running provincial ingest"
            )
        return account.id, account.currency

    account = accounts_repo.get_by_id(conn, account_id)
    if account is None:
        raise ValueError(f"account id {account_id} not found")
    return account_id, account.currency


__all__ = [
    "DEFAULT_ACCOUNT_NAME",
    "IngestReport",
    "RawProvincialRow",
    "SOURCE",
    "compute_source_ref",
    "ingest_csv",
    "iter_raw_rows",
]

# Silence unused-import warnings for symbols used only in type hints.
_ = (Iterable,)
