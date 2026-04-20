"""EPIC-012 one-time backfill orchestrator (ADR-004 / rule-004).

Reads the legacy ``data/Finanzas - *.csv`` sheets and feeds them through
the same Pydantic row models and insertion helpers used in production
ingest:

* Binance rows → ``finances.ingest.binance.RawBinance*Row.to_transaction``
  plus ``finances.domain.transfers.create_transfer`` for Internal Transfer.
* Provincial rows → ``finances.ingest.provincial.RawProvincialRow`` +
  ``compute_source_ref`` + ``transactions_repo.upsert_by_source_ref``.
* BCV rows → ``finances.ingest.bcv.RawBcvRow`` + ``rates_repo.insert``.

After all three sources are in, runs a single
``BankAnchoredP2pPairing`` reconciliation pass (ADR-002 amendment).

Per rule-004, nothing here shapes Transactions independently of the
production ingest modules — we only adapt the CSV-column differences.
"""
from __future__ import annotations

import csv
import hashlib
import re
import sqlite3
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterator

from finances.config import CARACAS_TZ
from finances.db.repos import accounts as accounts_repo
from finances.db.repos import rates as rates_repo
from finances.db.repos import transactions as transactions_repo
from finances.domain.models import Account, AccountKind, Rate, Transaction, TransactionKind
from finances.domain.categorization import CategorizationRequest, suggest
from finances.domain.reconciliation import (
    ReconciliationReport,
    run_reconciliation_pass,
)
from finances.domain.transfers import BankAnchoredP2pPairing, create_transfer
from finances.ingest.binance import (
    RawBinanceConvertRow,
    RawBinanceDepositRow,
    RawBinanceP2pRow,
    RawBinanceTransferRow,
    RawBinanceWithdrawRow,
)
from finances.ingest.bcv import RawBcvRow, SOURCE_NAME as BCV_SOURCE
from finances.ingest.provincial import (
    RawProvincialRow,
    compute_source_ref as provincial_source_ref,
)

BINANCE_CSV_NAME = "Finanzas - Binance.csv"
PROVINCIAL_CSV_NAME = "Finanzas - Provincial.csv"
BCV_CSV_NAME = "Finanzas - BCV.csv"

BINANCE_SOURCE = "binance"
PROVINCIAL_SOURCE = "provincial"

# Legacy accounts that historical CSVs assume exist.
_BACKFILL_ACCOUNTS: tuple[tuple[str, AccountKind, str, str | None], ...] = (
    ("Provincial Bolivares", AccountKind.BANK, "VES", "Provincial"),
    ("Binance Spot", AccountKind.CRYPTO_SPOT, "USDT", "Binance"),
    ("Binance Funding", AccountKind.CRYPTO_FUNDING, "USDT", "Binance"),
    ("Binance Earn", AccountKind.CRYPTO_EARN, "USDT", "Binance"),
    ("Cash USD", AccountKind.CASH, "USD", None),
)

_MONTH_ABBR = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dec": 12,
}
_MONTH_FULL = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5,
    "june": 6, "july": 7, "august": 8, "september": 9, "october": 10,
    "november": 11, "december": 12,
}
_LEGACY_DATE_RE = re.compile(r"^(\d{1,2})-([A-Za-z]+)-(\d{4})$")

_USD_AMOUNT_RE = re.compile(r"^\s*(-?)\$\s*([\d,]+(?:\.\d+)?)\s*$")
_BS_AMOUNT_RE = re.compile(
    r"^\s*(-?)\s*bs\.?\s*([\d.,]+)\s*$", re.IGNORECASE
)
_P2P_ORDER_RE = re.compile(r"P2P\s*-\s*(\S+)", re.IGNORECASE)


# ---------------------------------------------------------------------------
# Reports
# ---------------------------------------------------------------------------


@dataclass
class BackfillReport:
    binance_rows_seen: int = 0
    binance_rows_inserted: int = 0
    provincial_rows_seen: int = 0
    provincial_rows_inserted: int = 0
    bcv_rows_seen: int = 0
    bcv_rates_inserted: int = 0
    rows_legacy_mapped: int = 0
    rows_categorized: int = 0
    errors: list[str] = field(default_factory=list)
    reconciliation: ReconciliationReport | None = None


# Closed legacy Sub-Category → v1 category mapping (2026-04-20 session).
# Per rule-006, category additions require a forward migration; every
# destination name here must exist in the categories table after
# migrations 002-005 apply.
LEGACY_SUB_CATEGORY_TO_V1: dict[str, str] = {
    # Expense — direct legacy=v1 matches
    "Groceries": "Groceries",
    "Transport": "Transport",
    "Health": "Health",
    "Family": "Family",
    "Dating": "Dating",
    "Gifts": "Gifts",
    "Utilities": "Utilities",
    "Subscriptions": "Subscriptions",
    "Purchases": "Purchases",
    "Lending": "Lending",
    "Education": "Education",
    "Clothing": "Clothing",
    # Expense — renamed / re-routed
    "Food": "Groceries",            # 2026-04-20 rename
    "Outings": "Leisure",
    "Personal care": "Personal Care",
    "Commissions": "Fees",
    "Ant": "Other Expense",         # heterogeneous gastos hormiga
    "No ID": "Other Expense",
    "Debt": "Other Expense",
    # Income
    "Salary": "Salary",
    "Gigs": "Gigs",
    "Interest": "Interest",
    "Interests": "Interest",
    "Loan Payment": "Loan Repayment",
    # Transfer (legacy rows where kind got ingested as income/expense —
    # these get re-routed to transfer via reconciliation, but if
    # reconciliation missed them we still stamp a transfer category so
    # needs_review clears. ``Transit`` = external flow, ``Exchange`` =
    # own-account USDT↔bolivars).
    "Transit": "External Transfer",
    "Exchange": "Internal Transfer",
}


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------


def parse_legacy_date(raw: str) -> datetime:
    """``'30-Oct-2025'`` → aware datetime at midnight Caracas time."""
    match = _LEGACY_DATE_RE.match(raw.strip())
    if match is None:
        raise ValueError(f"unrecognized legacy date: {raw!r}")
    day, month_raw, year = match.groups()
    month_key = month_raw.lower()
    month = _MONTH_ABBR.get(month_key) or _MONTH_FULL.get(month_key)
    if month is None:
        raise ValueError(f"unknown month in legacy date: {raw!r}")
    return datetime(int(year), month, int(day), tzinfo=CARACAS_TZ)


def parse_usd_amount(raw: str) -> Decimal:
    """``'$66.00'`` → ``Decimal('66.00')``; supports commas and sign."""
    match = _USD_AMOUNT_RE.match(raw.strip())
    if match is None:
        raise ValueError(f"unrecognized USD amount: {raw!r}")
    sign, digits = match.groups()
    value = Decimal(digits.replace(",", ""))
    return -value if sign == "-" else value


def parse_bs_amount(raw: str) -> Decimal | None:
    """``'Bs 313.97'`` / ``'-Bs 9.41'`` / ``'Bs 30,000.00'`` → Decimal.

    Returns ``None`` on empty strings so callers can treat missing columns
    as absent rather than as parse errors.
    """
    text = raw.strip()
    if not text:
        return None
    match = _BS_AMOUNT_RE.match(text)
    if match is None:
        raise ValueError(f"unrecognized Bs amount: {raw!r}")
    sign, digits = match.groups()
    # Venezuelan format: "." = thousands, "," = decimal. But the legacy
    # sheet sometimes emits US format ("30,000.00") so we detect and
    # normalize whichever is ambiguous. Presence of "," AND "." means
    # "," is thousands (US); presence of only "," means "," is decimal.
    if "," in digits and "." in digits:
        normalized = digits.replace(",", "")
    elif "," in digits:
        normalized = digits.replace(".", "").replace(",", ".")
    else:
        normalized = digits
    try:
        value = Decimal(normalized)
    except InvalidOperation as exc:
        raise ValueError(f"invalid Bs amount: {raw!r}") from exc
    return -value if sign == "-" else value


def _hash_ref(*parts: object) -> str:
    payload = "|".join(str(p) for p in parts)
    return f"hash:{hashlib.sha256(payload.encode('utf-8')).hexdigest()[:16]}"


def _ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


# ---------------------------------------------------------------------------
# Account + category bootstrap
# ---------------------------------------------------------------------------


def ensure_accounts(conn: sqlite3.Connection) -> dict[str, int]:
    """Create missing v1 accounts; return ``name → account_id``."""
    ids: dict[str, int] = {}
    for name, kind, currency, institution in _BACKFILL_ACCOUNTS:
        existing = accounts_repo.get_by_name(conn, name)
        if existing is None:
            created = accounts_repo.insert(
                conn,
                Account(
                    name=name,
                    kind=kind,
                    currency=currency,
                    institution=institution,
                ),
            )
            assert created.id is not None
            ids[name] = created.id
        else:
            assert existing.id is not None
            ids[name] = existing.id
    return ids


# ---------------------------------------------------------------------------
# Provincial legacy CSV reader
# ---------------------------------------------------------------------------


_HEADER_TOKENS = frozenset({"Fecha", "Dia"})


def _iter_legacy_csv_rows(csv_path: Path) -> Iterator[dict[str, str]]:
    """Yield dict rows from a legacy CSV, skipping pre-header noise.

    Google Sheets exports often prepend totals/summary rows before the
    real header. We skip forward until a row contains a known header
    token (``Fecha`` for Binance/Provincial, ``Dia`` for BCV).
    """
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        header: list[str] | None = None
        for row in reader:
            if header is None:
                normalized = [cell.strip() for cell in row]
                if any(cell in _HEADER_TOKENS for cell in normalized):
                    header = normalized
                continue
            if not any(cell.strip() for cell in row):
                continue
            pairs: dict[str, str] = {}
            for idx, cell in enumerate(row):
                if idx >= len(header) or not header[idx]:
                    continue
                pairs[header[idx]] = cell
            yield pairs


def build_rate_index_from_provincial(csv_path: Path) -> dict[date, Decimal]:
    """``date → USDT rate`` from the legacy Provincial ``Tasa USDT`` column.

    Used to stamp ``user_rate`` on Binance P2P rows so the bank-anchored
    pairing can find them (``SQL_BINANCE_CANDIDATES`` filters on
    ``user_rate IS NOT NULL``).
    """
    index: dict[date, Decimal] = {}
    if not csv_path.exists():
        return index
    for row in _iter_legacy_csv_rows(csv_path):
        fecha = row.get("Fecha", "").strip()
        rate_raw = row.get("Tasa USDT", "").strip()
        if not fecha or not rate_raw:
            continue
        try:
            occurred = parse_legacy_date(fecha)
            rate = parse_bs_amount(rate_raw)
        except ValueError:
            continue
        if rate is None or rate <= 0:
            continue
        # Keep the last-seen rate for the day (legacy sheet lists them in
        # chronological order; the final value is the end-of-day snapshot).
        index[occurred.date()] = rate
    return index


def derive_p2p_rates_greedy(
    binance_path: Path,
    provincial_path: Path,
    *,
    rate_lookup: dict[date, Decimal],
) -> dict[str, Decimal]:
    """Per-order ``{source_ref → rate}`` for Binance P2P-Sells whose date
    is not covered by ``build_rate_index_from_provincial``.

    The April 2026 orphan case: the user's Google Sheet lost its ``Tasa
    USDT`` / ``Monto (USDT)`` values (``#DIV/0!``) but the Provincial
    ``Sub-Category=Exchange`` rows still record the Bs that landed. For
    each orphan date, pair same-day Binance USDT amounts with same-day
    Provincial Bs amounts sorted magnitude-descending and compute
    ``rate = Bs / |USDT|``. Dates with no Provincial Exchange row stay
    orphan (October 2025 case) — pairing must not invent data.
    """
    binance_by_date: dict[date, list[tuple[Decimal, str]]] = {}
    if binance_path.exists():
        for row in _iter_legacy_csv_rows(binance_path):
            op = (row.get("Operación") or row.get("Operacion") or "").strip().lower()
            if op != "p2p-sell":
                continue
            fecha = (row.get("Fecha") or "").strip()
            amount_raw = (row.get("Amount") or "").strip()
            remark = (row.get("Remark") or "").strip()
            coin = (row.get("Coin") or "").strip()
            if not fecha or not amount_raw:
                continue
            try:
                occurred = parse_legacy_date(fecha)
                amount = parse_usd_amount(amount_raw)
            except ValueError:
                continue
            if rate_lookup.get(occurred.date()):
                continue
            order_match = _P2P_ORDER_RE.search(remark)
            order_number = (
                order_match.group(1)
                if order_match is not None
                else _hash_ref("legacy-p2p", occurred.isoformat(), coin, amount)
            )
            binance_by_date.setdefault(occurred.date(), []).append(
                (abs(amount), f"p2p:{order_number}")
            )

    prov_by_date: dict[date, list[Decimal]] = {}
    if provincial_path.exists():
        for row in _iter_legacy_csv_rows(provincial_path):
            if (row.get("Sub-Category") or "").strip() != "Exchange":
                continue
            fecha = (row.get("Fecha") or "").strip()
            monto_raw = (row.get("Monto") or "").strip()
            if not fecha or not monto_raw:
                continue
            try:
                occurred = parse_legacy_date(fecha)
                bs = parse_bs_amount(monto_raw)
            except ValueError:
                continue
            if bs is None or bs <= 0:
                continue
            prov_by_date.setdefault(occurred.date(), []).append(bs)

    derived: dict[str, Decimal] = {}
    for d, bin_entries in binance_by_date.items():
        provincial_list = prov_by_date.get(d)
        if not provincial_list:
            continue
        bin_sorted = sorted(bin_entries, key=lambda t: -t[0])
        prov_sorted = sorted(provincial_list, reverse=True)
        for (usdt, source_ref), bs in zip(bin_sorted, prov_sorted):
            if usdt <= 0:
                continue
            derived[source_ref] = bs / usdt
    return derived


# ---------------------------------------------------------------------------
# BCV backfill
# ---------------------------------------------------------------------------


def backfill_bcv(
    conn: sqlite3.Connection,
    csv_path: Path,
    *,
    report: BackfillReport,
) -> None:
    """Insert Rate rows from the legacy BCV CSV using RawBcvRow."""
    for row in _iter_legacy_csv_rows(csv_path):
        fecha = row.get("Dia", "").strip()
        usd_raw = row.get("USD", "").strip()
        eur_raw = row.get("EURO", "").strip()
        if not fecha or not usd_raw:
            continue
        report.bcv_rows_seen += 1
        try:
            occurred = parse_legacy_date(fecha)
            usd = parse_bs_amount(usd_raw)
            eur = parse_bs_amount(eur_raw) if eur_raw else None
        except ValueError as exc:
            report.errors.append(f"bcv: {exc}")
            continue
        if usd is None:
            continue
        parsed = RawBcvRow(
            as_of_date=occurred.date(),
            usd=usd,
            eur=eur if eur is not None else Decimal("0"),
        )
        for base, value in (("USD", parsed.usd), ("EUR", parsed.eur)):
            if value <= 0:
                continue
            rate = Rate(
                as_of_date=parsed.as_of_date,
                base=base,
                quote="VES",
                rate=value,
                source=BCV_SOURCE,
            )
            try:
                rates_repo.insert(conn, rate)
                report.bcv_rates_inserted += 1
            except sqlite3.IntegrityError:
                continue


# ---------------------------------------------------------------------------
# Provincial backfill
# ---------------------------------------------------------------------------


def backfill_provincial(
    conn: sqlite3.Connection,
    csv_path: Path,
    *,
    account_ids: dict[str, int],
    report: BackfillReport,
) -> None:
    """Upsert Provincial rows using ``RawProvincialRow`` + ``user_rate``."""
    account_id = account_ids["Provincial Bolivares"]
    for raw_row in _iter_legacy_csv_rows(csv_path):
        fecha = raw_row.get("Fecha", "").strip()
        descripcion = (raw_row.get("Referencia") or "").strip()
        desc_note = (raw_row.get("Descripción") or "").strip()
        monto_raw = raw_row.get("Monto", "").strip()
        tasa_raw = raw_row.get("Tasa USDT", "").strip()
        if not fecha or not monto_raw:
            continue
        report.provincial_rows_seen += 1
        try:
            occurred = parse_legacy_date(fecha)
            monto = parse_bs_amount(monto_raw)
        except ValueError as exc:
            report.errors.append(f"provincial: {exc}")
            continue
        if monto is None:
            continue
        user_rate: Decimal | None = None
        if tasa_raw:
            try:
                user_rate = parse_bs_amount(tasa_raw)
            except ValueError:
                user_rate = None
            if user_rate is not None and user_rate <= 0:
                user_rate = None

        # Reuse Provincial's Pydantic validator + source_ref hasher so
        # the shape stays aligned with live CSV ingest.
        legacy_fecha = f"{occurred.day:02d}/{occurred.month:02d}/{occurred.year}"
        raw = RawProvincialRow(
            fecha=legacy_fecha,
            descripcion=descripcion or desc_note or "(no description)",
            monto=monto,
        )
        source_ref = provincial_source_ref(
            occurred_at=occurred,
            amount=raw.monto,
            description=raw.descripcion,
        )
        txn = Transaction(
            account_id=account_id,
            occurred_at=occurred,
            kind=raw.kind,
            amount=raw.monto,
            currency="VES",
            description=raw.descripcion,
            user_rate=user_rate,
            source=PROVINCIAL_SOURCE,
            source_ref=source_ref,
            needs_review=True,  # resolved interactively in cleanup
        )
        result = transactions_repo.upsert_by_source_ref(conn, txn)
        report.provincial_rows_inserted += result["rows_inserted"]


# ---------------------------------------------------------------------------
# Binance backfill
# ---------------------------------------------------------------------------


def _binance_account_id(cuenta: str, account_ids: dict[str, int]) -> int:
    mapping = {
        "spot": account_ids["Binance Spot"],
        "funding": account_ids["Binance Funding"],
        "earn": account_ids["Binance Earn"],
    }
    key = cuenta.strip().lower()
    if key not in mapping:
        raise ValueError(f"unknown Binance legacy account: {cuenta!r}")
    return mapping[key]


def _handle_binance_deposit(
    conn: sqlite3.Connection,
    *,
    occurred: datetime,
    coin: str,
    amount: Decimal,
    remark: str,
    account_id: int,
    report: BackfillReport,
) -> None:
    tx_id = _hash_ref("legacy-deposit", occurred.isoformat(), coin, amount, remark)
    raw = RawBinanceDepositRow(
        txId=tx_id,
        coin=coin,
        amount=abs(amount),
        insertTime=_ms(occurred),
    )
    txn = raw.to_transaction(spot_account_id=account_id).model_copy(
        update={"needs_review": True}
    )
    result = transactions_repo.upsert_by_source_ref(conn, txn)
    report.binance_rows_inserted += result["rows_inserted"]


def _handle_binance_send(
    conn: sqlite3.Connection,
    *,
    occurred: datetime,
    coin: str,
    amount: Decimal,
    remark: str,
    account_id: int,
    report: BackfillReport,
) -> None:
    send_id = _hash_ref("legacy-send", occurred.isoformat(), coin, amount, remark)
    raw = RawBinanceWithdrawRow(
        id=send_id,
        coin=coin,
        amount=abs(amount),
        applyTime=_ms(occurred),
    )
    txn = raw.to_transaction(spot_account_id=account_id).model_copy(
        update={
            "description": f"Binance send {coin.upper()} — {remark}" if remark else None
            or f"Binance send {coin.upper()}",
            "needs_review": True,
        }
    )
    result = transactions_repo.upsert_by_source_ref(conn, txn)
    report.binance_rows_inserted += result["rows_inserted"]


def _handle_binance_p2p_sell(
    conn: sqlite3.Connection,
    *,
    occurred: datetime,
    coin: str,
    amount: Decimal,
    remark: str,
    account_id: int,
    rate_lookup: dict[date, Decimal],
    derived_rates: dict[str, Decimal] | None,
    report: BackfillReport,
) -> None:
    order_match = _P2P_ORDER_RE.search(remark)
    order_number = (
        order_match.group(1)
        if order_match is not None
        else _hash_ref("legacy-p2p", occurred.isoformat(), coin, amount)
    )
    unit_price = rate_lookup.get(occurred.date())
    if (unit_price is None or unit_price <= 0) and derived_rates is not None:
        fallback = derived_rates.get(f"p2p:{order_number}")
        if fallback is not None and fallback > 0:
            unit_price = fallback
    if unit_price is None or unit_price <= 0:
        # Without a rate the row still has to exist (balances depend on
        # it); leave user_rate unset and let the cleanup pass collect it.
        unit_price = Decimal("0")
    raw = RawBinanceP2pRow(
        orderNumber=str(order_number),
        tradeType="SELL",
        asset=coin,
        amount=abs(amount),
        unitPrice=unit_price,
        fiat="VES",
        createTime=_ms(occurred),
    )
    txn = raw.to_transaction(spot_account_id=account_id)
    if unit_price == 0:
        txn = txn.model_copy(update={"user_rate": None})
    txn = txn.model_copy(update={"needs_review": True})
    result = transactions_repo.upsert_by_source_ref(conn, txn)
    report.binance_rows_inserted += result["rows_inserted"]


def _handle_binance_internal_transfer(
    conn: sqlite3.Connection,
    *,
    occurred: datetime,
    cuenta: str,
    coin: str,
    amount: Decimal,
    remark: str,
    account_ids: dict[str, int],
    report: BackfillReport,
) -> None:
    tran_id = _hash_ref(
        "legacy-transfer", occurred.isoformat(), cuenta, coin, amount, remark
    )
    # Legacy sheet only records one side. A positive amount on account X
    # means money flowed INTO X from the sibling (Spot↔Funding). Use the
    # sign to pick direction so the resulting pair is double-entry clean.
    cuenta_key = cuenta.strip().lower()
    if cuenta_key == "spot":
        to_account = "Binance Spot"
        from_account = "Binance Funding"
    else:
        to_account = "Binance Funding"
        from_account = "Binance Spot"
    if amount < 0:
        from_account, to_account = to_account, from_account
    validated = RawBinanceTransferRow(
        tranId=tran_id,
        type="MAIN_FUNDING" if to_account == "Binance Funding" else "FUNDING_MAIN",
        asset=coin,
        amount=abs(amount),
        timestamp=_ms(occurred),
    )
    source_ref_from = f"transfer:{tran_id}:from"
    existing = transactions_repo.get_by_source_ref(
        conn, BINANCE_SOURCE, source_ref_from
    )
    if existing is not None:
        return  # idempotent
    create_transfer(
        conn,
        from_account_id=account_ids[from_account],
        to_account_id=account_ids[to_account],
        amount=validated.amount,
        currency=validated.asset.upper(),
        occurred_at=occurred,
        description=f"Binance internal transfer ({cuenta} leg: {remark})",
        source=BINANCE_SOURCE,
        source_ref_from=source_ref_from,
        source_ref_to=f"transfer:{tran_id}:to",
    )
    report.binance_rows_inserted += 2


def _handle_binance_convert(
    conn: sqlite3.Connection,
    *,
    occurred: datetime,
    coin: str,
    amount: Decimal,
    remark: str,
    account_id: int,
    report: BackfillReport,
) -> None:
    """Legacy Convert rows are single-sided; ingest each as a solo row.

    The live ingest uses ``RawBinanceConvertRow.to_transactions()`` for a
    pair; the legacy sheet records only one leg per row, so we wrap the
    single leg in a ``RawBinanceConvertRow`` that self-converts (from →
    to same asset, zero opposite leg) and pick the relevant side.
    """
    tran_id = _hash_ref("legacy-convert", occurred.isoformat(), coin, amount, remark)
    is_out = amount < 0
    raw = RawBinanceConvertRow(
        tranId=tran_id,
        fromAsset=coin if is_out else "UNKNOWN",
        fromAmount=abs(amount) if is_out else Decimal("0"),
        toAsset="UNKNOWN" if is_out else coin,
        toAmount=Decimal("0") if is_out else abs(amount),
        createTime=_ms(occurred),
    )
    legs = raw.to_transactions(spot_account_id=account_id)
    side = legs[0] if is_out else legs[1]
    txn = side.model_copy(update={"needs_review": True})
    result = transactions_repo.upsert_by_source_ref(conn, txn)
    report.binance_rows_inserted += result["rows_inserted"]


def backfill_binance(
    conn: sqlite3.Connection,
    csv_path: Path,
    *,
    account_ids: dict[str, int],
    rate_lookup: dict[date, Decimal],
    derived_rates: dict[str, Decimal] | None = None,
    report: BackfillReport,
) -> None:
    for row in _iter_legacy_csv_rows(csv_path):
        fecha = (row.get("Fecha") or "").strip()
        cuenta = (row.get("Cuenta") or "").strip()
        operation = (row.get("Operación") or row.get("Operacion") or "").strip()
        coin = (row.get("Coin") or "").strip()
        amount_raw = (row.get("Amount") or "").strip()
        remark = (row.get("Remark") or "").strip()

        if not fecha or not operation or not amount_raw:
            continue
        report.binance_rows_seen += 1
        try:
            occurred = parse_legacy_date(fecha)
            amount = parse_usd_amount(amount_raw)
            account_id = _binance_account_id(cuenta, account_ids)
        except ValueError as exc:
            report.errors.append(f"binance: {exc}")
            continue

        op = operation.lower()
        try:
            if op == "deposit":
                _handle_binance_deposit(
                    conn,
                    occurred=occurred, coin=coin, amount=amount, remark=remark,
                    account_id=account_id, report=report,
                )
            elif op == "send":
                _handle_binance_send(
                    conn,
                    occurred=occurred, coin=coin, amount=amount, remark=remark,
                    account_id=account_id, report=report,
                )
            elif op == "p2p-sell":
                _handle_binance_p2p_sell(
                    conn,
                    occurred=occurred, coin=coin, amount=amount, remark=remark,
                    account_id=account_id, rate_lookup=rate_lookup,
                    derived_rates=derived_rates, report=report,
                )
            elif op == "internal transfer":
                _handle_binance_internal_transfer(
                    conn,
                    occurred=occurred, cuenta=cuenta, coin=coin, amount=amount,
                    remark=remark, account_ids=account_ids, report=report,
                )
            elif op in ("binance convert", "convert"):
                _handle_binance_convert(
                    conn,
                    occurred=occurred, coin=coin, amount=amount, remark=remark,
                    account_id=account_id, report=report,
                )
            else:
                report.errors.append(
                    f"binance: unsupported legacy operation {operation!r}"
                )
        except Exception as exc:  # noqa: BLE001
            report.errors.append(f"binance row {operation}/{coin}: {exc}")


# ---------------------------------------------------------------------------
# Top-level orchestrator
# ---------------------------------------------------------------------------


def run_backfill(
    conn: sqlite3.Connection,
    data_dir: Path,
    *,
    pairing_window_days: int = 2,
    force: bool = False,
) -> BackfillReport:
    """Read all three legacy CSVs and feed them through production ingest.

    Raises ``FileNotFoundError`` if either the Binance or Provincial CSV
    is missing. BCV is treated as optional — no BCV file is not fatal,
    only the v_transactions_usd fallback becomes less precise.

    Re-running the backfill resets ``needs_review``, ``category_id``, and
    ``transfer_id`` on every ingested row (``upsert_by_source_ref`` is
    deliberately authoritative about the source columns per ADR-010).
    Once the interactive cleanup pass has started writing categories,
    a second backfill would wipe that work. Guard against it by refusing
    to run on a non-empty ``transactions`` table unless ``force=True``.
    """
    binance_path = data_dir / BINANCE_CSV_NAME
    provincial_path = data_dir / PROVINCIAL_CSV_NAME
    bcv_path = data_dir / BCV_CSV_NAME

    for required in (binance_path, provincial_path):
        if not required.exists():
            raise FileNotFoundError(required)

    if not force:
        existing = conn.execute(
            "SELECT COUNT(*) FROM transactions"
        ).fetchone()[0]
        if existing:
            raise RuntimeError(
                f"transactions table already contains {existing} rows; "
                "re-running backfill would reset needs_review/category_id "
                "on every row. Pass force=True (CLI: --force) to override."
            )

    report = BackfillReport()
    account_ids = ensure_accounts(conn)
    rate_lookup = build_rate_index_from_provincial(provincial_path)
    derived_rates = derive_p2p_rates_greedy(
        binance_path, provincial_path, rate_lookup=rate_lookup
    )

    if bcv_path.exists():
        backfill_bcv(conn, bcv_path, report=report)
    backfill_provincial(
        conn, provincial_path, account_ids=account_ids, report=report
    )
    backfill_binance(
        conn,
        binance_path,
        account_ids=account_ids,
        rate_lookup=rate_lookup,
        derived_rates=derived_rates,
        report=report,
    )

    strategy = BankAnchoredP2pPairing(
        conn,
        window_days=pairing_window_days,
        bank_source=PROVINCIAL_SOURCE,
        binance_source=BINANCE_SOURCE,
    )
    report.reconciliation = run_reconciliation_pass(strategy)

    report.rows_legacy_mapped = apply_legacy_category_annotations(conn, data_dir)
    report.rows_categorized = apply_category_rules(conn)
    return report


def apply_legacy_category_annotations(
    conn: sqlite3.Connection, data_dir: Path
) -> int:
    """Stamp ``category_id`` + clear ``needs_review`` on every row whose
    legacy CSV carried a ``Sub-Category`` resolvable via
    ``LEGACY_SUB_CATEGORY_TO_V1``. This is Julio's own hand-categorization
    being ported to the v1 taxonomy — ground truth, preferred over the
    regex rules engine. Called before ``apply_category_rules`` so any
    rows without a legacy annotation (e.g. live Binance sync rows) still
    get a shot at auto-classification.

    Unrecognized Sub-Category values are skipped (per user rule: ask
    before mapping anything outside the closed list). Those rows stay
    ``needs_review=1`` and flow into Phase F residual cleanup.
    """
    cat_ids: dict[str, int] = {
        row["name"]: int(row["id"])
        for row in conn.execute("SELECT id, name FROM categories")
    }
    mapped = 0
    for source, source_ref, sub_cat, _category in iter_legacy_annotations(data_dir):
        v1_name = LEGACY_SUB_CATEGORY_TO_V1.get(sub_cat)
        if v1_name is None:
            continue
        v1_id = cat_ids.get(v1_name)
        if v1_id is None:
            continue  # migration gap — category not in DB yet; skip safely
        cursor = conn.execute(
            "UPDATE transactions SET category_id = ?, needs_review = 0 "
            "WHERE source = ? AND source_ref = ? AND category_id IS NULL",
            (v1_id, source, source_ref),
        )
        mapped += cursor.rowcount
    conn.commit()
    return mapped


def apply_category_rules(conn: sqlite3.Connection) -> int:
    """Run the seeded categorization engine across every row with
    ``category_id IS NULL``. Per rule-006 this is the one sanctioned
    place for the engine to stamp ``category_id`` during backfill;
    ingesters themselves never set it directly. Rows that match a rule
    get the rule's category_id and ``needs_review=0``; unmatched rows
    stay exactly as they were."""
    rows = conn.execute(
        "SELECT id, description, source, account_id "
        "FROM transactions WHERE category_id IS NULL"
    ).fetchall()
    categorized = 0
    for row in rows:
        match = suggest(
            conn,
            CategorizationRequest(
                description=row["description"],
                source=row["source"],
                account_id=row["account_id"],
            ),
        )
        if match is None:
            continue
        conn.execute(
            "UPDATE transactions SET category_id = ?, needs_review = 0 "
            "WHERE id = ?",
            (match.category_id, int(row["id"])),
        )
        categorized += 1
    conn.commit()
    return categorized


def iter_legacy_annotations(
    data_dir: Path,
) -> Iterator[tuple[str, str, str, str]]:
    """Yield ``(source, source_ref, sub_category, category)`` tuples.

    Re-reads the legacy CSVs and applies the *same* ``source_ref``
    hashing the backfill uses, so callers can join on ``(source,
    source_ref)`` against the ledger. Used by the cleanup export to
    surface the user's prior Sub-Category / Category labels next to
    each needs_review row.
    """
    binance_path = data_dir / BINANCE_CSV_NAME
    if binance_path.exists():
        yield from _iter_binance_annotations(binance_path)

    provincial_path = data_dir / PROVINCIAL_CSV_NAME
    if provincial_path.exists():
        yield from _iter_provincial_annotations(provincial_path)


def _iter_binance_annotations(
    csv_path: Path,
) -> Iterator[tuple[str, str, str, str]]:
    for row in _iter_legacy_csv_rows(csv_path):
        fecha = (row.get("Fecha") or "").strip()
        cuenta = (row.get("Cuenta") or "").strip()
        operation = (row.get("Operación") or row.get("Operacion") or "").strip()
        coin = (row.get("Coin") or "").strip()
        amount_raw = (row.get("Amount") or "").strip()
        remark = (row.get("Remark") or "").strip()
        sub_cat = (row.get("Sub-Category") or "").strip()
        category = (row.get("Category") or "").strip()
        if not fecha or not operation or not amount_raw:
            continue
        try:
            occurred = parse_legacy_date(fecha)
            amount = parse_usd_amount(amount_raw)
        except ValueError:
            continue

        op = operation.lower()
        occ_iso = occurred.isoformat()
        if op == "deposit":
            ref = f"deposit:{_hash_ref('legacy-deposit', occ_iso, coin, amount, remark)}"
            yield (BINANCE_SOURCE, ref, sub_cat, category)
        elif op == "send":
            ref = f"withdraw:{_hash_ref('legacy-send', occ_iso, coin, amount, remark)}"
            yield (BINANCE_SOURCE, ref, sub_cat, category)
        elif op == "p2p-sell":
            match = _P2P_ORDER_RE.search(remark)
            order_number = (
                match.group(1)
                if match is not None
                else _hash_ref("legacy-p2p", occ_iso, coin, amount)
            )
            yield (BINANCE_SOURCE, f"p2p:{order_number}", sub_cat, category)
        elif op == "internal transfer":
            tran_id = _hash_ref(
                "legacy-transfer", occ_iso, cuenta, coin, amount, remark
            )
            # Both legs share the same Sub-Category / Category in the sheet.
            yield (BINANCE_SOURCE, f"transfer:{tran_id}:from", sub_cat, category)
            yield (BINANCE_SOURCE, f"transfer:{tran_id}:to", sub_cat, category)
        elif op in ("binance convert", "convert"):
            tran_id = _hash_ref(
                "legacy-convert", occ_iso, coin, amount, remark
            )
            suffix = "from" if amount < 0 else "to"
            yield (BINANCE_SOURCE, f"convert:{tran_id}:{suffix}", sub_cat, category)


def _iter_provincial_annotations(
    csv_path: Path,
) -> Iterator[tuple[str, str, str, str]]:
    for row in _iter_legacy_csv_rows(csv_path):
        fecha = (row.get("Fecha") or "").strip()
        descripcion = (row.get("Referencia") or "").strip()
        desc_note = (row.get("Descripción") or "").strip()
        monto_raw = (row.get("Monto") or "").strip()
        sub_cat = (row.get("Sub-Category") or "").strip()
        category = (row.get("Category") or "").strip()
        if not fecha or not monto_raw:
            continue
        try:
            occurred = parse_legacy_date(fecha)
            monto = parse_bs_amount(monto_raw)
        except ValueError:
            continue
        if monto is None:
            continue
        description = descripcion or desc_note or "(no description)"
        source_ref = provincial_source_ref(
            occurred_at=occurred, amount=monto, description=description
        )
        yield (PROVINCIAL_SOURCE, source_ref, sub_cat, category)


__all__ = [
    "BackfillReport",
    "BCV_CSV_NAME",
    "BINANCE_CSV_NAME",
    "PROVINCIAL_CSV_NAME",
    "backfill_bcv",
    "backfill_binance",
    "backfill_provincial",
    "build_rate_index_from_provincial",
    "ensure_accounts",
    "iter_legacy_annotations",
    "parse_bs_amount",
    "parse_legacy_date",
    "parse_usd_amount",
    "run_backfill",
]

# Silence unused-import lint on type-only references exposed for completeness.
_ = (Any, TransactionKind)
