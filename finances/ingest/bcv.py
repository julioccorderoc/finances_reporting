"""BCV exchange-rate scraper (EPIC-009 / ADR-007 / rule-007).

Fetches the BCV daily reference rates (USD/VES, EUR/VES) from the public
page, parses them, and writes them to the ``rates`` table under
``source='bcv'``. On any failure the ``import_runs`` row for the run is
flagged ``status='error'``, the exception is re-raised (so the process
exits non-zero), and existing ``rates`` rows are left untouched.
"""
from __future__ import annotations

import re
import sqlite3
from datetime import date
from decimal import Decimal
from typing import Any

import httpx
from bs4 import BeautifulSoup
from pydantic import BaseModel, ConfigDict, field_validator

from finances.db.repos import import_state, rates as rates_repo
from finances.domain.models import Rate

BCV_URL = "https://www.bcv.org.ve/estadisticas/tipo-de-cambio-de-referencia-smc"
SOURCE_NAME = "bcv"

_MONTH_MAP: dict[str, int] = {
    "enero": 1,
    "febrero": 2,
    "marzo": 3,
    "abril": 4,
    "mayo": 5,
    "junio": 6,
    "julio": 7,
    "agosto": 8,
    "septiembre": 9,
    "octubre": 10,
    "noviembre": 11,
    "diciembre": 12,
}

_CURRENCY_RE = re.compile(r"(\d+,\d+)")


class BcvParseError(RuntimeError):
    """Raised when the BCV page cannot be parsed into any usable rows."""


class RawBcvRow(BaseModel):
    model_config = ConfigDict(strict=False, extra="forbid")

    as_of_date: date
    usd: Decimal
    eur: Decimal

    @field_validator("usd", "eur", mode="before")
    @classmethod
    def _decimal_fields(cls, v: Any) -> Any:
        if isinstance(v, bool):
            raise ValueError("bool is not a valid monetary value")
        if isinstance(v, float):
            raise ValueError("float monetary inputs are forbidden; use Decimal or str")
        if isinstance(v, Decimal):
            return v
        return Decimal(str(v))


def parse_spanish_date(date_str: str) -> date | None:
    """Parse 'Viernes, 17 de abril de 2026' → date(2026, 4, 17).

    Returns ``None`` for any malformed input rather than raising, so the
    row-level parser can skip bad rows without aborting the whole page.
    """
    if not date_str:
        return None
    parts = date_str.split(",", 1)
    if len(parts) < 2:
        return None
    clean = parts[1].strip()
    components = clean.split(" de ")
    if len(components) != 3:
        return None
    day_raw, month_raw, year_raw = components
    try:
        day = int(day_raw.strip())
        year = int(year_raw.strip())
    except ValueError:
        return None
    month = _MONTH_MAP.get(month_raw.strip().lower())
    if month is None:
        return None
    try:
        return date(year, month, day)
    except ValueError:
        return None


def clean_currency(text: str) -> Decimal:
    """'Bs.S 480,25' → Decimal('480.25'). Raises ValueError on unparseable."""
    cleaned = text.replace("Bs.S", "").replace("\xa0", "").strip()
    match = _CURRENCY_RE.search(cleaned)
    if not match:
        raise ValueError(f"unparseable BCV currency: {text!r}")
    return Decimal(match.group(1).replace(",", "."))


def parse_bcv_html(html: str) -> list[RawBcvRow]:
    """Parse the BCV HTML table into ``RawBcvRow`` instances.

    Raises ``BcvParseError`` when the structural parse yields zero valid
    rows — the caller uses this to trigger rule-007's error path.
    """
    soup = BeautifulSoup(html, "html.parser")
    tbody = soup.find("tbody")
    if tbody is None:
        raise BcvParseError("BCV page has no <tbody> element")

    trs = tbody.find_all("tr")
    if not trs:
        raise BcvParseError("BCV <tbody> has no <tr> rows")

    rows: list[RawBcvRow] = []
    for tr in trs:
        cols = tr.find_all("td")
        if len(cols) < 3:
            continue
        date_raw = cols[0].get_text(strip=True)
        usd_span = cols[1].find("span")
        eur_span = cols[2].find("span")
        if usd_span is None or eur_span is None:
            continue
        dt = parse_spanish_date(date_raw)
        if dt is None:
            continue
        try:
            usd = clean_currency(usd_span.get_text(strip=True))
            eur = clean_currency(eur_span.get_text(strip=True))
        except ValueError:
            continue
        rows.append(RawBcvRow(as_of_date=dt, usd=usd, eur=eur))

    if not rows:
        raise BcvParseError("BCV page produced zero valid rate rows")
    return rows


def fetch_bcv_html(url: str = BCV_URL, *, timeout: float = 10.0) -> str:
    """Fetch the BCV page HTML over HTTPS. Raises httpx errors on failure."""
    with httpx.Client(timeout=timeout) as client:
        response = client.get(url)
        response.raise_for_status()
        return response.text


def ingest_bcv(
    conn: sqlite3.Connection,
    *,
    html: str | None = None,
    url: str = BCV_URL,
) -> int:
    """Ingest BCV rates into the ``rates`` table.

    Returns the number of rate rows actually inserted; duplicates under the
    ``UNIQUE (as_of_date, base, quote, source)`` constraint are silently
    counted as skipped (idempotent re-runs return 0).

    On any failure (fetch, parse, or DB), the ``import_runs`` row is
    marked ``status='error'`` with the exception summary, any partial
    writes are rolled back, and the exception is re-raised.
    """
    run_id = import_state.start_run(conn, SOURCE_NAME)
    try:
        raw_html = html if html is not None else fetch_bcv_html(url)
        parsed = parse_bcv_html(raw_html)

        conn.execute("BEGIN")
        try:
            inserted = 0
            for row in parsed:
                for base, value in (("USD", row.usd), ("EUR", row.eur)):
                    rate = Rate(
                        as_of_date=row.as_of_date,
                        base=base,
                        quote="VES",
                        rate=value,
                        source=SOURCE_NAME,
                    )
                    try:
                        rates_repo.insert(conn, rate)
                        inserted += 1
                    except sqlite3.IntegrityError:
                        continue
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

        import_state.upsert_state(conn, source=SOURCE_NAME)
        import_state.finish_run(
            conn, run_id, status="success", rows_inserted=inserted
        )
        return inserted
    except Exception as exc:
        error_msg = f"{type(exc).__name__}: {exc}"[:4000]
        import_state.finish_run(conn, run_id, status="error", error=error_msg)
        raise


__all__ = [
    "BCV_URL",
    "BcvParseError",
    "RawBcvRow",
    "SOURCE_NAME",
    "clean_currency",
    "fetch_bcv_html",
    "ingest_bcv",
    "parse_bcv_html",
    "parse_spanish_date",
]
