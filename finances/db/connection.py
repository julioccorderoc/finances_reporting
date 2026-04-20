from __future__ import annotations

import sqlite3
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

_ADAPTERS_REGISTERED = False


def _register_decimal_adapters() -> None:
    """Register Decimal, datetime, and date adapters/converters for sqlite3.

    Runs once per process. Decimal round-trips as TEXT. Datetimes round-trip
    as ISO-8601 with timezone offset (Python's stdlib converter chokes on the
    ``T`` separator and on timezone offsets, so we replace it).
    """
    global _ADAPTERS_REGISTERED
    if _ADAPTERS_REGISTERED:
        return

    def _adapt_decimal(value: Decimal) -> str:
        return format(value, "f")

    def _convert_decimal(raw: bytes) -> Decimal:
        return Decimal(raw.decode("utf-8"))

    def _adapt_datetime(value: datetime) -> str:
        return value.isoformat()

    def _convert_datetime(raw: bytes) -> datetime:
        text = raw.decode("utf-8")
        # Accept "YYYY-MM-DD HH:MM:SS[.ffff]" (sqlite CURRENT_TIMESTAMP)
        # and "YYYY-MM-DDTHH:MM:SS[+HH:MM]" (our ISO output).
        try:
            return datetime.fromisoformat(text)
        except ValueError:
            return datetime.fromisoformat(text.replace(" ", "T"))

    def _adapt_date(value: date) -> str:
        return value.isoformat()

    def _convert_date(raw: bytes) -> date:
        return date.fromisoformat(raw.decode("utf-8"))

    sqlite3.register_adapter(Decimal, _adapt_decimal)
    sqlite3.register_converter("DECIMAL", _convert_decimal)
    sqlite3.register_adapter(datetime, _adapt_datetime)
    sqlite3.register_converter("TIMESTAMP", _convert_datetime)
    sqlite3.register_adapter(date, _adapt_date)
    sqlite3.register_converter("DATE", _convert_date)
    _ADAPTERS_REGISTERED = True


def get_connection(db_path: str | Path) -> sqlite3.Connection:
    """Return a configured sqlite3 connection.

    Applies WAL mode, enforces foreign keys, sets Row factory, and enables
    declared-type parsing so DECIMAL columns round-trip as Python Decimal.
    """
    _register_decimal_adapters()
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(
        str(path),
        detect_types=sqlite3.PARSE_DECLTYPES,
        isolation_level=None,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA synchronous = NORMAL")
    return conn
