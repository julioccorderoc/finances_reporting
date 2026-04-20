from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from pathlib import Path

import pytest

from finances.db.connection import get_connection
from finances.db.migrate import apply_migrations


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "test.db"


@pytest.fixture
def db_conn(db_path: Path) -> Iterator[sqlite3.Connection]:
    conn = get_connection(db_path)
    apply_migrations(conn)
    try:
        yield conn
    finally:
        conn.close()
