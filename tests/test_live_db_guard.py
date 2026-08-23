"""The suite must never touch the live ``finances.db`` (see tests/conftest.py).

These tests pin the guard itself: if someone removes or weakens it, this file
goes red before the ledger file does.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from finances import config


def test_connecting_to_the_live_db_path_is_rejected() -> None:
    with pytest.raises(AssertionError, match="live database"):
        sqlite3.connect(str(config.DB_PATH))


def test_connecting_to_the_live_db_path_as_a_path_object_is_rejected() -> None:
    with pytest.raises(AssertionError, match="live database"):
        sqlite3.connect(config.DB_PATH)


def test_connecting_to_the_live_db_path_via_a_uri_is_rejected() -> None:
    with pytest.raises(AssertionError, match="live database"):
        sqlite3.connect(f"file:{config.DB_PATH}?mode=ro&immutable=1", uri=True)


def test_get_connection_on_the_live_db_path_is_rejected() -> None:
    from finances.db.connection import get_connection

    with pytest.raises(AssertionError, match="live database"):
        get_connection(config.DB_PATH)


def test_the_guard_leaves_ordinary_paths_alone(tmp_path: Path) -> None:
    conn = sqlite3.connect(tmp_path / "scratch.db")
    try:
        conn.execute("CREATE TABLE t (x INTEGER)")
    finally:
        conn.close()
    assert (tmp_path / "scratch.db").exists()


def test_the_guard_leaves_in_memory_connections_alone() -> None:
    conn = sqlite3.connect(":memory:")
    try:
        assert conn.execute("SELECT 1").fetchone()[0] == 1
    finally:
        conn.close()


def test_the_live_db_is_not_materialised_by_the_suite() -> None:
    """Sanity net for a fresh worktree: nothing created the stub."""
    assert not config.DB_PATH.exists() or config.DB_PATH.stat().st_size > 4096
