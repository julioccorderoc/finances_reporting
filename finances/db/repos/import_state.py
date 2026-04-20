from __future__ import annotations

import sqlite3
from datetime import datetime
from typing import Any

# import_state and import_runs are internal ledger plumbing rather than
# trust-boundary data, so this repo exposes typed dicts rather than Pydantic
# models. External ingesters still produce Pydantic models for the domain
# objects they insert via the transactions repo (per ADR-009).


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    return value.isoformat() if hasattr(value, "isoformat") else str(value)


def get_state(conn: sqlite3.Connection, source: str) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT source, last_synced_at, cursor, updated_at FROM import_state WHERE source = ?",
        (source,),
    ).fetchone()
    if row is None:
        return None
    return {
        "source": row["source"],
        "last_synced_at": row["last_synced_at"],
        "cursor": row["cursor"],
        "updated_at": row["updated_at"],
    }


def upsert_state(
    conn: sqlite3.Connection,
    *,
    source: str,
    last_synced_at: datetime | None = None,
    cursor: str | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO import_state (source, last_synced_at, cursor, updated_at)
        VALUES (?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(source) DO UPDATE SET
            last_synced_at = excluded.last_synced_at,
            cursor         = excluded.cursor,
            updated_at     = CURRENT_TIMESTAMP
        """,
        (source, _iso(last_synced_at), cursor),
    )


def start_run(conn: sqlite3.Connection, source: str) -> int:
    cur = conn.execute(
        "INSERT INTO import_runs (source, status) VALUES (?, 'running')",
        (source,),
    )
    return int(cur.lastrowid)


def finish_run(
    conn: sqlite3.Connection,
    run_id: int,
    *,
    status: str,
    rows_inserted: int = 0,
    rows_updated: int = 0,
    rows_skipped: int = 0,
    error: str | None = None,
) -> None:
    if status not in ("running", "success", "error"):
        raise ValueError(f"invalid status: {status}")
    conn.execute(
        """
        UPDATE import_runs SET
            status         = ?,
            finished_at    = CURRENT_TIMESTAMP,
            rows_inserted  = ?,
            rows_updated   = ?,
            rows_skipped   = ?,
            error          = ?
        WHERE id = ?
        """,
        (status, rows_inserted, rows_updated, rows_skipped, error, run_id),
    )


def get_run(conn: sqlite3.Connection, run_id: int) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT id, source, started_at, finished_at, rows_inserted, rows_updated,
               rows_skipped, status, error
        FROM import_runs WHERE id = ?
        """,
        (run_id,),
    ).fetchone()
    if row is None:
        return None
    return dict(row)
