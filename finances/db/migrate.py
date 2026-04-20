from __future__ import annotations

import re
import sqlite3
import sys
from pathlib import Path

from finances.config import DB_PATH
from finances.db.connection import get_connection

MIGRATIONS_DIR = Path(__file__).resolve().parent / "migrations"
_FILENAME_RE = re.compile(r"^\d{3}_.+\.sql$")


def _ensure_migrations_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS _migrations (
            filename   TEXT PRIMARY KEY,
            applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )


def _applied(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute("SELECT filename FROM _migrations").fetchall()
    return {r["filename"] if isinstance(r, sqlite3.Row) else r[0] for r in rows}


def _pending(conn: sqlite3.Connection, migrations_dir: Path) -> list[Path]:
    already = _applied(conn)
    candidates = sorted(p for p in migrations_dir.glob("*.sql") if _FILENAME_RE.match(p.name))
    return [p for p in candidates if p.name not in already]


def apply_migrations(conn: sqlite3.Connection, migrations_dir: Path | None = None) -> list[str]:
    """Apply any unapplied migrations in order. Returns the filenames applied."""
    migrations_dir = migrations_dir or MIGRATIONS_DIR
    _ensure_migrations_table(conn)
    applied: list[str] = []
    for path in _pending(conn, migrations_dir):
        sql = path.read_text(encoding="utf-8")
        # executescript manages its own transaction and commits any pending
        # work, so we apply the SQL first and then record the migration in a
        # separate statement. A failure in either leaves partial state that is
        # surfaced to the caller.
        conn.executescript(sql)
        conn.execute(
            "INSERT INTO _migrations(filename) VALUES (?)",
            (path.name,),
        )
        applied.append(path.name)
    return applied


def migrate(db_path: Path | str | None = None) -> list[str]:
    db_path = Path(db_path) if db_path is not None else DB_PATH
    conn = get_connection(db_path)
    try:
        return apply_migrations(conn)
    finally:
        conn.close()


def main() -> int:
    applied = migrate()
    if applied:
        for name in applied:
            print(f"applied: {name}")
    else:
        print("no pending migrations")
    return 0


if __name__ == "__main__":
    sys.exit(main())
