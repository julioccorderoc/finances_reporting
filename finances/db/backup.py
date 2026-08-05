"""DB snapshots — always into ``backups/``, never next to the live DB.

Uses the sqlite3 online-backup API so a snapshot is safe while the viewer
holds a WAL connection, and produces exactly one file (no ``-shm``/``-wal``
siblings to keep in sync).
"""

from __future__ import annotations

import re
import sqlite3
from datetime import datetime
from pathlib import Path

from finances.config import CARACAS_TZ

_LABEL_RE = re.compile(r"^[A-Za-z0-9._-]+$")

# Filename shapes ad-hoc snapshots have historically taken in the repo root.
_STRAY_PATTERNS = ("finances.db.bak*", "finances.db-bak*", "finances-backup-*")


def create_backup(
    db_path: Path,
    backups_dir: Path,
    label: str | None = None,
    now: datetime | None = None,
) -> Path:
    """Snapshot ``db_path`` into ``backups_dir`` and return the new file."""
    if label is not None and not _LABEL_RE.match(label):
        raise ValueError(
            f"label {label!r} may only contain letters, digits, '.', '_' and '-'"
        )
    stamp = (now or datetime.now(tz=CARACAS_TZ)).strftime("%Y%m%d-%H%M%S")
    name = f"finances-{stamp}{f'-{label}' if label else ''}.db"
    backups_dir.mkdir(parents=True, exist_ok=True)
    dest = backups_dir / name

    src = sqlite3.connect(db_path)
    try:
        dst = sqlite3.connect(dest)
        try:
            src.backup(dst)
        finally:
            dst.close()
    finally:
        src.close()
    return dest


def stray_backups(root: Path) -> list[Path]:
    """Backup-looking files sitting in the project root instead of backups/."""
    found: list[Path] = []
    for pattern in _STRAY_PATTERNS:
        found.extend(p for p in root.glob(pattern) if p.is_file())
    return sorted(set(found))
