"""Pin the canonicalize script's backup hint to the backups/ folder.

``scripts/`` is not a Python package; loaded by file path via importlib,
same as test_import_bcv_history.py.
"""
from __future__ import annotations

import importlib.util
from pathlib import Path


def _load_script():
    path = (
        Path(__file__).resolve().parent.parent
        / "scripts"
        / "canonicalize_provincial_refs.py"
    )
    spec = importlib.util.spec_from_file_location("canonicalize_provincial_refs", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_backup_hint_targets_backups_folder() -> None:
    mod = _load_script()
    assert mod.BACKUP_HINT.startswith(
        'sqlite3 finances.db ".backup backups/finances-backup-'
    )
