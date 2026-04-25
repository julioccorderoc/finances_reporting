"""Per-source guarantee that every ingest writes one ``import_runs`` row.

Rule-007 / ADR-007: every ingest run MUST start an ``import_runs`` row at
entry and finish it (status='success' on success, status='error' with an
``error`` message on failure) at exit. ``finances/ingest/bcv.py`` is the
reference implementation. These tests pin the same behaviour on every
other ingest entry point.

Use the existing ``db_conn`` fixture from ``tests/conftest.py`` (a
file-backed, migrated sqlite3 connection that closes on teardown).

Each test below is owned by a single ingest module; tests in this file
are intentionally short and isolated so the contract is obvious.
"""
from __future__ import annotations

import pytest


# === BEGIN binance ============================================================
# Owned by Task 2. Asserts ``finances.ingest.binance.sync_binance`` writes
# one row to ``import_runs`` with status='success' on the no-op happy path.
# === END binance ==============================================================


# === BEGIN provincial =========================================================
# Owned by Task 3. Asserts ``finances.ingest.provincial.ingest_csv`` writes
# one row to ``import_runs`` with status='success' on a minimal valid CSV.
# === END provincial ===========================================================


# === BEGIN p2p_rates ==========================================================
# Owned by Task 4. Asserts ``finances.ingest.p2p_rates.ingest_p2p_rates``
# writes one row to ``import_runs`` with status='success'.
# === END p2p_rates ============================================================


# === BEGIN backfill ===========================================================
# Owned by Task 5. Asserts ``finances.migration.backfill.run_backfill``
# writes one row to ``import_runs`` with source='backfill' and
# status='success' on an empty input directory.
# === END backfill =============================================================
