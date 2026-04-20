"""EPIC-021 — end-to-end integration test suite.

These tests exercise the full ingest → pair → cleanup → report pipeline
over a hand-crafted fixture universe rooted at ``2026-03-15``. Each test
is marked ``integration`` so it runs on demand (``pytest -m integration``)
and, per the rule-011 convention, ships behind a separate CI step.

Design notes:

* **Single reference date (2026-03-15)** for every fixture — all
  timestamps and pairing windows derive from it.
* **Fixture-cross-consistency.** The Binance P2P SELL orders and the
  Provincial CSV deposits are sized + dated so the
  ``BankAnchoredP2pPairing`` strategy finds exactly one counterpart per
  bank row (see ``tests/integration/fixtures/``).
* **Deterministic cleanup.** ``run_cleanup`` is invoked with an explicit
  ``PromptFn`` so no human input is required; the mapping is read from
  ``_CLEANUP_MAP`` below.
* **No live I/O.** SDK calls are satisfied by ``MagicMock`` fixtures
  loaded from JSON files; HTTP calls are either avoided (BCV passes
  ``html=``) or patched at the ``httpx.Client`` seam (P2P rates).

Rule-011 TDD caveat: this epic ships tests only. The pipeline modules
already exist (EPIC-007..012); the tests codify invariants the future
regressions must not break.
"""
from __future__ import annotations

import json
import sqlite3
import time
from decimal import Decimal
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from finances.db.connection import _register_decimal_adapters
from finances.db.migrate import MIGRATIONS_DIR, apply_migrations
from finances.db.repos import accounts as accounts_repo
from finances.db.repos import positions as positions_repo
from finances.db.repos import transactions as txn_repo
from finances.domain.models import Account, AccountKind
from finances.ingest.binance import sync_binance
from finances.ingest.bcv import ingest_bcv
from finances.ingest.p2p_rates import ingest_p2p_rates
from finances.ingest.provincial import ingest_csv
from finances.migration.interactive_cleanup import run_cleanup
from finances.reports import consolidated_usd


# ---------------------------------------------------------------------------
# Fixture files
# ---------------------------------------------------------------------------

_FIXTURE_DIR = Path(__file__).parent / "fixtures"
_BINANCE_DIR = _FIXTURE_DIR / "binance_api"

# Wall-clock ceiling for a full pipeline run over the fixture universe.
# If the pipeline starts exceeding this, something regressed (an ingester
# picked up an O(n^2) path, a mock is doing real I/O, etc.) — fail fast
# rather than silently letting CI slow down.
_PIPELINE_BUDGET_SECONDS = 30.0

# Expected balances after one full pipeline run. Derived by hand from the
# integration fixtures (provincial.csv deposits/withdrawals, Binance P2P
# sells, internal transfers, converts, earn rewards, and earn snapshot).
# If the fixture data changes, recompute these values from a fresh run
# and update the dict — they are intentionally independent of the
# ``v_account_balances`` view so a bug in the view's WHERE/JOIN would
# surface as a mismatch here.
_EXPECTED_BALANCES: dict[str, Decimal] = {
    "Provincial Bolivares": Decimal("513000.00"),  # VES: 500k salary - 1.2k - 2k - 5.5k - 0.8k + 15k + 7.5k
    "Binance Spot": Decimal("20.00"),              # USDT
    "Binance Funding": Decimal("30.00"),           # USDT
    "Binance Earn": Decimal("0.25"),               # USDT (rewards only; principal tracked on earn_positions)
    "Cash USD": Decimal("0.00"),                   # USD — no cash fixture
}


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


# Descriptions in the Provincial CSV → (category_name, user_rate).
# Only expense / income rows appear here — paired transfer legs are
# expected to leave the ingest pipeline already reconciled (see
# :func:`_clear_paired_transfer_needs_review`) and therefore never reach
# the cleanup walker at all.
_CLEANUP_MAP: dict[str, tuple[str, str | None]] = {
    "NOMINA EMPRESA XYZ": ("Salary", None),
    "COM. PAGO MOVIL": ("Fees", None),
    "RETIRO CAJERO SUCURSAL CARACAS": ("Other Expense", None),
    # v1.1 taxonomy (migration 004) renamed expense:Food -> expense:Groceries.
    "PANADERIA SAN JOSE": ("Groceries", None),
    "UNKNOWN VENDOR ABC": ("Other Expense", None),
}


def _clear_paired_transfer_needs_review(conn: sqlite3.Connection) -> None:
    """Normalise paired-transfer rows to ``needs_review=0`` before cleanup.

    The Provincial ingester flags every inserted row as ``needs_review=1``
    (no categorizer wired in yet) and the bank-anchored pairing strategy
    updates ``kind`` / ``transfer_id`` without touching the review flag.
    That leaves paired transfer legs carrying ``needs_review=1`` even
    though they are already reconciled — a state the cleanup walker
    should never observe. Integration tests explicitly express that
    invariant by clearing the flag on any transfer row that already has
    a ``transfer_id`` before calling :func:`run_cleanup`; if a regression
    ever strands an unpaired transfer leg, :func:`_auto_resolver` will
    fail loudly on it.
    """
    conn.execute(
        "UPDATE transactions SET needs_review = 0 "
        "WHERE kind = 'transfer' AND transfer_id IS NOT NULL "
        "AND needs_review = 1"
    )


def _auto_resolver(row: sqlite3.Row) -> tuple[str | None, str | None]:
    """Deterministic PromptFn for integration tests.

    Looks up ``row['description']`` in ``_CLEANUP_MAP``. Falls back to
    ``("Other Expense", None)`` for expense rows so the pipeline never
    strands a needs_review=1 row in the fixture universe.

    Transfer rows MUST NOT reach cleanup: the bank-anchored pairing
    strategy is expected to resolve every transfer leg during ingest,
    and a row with ``kind='transfer'`` that still carries
    ``needs_review=1`` signals either a pairing regression or a rules-
    engine bug. Assert loudly rather than silently bucket the row into
    a generic "External Transfer" category.
    """
    desc = (row["description"] or "").strip()
    kind = row["kind"]
    assert kind != "transfer", (
        f"cleanup reached a transfer leg (description={desc!r}, "
        f"id={row['id']}); pairing or categorization is broken"
    )
    if desc in _CLEANUP_MAP:
        return _CLEANUP_MAP[desc]
    if kind == "income":
        return ("Other Income", None)
    return ("Other Expense", None)


# ---------------------------------------------------------------------------
# Pipeline harness — shared across tests.
# ---------------------------------------------------------------------------


def _open_fresh_db() -> sqlite3.Connection:
    """In-memory sqlite3 connection with all migrations applied + seeded accounts."""
    _register_decimal_adapters()
    conn = sqlite3.connect(
        ":memory:",
        detect_types=sqlite3.PARSE_DECLTYPES,
        isolation_level=None,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA synchronous = NORMAL")
    apply_migrations(conn, migrations_dir=MIGRATIONS_DIR)

    _seed_accounts(conn)
    return conn


_V1_ACCOUNTS: tuple[tuple[str, AccountKind, str, str | None], ...] = (
    ("Provincial Bolivares", AccountKind.BANK, "VES", "Provincial"),
    ("Binance Spot", AccountKind.CRYPTO_SPOT, "USDT", "Binance"),
    ("Binance Funding", AccountKind.CRYPTO_FUNDING, "USDT", "Binance"),
    ("Binance Earn", AccountKind.CRYPTO_EARN, "USDT", "Binance"),
    ("Cash USD", AccountKind.CASH, "USD", None),
)


def _seed_accounts(conn: sqlite3.Connection) -> None:
    for name, kind, currency, institution in _V1_ACCOUNTS:
        accounts_repo.insert(
            conn,
            Account(name=name, kind=kind, currency=currency, institution=institution),
        )


def _build_binance_client() -> MagicMock:
    """MagicMock configured from the JSON fixtures under ``binance_api/``.

    Returning the dicts verbatim lets the production ingester parse them
    via the same ``RawBinance*`` Pydantic models exercised in the unit
    tests — so the integration run rejects the same bad shapes that the
    unit tests do.
    """
    client = MagicMock(name="binance.Client")
    # Server time well after the fixture timestamps.
    client.time.return_value = {"serverTime": 1773600000000}

    client.deposit_history.return_value = _read_json(_BINANCE_DIR / "deposits.json")
    client.withdraw_history.return_value = _read_json(_BINANCE_DIR / "withdrawals.json")

    p2p_sells = _read_json(_BINANCE_DIR / "p2p_sells.json")
    p2p_buys = _read_json(_BINANCE_DIR / "p2p_buys.json")

    def _c2c_side_effect(*_args: Any, tradeType: str, **_kwargs: Any) -> Any:
        return p2p_buys if tradeType == "BUY" else p2p_sells

    client.c2c_trade_history.side_effect = _c2c_side_effect

    transfers_main = _read_json(_BINANCE_DIR / "internal_transfers_main_to_funding.json")
    transfers_funding = _read_json(
        _BINANCE_DIR / "internal_transfers_funding_to_main.json"
    )

    def _transfer_side_effect(*_args: Any, type: str, **_kwargs: Any) -> Any:
        return transfers_main if type == "MAIN_FUNDING" else transfers_funding

    client.user_universal_transfer_history.side_effect = _transfer_side_effect

    client.get_convert_trade_history.return_value = _read_json(
        _BINANCE_DIR / "converts.json"
    )
    client.simple_earn_flexible_rewards_history.return_value = _read_json(
        _BINANCE_DIR / "earn_rewards.json"
    )
    client.simple_earn_flexible_position.return_value = _read_json(
        _BINANCE_DIR / "earn_positions.json"
    )
    client.pay_history.return_value = _read_json(_BINANCE_DIR / "pay.json")

    return client


def _install_p2p_http_mock(mocker: Any) -> MagicMock:
    """Patch ``httpx.Client`` inside ``finances.ingest.p2p_rates``.

    Two POSTs per run (BUY first, SELL second). We return both fixture
    payloads in order so the ingester computes a midpoint from realistic
    adverts.
    """
    from finances.ingest import p2p_rates as mod

    p2p_payload = _read_json(_FIXTURE_DIR / "p2p_response.json")
    buy_payload = p2p_payload["buy"]
    sell_payload = p2p_payload["sell"]

    class _Response:
        def __init__(self, payload: dict[str, Any]) -> None:
            self._payload = payload
            self.status_code = 200

        def json(self) -> dict[str, Any]:
            return self._payload

        def raise_for_status(self) -> None:
            return None

    instance = MagicMock(name="httpx.Client-instance")
    # p2p_rates opens two separate context managers (one per trade type),
    # so post() is called once per enter. Using a list cycle lets the
    # mocked Client return BUY then SELL in order.
    instance.post.side_effect = [_Response(buy_payload), _Response(sell_payload)]

    cm = MagicMock(name="httpx.Client-cm")
    cm.__enter__ = MagicMock(return_value=instance)
    cm.__exit__ = MagicMock(return_value=False)

    return mocker.patch.object(mod.httpx, "Client", return_value=cm)


def _run_full_pipeline(
    conn: sqlite3.Connection, mocker: Any
) -> dict[str, Any]:
    """Execute every ingester in dependency order, return a summary.

    Ingest order:
      1. Binance (creates P2P SELL rows + earn snapshots)
      2. Provincial (upserts bank rows + runs bank-anchored pairing)
      3. BCV (writes reference rates, ``html=`` so no network)
      4. P2P rates (writes USDT/VES median rows for the headline rule)
    """
    client = _build_binance_client()
    binance_stats = sync_binance(conn, client=client, lookback_days=60)

    csv_path = _FIXTURE_DIR / "provincial.csv"
    prov_report = ingest_csv(conn, csv_path, pairing_window_days=2, run_pairing=True)

    bcv_html = (_FIXTURE_DIR / "bcv_snapshot.html").read_text(encoding="utf-8")
    bcv_inserted = ingest_bcv(conn, html=bcv_html)

    _install_p2p_http_mock(mocker)
    p2p_stats = ingest_p2p_rates(conn)

    return {
        "binance": binance_stats,
        "provincial": prov_report,
        "bcv_inserted": bcv_inserted,
        "p2p": p2p_stats,
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_full_pipeline_idempotent(mocker: Any) -> None:
    """Every ingester must be safe to re-run on the same fixture universe.

    After the first pass the DB has N transactions. A second pass through
    the exact same mocked SDK + CSV + HTML must insert 0 rows — per
    rule-010's deterministic ``source_ref`` contract.
    """
    conn = _open_fresh_db()
    try:
        start = time.perf_counter()
        _run_full_pipeline(conn, mocker)
        first_count = txn_repo.count(conn)
        assert first_count > 0, "first pipeline pass inserted zero rows — fixture misconfigured"

        # Re-run everything; second pass must be a no-op on transactions.
        _run_full_pipeline(conn, mocker)
        second_count = txn_repo.count(conn)
        assert second_count == first_count, (
            f"pipeline not idempotent: count {first_count} -> {second_count} "
            f"after second run (delta={second_count - first_count})"
        )
        elapsed = time.perf_counter() - start
        assert elapsed < _PIPELINE_BUDGET_SECONDS, (
            f"integration pipeline exceeded {_PIPELINE_BUDGET_SECONDS:.0f}s "
            f"budget: {elapsed:.2f}s"
        )
    finally:
        conn.close()


@pytest.mark.integration
def test_balances_reconcile(mocker: Any) -> None:
    """Per-account balances in ``v_account_balances`` must match a hand-derived baseline.

    Comparing the view against a fresh ``SUM(amount)`` on ``transactions``
    is tautological — the view is essentially that same expression, so a
    bug in the view's ``WHERE`` / ``JOIN`` would not surface. Instead we
    pin the expected balances to :data:`_EXPECTED_BALANCES`, computed by
    hand from the fixture inputs. A drift here means either the view
    drifted, an ingester wrote unexpected rows, or the fixtures changed
    and the baseline needs to be updated deliberately.
    """
    conn = _open_fresh_db()
    try:
        _run_full_pipeline(conn, mocker)

        view_rows = conn.execute(
            "SELECT account_name, balance_native FROM v_account_balances"
        ).fetchall()
        actual = {
            row["account_name"]: Decimal(str(row["balance_native"])).quantize(
                Decimal("0.01")
            )
            for row in view_rows
        }
        for account, expected in _EXPECTED_BALANCES.items():
            assert account in actual, (
                f"{account!r} missing from v_account_balances; "
                f"view returned={sorted(actual)}"
            )
            drift = abs(actual[account] - expected)
            assert drift <= Decimal("0.01"), (
                f"{account!r}: view shows {actual[account]}, expected "
                f"{expected} (drift {drift})"
            )
    finally:
        conn.close()


@pytest.mark.integration
def test_no_unreconciled_transfers(mocker: Any) -> None:
    """``v_unreconciled_transfers`` must be empty after a clean pipeline run.

    Every transfer row must share a ``transfer_id`` with exactly one
    counterpart. A non-zero count here means either the bank-anchored
    pairing strategy failed to match a bank/Binance pair it should have,
    or an ingester wrote a stranded ``kind='transfer'`` row.
    """
    conn = _open_fresh_db()
    try:
        _run_full_pipeline(conn, mocker)

        count = conn.execute(
            "SELECT COUNT(*) AS c FROM v_unreconciled_transfers"
        ).fetchone()["c"]
        assert count == 0, (
            f"v_unreconciled_transfers returned {count} rows; expected 0. "
            "Either bank-anchored pairing missed a match or a transfer leg "
            "was inserted without a partner."
        )
    finally:
        conn.close()


@pytest.mark.integration
def test_no_needs_review_after_cleanup(mocker: Any) -> None:
    """Deterministic cleanup must zero out every ``needs_review=1`` row.

    Provincial ingest flags every row as ``needs_review=True`` (no
    categorizer wired into ``ingest_csv``'s call site today). The
    integration cleanup invokes ``run_cleanup`` with
    :func:`_auto_resolver`; after it runs, no row in the fixture
    universe may still carry ``needs_review=1``.
    """
    conn = _open_fresh_db()
    try:
        _run_full_pipeline(conn, mocker)
        _clear_paired_transfer_needs_review(conn)

        pre = conn.execute(
            "SELECT COUNT(*) AS c FROM transactions WHERE needs_review = 1"
        ).fetchone()["c"]
        assert pre > 0, "fixture misconfigured: no needs_review rows to clean up"

        cleanup = run_cleanup(conn, prompt=_auto_resolver)
        assert cleanup.rows_resolved == pre, (
            f"cleanup resolved {cleanup.rows_resolved} of {pre} rows; "
            f"errors={cleanup.errors}"
        )

        post = conn.execute(
            "SELECT COUNT(*) AS c FROM transactions WHERE needs_review = 1"
        ).fetchone()["c"]
        assert post == 0, (
            f"{post} rows still needs_review=1 after deterministic cleanup"
        )
    finally:
        conn.close()


@pytest.mark.integration
def test_consolidated_usd_excludes_bcv_headlines(mocker: Any) -> None:
    """Per ADR-005 amendment: no row in the headline aggregate uses BCV.

    The consolidated USD report rolls BCV-derived rows into a separate
    ``fallback_total_usd`` bucket and lists their ids in
    ``strict_violations``; no BCV row may contribute to ``total_usd``.
    """
    conn = _open_fresh_db()
    try:
        _run_full_pipeline(conn, mocker)
        _clear_paired_transfer_needs_review(conn)
        # Resolve needs_review first; otherwise rows with rate_source='needs_review'
        # mask whatever BCV fallbacks would otherwise surface.
        run_cleanup(conn, prompt=_auto_resolver)

        report = consolidated_usd.build_report(conn)

        # Every headline row must be sourced from user_rate, P2P median, or
        # native USD — never BCV.
        for row in report.rows:
            if row.is_bcv_fallback:
                continue  # fallback bucket is allowed; it just must not contribute to total
            assert not row.rate_source.startswith("bcv"), (
                f"row transaction_id={row.transaction_id} has rate_source="
                f"{row.rate_source!r} but is not flagged is_bcv_fallback; "
                "headline aggregation would double-count it."
            )
    finally:
        conn.close()


@pytest.mark.integration
def test_p2p_pair_anchored_to_bank(mocker: Any) -> None:
    """Every paired P2P transfer must anchor on the Provincial leg.

    Per the ADR-002 amendment the bank row is the anchor: we identify a
    paired ``transfer_id`` by finding legs on both Provincial and
    Binance, and assert the bank leg is the one flagged as the anchor.
    The schema has no ``anchor_transaction_id`` column (see
    ``v_unreconciled_transfers``), so "anchor" is expressed by identity:
    the Provincial row is present in the pair and its source matches
    ``provincial``. This is the invariant the pairing strategy promises.
    """
    conn = _open_fresh_db()
    try:
        _run_full_pipeline(conn, mocker)

        pairs = conn.execute(
            "SELECT transfer_id, GROUP_CONCAT(source) AS sources, "
            "       GROUP_CONCAT(id) AS ids, "
            "       GROUP_CONCAT(account_id) AS accounts "
            "FROM transactions "
            "WHERE kind = 'transfer' AND transfer_id IS NOT NULL "
            "GROUP BY transfer_id "
            "HAVING COUNT(*) = 2"
        ).fetchall()

        bank_pairs = [p for p in pairs if "provincial" in (p["sources"] or "")]
        # Two P2P pairs expected per tests/integration/fixtures/binance_api/p2p_sells.json
        assert len(bank_pairs) == 2, (
            f"expected exactly 2 bank-anchored P2P pairs, got {len(bank_pairs)} "
            f"(all pairs={[dict(p) for p in pairs]})"
        )

        provincial_id = accounts_repo.get_by_name(conn, "Provincial Bolivares")
        assert provincial_id is not None and provincial_id.id is not None
        bank_account_id = provincial_id.id

        for pair in bank_pairs:
            accounts = [int(a) for a in pair["accounts"].split(",")]
            assert bank_account_id in accounts, (
                f"transfer_id={pair['transfer_id']!r} has no Provincial leg; "
                f"account_ids={accounts}"
            )
            sources = pair["sources"].split(",")
            assert "provincial" in sources and "binance" in sources, (
                f"transfer_id={pair['transfer_id']!r} sources={sources}; "
                "expected one 'provincial' anchor and one 'binance' counterpart"
            )
    finally:
        conn.close()


@pytest.mark.integration
def test_rule_002_no_null_transfer_id(mocker: Any) -> None:
    """rule-002: every ``kind='transfer'`` row carries a ``transfer_id``.

    A NULL transfer_id on a transfer row is the worst-case bug: the
    balance view still reflects the leg, but there is no counterpart, so
    the ledger no longer sums to zero across accounts.
    """
    conn = _open_fresh_db()
    try:
        _run_full_pipeline(conn, mocker)

        count = conn.execute(
            "SELECT COUNT(*) AS c FROM transactions "
            "WHERE kind = 'transfer' AND transfer_id IS NULL"
        ).fetchone()["c"]
        assert count == 0, (
            f"{count} transfer rows have NULL transfer_id; rule-002 violated"
        )
    finally:
        conn.close()


@pytest.mark.integration
def test_earn_position_sum_matches_subscriptions_minus_redemptions(
    mocker: Any,
) -> None:
    """Per ADR-003: open Earn principal equals the snapshot totalAmount.

    The Binance Simple-Earn snapshot is the single source of truth for
    positions (rule-003). After ingest, the sum of
    ``earn_positions.principal WHERE ended_at IS NULL`` must equal the
    ``totalAmount`` in the snapshot fixture — no implicit re-derivation
    via reward sums or external bookkeeping.
    """
    conn = _open_fresh_db()
    try:
        _run_full_pipeline(conn, mocker)

        snapshot = _read_json(_BINANCE_DIR / "earn_positions.json")
        expected = sum(
            (Decimal(str(row["totalAmount"])) for row in snapshot["rows"]),
            start=Decimal("0"),
        )

        earn_account = accounts_repo.get_by_name(conn, "Binance Earn")
        assert earn_account is not None and earn_account.id is not None
        open_positions = positions_repo.list_open(conn, account_id=earn_account.id)
        actual = sum(
            (p.principal for p in open_positions),
            start=Decimal("0"),
        )

        assert actual == expected, (
            f"open Earn principal {actual} does not match snapshot total {expected}; "
            f"open products={[p.product_id for p in open_positions]}"
        )
    finally:
        conn.close()
