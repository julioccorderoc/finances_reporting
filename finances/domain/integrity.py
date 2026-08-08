"""Ledger integrity checks — the invariants, executable.

CLAUDE.md states a set of invariants the whole system assumes ("transfers:
two rows, shared transfer_id, signed, sum=0", "transactions.source_ref
always set", and so on). They were prose. Tests enforced some of them
incidentally, against freshly seeded fixtures.

That leaves a gap nothing else covers: a defect that corrupts *real* rows
is invisible to a test suite that only ever sees fixtures. The paired-
transfer review flag proved it — fifteen rows wrong for three months,
under a fully green suite, found only because a human opened the right
screen.

This module states each invariant once, as SQL that returns the offending
transaction ids, so the same check can run against fixtures in CI and
against the production ledger from ``finances doctor``.

Severity is the difference between "this is broken" and "this is work you
have not done yet":

* ``ERROR`` — an invariant is violated. The data is wrong.
* ``WARNING`` — the data is consistent but something is outstanding, e.g.
  P2P sells with no bank counterpart. Warnings never fail a run; a
  backlog is not a defect.

Read-only. Nothing here mutates the database.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from decimal import Decimal
from enum import Enum
from typing import NamedTuple

from pydantic import BaseModel, ConfigDict

# How many offending ids to carry per finding. Enough to start
# investigating, not enough to bury the summary.
SAMPLE_LIMIT = 10

# How far a cross-currency transfer pair may fail to net to zero in USD
# before it is worth the owner's attention. Priced through the resolver, the
# live ledger's 95 cross-currency pairs net to $0.72 in total and the worst
# single pair is $3.82 off, so this is quiet on real data — while the
# mis-clicked pair that motivated the check was out by $200.
TRANSFER_USD_TOLERANCE = Decimal("50")


class Severity(str, Enum):
    ERROR = "error"
    WARNING = "warning"


class IntegrityCheck(NamedTuple):
    """One invariant, returning the offending transaction ids.

    Most invariants are a single SELECT and stay that way — SQL keeps them
    short enough to read as a statement of the rule. ``fn`` exists for the
    one class that cannot be: anything needing the rate resolver, because
    rule-005 forbids a second USD chain and SQL cannot call the first one.
    Exactly one of ``sql`` / ``fn`` is set.
    """

    name: str
    severity: Severity
    description: str
    sql: str | None = None
    fn: Callable[[sqlite3.Connection], list[int]] | None = None


class IntegrityFinding(BaseModel):
    """A check that found something."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    check: str
    severity: Severity
    description: str
    count: int
    sample_ids: list[int]


class IntegrityReport(BaseModel):
    """Outcome of a full pass."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    findings: list[IntegrityFinding]
    checks_run: int

    @property
    def has_errors(self) -> bool:
        return any(f.severity is Severity.ERROR for f in self.findings)

    @property
    def ok(self) -> bool:
        """True when no invariant is violated. Warnings do not count."""
        return not self.has_errors


# ---------------------------------------------------------------------------
# The one invariant that needs the resolver
# ---------------------------------------------------------------------------


def _transfer_usd_imbalance(conn: sqlite3.Connection) -> list[int]:
    """Transfer pairs that do not net to zero once both legs are in USD.

    ``transfer_same_currency_imbalance`` exempts cross-currency pairs,
    because before ADR-013 there was no honest way to price a bolívar leg.
    There is now, and the exemption is precisely the hole a mis-clicked
    manual pairing walks through: the review confirmed a 2 261 Bs deposit
    could be paired with a 200.44 USDT sell eight months apart, and both
    rows then dropped out of every income and expense aggregate for good.

    Pricing goes through :func:`finances.domain.money.to_usd` — the same
    single implementation the reports use — so this check can never disagree
    with them about what a leg is worth. A pair with a leg the resolver
    cannot price is skipped rather than reported: missing rate data is a
    different problem with a different remedy, and it already has its own
    surface.
    """
    # Imported here rather than at module scope: money imports the resolver,
    # which imports the rates repo, and integrity is imported by the CLI at
    # startup. Keeping it local holds the import graph flat.
    from finances.db.repos.transactions import _row_to_transaction
    from finances.domain import money

    rows = conn.execute(
        """
        SELECT id, account_id, occurred_at, kind, amount, currency, description,
               category_id, transfer_id, user_rate, source, source_ref,
               needs_review, parked, notes
        FROM transactions
        WHERE kind = 'transfer' AND transfer_id IS NOT NULL
        ORDER BY transfer_id, id
        """
    ).fetchall()

    pairs: dict[str, list] = {}
    for row in rows:
        pairs.setdefault(row["transfer_id"], []).append(_row_to_transaction(row))

    offenders: list[int] = []
    for legs in pairs.values():
        # Leg-count problems are their own check; do not report them twice.
        if len(legs) != 2:
            continue
        if legs[0].currency == legs[1].currency:
            continue  # covered by transfer_same_currency_imbalance
        values = [money.to_usd(conn, leg)[0] for leg in legs]
        if any(v is None for v in values):
            continue
        if abs(values[0] + values[1]) > TRANSFER_USD_TOLERANCE:
            offenders.extend(leg.id for leg in legs if leg.id is not None)

    return sorted(offenders)


# ---------------------------------------------------------------------------
# The invariants
# ---------------------------------------------------------------------------

CHECKS: tuple[IntegrityCheck, ...] = (
    IntegrityCheck(
        name="transfer_missing_transfer_id",
        severity=Severity.ERROR,
        description=(
            "kind='transfer' rows with no transfer_id — orphan half-transfers "
            "(rule-002). Money left an account with nothing recording where it "
            "landed."
        ),
        sql="""
            SELECT id FROM transactions
             WHERE kind = 'transfer' AND transfer_id IS NULL
             ORDER BY id
        """,
    ),
    IntegrityCheck(
        name="transfer_id_on_wrong_kind",
        severity=Severity.ERROR,
        description=(
            "Rows carrying a transfer_id whose kind is not 'transfer'. They "
            "are part of a pair but will be counted as income or expense."
        ),
        sql="""
            SELECT id FROM transactions
             WHERE transfer_id IS NOT NULL AND kind <> 'transfer'
             ORDER BY id
        """,
    ),
    IntegrityCheck(
        name="transfer_leg_count",
        severity=Severity.ERROR,
        description=(
            "Transfers without exactly two legs. Double-entry requires a pair "
            "(rule-002); anything else means a leg was lost or duplicated."
        ),
        sql="""
            SELECT id FROM transactions
             WHERE transfer_id IN (
                   SELECT transfer_id FROM transactions
                    WHERE transfer_id IS NOT NULL
                    GROUP BY transfer_id
                   HAVING COUNT(*) <> 2
             )
             ORDER BY id
        """,
    ),
    IntegrityCheck(
        name="transfer_legs_same_account",
        severity=Severity.ERROR,
        description=(
            "Both legs of a transfer in the same position — one account and "
            "one currency. Nothing moved, and a real movement is hidden. A "
            "conversion (one account, two currencies) is not this: it moves "
            "value between two positions the owner holds, and is the case "
            "the ledger previously could not express at all."
        ),
        sql="""
            SELECT id FROM transactions
             WHERE transfer_id IN (
                   SELECT transfer_id FROM transactions
                    WHERE transfer_id IS NOT NULL
                    GROUP BY transfer_id
                   HAVING COUNT(DISTINCT account_id) = 1
                      AND COUNT(DISTINCT currency) = 1
             )
             ORDER BY id
        """,
    ),
    IntegrityCheck(
        name="transfer_same_currency_imbalance",
        severity=Severity.ERROR,
        description=(
            "Same-currency transfer legs that do not net to zero within a "
            "cent. Cross-currency pairs are exempt — they net only after rate "
            "conversion."
        ),
        sql="""
            SELECT id FROM transactions
             WHERE transfer_id IN (
                   SELECT transfer_id FROM transactions
                    WHERE transfer_id IS NOT NULL
                    GROUP BY transfer_id
                   HAVING COUNT(DISTINCT currency) = 1
                      AND ABS(SUM(CAST(amount AS REAL))) > 0.01
             )
             ORDER BY id
        """,
    ),
    IntegrityCheck(
        name="paired_transfer_needs_review",
        severity=Severity.ERROR,
        description=(
            "Fully paired transfers still flagged needs_review. Pairing "
            "answers the only question that flag asks, and triage already "
            "treats transfers as resolved — so every other surface reading "
            "the raw column queues finished work."
        ),
        sql="""
            SELECT id FROM transactions
             WHERE kind = 'transfer'
               AND transfer_id IS NOT NULL
               AND needs_review = 1
             ORDER BY id
        """,
    ),
    IntegrityCheck(
        name="missing_source_ref",
        severity=Severity.ERROR,
        description=(
            "Transactions with no source_ref. Dedup is keyed on "
            "(source, source_ref) (rule-010), so a row without one will "
            "duplicate itself on the next ingest."
        ),
        sql="""
            SELECT id FROM transactions
             WHERE source_ref IS NULL OR TRIM(source_ref) = ''
             ORDER BY id
        """,
    ),
    IntegrityCheck(
        name="duplicate_source_ref",
        severity=Severity.ERROR,
        description=(
            "Repeated (source, source_ref) pairs. The schema has a UNIQUE "
            "constraint, so a hit here means it was dropped or bypassed."
        ),
        sql="""
            SELECT id FROM transactions
             WHERE (source, source_ref) IN (
                   SELECT source, source_ref FROM transactions
                    WHERE source_ref IS NOT NULL
                    GROUP BY source, source_ref
                   HAVING COUNT(*) > 1
             )
             ORDER BY id
        """,
    ),
    IntegrityCheck(
        name="convert_leg_without_counterpart",
        severity=Severity.WARNING,
        description=(
            "Binance convert legs missing their other half. A conversion "
            "writes two rows sharing one order id — asset out, asset in. A "
            "lone outgoing leg reads as money spent, because reports count "
            "every non-transfer row. Legacy backfill hashed each leg "
            "separately, so its halves never shared an id. Run "
            "`finances reconcile converts` to pair what can be paired."
        ),
        # Strip the trailing ':from' / ':to' to recover the order id, then
        # require both halves to exist for it. Live API rows share a real
        # order id; backfilled ones hashed per-leg and so can never match —
        # which is exactly the finding.
        #
        # Legs that already carry a transfer_id are excluded. Pairing does
        # not rewrite source_ref and must not — it is the dedup key
        # (rule-010) — so a repaired legacy pair keeps its mismatched keys
        # for good. The question this check asks is whether a lone leg reads
        # as money spent, and a paired leg does not: reports drop it by kind.
        # Without this clause the check would report finished work forever.
        sql="""
            WITH legs AS (
                SELECT id,
                       CASE
                         WHEN source_ref LIKE '%:from'
                           THEN SUBSTR(source_ref, 1, LENGTH(source_ref) - 5)
                         WHEN source_ref LIKE '%:to'
                           THEN SUBSTR(source_ref, 1, LENGTH(source_ref) - 3)
                         ELSE source_ref
                       END AS order_key,
                       CASE
                         WHEN source_ref LIKE '%:from' THEN 'from'
                         WHEN source_ref LIKE '%:to'   THEN 'to'
                         ELSE 'unknown'
                       END AS side
                  FROM transactions
                 WHERE source_ref LIKE 'convert:%'
                   AND transfer_id IS NULL
            )
            SELECT id FROM legs
             WHERE order_key IN (
                   SELECT order_key FROM legs
                    GROUP BY order_key
                   HAVING COUNT(DISTINCT side) < 2
             )
             ORDER BY id
        """,
    ),
    IntegrityCheck(
        name="negative_asset_balance",
        severity=Severity.ERROR,
        description=(
            "An account holding a negative amount of an asset. A balance below "
            "zero means an arrival was never recorded, not that money was "
            "overspent — you cannot sell what you never received. Checked per "
            "(account, currency) so a healthy balance in one asset cannot "
            "mask a broken one in another."
        ),
        sql="""
            SELECT id FROM transactions
             WHERE (account_id, currency) IN (
                   SELECT account_id, currency FROM transactions
                    GROUP BY account_id, currency
                   HAVING SUM(CAST(amount AS REAL)) < -0.01
             )
               AND id IN (
                   SELECT MIN(id) FROM transactions
                    GROUP BY account_id, currency
             )
             ORDER BY id
        """,
    ),
    IntegrityCheck(
        name="transfer_usd_imbalance",
        severity=Severity.ERROR,
        description=(
            "Cross-currency transfer pairs that do not net to zero once both "
            "legs are priced in USD. The same-currency check exempts these, "
            "which is the hole a mis-clicked manual pairing walks through — "
            "and a bad pair removes both rows from income and expense "
            "permanently."
        ),
        fn=_transfer_usd_imbalance,
    ),
    IntegrityCheck(
        name="category_kind_mismatch",
        severity=Severity.WARNING,
        description=(
            "Rows whose category belongs to a contradicting kind — an "
            "expense filed under an income category, or the reverse. A "
            "transfer-kind category is deliberately not counted: on an "
            "income or expense row it is how money movement is declared, and "
            "the reports act on it."
        ),
        sql="""
            SELECT t.id FROM transactions t
              JOIN categories c ON c.id = t.category_id
             WHERE c.kind <> t.kind
               AND c.kind <> 'transfer'
             ORDER BY t.id
        """,
    ),
    IntegrityCheck(
        name="uncategorized_not_flagged",
        severity=Severity.WARNING,
        description=(
            "Rows with no category that are not flagged needs_review. The "
            "flag is derived from rate resolution alone, so an uncategorised "
            "row with a resolvable rate looks finished to every surface that "
            "reads the column. The viewer's triage queue asks about category "
            "separately; nothing else does."
        ),
        sql="""
            SELECT id FROM transactions
             WHERE category_id IS NULL
               AND needs_review = 0
               AND parked = 0
               AND kind NOT IN ('transfer', 'adjustment')
             ORDER BY id
        """,
    ),
    IntegrityCheck(
        name="legacy_ref_superseded",
        severity=Severity.ERROR,
        description=(
            "One event on the books twice under two source_ref schemes. The "
            "backfill hashes what the legacy CSV gave no id for "
            "(<prefix>:hash:<sha>); the live sync uses the real external id. "
            "Dedup is keyed on (source, source_ref) (rule-010), so UNIQUE "
            "cannot see they are the same event. Run "
            "`finances reconcile legacy-dupes`."
        ),
        sql="""
            SELECT legacy.id
              FROM transactions AS legacy
              JOIN transactions AS native
                ON native.source = legacy.source
               AND native.currency = legacy.currency
               AND native.id <> legacy.id
               AND instr(native.source_ref, ':hash:') = 0
               AND substr(native.source_ref, 1, instr(native.source_ref, ':'))
                 = substr(legacy.source_ref, 1, instr(legacy.source_ref, ':'))
               AND ABS(julianday(substr(native.occurred_at, 1, 10))
                     - julianday(substr(legacy.occurred_at, 1, 10))) <= 1
               AND ABS(ABS(CAST(native.amount AS REAL))
                     - ABS(CAST(legacy.amount AS REAL)))
                 <= 0.01 * ABS(CAST(legacy.amount AS REAL))
               AND (CAST(native.amount AS REAL) > 0)
                 = (CAST(legacy.amount AS REAL) > 0)
             WHERE instr(legacy.source_ref, ':hash:') > 0
             ORDER BY legacy.id
        """,
    ),
    IntegrityCheck(
        name="unpaired_p2p_sells",
        severity=Severity.WARNING,
        description=(
            "P2P sells that could still find a bank counterpart and have "
            "not. Each is a currency conversion still counted as an "
            "expense. Sells that no deposit could ever match are excluded: "
            "those predating the first bank statement the ledger holds, and "
            "those priced in a fiat no bank account is denominated in."
        ),
        sql="""
            SELECT sell.id FROM transactions AS sell
             WHERE sell.source_ref LIKE 'p2p:%'
               AND CAST(sell.amount AS REAL) < 0
               AND sell.transfer_id IS NULL
               -- Statements start when they start. A sell from before the
               -- earliest one has no deposit to pair with and never will,
               -- so it is history, not a backlog item.
               AND substr(sell.occurred_at, 1, 10) >= COALESCE(
                   (SELECT MIN(substr(bank.occurred_at, 1, 10))
                      FROM transactions AS bank
                      JOIN accounts AS a ON a.id = bank.account_id
                     WHERE a.kind = 'bank'),
                   substr(sell.occurred_at, 1, 10)
               )
               -- Reject only a KNOWN fiat mismatch, mirroring
               -- transfers._fiat_is_compatible. The ingest writes
               -- '@ <rate> <FIAT> (order ...)'; a description without that
               -- shape never had its denomination recorded, and those are
               -- the rows that most need a human.
               AND (
                   sell.description IS NULL
                   OR sell.description NOT LIKE '%(order%'
                   OR EXISTS (
                       SELECT 1 FROM accounts AS a
                        WHERE a.kind = 'bank'
                          AND sell.description
                              LIKE '% ' || a.currency || ' (order%'
                   )
               )
             ORDER BY sell.id
        """,
    ),
)


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def run_checks(conn: sqlite3.Connection) -> IntegrityReport:
    """Run every check; return only those that found something.

    Findings come back errors first, then warnings, each group in check
    declaration order — so the summary reads worst-first.
    """
    findings: list[IntegrityFinding] = []

    for check in CHECKS:
        if check.fn is not None:
            ids = check.fn(conn)
        else:
            assert check.sql is not None, f"{check.name} has neither sql nor fn"
            ids = [int(row[0]) for row in conn.execute(check.sql)]
        if not ids:
            continue
        findings.append(
            IntegrityFinding(
                check=check.name,
                severity=check.severity,
                description=check.description,
                count=len(ids),
                sample_ids=ids[:SAMPLE_LIMIT],
            )
        )

    findings.sort(key=lambda f: 0 if f.severity is Severity.ERROR else 1)
    return IntegrityReport(findings=findings, checks_run=len(CHECKS))


def render_report(report: IntegrityReport) -> str:
    """Human-readable summary for the CLI."""
    if not report.findings:
        return f"ledger ok — {report.checks_run} checks, no findings"

    errors = sum(1 for f in report.findings if f.severity is Severity.ERROR)
    warnings = len(report.findings) - errors

    lines = [
        f"ledger: {errors} error(s), {warnings} warning(s) "
        f"across {report.checks_run} checks"
    ]
    for finding in report.findings:
        marker = "ERROR" if finding.severity is Severity.ERROR else "warn "
        ids = ", ".join(str(i) for i in finding.sample_ids)
        more = "" if finding.count <= len(finding.sample_ids) else ", …"
        lines.append(f"  [{marker}] {finding.check}: {finding.count} row(s)")
        lines.append(f"          {finding.description}")
        lines.append(f"          ids: {ids}{more}")
    return "\n".join(lines)


__all__ = [
    "CHECKS",
    "IntegrityCheck",
    "IntegrityFinding",
    "IntegrityReport",
    "SAMPLE_LIMIT",
    "Severity",
    "render_report",
    "run_checks",
]
