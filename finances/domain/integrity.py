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
from enum import Enum
from typing import NamedTuple

from pydantic import BaseModel, ConfigDict

# How many offending ids to carry per finding. Enough to start
# investigating, not enough to bury the summary.
SAMPLE_LIMIT = 10


class Severity(str, Enum):
    ERROR = "error"
    WARNING = "warning"


class IntegrityCheck(NamedTuple):
    """One invariant, expressed as SQL returning offending ids."""

    name: str
    severity: Severity
    description: str
    sql: str


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
            "Both legs of a transfer on one account. A transfer moves money "
            "between accounts; this nets to nothing and hides a real movement."
        ),
        sql="""
            SELECT id FROM transactions
             WHERE transfer_id IN (
                   SELECT transfer_id FROM transactions
                    WHERE transfer_id IS NOT NULL
                    GROUP BY transfer_id
                   HAVING COUNT(DISTINCT account_id) = 1
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
        name="unpaired_p2p_sells",
        severity=Severity.WARNING,
        description=(
            "P2P sells with no bank counterpart. Each is a currency "
            "conversion still counted as an expense. Usually means bank "
            "statements are missing for those dates rather than anything "
            "being broken."
        ),
        sql="""
            SELECT id FROM transactions
             WHERE source_ref LIKE 'p2p:%'
               AND CAST(amount AS REAL) < 0
               AND transfer_id IS NULL
             ORDER BY id
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
