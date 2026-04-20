"""Reconciliation engine primitives (EPIC-006).

This module defines the strategy-agnostic contract used by the
reconciliation runner. A strategy proposes pair matches via ``match()``
and performs the pair creation side-effect via ``apply()``. The runner
tallies successes and captures failures as strings.

Concrete strategies (e.g. BankAnchoredP2pPairing) live in
``finances.domain.transfers``.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable


@dataclass(frozen=True)
class MatchProposal:
    """An immutable suggestion produced by a reconciliation strategy.

    ``details`` is an arbitrary payload whose shape is agreed between the
    strategy that emitted it and the ``apply()`` that consumes it. The
    runner never inspects it.
    """

    strategy: str
    details: dict[str, Any]
    confidence: float = 1.0


@dataclass
class ReconciliationReport:
    """Summary of a single reconciliation pass."""

    strategy: str
    proposals_found: int = 0
    proposals_applied: int = 0
    errors: list[str] = field(default_factory=list)


@runtime_checkable
class ReconciliationStrategy(Protocol):
    """Strategy interface consumed by :func:`run_reconciliation_pass`."""

    name: str

    def match(self) -> list[MatchProposal]:
        ...

    def apply(self, proposal: MatchProposal) -> None:
        ...


def run_reconciliation_pass(strategy: ReconciliationStrategy) -> ReconciliationReport:
    """Run one reconciliation pass against ``strategy``.

    - Calls ``strategy.match()`` once; ``proposals_found`` is the length
      of the returned list.
    - Iterates proposals in order, calling ``strategy.apply(proposal)``
      on each. If ``apply`` raises, the formatted exception is appended
      to ``report.errors`` and iteration continues.
    - ``proposals_applied`` counts proposals whose ``apply`` returned
      without raising.
    - ``report.strategy`` mirrors ``strategy.name``.
    """
    report = ReconciliationReport(strategy=strategy.name)

    proposals = strategy.match()
    report.proposals_found = len(proposals)

    for proposal in proposals:
        try:
            strategy.apply(proposal)
        except Exception as exc:  # noqa: BLE001 — runner intentionally captures all
            report.errors.append(f"{type(exc).__name__}: {exc}")
            continue
        report.proposals_applied += 1

    return report


__all__ = [
    "MatchProposal",
    "ReconciliationReport",
    "ReconciliationStrategy",
    "run_reconciliation_pass",
]
