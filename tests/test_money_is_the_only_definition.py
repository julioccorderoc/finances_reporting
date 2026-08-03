"""One definition of "which currencies are already dollars". Enforced.

The 2026-08-03 review found this frozenset written out four times, once
per surface, with the fourth under a different name. Consolidating it was
part of the fix — and while that work was in flight a *sixth* copy landed
on main from an unrelated branch, which is the whole problem restated: a
fact that lives in more than one place will be added to in more than one
place, and the copies drift silently.

So it is pinned. These tests fail if anyone writes the literal set again
instead of importing it.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

from finances.domain import money

PACKAGE = Path(__file__).resolve().parent.parent / "finances"
OWNER = PACKAGE / "domain" / "money.py"

# "USD", "USDT", "USDC" as a set/frozenset literal, in any order, however
# it is spaced or quoted.
_LITERAL = re.compile(
    r"frozenset\s*\(\s*\{[^}]*USDT[^}]*\}\s*\)|\{\s*[\"'](?:USD|USDT|USDC)[\"']"
    r"\s*,\s*[\"'](?:USD|USDT|USDC)[\"']\s*,\s*[\"'](?:USD|USDT|USDC)[\"']\s*\}"
)


def _python_sources() -> list[Path]:
    return [p for p in PACKAGE.rglob("*.py") if p != OWNER]


def test_the_set_is_defined_exactly_once():
    offenders = []
    for path in _python_sources():
        for lineno, line in enumerate(path.read_text().splitlines(), 1):
            if line.lstrip().startswith("#"):
                continue
            if _LITERAL.search(line):
                offenders.append(f"{path.relative_to(PACKAGE.parent)}:{lineno}")

    assert not offenders, (
        "the native-USD currency set is written out again instead of imported "
        "from finances.domain.money.NATIVE_USD_CURRENCIES: " + ", ".join(offenders)
    )


def test_the_owner_actually_defines_it():
    """Guard against the check above passing because the fact moved away."""
    assert money.NATIVE_USD_CURRENCIES == frozenset({"USD", "USDT", "USDC"})
    assert _LITERAL.search(OWNER.read_text())


@pytest.mark.parametrize(
    "module",
    [
        "finances.reports.consolidated_usd",
        "finances.reports.monthly",
        "finances.web.services.transactions_query",
        "finances.web.services.net_worth",
        "finances.domain.transfers",
    ],
)
def test_every_surface_that_needs_it_imports_it(module):
    """Each of the five copies the review found now resolves to one object."""
    import importlib

    source = Path(importlib.import_module(module).__file__).read_text()
    tree = ast.parse(source)
    imports_money = any(
        isinstance(node, ast.ImportFrom)
        and node.module
        and node.module.startswith("finances.domain")
        and any(a.name == "money" for a in node.names)
        for node in ast.walk(tree)
    ) or "from finances.domain.money import" in source
    assert imports_money, f"{module} must source the fact from domain.money"


def test_to_usd_at_prices_a_leg_at_a_given_rate():
    """The second shape: convert at a rate the caller already holds.

    ``to_usd`` resolves a transaction. ``validate`` and the triage rate
    panel instead price a known amount at a *specific* tier's rate, which
    is a different question and used to be a third and fourth copy of the
    same arithmetic.
    """
    from decimal import Decimal

    assert money.to_usd_at(Decimal("-50"), "USDT", None) == Decimal("-50")
    assert money.to_usd_at(Decimal("80000"), "VES", Decimal("800")) == Decimal("100")
    assert money.to_usd_at(Decimal("80000"), "VES", None) is None
    assert money.to_usd_at(Decimal("80000"), "VES", Decimal("0")) is None
