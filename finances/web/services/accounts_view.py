"""Account-card builder for /accounts (EPIC-023, Phase 2d).

This service projects ``v_account_balances`` rows into ``AccountCard``
DTOs suitable for the read-only Accounts grid. Each card carries the
account's native balance plus a USDT-equivalent value resolved through
the ADR-005 priority chain:

* USD / USDT / USDC native → USDT-equivalent equals the native balance
  (1:1 per the ADR-005 amendment).
* Otherwise → look up
  ``rates(base=native_currency, quote='USDT', source='binance_p2p_median')``
  on or before ``today``; missing → ``balance_usdt = None``.
* Per rule-005 we never use BCV for the headline USDT figure.

NOTE — temporary code duplication
---------------------------------
The USDT-equivalent math below is intentionally inlined here while
Phase 2a's ``finances/web/services/net_worth.py`` lands in a parallel
worktree. After both phases merge, the merge-back step must DRY this
into a shared helper in ``services/net_worth.py`` (or a sibling
``services/usdt_value.py``) so both pages call the same code.

See ADR-005 (rate resolution priority) and
docs/architecture/rules/rule-012-web-viewer-uses-existing-domain.md.
TODO(merge-back: ADR-005, rule-012): collapse this with
finances/web/services/net_worth.py once it exists on main.
"""

from __future__ import annotations

import sqlite3
from datetime import date
from decimal import Decimal
from urllib.parse import urlencode

from pydantic import BaseModel, ConfigDict

from finances.db.repos import accounts as accounts_repo
from finances.db.repos import rates as rates_repo
from finances.reports.balances import get_balances

_NATIVE_USD_CURRENCIES = frozenset({"USD", "USDT", "USDC"})
_USDT_QUOTE = "USDT"
_P2P_SOURCE = "binance_p2p_median"


class AccountCard(BaseModel):
    """One account, projected for the /accounts card grid."""

    model_config = ConfigDict(extra="forbid")

    id: int
    name: str
    kind: str
    institution: str | None
    currency: str
    balance_native: Decimal
    balance_usdt: Decimal | None
    active: bool
    drill_url: str


def _balance_usdt(
    conn: sqlite3.Connection, *, currency: str, balance_native: Decimal, today: date
) -> Decimal | None:
    """Compute USDT-equivalent for ``balance_native``.

    See module docstring for the duplication note. This logic mirrors
    the ADR-005 amendment for headline USD values: USDT, never BCV.
    """
    if currency in _NATIVE_USD_CURRENCIES:
        return balance_native

    rate = rates_repo.latest_on_or_before(
        conn,
        as_of_date=today,
        base=currency,
        quote=_USDT_QUOTE,
        source=_P2P_SOURCE,
    )
    if rate is None:
        # Try the inverse pair (USDT->native) and invert if available.
        # This covers the seeded layout where rates live as
        # base=USDT, quote=VES, source=binance_p2p_median.
        inverse = rates_repo.latest_on_or_before(
            conn,
            as_of_date=today,
            base=_USDT_QUOTE,
            quote=currency,
            source=_P2P_SOURCE,
        )
        if inverse is None or inverse.rate == 0:
            return None
        return balance_native / inverse.rate
    if rate.rate == 0:
        return None
    return balance_native * rate.rate


def build_account_cards(
    conn: sqlite3.Connection, *, today: date
) -> list[AccountCard]:
    """Return ``AccountCard`` projections sorted active-first then by name."""
    balances = {b.account_id: b for b in get_balances(conn)}
    accounts = accounts_repo.list_all(conn, include_inactive=True)

    cards: list[AccountCard] = []
    for account in accounts:
        assert account.id is not None
        bal = balances.get(account.id)
        balance_native = bal.balance_native if bal is not None else Decimal("0")
        currency = bal.currency if bal is not None else account.currency
        balance_usdt = _balance_usdt(
            conn,
            currency=currency,
            balance_native=balance_native,
            today=today,
        )
        drill_url = "/transactions?" + urlencode({"accounts": account.name})
        cards.append(
            AccountCard(
                id=account.id,
                name=account.name,
                kind=account.kind.value,
                institution=account.institution,
                currency=currency,
                balance_native=balance_native,
                balance_usdt=balance_usdt,
                active=account.active,
                drill_url=drill_url,
            )
        )

    # Active first, then by name. Stable sort on a (not active, name) key.
    cards.sort(key=lambda c: (not c.active, c.name.lower()))
    return cards


__all__ = ["AccountCard", "build_account_cards"]
