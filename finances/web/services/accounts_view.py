"""Account-card builder for /accounts (EPIC-023, Phase 2d).

Projects ``v_account_balances`` rows into ``AccountCard`` DTOs for the
read-only Accounts grid. Each card carries the account's native balance
plus a USDT-equivalent value, computed by
:func:`finances.web.services.net_worth.usdt_value` so the conversion
path stays in one place (rule-005, rule-012, ADR-005 amendment).

USD / USDT / USDC native balances are 1:1 with USDT. Other currencies
are converted through ``rates(base='USDT', quote=<currency>,
source='binance_p2p_median')``; the page shows ``—`` if the P2P rate is
missing (we never silently fall back to BCV).
"""

from __future__ import annotations

import sqlite3
from datetime import date
from decimal import Decimal
from urllib.parse import urlencode

from pydantic import BaseModel, ConfigDict

from finances.db.repos import accounts as accounts_repo
from finances.reports.balances import get_balances
from finances.web.services.net_worth import usdt_value


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
        balance_usdt = usdt_value(
            conn,
            currency=currency,
            amount_native=balance_native,
            as_of_date=today,
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

    cards.sort(key=lambda c: (not c.active, c.name.lower()))
    return cards


__all__ = ["AccountCard", "build_account_cards"]
