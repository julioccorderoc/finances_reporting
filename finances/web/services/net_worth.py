"""USDT-denominated net-worth aggregation for the dashboard (EPIC-023, Phase 2a).

Per ADR-005 (amendment 2026-04-19) and rule-005, the headline net-worth
tile must NEVER fall back to BCV. This module is the single auditable
implementation of that policy:

* USD / USDT / USDC accounts contribute native_balance * 1.
* Other native currencies are converted via
  ``rates_repo.latest_on_or_before(.., source='binance_p2p_median')``.
  If no P2P rate is available for the (native -> USDT) pair, the account
  is reported with ``contribution_usdt = None`` and the dashboard
  displays a missing-pair warning. We do not silently substitute BCV.

The function is pure with respect to ``conn`` and ``as_of_date`` — it
never writes to the DB. Tests can pin behaviour by seeding rate rows.
"""

from __future__ import annotations

import sqlite3
from datetime import date
from decimal import Decimal

from pydantic import BaseModel, ConfigDict

from finances.db.repos import accounts as accounts_repo
from finances.db.repos import rates as rates_repo
from finances.reports.balances import get_balances


_NATIVE_USDT_CURRENCIES = frozenset({"USD", "USDT", "USDC"})
_P2P_SOURCE = "binance_p2p_median"
_P2P_BASE = "USDT"


class AccountContribution(BaseModel):
    """One account's projection into the net-worth tile."""

    model_config = ConfigDict(extra="forbid")

    account_id: int
    account_name: str
    currency: str
    balance_native: Decimal
    rate_to_usdt: Decimal | None
    contribution_usdt: Decimal | None


class NetWorth(BaseModel):
    """Total + per-account breakdown + missing-pair warnings."""

    model_config = ConfigDict(extra="forbid")

    total_usdt: Decimal
    contributions: list[AccountContribution]
    missing_pairs: list[str]
    as_of_date: date


def compute_net_worth(
    conn: sqlite3.Connection,
    *,
    as_of_date: date,
) -> NetWorth:
    """Compute USDT-denominated net worth from account balances.

    Inactive accounts are excluded (they do not appear in
    :func:`accounts_repo.list_all` with the default ``include_inactive=False``).

    The function pulls native balances via
    :func:`finances.reports.balances.get_balances` (which mirrors the
    ``v_account_balances`` view) so every account that ever held a row
    surfaces here.
    """
    # Build a set of active account names so we can drop inactive rows
    # that the view still emits.
    active = {a.id: a for a in accounts_repo.list_all(conn, include_inactive=False)}

    contributions: list[AccountContribution] = []
    missing_pairs_set: set[str] = set()
    total = Decimal("0")

    for bal in get_balances(conn):
        if bal.account_id not in active:
            continue
        currency = bal.currency.upper()

        if currency in _NATIVE_USDT_CURRENCIES:
            rate = Decimal("1")
            contribution = bal.balance_native * rate
            total += contribution
            contributions.append(
                AccountContribution(
                    account_id=bal.account_id,
                    account_name=bal.account_name,
                    currency=currency,
                    balance_native=bal.balance_native,
                    rate_to_usdt=rate,
                    contribution_usdt=contribution,
                )
            )
            continue

        # Non-native: USDT / native via P2P median, never BCV.
        rate_row = rates_repo.latest_on_or_before(
            conn,
            as_of_date=as_of_date,
            base=_P2P_BASE,
            quote=currency,
            source=_P2P_SOURCE,
        )
        if rate_row is None or rate_row.rate == 0:
            missing_pairs_set.add(f"{currency}→USDT")
            contributions.append(
                AccountContribution(
                    account_id=bal.account_id,
                    account_name=bal.account_name,
                    currency=currency,
                    balance_native=bal.balance_native,
                    rate_to_usdt=None,
                    contribution_usdt=None,
                )
            )
            continue

        contribution = bal.balance_native / rate_row.rate
        total += contribution
        contributions.append(
            AccountContribution(
                account_id=bal.account_id,
                account_name=bal.account_name,
                currency=currency,
                balance_native=bal.balance_native,
                rate_to_usdt=rate_row.rate,
                contribution_usdt=contribution,
            )
        )

    # Stable, deterministic ordering (matches accounts_repo.list_all "ORDER BY name").
    contributions.sort(key=lambda c: c.account_name)
    missing_pairs = sorted(missing_pairs_set)

    return NetWorth(
        total_usdt=total,
        contributions=contributions,
        missing_pairs=missing_pairs,
        as_of_date=as_of_date,
    )


__all__ = [
    "AccountContribution",
    "NetWorth",
    "compute_net_worth",
]
