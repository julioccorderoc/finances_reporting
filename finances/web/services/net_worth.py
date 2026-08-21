"""USDT-denominated net-worth aggregation for the dashboard (EPIC-023, Phase 2a).

Per ADR-005 (amendment 2026-04-19) and rule-005, the headline net-worth
tile must NEVER fall back to BCV:

* USD / USDT / USDC accounts contribute their native balance 1:1.
* Every other currency goes through :func:`finances.domain.money.to_usd` —
  the resolver, the same one the reports and the transaction list use.
* If the resolver cannot price the currency, or can only reach BCV, the
  account reports ``contribution_usdt = None``, the dashboard shows a
  missing-pair warning, and the total omits it. BCV is never substituted.

This module used to describe itself as "the single auditable
implementation of that policy" while carrying a *private* rate chain that
read ``binance_p2p_median`` and nothing else — no ``user_rate``, no
ADR-013 realized cost basis. It was the fifth independent copy of this
arithmetic in the codebase and the one that had drifted, on the most
prominent number in the application. See :func:`usdt_value`.

The functions are pure with respect to ``conn`` and ``as_of_date`` — they
never write to the DB. Tests can pin behaviour by seeding rate rows.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, date, datetime, time
from decimal import Decimal

from pydantic import BaseModel, ConfigDict

from finances.db.repos import accounts as accounts_repo
from finances.domain import money
from finances.domain.models import Transaction, TransactionKind
from finances.reports.balances import get_balances


_NATIVE_USDT_CURRENCIES = money.NATIVE_USD_CURRENCIES


def _probe(currency: str, amount_native: Decimal, as_of_date: date) -> Transaction:
    """A stand-in transaction, so a *balance* can use the row resolver.

    ``rates.resolve`` takes a transaction because that is what it prices, and
    rule-005 forbids writing a second chain for anything else. Valuing a
    balance is the same question asked of a different noun — "what is this
    much of this currency worth on this date" — so it is asked in the form
    the resolver accepts rather than by reimplementing it.

    ``user_rate`` is deliberately ``None``: a balance has no realized rate of
    its own. The remaining fields exist only to satisfy the model.
    """
    return Transaction(
        account_id=0,
        occurred_at=datetime.combine(as_of_date, time(0, 0), tzinfo=UTC),
        kind=TransactionKind.ADJUSTMENT,
        amount=amount_native,
        currency=currency,
        description="net-worth valuation probe",
        source="net_worth",
        source_ref="probe",
    )


def usdt_value(
    conn: sqlite3.Connection,
    *,
    currency: str,
    amount_native: Decimal,
    as_of_date: date,
) -> Decimal | None:
    """Return the USDT-equivalent of ``amount_native`` on ``as_of_date``.

    Routed through :func:`finances.domain.money.to_usd`, which is the
    resolver, so this surface prices a bolívar exactly as the reports and
    the transaction list do.

    It did not used to. This module carried its own chain that consulted
    ``binance_p2p_median`` and nothing else — not ``user_rate``, not the
    ADR-013 realized cost basis. That source holds **8 rows in fifteen
    months** of live data, so before 2026-04-27 the bank account contributed
    *nothing at all* to net worth and the tile silently reported a
    missing-pair warning instead; after it, the rate was carried up to 74
    days with no age cap, while every other surface capped at 14. It was the
    fifth independent implementation of this arithmetic and the one that had
    drifted, on the most prominent number in the application.

    The ADR-005 amendment still holds: BCV must never reach a headline
    figure. That is now enforced by *reading the resolver's answer* rather
    than by keeping a private chain — a BCV-sourced result returns ``None``,
    the account reports as a missing pair, and the total omits it, exactly
    as before. ``is_bcv_sourced`` is prefix-matched, so ADR-021's
    ``bcv_nearest`` is barred by the same line that bars ``bcv_carry``; an
    approximation of a *market* tier is allowed through, because the rule
    is about BCV, not about precision.

    Shared by :func:`compute_net_worth` (dashboard) and
    :func:`finances.web.services.accounts_view.build_account_cards`
    (accounts page) so both surfaces use one auditable conversion path.
    """
    code = currency.upper()
    if code in _NATIVE_USDT_CURRENCIES:
        return amount_native

    amount_usd, source = money.to_usd(conn, _probe(code, amount_native, as_of_date))
    if amount_usd is None or money.is_bcv_sourced(source):
        return None
    return amount_usd


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

        # Non-native: through the resolver, never BCV.
        contribution = usdt_value(
            conn,
            currency=currency,
            amount_native=bal.balance_native,
            as_of_date=as_of_date,
        )
        if contribution is None:
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

        # The rate the resolver actually used, recovered from the result so
        # the card cannot display one number and total another.
        rate_used = (
            bal.balance_native / contribution if contribution != 0 else None
        )
        total += contribution
        contributions.append(
            AccountContribution(
                account_id=bal.account_id,
                account_name=bal.account_name,
                currency=currency,
                balance_native=bal.balance_native,
                rate_to_usdt=rate_used,
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
    "usdt_value",
]
