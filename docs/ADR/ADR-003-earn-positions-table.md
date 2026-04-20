# ADR-003: Binance Earn as Its Own Account + `earn_positions` Table

**Date:** 2026-04-19
**Status:** Accepted

## 1. Context

The user uses Binance Earn to invest idle USDC/USDT but has no visibility today into how much is currently invested or what the principal is per product. The existing `download_binance.py` already fetches Earn rewards (via `simple_earn_flexible_rewards_history`) but treats them as a flat list with no notion of position state.

Two options:

1. **Earn as its own account + dedicated `earn_positions` table.** Subscriptions = transfer Spot → Earn. Redemptions = transfer Earn → Spot. Rewards = income credited to Earn. A separate table tracks current principal + APY snapshot per product.
2. **Just log rewards as income, ignore principal.** Loses the investment-tracking goal.

## 2. Decision

Treat **Binance Earn as a first-class account** in `accounts` (kind=`crypto_earn`, currency varies per product). Subscriptions and redemptions are double-entry transfers per ADR-002. Rewards are `income` transactions on the Earn account, categorized as `Interest`. A separate `earn_positions` table tracks the current `principal`, `apy`, `started_at`, `ended_at` per product, refreshed on every Binance ingest run from the SDK's `simple_earn_flexible_position` endpoint.

## 3. Consequences (The "Why")

### Positive

- The Earn balance is a real, queryable number — `SELECT SUM(principal) FROM earn_positions WHERE ended_at IS NULL`.
- Rewards naturally flow into income totals and the consolidated USD view via existing infrastructure.
- Subscribing to a new Earn product is just another transfer — no new schema needed per product.
- APY snapshots over time enable simple "is my Earn yield improving?" queries.

### Negative

- One more table to maintain and one more thing the Binance ingest must reconcile.
- If Binance changes the position-fetch endpoint, position tracking breaks (rewards still work).
- Position is a snapshot; deriving historical principal at an arbitrary past date requires replaying transfers + rewards rather than reading a stored value.

## 4. Rule Extraction (The "How" for Agents)

**Target File:** `docs/architecture/rules/rule-003-earn-position-source.md`
**Injected Constraint:** `earn_positions` is only written by `finances/ingest/binance.py` after a successful `simple_earn_flexible_position` fetch. No other ingester or domain module may insert into `earn_positions`. Every Earn reward must be inserted as an `income` transaction on the `Binance Earn` account with category `Interest`; reward inserts elsewhere violate this rule.
