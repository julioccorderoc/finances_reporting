# ADR-015: The P2P Median Tier Expires; It No Longer Carries Forward Without Limit

**Date:** 2026-08-03
**Status:** Accepted
**Amends:** [ADR-005](./ADR-005-rate-resolution-priority.md) — bounds the carry-forward of tier 3
**Related:** [ADR-013](./ADR-013-realized-cost-basis.md) — establishes the age-cap pattern this reuses
**Rule:** [rule-005](../architecture/rules/rule-005-single-rate-resolver.md)

## 1. Context

ADR-005 gave every fallback tier unlimited carry-forward: `rates_repo.latest_on_or_before` returns the newest row on or before the transaction's day, however old, and the resolver appends `_carry` to the source label. ADR-013 later capped the realized tier at 14 days, on the reasoning that "a realized rate that has gone stale would misprice spending badly in a fast-moving currency."

That reasoning applies with equal force to the market median, but the cap was never extended to it. The consequence surfaced in production.

`binance_p2p_median` is only written when `finances update` runs, and there is no way to backfill it: Binance's public P2P search endpoint returns the live order book only, with no historical endpoint. As of 2026-08-03 the table holds **eight** median rows, against 478 BCV rows and 103 realized rows, with a 73-day gap between 2026-04-27 and 2026-07-09.

With no cap, that single April row was presented as the P2P figure for every day up to July. In the triage modal's rate panel:

| transaction day | realized | "USDT P2P" displayed | BCV |
|---|---|---|---|
| 2026-05-15 | 677.25 | 633.52 *(from 04-27)* | 515.17 |
| 2026-06-15 | 791.50 | 633.52 *(from 04-27)* | 587.40 |
| 2026-06-30 | 727.00 | 633.52 *(from 04-27)* | 623.02 |

On 2026-06-30 the displayed P2P rate sat 1.7% from BCV and 13% from realized. This is not coincidence: a stale market rate and a current BCV rate both lag the real market, so they converge. The owner reported the ledger's P2P figures "look more like BCV prices, which makes no sense" — a correct reading of a wrong number.

The underlying fetch is sound. On every day it actually ran, the median tracks realized closely (633.5/626.8, 828.3/816.0, 865.3/864.0, 845.4/848.1). The defect is carry-forward without expiry, not sampling.

## 2. Decision

Cap the `binance_p2p_median` tier at **14 days**, matching ADR-013's realized cap. Past the cap the resolver falls through to BCV exactly as it does for an expired realized rate. The boundary is inclusive: a median exactly 14 days old still applies.

The chain is otherwise unchanged:

1. `transactions.user_rate` — manual override
2. `rates(USDT, VES, source='binance_p2p_realized')` — capped at 14 days (ADR-013)
3. `rates(USDT, VES, source='binance_p2p_median')` — **capped at 14 days (this ADR)**
4. `rates(USD, VES, source='bcv')` — **uncapped**, the floor of the chain
5. none → `needs_review = 1`

**BCV stays uncapped.** It is the last tier before `needs_review`; capping it would convert a stale-but-directionally-useful reference into a triage item, which trades a small pricing error for owner workload. It is also published nearly every banking day, so its carry is short in practice.

**Why 14 days and not tighter.** Rejected: 7 and 3 days. Both bound the error more tightly — VES moved ~2.3% in the two weeks to 2026-08-02 — but any refresh gap then drops spending to BCV, which misprices it far worse than a slightly stale median. Symmetry with ADR-013 also means one number governs both market tiers, which is easier to hold in the head and easier to state in rule-005.

**Why not flag `needs_review` on expiry.** Rejected for the same reason ADR-013 rejected it: the tier below already produces a usable number, and the owner does not want triage burden for a case the chain already handles.

### 2.1 Display

`finances/web/services/rates_view.rates_for_day` builds the triage modal's rate panel and performs its own `latest_on_or_before` lookup per tier. It must apply the same cap, or the modal would keep displaying the number the resolver has stopped using.

An expired tier is **shown, not hidden**, marked as expired and labelled with its age ("64d old — not used"). Hiding it would leave the owner unable to tell "no P2P data exists for this period" from "P2P data exists and was rejected as stale" — and that ambiguity is what made this bug hard to see. `amount_usd` is suppressed for an expired tier, so no dollar figure is ever rendered from a rate the chain refused.

## 3. Consequences

**No transaction changes price.** Resolving all 1,172 VES transactions against the live ledger yields `user_rate` (61.4%), `binance_p2p_realized` (21.6%), `binance_p2p_realized_carry` (17.0%). Not one reaches tier 3, so the cap is inert for current pricing. It corrects what the modal *displays* and guards the resolver against the next coverage gap.

**Median coverage becomes load-bearing.** Once capped, a gap longer than 14 days drops the chain to BCV rather than silently carrying an old median. This makes regular refresh a correctness requirement, not a nicety, and motivates the background refresh-on-viewer-start work tracked alongside this ADR.

**Two existing hypothesis properties narrow.** `test_property_p2p_source_suffix_matches_date_offset` (offsets 0-60) and `test_property_p2p_always_beats_bcv_when_both_present` (offsets 0-30) encode unlimited carry. Their offset ranges bound to the cap, and companion properties assert the expiry behaviour above it.

**The gap already in the data stays.** This ADR does not recover the missing medians — they are unrecoverable. It stops them being misrepresented.
