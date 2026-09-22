# ADR-028: The Pre-2026-09-08 BONUS Rewards Are Spot's, And Earn Answers To The Exchange

**Date:** 2026-09-22
**Status:** Accepted (owner, 2026-09-22)
**Extends:** [ADR-027](./ADR-027-the-ingest-reads-what-binance-names.md) §2.3 — the destination rule now reaches back past the frozen window; [ADR-003](./ADR-003-earn-positions-table.md) — `earn_positions` gains the check it was built for; [ADR-022](./ADR-022-deleting-a-transaction.md) — a reconciliation plug becomes reversible through the tombstone path
**Related:** [ADR-018](./ADR-018-reconciliation-adjustments.md) / [ADR-020](./ADR-020-opening-positions.md) — the plugs this reverses and the opening positions this restates; [ADR-023](./ADR-023-exchange-balance-reconciliation.md) — the same outside-figure principle, one wallet over; [ADR-010](./ADR-010-deterministic-source-ref.md) — why the repair is a re-ingest, not a rewrite
**Rule:** [rule-003](../architecture/rules/rule-003-earn-position-source.md), [rule-010](../architecture/rules/rule-010-deterministic-source-ref.md), [rule-012](../architecture/rules/rule-012-reconciliation-adjustments.md)

## 1. Context

ADR-027 §2.3 stopped booking BONUS rewards to Earn from 2026-09-08 forward.
Its last line was explicit about the rest: *"Pre-2026-09-08 BONUS rows remain
on Earn. They are history the owner will restate deliberately in a later
wave."* This is that wave, and the measurement it was sized from.

Before the repair, on 2026-09-22:

| | ledger | exchange | off by |
| --- | ---: | ---: | ---: |
| Binance Earn USDT | 1,701.89746666 | 1,700.96763244 | **+0.92983422** |
| Binance Earn USDC | 5,003.46735691 | 5,002.88970069 | **+0.57765622** |

**410 BONUS rows** (2026-01-05 → 2026-09-07) sat on Earn — 4.97640115 USDT
and 5.739462 USDC. Two plugs from 2026-08-08, **7408** (−4.55617999 USDC) and
**7409** (−3.92884318 USDT), were holding the same difference closed: summed up
to the plug's own timestamp, the BONUS-on-Earn rows come to
3.98468515 USDT / 5.123052 USDC, so each plug was the misplacement plus
whatever else had drifted by that day (−0.055842 USDT / −0.566872 USDC, a
composition that cannot be recovered). Every check the ledger had was quiet:
`position_disagrees_with_exchange` compares Spot and Funding only, and nothing
ever compared Earn to the principal `earn_positions` has held since ADR-003.

### 1.1 The reward history is still served; the transfer history is not

ADR-025 §2 named the hazard of any deep `--since`: legacy `:hash:` `source_ref`s
colliding with native ids and duplicating history. Re-measured before acting,
the 2026-01-05 → now window holds **36** `:hash:` rows — no longer none, as
ADR-025's window had it. They are inert, and the dry run proved it rather than
argued it. Binance serves the reward stream back to January
(`get_flexible_rewards_history(type=BONUS, 01-05..02-03)` → 31 rows), and
`withdraw`/`deposit` answer for the window, but
`user_universal_transfer_history` refuses any start older than roughly six
months with `-5026`, exactly as ADR-018 §1.1 documented. The events the
`:hash:` rows carry are the ones the API no longer re-serves, so no native twin
can be written for them. The instrumented dry run confirmed it: **3 would-be
inserts, none of them a `:hash:` twin** — two genuinely missing REALTIME reward
rows (2026-08-04) and one live P2P sell from minutes earlier.

## 2. Decision

### 2.1 Every BONUS reward row belongs on Spot, and the repair is the ingest

The destination rule is not a date rule. A BONUS reward was paid into Spot in
January for the same reason it was in September, so the 410 rows move, and they
move the way ADR-027 §3 said movements move: **`finances ingest binance --since
2026-01-05`**, because the ingest is the single writer and
`upsert_by_source_ref` already treats `account_id` as statement-sourced. A
repair command would be a second writer for a movement the ingest owns.

The window is pinned to UTC. The CLI localizes a naive `--since` to Caracas,
so the literal `2026-01-05` starts at `04:00Z` and misses the first BONUS row
(`2026-01-05T03:12:19Z`, 0.027396 USDT). The run that was executed is
`--since 2026-01-04T20:00:00` — `2026-01-05T00:00:00Z` — and it moved **410 of
410**.

### 2.2 The 8/8 plugs are reversed through the ADR-022 tombstone path

They closed a difference this ADR explains, so carrying them would be a second
correction on a corrected ledger — and every report before 2026-08-08 wrong
because of it. `finances reconcile reverse-adjustment --id N --reason "..."`
was added as the smallest command; it calls
`reconciliation_adjustments.reverse_adjustment`, which delegates the write to
`transactions_repo.delete(..., allow_ledger_corrections=True)`. The tombstone
(ADR-022 §2.1) records the source_ref and the owner's reason, and nothing can
resurrect the row.

The door is one caller wide. Only a `reconciliation` plug is eligible: an
opening position is refused and named as something to *restate* through
`record_opening` (ADR-020 §2.4), and an ordinary row is refused as not this
module's business. Every other caller of `delete` still gets ADR-022 §2.3's
refusal — pinned by test.

### 2.3 Every affected position is restated to the exchange's figure — six, not four

The deep re-ingest had a second effect the premise did not list: ADR-027 §2.2's
`takerAmount` rule had only ever reached the post-2026-09-08 window, and a
9-month re-ingest re-read **104 historical P2P rows** at the amount that
actually moved (±0.06–0.08 each, the taker commission). Funding USDT fell a net
**6.32** against the exchange because of it. That is the rule working, not
damage — but it breaks Funding exactly as the BONUS move breaks Spot, so
Funding is an affected position and is restated with the rest.

| position | before | after | shape |
| --- | ---: | ---: | --- |
| Binance Spot USDT | opening 2,534.10746658 | opening **2,529.13106543** | balance, −4.97640115 (the BONUS total) |
| Binance Spot USDC | opening 5.584824 | opening-transfer 2→3 **0.191772** | transfer (the ADR-025 over-draw) |
| Binance Funding USDT | opening 797.94 | opening **804.26** | balance, +6.32 (the taker commissions) |
| Binance Funding USDC | opening 0.191772 | **cleared** | rows net 0 once the over-draw is movement |
| Binance Earn USDT | none | opening **0.09627095** | balance |
| Binance Earn USDC | none | opening **0.23111743** | balance |

`record_opening` clears a position's prior opening rows before measuring, so
restating Spot USDT never needed `--moved-to` despite Spot reading over the
exchange: without the old 2,534.11 opening its rows are 2,528.52 short. The
refusal fires only where the rows *alone* exceed the custodian — Spot USDC,
over by 0.191772, the amount ADR-025 identified as having come from Spot to
cover a Funding over-draw. Recording that as the movement it was is what lets
Funding USDC's approximation (ADR-025 §4's "absorbed at the ledger's start")
retire.

### 2.4 The blind spot gets a check

`earn_position_disagrees_with_principal` (ERROR): for each asset the exchange
reports an active Earn principal for, the ledger's Earn balance must agree
within `EXCHANGE_BALANCE_TOLERANCE`. Compared **as of the position's snapshot**,
so a stale claim is not a defect, and per asset, so a healthy USDT balance
cannot mask a broken USDC one. An asset the exchange reports and the ledger has
no row for is skipped — it has no id to name, the same convention
`position_disagrees_with_exchange` uses.

The check is the one that would have caught this on the day it landed: after
the BONUS rows moved and before the restatements, it fired with two rows while
every pre-existing check stayed green.

## 3. Consequences

**Every Binance position the exchange reports now equals it, to the last
digit**, as of the 2026-09-22T15:21:23Z capture:

| position | ledger | exchange |
| --- | ---: | ---: |
| Binance Spot USDT | 0.613648 | 0.613648 |
| Binance Spot USDC | 65.287658 | 65.287658 |
| Binance Funding USDT | 13.10111327 | 13.10111327 |
| Binance Funding USDC | 0 | 0 |
| Binance Earn USDT | 1,700.9796737 | 1,700.9796737 |
| Binance Earn USDC | 5,002.91827473 | 5,002.91827473 |

**The residual, named.** The pre-repair residuals were +0.92983422 USDT and
+0.57765622 USDC. Moving the BONUS rows is balance-neutral and reversing the
plugs adds value back, so those figures cannot be made to cancel — they are
absorbed where the tools state them, and the only figures no history explains
are Earn's opening balances:

```text
opening:4:USDT  +0.09627095   (ledger 1700.88340275, custodian 1700.9796737)
opening:4:USDC  +0.23111743   (ledger 5002.68715730, custodian 5002.91827473)
```

That is the accrual the exchange has credited and the reward rows have not yet
paid — several times inside the 1.0 tolerance, and *visible in a row that says
what it is* rather than hidden in a balance. Two further shifts
are stated the same way: Funding's opening grew 797.94 → 804.26 (the 104 taker
commissions) and Spot USDC's opening became a 0.191772 movement (the over-draw).

**The deep run records `status='error'` with six refusals** from
`user_universal_transfer_history` — `-5026`, starts older than six months. That
is ADR-018 §1.1's documented retention, not a defect; the watermark is left
where it was and the next ordinary sync re-covers from it. `finances doctor`
reads **0 errors**; `--strict` exits 0.

**Warnings left in `doctor`:** `uncategorized_not_flagged` (12),
`unpaired_p2p_sells` (9 — Provincial still lags, one of them today's live sell),
and `reconciliation_adjustments` (2: 7404/7405, the Provincial and Cash plugs,
out of this ADR's scope).

**The 30 post-9/8 BONUS rows on Spot were already right** (ADR-027 §2.3). They
were re-upserted, not moved, and all 440 BONUS rows in the ledger now sit on
the wallet Binance paid them into: 6.25848715 USDT / 6.355872 USDC.

## 4. Rejected alternatives

- **Hand-move the 410 rows.** Ten months of rows on the wrong wallet came from
  a writer that assumed a destination; a repair command is the same class of
  writer. The ingest now names the wallet, and re-reading the records is the
  repair (ADR-027 §3's argument, applied to history).
- **Keep the plugs and restate around them.** Then the ledger carries an
  explanation for a difference that no longer exists, and every report before
  2026-08-08 stays wrong by the plug's amount.
- **Reversing by raw SQL.** Loses the tombstone, the reason, and ADR-022's
  single delete path. The command exists so the act is one invocation with
  its reason attached.
- **Restating only the four positions in the premise.** Funding was moved 6.32
  by the same run's takerAmount corrections. Leaving it would have left a
  permanent ERROR in the check the previous ADR built, which is the failure
  mode (a green-looking ledger with a known unexplained difference) this whole
  family of ADRs exists to prevent.
- **Widening `EXCHANGE_BALANCE_TOLERANCE` so the Earn residual is "inside
  tolerance" by fiat.** The residual is 0.096/0.231 — already five to ten times
  inside a tolerance sized for dust. Nothing needed widening; had it been
  needed, that would have been the signal to restate, not to loosen.
- **Waiting for the next BONUS payment to "close" the residual.** Reward rows
  move the exchange's principal and the ledger's balance together; the gap is
  accrual timing, and it is bounded by the tolerance by construction, not
  closed by waiting.

## 5. Out of scope, deliberately

The exchange-only `BONK` / `NOT` / `STRAX` balances on Funding (no ledger
rows, no claim), the Provincial pairing backlog, and the two dated plugs
(7404/7405) that belong to their own positions. None of this ADR's decisions
reach them.
