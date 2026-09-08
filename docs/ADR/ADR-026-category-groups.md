# ADR-026: Categories Roll Up Into Groups, and the Chart Reads Groups

**Date:** 2026-09-07
**Status:** Accepted 2026-09-07 — the owner confirmed the mapping and the
storage choice, then delegated the remaining calls ("do what's best").
Implemented 2026-09-08 (migrations 027 and 028) from
[the handoff prompt](../plans/2026-09-07-category-groups-prompt.md).
§2.6 amended 2026-09-08: Lending is folded into Other by data.
**Amends:** [ADR-006](./ADR-006-category-taxonomy-revamp.md) — a category is still
the unit a transaction carries; the group is a second, coarser axis over it,
never a replacement
**Related:** [ADR-012](./ADR-012-local-web-viewer.md) (the viewer reads the
domain, it does not reimplement it), [ADR-009](./ADR-009-pydantic-for-normalization.md)
(the group crosses boundaries as a typed field)
**Rule:** [rule-006](../architecture/rules/rule-006-categorization-pipeline.md)

## 1. Context

Owner request, 2026-09-07, reading the /monthly chart:

> *multiple items here must be their own color: rent (consolidated with
> utilities, personal care and transport); lending can go into others;
> dating, going out, leisure, family and gifts must be one consolidated
> thing*

The chart draws the top five categories by size and folds the rest into
"Other". On the real ledger that is not a tidy remainder — it is the
largest block in four of six months, and **45.8% of July 2026** on its
own. The hover overlay (shipped the same day) is what made this legible:
opening Other for July shows Rent -$240.00, Going Out -$135.87, Family
-$104.75, Utilities -$75.48, Transport -$59.93, Personal Care -$51.84.

The failure is structural, not cosmetic. Eighteen expense categories are
in use. Fixed household costs are split across four of them, and none is
individually large enough to reach the top five — so the single biggest
fact about a month (what the fixed costs were) is the one thing the chart
cannot show. Meanwhile five sociable categories fragment the same way.

Two fixes were rejected:

- **Raise `CHART_TOP_N`.** The cap equals the number of validated palette
  hues (five). No six-hue set clears the colour-separation floors on all
  pairs — the `dataviz` reference palette does not either — so a bigger
  cap means two categories drawn in the same colour, which is worse than
  folding one away. See `--series-*` in signal.css.
- **A display-only mapping in `monthly_view`.** Cheap, but it puts the
  taxonomy in code where only one chart can use it, and changing it means
  a code edit rather than a data edit.

## 2. Decision (proposed)

**A category may belong to a group. The group is stored on the category,
and it is the axis the /monthly chart draws.**

### 2.1 The column

Migration 027 adds `categories.group_name TEXT NULL`. Null means "this
category stands for itself" — it is not an error state, and the majority
of income and transfer categories will keep it.

The group is a *label on the category*, not a table of its own. There is
no group id, no second foreign key, and a transaction still carries
exactly one `category_id`. This is deliberate: the ledger's unit of
meaning is unchanged, so nothing about ingest, categorization (ADR-006),
triage or the rate chain has to know groups exist.

### 2.2 The mapping

Confirmed with the owner, 2026-09-07:

| Group | Categories |
|---|---|
| **Home** | Rent, Utilities, Transport, Personal Care |
| **Social** | Dating, Going Out, Leisure, Family, Gifts |
| *(none)* | Purchases, Groceries, Health — each stands alone |
| *(none)* | Fees, Other Expense, Education, Subscriptions |
| **Other** *(folded, 2026-09-08)* | Lending — see §2.6 |

Five things then compete for the chart's five slots, and the remainder is
genuinely minor: July's Other falls from -$696.11 (45.8%) to roughly
-$58 (3.8%).

"Lending can go into others" was first honoured by leaving it ungrouped
and expecting the ranking to do the rest. It did not — §2.6 records why,
and what replaced it.

### 2.3 What reads the group

- **/monthly chart** rolls up by group. A grouped category contributes to
  its group's series; an ungrouped one is its own series, as today. The
  top-N cap and the "Other" remainder then apply to *that* list.
- **/monthly pivot** stays per-category. It is the detail view, its cap is
  25, and collapsing it would hide the very rows the owner just used to
  diagnose this. The chart answers "what shape was the month", the pivot
  answers "what exactly was in it".
- **The hover overlay** opens any series that has members — a group into
  its categories, Other into what missed the cap. One mechanism, see §2.4.
- **Everything else** is unchanged for now. The column is available to
  /transactions filters and future reports; wiring those is not part of
  this decision.

### 2.4 A series can be opened, whether it is a group or the remainder

`MonthlyChartSeries` gains `members: list[MonthlyChartSeries]` — the things
inside it, empty when there is nothing inside. "Other" fills it with the
categories that missed the cap; a group fills it with its categories. The
hover overlay then has *one* mechanism to open a block instead of two, and
`MonthlyChart.other_members` (added earlier the same day) is folded into it
rather than kept as a parallel field.

### 2.5 The category filter still filters categories

Grouping always applies; the filter narrows which categories contribute to
each series. Selecting only Rent shows a **Home** bar containing Rent alone,
not a bar labelled Rent. The alternative — ungrouping whenever a filter is
active — makes the chart change shape as well as content, which is the thing
that made the old rank-keyed colour so disorienting.

Clicking a group drills to `/transactions` with one `categories=` parameter
per member, which the existing repeated-param contract already accepts.

### 2.6 "Other" is still computed, never stored

There is no `Other` group. "Other" remains what the chart computes when
more things are in play than there are slots, and it stays a neutral
colour because it is a remainder rather than a thing that happened. A
stored `Other` group would be a lie the moment the cap or the mapping
changed.

**Amended 2026-09-08.** Lending did not fall into Other on its own. The
chart ranks a series by its total over the whole window, not the month
being read, and one large loan month kept Lending in the top five for six
months while Health — larger in July — fell into Other: the opposite of
the request. On the live ledger the first implementation read July's
Other at 5.3% (Health, Other Expense, Fees), not the 3.8% above. Owner
decision: Lending goes into Other regardless of rank.

The mechanism is the same column with the label `Other` (migration 028).
This does not contradict the paragraph above, because a stored `Other` is
not a group. It is an *instruction to fold* the category into the computed
remainder whatever its rank: it never competes for a slot, holds no
palette rank, draws as Other even when it is the only category selected
(the §2.5 rule), and its categories sit flat under the one Other series
beside whatever missed the cap, largest first. The remainder is still
computed for everything else — nothing is written down for Fees, Other
Expense, Education or Subscriptions. The lie the paragraph above guards
against cannot arise: the instruction is unconditional, so it stays true
however the cap or the mapping moves. `finances doctor` knows Lending as
a grouped category from 028 on.

### 2.7 The mapping is data, not code

Group membership lives in rows, so it can be edited without a deploy, and
a future viewer surface can edit it. Migration 027 seeds the table above;
it does not own it afterwards.

## 3. Consequences

**Good**

- The chart shows the largest true fact about a month instead of hiding it
  in a grey block.
- Five groups map exactly onto five validated hues, so the palette holds
  without re-deriving it.
- Categories keep their meaning. The owner's standing instruction is not
  to collapse his labels, and this does not: Dating, Family and Gifts stay
  distinct in the pivot, the triage picker and every report. Only the
  *chart* rolls them up.

**Costs and risks**

- A second axis over categories is a thing future code can forget to
  honour. Mitigated by keeping it read-only outside /monthly for now.
- A category can be renamed while `group_name` still points at the old
  label. The group is a plain string, so this is a real risk; the guard is
  a `doctor` check that every non-null `group_name` is one the seed knows,
  and a test that the mapping covers only categories that exist.
- Rolling up hides within-group movement on the chart: a month where
  Transport doubles and Rent halves looks flat under **Home**. The overlay
  opening the group is the answer, and the pivot remains per-category.

## 4. Alternatives considered

- **A `category_groups` table with a foreign key.** Correct-by-construction
  for renames, but it adds a join to every category read for a taxonomy
  with five members and no attributes of its own. Revisit if groups ever
  need an icon, an order, or a budget.
- **Reusing `categories.kind`.** Already means income/expense/transfer;
  overloading it would break the categorization pipeline.
- **Grouping only in the chart's own code.** Rejected above — the mapping
  would not be editable and nothing else could use it.

## 5. Verification

- Migration 027 is idempotent and re-runnable; re-applying changes no rows.
- `build_chart` groups: with the seed above, a month's series are the
  groups plus ungrouped categories, ranked together.
- The July 2026 regression: Other is under 5% of the month.
- The pivot still lists every category.
- `doctor` flags a `group_name` on a category the seed does not know, which
  is how a later rename gets caught.
- Opening a **Home** bar in the overlay lists Rent, Utilities, Transport and
  Personal Care; opening Other lists what missed the cap.
- A category filter narrows a group rather than dissolving it: filtering to
  Rent alone still draws a **Home** series.
- A category stored as `Other` (Lending, migration 028) never takes a slot
  or a palette rank, draws as Other even when filtered to alone, and sits
  flat under the one Other series with whatever missed the cap. Migration
  028 is idempotent.
- Colour slots stay stable under the category filter (ADR-unnumbered
  behaviour pinned by `tests/web/test_monthly_chart_palette.py`).
