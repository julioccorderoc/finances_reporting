# Fill needs_review.csv `category` column — orchestrator prompt

Paste this verbatim into a fresh Claude Code session at the repo root
(`/Users/juliocordero/Documents/finances_reporting`). It will slice the
CSV, spawn N parallel subagents, fill the `category` column on every
row, and merge the result back. You then review + run `cleanup-apply`.

---

## PROMPT (copy from here ↓)

I need you to fill the `category` column of `needs_review.csv` (at the
repo root) using parallel subagents. 934 rows total — too many for one
context, so slice and parallelize. I'll review and apply the result.

### Strict rules

1. **Only write to the `category` column.** Never change `id`,
   `occurred_at`, `source`, `kind`, `amount`, `currency`, `description`,
   `suggested_category`, `user_rate`, `legacy_sub_category`,
   `legacy_category`. Column order, quoting, header row, row order must
   round-trip byte-for-byte identical except for the `category` cell.
2. **Kind-scoped categories.** The `category` you write MUST be valid
   for that row's `kind` value. See the taxonomy below.
3. **Leave blank when uncertain.** Blank category = "skip this row, I'll
   review manually." Never guess on ambiguous cases. Coverage matters
   less than correctness.
4. **Prefer `suggested_category` when it's non-empty** — the rules
   engine already matched a pattern; trust it unless the description
   obviously contradicts it.
5. **When `suggested_category` is blank**, use the legacy columns +
   description to decide. See the translation hints below.

### v1 taxonomy (valid `category` values, scoped by `kind`)

| `kind` | Allowed `category` values |
|---|---|
| `income` | `Salary` · `Gigs` · `Interest` · `Other Income` |
| `expense` | `Food` · `Transport` · `Health` · `Family` · `Lifestyle` · `Subscriptions` · `Purchases` · `Fees` · `Tools` · `Other Expense` |
| `transfer` | `Internal Transfer` · `External Transfer` |
| `adjustment` | `Reconciliation` · `FX Diff` |

### Legacy → v1 translation hints

The legacy sheets used a different taxonomy. Map as follows; when the
mapping is ambiguous (e.g. legacy `NA`, `Others`), leave blank.

| Legacy Sub-Category | Legacy Category | → v1 `category` |
|---|---|---|
| Commissions | Maintenance / any | `Fees` |
| Exchange | any | `Internal Transfer` if both legs are own accounts, else `External Transfer`. If unsure → blank. |
| Transit | any | `Internal Transfer` |
| Transport | any | `Transport` |
| Outings | any | `Food` |
| Groceries | any | `Food` |
| Food | any | `Food` |
| Family | any | `Family` |
| Ant (= kid allowance) | any | `Family` |
| Dating | any | `Lifestyle` |
| Gifts | any | `Lifestyle` |
| Purchases | any | `Purchases` |
| Utilities | any | `Subscriptions` |
| Subscriptions | any | `Subscriptions` |
| Health | any | `Health` |
| Personal care | any | `Health` |
| Lending | any | `Other Expense` |
| Debt | any | `Other Expense` |
| Education | any | `Tools` |
| Salary | Inflow | `Salary` |
| Interest / Interests | any | `Interest` |
| Gigs | any | `Gigs` |
| No ID | any | blank (user must decide) |
| `(blank)` | `(blank)` | blank |

For `kind=transfer` rows, almost always `Internal Transfer`. Only use
`External Transfer` if the description clearly names a different person
(e.g. "Cambio $700 efectivo Jorge" where Jorge is an external party).

For `kind=income` on Binance that isn't Salary/Interest, use
`Other Income`.

### Workflow

Do these steps in order:

**Step 1 — slice.** Run this bash (I've tested it; don't modify):

```bash
rm -rf slices && mkdir slices
awk -v n=100 'NR==1{h=$0; next} {
  idx = int((NR-2)/n) + 1;
  file = sprintf("slices/slice_%03d.csv", idx);
  if (!(file in seen)) { print h > file; seen[file]=1 }
  print >> file
}' needs_review.csv
ls slices/
```

That yields `slice_001.csv` … `slice_010.csv`, 100 rows each (last one
<= 100). Header row is replicated in each slice.

**Step 2 — spawn parallel subagents.** Launch **one subagent per slice
file** in a single message (multiple `Task` tool calls in one
response). Each subagent gets the SUBAGENT TASK prompt below with
`{{slice_path}}` substituted to that subagent's slice.

Use `subagent_type: general-purpose`. Do NOT use a specialized agent.

**Step 3 — merge.** After every subagent returns, concatenate:

```bash
head -n 1 slices/slice_001.csv > needs_review.csv
for f in slices/slice_*.csv; do tail -n +2 "$f" >> needs_review.csv; done
wc -l needs_review.csv
```

Should print `935 needs_review.csv` (header + 934 rows).

**Step 4 — report.** Tell me:
- How many rows got a `category` filled (non-blank)
- How many stayed blank (I'll handle those manually)
- Any rows where the legacy columns or description looked wrong/
  confusing — call them out by `id`

---

### SUBAGENT TASK (copy into every parallel subagent)

```
You are filling the `category` column of a CSV slice at {{slice_path}}.

STRICT RULES:
1. Read {{slice_path}}.
2. For every data row, decide the `category` value based on:
   - `kind` (determines which categories are valid — see taxonomy)
   - `suggested_category` (if non-empty, use it unchanged)
   - `legacy_sub_category` + `legacy_category` (translate via hints)
   - `description` (tiebreaker)
3. Write the modified CSV back to the SAME path {{slice_path}}, preserving:
   - Column order (exactly: id, occurred_at, source, kind, amount,
     currency, description, suggested_category, category, user_rate,
     legacy_sub_category, legacy_category)
   - Header row unchanged
   - All other columns unchanged on every row
   - CSV quoting (use csv module, quote fields containing commas)
4. NEVER modify id, amount, description, etc. Only the `category` cell.
5. Blank category is a valid answer when unsure. Correctness > coverage.

v1 TAXONOMY (category must be valid for the row's kind):
- income:    Salary | Gigs | Interest | Other Income
- expense:   Food | Transport | Health | Family | Lifestyle |
             Subscriptions | Purchases | Fees | Tools | Other Expense
- transfer:  Internal Transfer | External Transfer
- adjustment: Reconciliation | FX Diff

LEGACY → v1 MAPPING HINTS:
- Commissions → Fees
- Transit → Internal Transfer
- Exchange → Internal Transfer (if own-account move) else External Transfer;
             if unsure, blank
- Transport → Transport
- Groceries / Food / Outings → Food
- Family / Ant → Family
- Dating / Gifts → Lifestyle
- Purchases → Purchases
- Utilities / Subscriptions → Subscriptions
- Health / Personal care → Health
- Lending / Debt → Other Expense
- Education → Tools
- Salary (Inflow) → Salary
- Interest → Interest
- Gigs → Gigs
- No ID → blank
- blank/NA → blank

For kind=transfer rows, almost always Internal Transfer unless the
description names an external party (e.g. a person's name as recipient).

When you finish, respond with:
- Number of rows you filled (non-blank category)
- Number left blank
- Any specific row ids you flagged as suspicious

Do NOT respond with the full CSV contents. Just the summary.
```

---

Start now. Step 1 first, then parallel Step 2.
