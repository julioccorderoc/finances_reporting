
## Amendment 2026-09-04 — Breaking a pair, the gap §3 named as next

**Context:** §2.3 refuses to delete a paired row, and §3 says why the right
fix for a wrong pairing is "to break the pair and keep the two real rows",
noting it as the next gap. That gap was not merely unbuilt — it was
*unbuildable*. `transfers._promote_to_transfer` overwrote `kind` and
`needs_review` with a raw UPDATE and recorded neither, so breaking a pair
could only guess them back, and guessing is wrong for the many legs an
importer created as `kind='transfer'` in the first place. Meanwhile the
viewer gained a control (Became cash) that *creates* pairs, which would have
made the one-way door reachable from a footer button on 1,492 rows.

**Amendment:** `finances.domain.transfers.unpair` breaks a transfer back into
the two rows it was made from. Migration 024 (`transfer_pairings`) records
each leg's pre-image — `kind`, `needs_review`, `user_rate` — at promotion
time; `unpair` replays it, clears `transfer_id` on every leg, and consumes
the pre-image so it cannot be replayed twice.

**Invariants:**

- **Unpair never deletes.** Both rows stay. The orphan is then an ordinary
  unpaired row, and §2's delete — with its tombstone rules — takes it from
  there. Deletion happens in one place under one set of rules; pairing does
  not grow a second delete path. The viewer's toast says so explicitly,
  because an owner who is not told assumes the row has gone and leaves a
  stray leg inflating a balance nobody checks.
- **No pre-image, no unpair.** The 270 uuid4 pairs that predate migration
  024, and every pair an importer authored, are refused with that reason. A
  leg born `kind='transfer'` never was an expense; restoring it to one
  because its amount is negative would invent history, not recover it.
- **`user_rate` is part of the pre-image, not an afterthought.** A cash
  conversion writes the price it was struck at (ADR-015). Without restoring
  it, a cancelled conversion left the row priced at exactly the figure the
  owner had just rejected — permanently, and invisibly, since nothing on the
  row says where a rate came from. The pre-image is *what the row was when it
  was paired*, so a rate edited after pairing is reverted too;
  `transaction_edits` keeps the trail either way.
- **Recording is non-destructive.** The insert is `ON CONFLICT DO NOTHING`:
  re-promoting an already-promoted row must not overwrite the truth with
  `prior_kind='transfer'`. The first write is the only honest one.
- **The ledger's own corrections are refused**, matching §2.3 — an ADR-020
  opening pair is restated through `record_opening`, never broken from a
  footer button.

**Consequence:** with unpair in place, §2.3's refusal stops being a dead end.
The message it gives ("the pair has to be broken first") now names something
the owner can actually do.
