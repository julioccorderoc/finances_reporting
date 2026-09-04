-- 024: what a row was before it became a transfer leg (ADR-022 amendment).
--
-- `transfers._promote_to_transfer` overwrites `kind` and `needs_review` with
-- a raw UPDATE. Until now it recorded nothing, so ADR-022's delete refused
-- every paired row -- "the pair has to be broken first, and no surface does
-- that yet" -- and no surface could exist, because breaking a pair means
-- putting back values nothing remembered.
--
-- `transaction_edits` is the wrong home twice over: migration 009 constrains
-- its `field` to ('category_id','user_rate','notes'), and that table is the
-- owner's edit history, shown in the modal. A machine promotion is not an
-- edit the owner made.
--
-- `prior_user_rate` is here because a conversion writes a struck rate on the
-- row it is pairing (ADR-015). Without it, cancelling a conversion left the
-- row priced at exactly the figure the owner had just rejected -- forever,
-- and invisibly, since nothing on the row says where the rate came from.
--
-- One row per LEG, keyed on transaction_id: a transaction belongs to at most
-- one transfer at a time, so a second pre-image for the same leg would make
-- the replay ambiguous. ON DELETE CASCADE keeps a deleted row from stranding
-- its pre-image for some future rowid to inherit.
--
-- Absence is meaningful. The 270 uuid4 pairs that predate this table have no
-- row here and cannot be unpaired: their pre-image is genuinely unknown, and
-- guessing `expense` back from a negative sign would be wrong for every leg
-- an importer created as a transfer in the first place. `unpair` refuses
-- them by design rather than inventing an answer.

CREATE TABLE IF NOT EXISTS transfer_pairings (
    transfer_id        TEXT    NOT NULL,
    transaction_id     INTEGER NOT NULL PRIMARY KEY
                               REFERENCES transactions(id) ON DELETE CASCADE,
    prior_kind         TEXT    NOT NULL,   -- kind before the promotion
    prior_needs_review INTEGER NOT NULL,   -- needs_review before the promotion
    prior_user_rate    TEXT,               -- user_rate before the promotion, if any
    created_at         TEXT    NOT NULL    -- UTC ISO-8601
);

CREATE INDEX IF NOT EXISTS idx_transfer_pairings_transfer
    ON transfer_pairings(transfer_id);
