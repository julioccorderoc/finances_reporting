-- 007_going_out_and_bank_fee_rules.sql
-- Maintenance amendment (2026-07-11), two parts:
--
-- 1. Add expense category `Going Out` — eating out, drinks, bars, cafés.
--    Julio's taxonomy had no home for social/dining-out spend (distinct from
--    `Groceries`). Category only; no merchant auto-rules (merchant names are
--    context-dependent per 006_prune_ambiguous_rules — Julio tags these by
--    hand in the viewer).
--
-- 2. Extend the bank-fee (Fees) rules. The seed only matched `COM\. PAGO
--    MOVIL`, `COM\. .*IVR`, and `COM\. MANTENIMIENTO`, but the Provincial
--    export writes several other bank-commission strings that were landing as
--    needs_review:
--       COM. PAGO MOV PB     (pago móvil fee — abbreviated, no "MOVIL")
--       COM.MTTO.CTA.        (mantenimiento de cuenta — "MTTO", not "MANTEN…")
--       COM.EM.EDO.CTA       (emisión estado de cuenta — statement fee)
--       COM.RZO.PAG.MOVIL    (reverso pago móvil commission)
--       COM.REF.BANC.        (referencia bancaria fee)
--       REVERSO CARGO        (bank-initiated charge reversal — treat as a fee)
--    All are bank-written commission markers (context-free per rule-006), so
--    they belong to the auto-categorization tier. Patterns are anchored on
--    `COM\.` / the exact `REVERSO CARGO` string so they never match merchant
--    names like `COMERCIAL LA 11` or `COM NASHIRA ESTIVANELI`.
--
-- INSERT OR IGNORE so re-running on a DB that already has the category is a
-- no-op (the migration runner tracks applied files, but stay defensive).

INSERT OR IGNORE INTO categories (kind, name) VALUES
    ('expense', 'Going Out');

INSERT INTO category_rules (pattern, category_id, source, priority) VALUES
    ('COM\. PAGO MOV',
        (SELECT id FROM categories WHERE kind='expense' AND name='Fees'),
        'provincial', 10),
    ('COM\.MTTO\.CTA',
        (SELECT id FROM categories WHERE kind='expense' AND name='Fees'),
        'provincial', 10),
    ('COM\.EM\.EDO\.CTA',
        (SELECT id FROM categories WHERE kind='expense' AND name='Fees'),
        'provincial', 10),
    ('COM\.RZO\.PAG\.MOVIL',
        (SELECT id FROM categories WHERE kind='expense' AND name='Fees'),
        'provincial', 10),
    ('COM\.REF\.BANC',
        (SELECT id FROM categories WHERE kind='expense' AND name='Fees'),
        'provincial', 10),
    ('REVERSO CARGO',
        (SELECT id FROM categories WHERE kind='expense' AND name='Fees'),
        'provincial', 10);
