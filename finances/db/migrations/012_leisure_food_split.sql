-- 012_leisure_food_split.sql
-- Taxonomy amendment (ADR-006, owner decision 2026-07-21).
--
-- `Leisure` is redefined as NON-FOOD recreation only. It had been the
-- legacy backfill bucket for going-out food (migration 004 comment:
-- "going-out food now lives under Leisure"), but migration 007 added
-- `Going Out` for exactly that. Two categories, one meaning.
--
-- New lines:
--     Leisure    non-food recreation / experiences (tours, events, cinema)
--     Going Out  food + drink consumed out
--     Groceries  food consumed at home
--
-- Only mechanically unambiguous rows move. Named food merchants go to
-- `Going Out`; supermarkets and butchers go to `Groceries`. Everything
-- else stays in `Leisure` for hand triage: pago movil (`CAR.DRV*`), bank
-- transfers (`DR OB *`), bare person names, `EVENTOS RZEMIEN CA`,
-- `DOGGO53 C A`, and the two positive-amount rows (`TRAV…`, `ABO.DRV…`)
-- that are inflows mis-filed under an expense category. Per rule-006 and
-- migration 006, merchant names alone do not encode intent — those rows
-- need the owner's memory of the spend, not a pattern.
--
-- Matching is by exact description AND current category = Leisure, so:
--   * rows the owner already hand-tagged elsewhere (a pizzeria visit that
--     was a date) are never touched;
--   * re-running is a no-op — after the first pass nothing is in Leisure
--     with these descriptions anymore.
--
-- `needs_review` is deliberately left alone: these rows are being
-- corrected, not un-categorized, and must not re-enter the triage queue.

-- Named food merchants -> Going Out.
UPDATE transactions
   SET category_id = (SELECT id FROM categories WHERE kind = 'expense' AND name = 'Going Out'),
       updated_at  = CURRENT_TIMESTAMP
 WHERE category_id = (SELECT id FROM categories WHERE kind = 'expense' AND name = 'Leisure')
   AND description IN (
        'LUNCHERIA MILY GOURMET',
        'EL GRAN HORNO CA',
        'GUILLEN PASTELES Y EMPAN',
        'PICA FOOD CA',
        'PICA FOOD C A',
        'PAN PAST PIZZE Y CHAR',
        'CUATRO TENEDORES GASTR',
        'CAFFE NOVENTA',
        'NEGRONI GROUP 25',
        'LA PANADERIA 2025 CA',
        'AL CARBON C A',
        'LOS CHINOS LAW C A',
        'PIZZERIA PERTUTTI CA',
        'HELADERIA EFE',
        'PIZZA DE VERDAD',
        'VAMOS PA QUE MENCHO'
   );

-- Supermarkets / butchers -> Groceries (home food, never "going out").
UPDATE transactions
   SET category_id = (SELECT id FROM categories WHERE kind = 'expense' AND name = 'Groceries'),
       updated_at  = CURRENT_TIMESTAMP
 WHERE category_id = (SELECT id FROM categories WHERE kind = 'expense' AND name = 'Leisure')
   AND description IN (
        'HIPERMERCADO KARI C,A',
        'HIPERMERCADO KARI C.A',
        'MI SUPER, C.A',
        'FIGUEROA MINI ABASTO Y V',
        'CARNICER Y CHARCUTER T'
   );
