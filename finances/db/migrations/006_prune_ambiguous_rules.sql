-- 006_prune_ambiguous_rules.sql
-- ADR-006 refinement (2026-04-20): restrict auto-categorization rules to
-- patterns the user considers context-free. Merchant names (PANADERIA,
-- SUPERMERCADO, HORNO...) are context-dependent — a bakery visit might
-- be a date, a supermarket might be stocking up for a BBQ (Leisure).
-- Transfer descriptions (DR OB ... BANCA, Préstamo) can be gifts,
-- lending, or transit — not reliably External Transfer.
--
-- Keep only:
--   * Bank-written commission markers (COM\.)
--   * Platform-internal markers (Binance Earn, Binance internal transfer)
--   * Definitional subscriptions (Netflix/Spotify/etc are always subs)
--
-- Deactivate rather than DELETE so the patterns stay queryable for
-- future revival without re-typing.

UPDATE category_rules
   SET active = 0
 WHERE id IN (
    4,   -- PANADERIA → Groceries     (could be a date)
    5,   -- LUNCHERIA → Groceries     (could be a date)
    6,   -- HIPERMERCADO/SUPERMERCADO → Groceries (could be BBQ/Leisure)
    7,   -- EL GRAN HORNO → Groceries (same)
    8,   -- CAR.DRV\d+ → Transport    (actually pago móvil, not taxi)
    9,   -- DIGITEL/MOVISTAR → Subscriptions (context — phone, data, prepaid)
    10,  -- DR OB .* 191NAC → External Transfer (could be Dating/Gifts)
    11,  -- DR OB .* BANCA  → External Transfer (could be Lending/Gifts)
    12,  -- cuota cashea → Purchases  (could be Clothing, Tools)
    13,  -- [Pp]réstamo → External Transfer (could be Lending)
    16,  -- paycheck/salary → Salary  (Julio prefers explicit Sub-Category)
    17,  -- bonus → Salary            (could be a gift)
    19,  -- P2P\s*-?\s*\d+ → External Transfer (reconciliation handles)
    20,  -- ortodoncia/doctor/salud → Health (could be family/gifts)
    21   -- uber/didi/yummy/gasolina → Transport (could be a date)
 );
