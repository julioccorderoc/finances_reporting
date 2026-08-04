-- 017_amount_scoped_rules.sql
--
-- Two additions, both aimed at the same thing: rows reaching the triage
-- queue that never needed a human.
--
-- 1. category_rules gains optional amount bounds.
--
--    ADR-006 describes an open-ended priority chain, but a rule could only
--    ever match on description text and scope. The owner's actual rule for
--    salary is "a Binance deposit over $1,000", and the description is
--    identical either way ("Binance deposit USDC"), so it was inexpressible
--    and every payslip went to triage.
--
--    Bounds are TEXT for the same reason transactions.amount is: money never
--    passes through a float (ADR-009). Both are NULL by default, so every
--    existing rule keeps its exact current behaviour.
--
-- 2. Two rules that should have existed from the start.
--
--    A P2P sell moves value between the owner's own accounts — USDT out,
--    bolívares into Provincial. It is not spending, and filing it as an
--    uncategorised expense put 97 rows in the queue and inflated reported
--    spending by their gross. Categorising it under Internal Transfer (a
--    kind='transfer' category) is how money movement is declared on a row
--    that is not itself kind='transfer' — see money.SQL_NOT_CURRENCY_MOVEMENT.
--
--    The pattern is anchored on "P2P SELL", the shape finances/ingest/binance.py
--    writes, so a P2P BUY is deliberately not matched: a buy brings value in
--    and is a different question.

ALTER TABLE category_rules ADD COLUMN min_amount TEXT;
ALTER TABLE category_rules ADD COLUMN max_amount TEXT;

-- Priority 20: below the hand-written provincial rules (10) so those still
-- win, above the generic tier.
INSERT INTO category_rules (pattern, category_id, source, priority, active, min_amount)
SELECT
    'P2P SELL',
    (SELECT id FROM categories WHERE kind = 'transfer' AND name = 'Internal Transfer'),
    'binance',
    20,
    1,
    NULL
WHERE NOT EXISTS (
    SELECT 1 FROM category_rules WHERE pattern = 'P2P SELL' AND source = 'binance'
);

-- Salary: a Binance deposit at or above 1000, in absolute terms. Matching is
-- on the absolute amount so the bound reads the same for an inflow as it
-- would for an outflow.
INSERT INTO category_rules (pattern, category_id, source, priority, active, min_amount)
SELECT
    'Binance deposit',
    (SELECT id FROM categories WHERE kind = 'income' AND name = 'Salary'),
    'binance',
    20,
    1,
    '1000'
WHERE NOT EXISTS (
    SELECT 1 FROM category_rules
     WHERE pattern = 'Binance deposit' AND source = 'binance'
);
