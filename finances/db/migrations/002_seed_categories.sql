-- 002_seed_categories.sql
-- EPIC-004: seed v1 category taxonomy + baseline description-based rules.
-- Taxonomy comes verbatim from ADR-006 §Decision. Rule patterns come from
-- recurring shapes observed in data/Finanzas - Provincial.csv and
-- data/Finanzas - Binance.csv. Patterns are case-insensitive (handled by the
-- engine in finances/domain/categorization.py, not by SQL).
--
-- Per rule-006, new categories or rules must land as a forward migration;
-- ad-hoc INSERTs from application code are forbidden. Ingesters never set
-- category_id directly.
--
-- INSERT OR IGNORE so re-running on a DB that already has a subset of rows
-- (e.g. from a partial earlier migration) is safe. The migration runner's
-- idempotency comes from _migrations, but defense-in-depth doesn't hurt.

--------------------------------------------------------------------------------
-- v1 taxonomy (ADR-006 §Decision)
--------------------------------------------------------------------------------
INSERT OR IGNORE INTO categories (kind, name) VALUES
    -- income
    ('income', 'Salary'),
    ('income', 'Gigs'),
    ('income', 'Interest'),
    ('income', 'Other Income'),
    -- expense
    ('expense', 'Food'),
    ('expense', 'Transport'),
    ('expense', 'Health'),
    ('expense', 'Family'),
    ('expense', 'Lifestyle'),
    ('expense', 'Subscriptions'),
    ('expense', 'Purchases'),
    ('expense', 'Fees'),
    ('expense', 'Tools'),
    ('expense', 'Other Expense'),
    ('expense', 'Dating'),
    ('expense', 'Gifts'),
    -- transfer
    ('transfer', 'Internal Transfer'),
    ('transfer', 'External Transfer'),
    -- adjustment
    ('adjustment', 'Reconciliation'),
    ('adjustment', 'FX Diff');

--------------------------------------------------------------------------------
-- Baseline description → category rules.
--
-- Priority convention (lower wins):
--   10  — high-confidence, source-scoped, recurring patterns
--   50  — cross-source common patterns
--  100  — default fallback bucket-level heuristics
--
-- Patterns are Python-compatible regex; case-insensitive is applied by the
-- engine. Keep patterns narrow — a false positive on a $1,000 row is more
-- expensive than a miss.
--------------------------------------------------------------------------------

-- ---- Provincial (VES bank) --------------------------------------------------

-- Bank commissions / pago móvil fees.
INSERT OR IGNORE INTO category_rules (pattern, category_id, source, priority) VALUES
    ('COM\. PAGO MOVIL',
        (SELECT id FROM categories WHERE kind='expense' AND name='Fees'),
        'provincial', 10),
    ('COM\. .*IVR',
        (SELECT id FROM categories WHERE kind='expense' AND name='Fees'),
        'provincial', 10),
    ('COM\. MANTENIMIENTO|MANTENIMIENTO CTA',
        (SELECT id FROM categories WHERE kind='expense' AND name='Fees'),
        'provincial', 10);

-- Food / bakeries / lunch spots / groceries.
INSERT OR IGNORE INTO category_rules (pattern, category_id, source, priority) VALUES
    ('PANADERIA|PANADERÍA',
        (SELECT id FROM categories WHERE kind='expense' AND name='Food'),
        'provincial', 10),
    ('LUNCHERIA|LUNCHERÍA',
        (SELECT id FROM categories WHERE kind='expense' AND name='Food'),
        'provincial', 10),
    ('HIPERMERCADO|SUPERMERCADO|AUTOMERCADO',
        (SELECT id FROM categories WHERE kind='expense' AND name='Food'),
        'provincial', 10),
    ('EL GRAN HORNO|HORNO',
        (SELECT id FROM categories WHERE kind='expense' AND name='Food'),
        'provincial', 10);

-- Transport (Uber / rides encoded as CAR.DRV####).
INSERT OR IGNORE INTO category_rules (pattern, category_id, source, priority) VALUES
    ('CAR\.DRV\d+',
        (SELECT id FROM categories WHERE kind='expense' AND name='Transport'),
        'provincial', 10);

-- Phone / telecom subscriptions.
INSERT OR IGNORE INTO category_rules (pattern, category_id, source, priority) VALUES
    ('DIGITEL|MOVISTAR|MOVILNET|CANTV',
        (SELECT id FROM categories WHERE kind='expense' AND name='Subscriptions'),
        'provincial', 10);

-- Outbound bank transfers (DR OB ... to other institutions) → External
-- Transfer, not lumped as an expense.
INSERT OR IGNORE INTO category_rules (pattern, category_id, source, priority) VALUES
    ('DR OB .* 191NAC',
        (SELECT id FROM categories WHERE kind='transfer' AND name='External Transfer'),
        'provincial', 50),
    ('DR OB .* BANCA',
        (SELECT id FROM categories WHERE kind='transfer' AND name='External Transfer'),
        'provincial', 50);

-- Loan repayment (Cashea / préstamo).
INSERT OR IGNORE INTO category_rules (pattern, category_id, source, priority) VALUES
    ('cuota cashea|cashea',
        (SELECT id FROM categories WHERE kind='expense' AND name='Purchases'),
        'provincial', 50),
    ('[Pp]réstamo',
        (SELECT id FROM categories WHERE kind='transfer' AND name='External Transfer'),
        'provincial', 50);

-- ---- Binance ----------------------------------------------------------------

-- Earn rewards / flexible earnings are interest income.
INSERT OR IGNORE INTO category_rules (pattern, category_id, source, priority) VALUES
    ('[Ee]arn reward|[Ff]lexible earning|Simple Earn',
        (SELECT id FROM categories WHERE kind='income' AND name='Interest'),
        'binance', 10);

-- Internal Binance tier transfers (funding ↔ spot) identified by memo text
-- before create_transfer overrides kind='transfer' explicitly.
INSERT OR IGNORE INTO category_rules (pattern, category_id, source, priority) VALUES
    ('User Transfer|External Scenario',
        (SELECT id FROM categories WHERE kind='transfer' AND name='Internal Transfer'),
        'binance', 10);

-- Salary / paycheck / bonus keywords.
INSERT OR IGNORE INTO category_rules (pattern, category_id, source, priority) VALUES
    ('[Pp]aycheck|[Ss]alary|[Nn]et:\s*SOL',
        (SELECT id FROM categories WHERE kind='income' AND name='Salary'),
        'binance', 10),
    ('[Bb]onus',
        (SELECT id FROM categories WHERE kind='income' AND name='Salary'),
        'binance', 50);

-- Subscriptions captured in Binance memo lines.
INSERT OR IGNORE INTO category_rules (pattern, category_id, source, priority) VALUES
    ('[Nn]etflix|[Ss]potify|[Dd]isney|[Aa]pple Music|[Yy]ou[Tt]ube Premium|[Ss]uscripción|[Ss]ubscription',
        (SELECT id FROM categories WHERE kind='expense' AND name='Subscriptions'),
        'binance', 10);

-- P2P ads and cash-conversion memo lines → External Transfer pending pairing
-- by EPIC-006 / EPIC-008's reconciliation pass.
INSERT OR IGNORE INTO category_rules (pattern, category_id, source, priority) VALUES
    ('P2P\s*-?\s*\d+|Cambio.*efectivo',
        (SELECT id FROM categories WHERE kind='transfer' AND name='External Transfer'),
        'binance', 50);

-- ---- Cross-source (source IS NULL) -----------------------------------------

-- Explicit human keywords — catch-alls that apply regardless of source.
INSERT OR IGNORE INTO category_rules (pattern, category_id, priority) VALUES
    ('[Cc]ontrol ortodoncia|[Oo]rtodoncia|[Dd]entista|[Dd]octor|[Ss]alud|[Ff]armacia|[Ff]armatodo|[Ll]ocatel',
        (SELECT id FROM categories WHERE kind='expense' AND name='Health'),
        50),
    ('[Uu]ber|[Dd]idi|[Yy]ummy|[Gg]asolina|[Cc]ombustible',
        (SELECT id FROM categories WHERE kind='expense' AND name='Transport'),
        50);
