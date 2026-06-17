-- Migration 140: silver.corn_products + gold.corn_products_wide
-- =============================================================================
-- silver.corn_products: monthly, canonical-unit, long/tidy. Populated by the
-- transform src/transforms/build_corn_products_monthly.py (hybrid
-- monthlyization: annual/MY controls allocated across months proportional to
-- the gold.corn_grind_monthly wet-mill driver, true-up automatic).
--
-- product -> corn_products tab column (gold view):
--   corn_for_hfcs            -> B   (mil bu)
--   corn_for_glucose_dextrose-> C
--   corn_for_starch          -> D
--   corn_for_cereals_other   -> E
--   hfcs_42                  -> F   (000 st dry)
--   hfcs_55                  -> G
--   glucose                  -> I
--   dextrose                 -> J
--   corn_starch              -> K   (000 st; derived = corn_for_starch x 15.75)
-- L/M (flour-meal-grits, hominy) intentionally absent: no yields supplied.
-- N-P stocks are tab placeholders. H/Q/R/S are in-sheet formulas.
-- =============================================================================

CREATE TABLE IF NOT EXISTS silver.corn_products (
    obs_date    DATE NOT NULL,            -- first of month
    product     TEXT NOT NULL,
    measure     TEXT NOT NULL,            -- corn_input | production
    value       NUMERIC,
    unit        TEXT,
    is_derived  BOOLEAN NOT NULL DEFAULT FALSE,
    confidence  TEXT NOT NULL DEFAULT 'medium',   -- high | medium | low
    vintage     DATE,
    source      TEXT,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (obs_date, product)
);
CREATE INDEX IF NOT EXISTS idx_corn_products_product ON silver.corn_products (product);

CREATE OR REPLACE VIEW gold.corn_products_wide AS
WITH m(product, target_col) AS (VALUES
    ('corn_for_hfcs','B'), ('corn_for_glucose_dextrose','C'),
    ('corn_for_starch','D'), ('corn_for_cereals_other','E'),
    ('hfcs_42','F'), ('hfcs_55','G'), ('glucose','I'), ('dextrose','J'),
    ('corn_starch','K')
)
SELECT EXTRACT(YEAR FROM s.obs_date)::int  AS year,
       EXTRACT(MONTH FROM s.obs_date)::int AS month,
       s.obs_date AS month_date,
       m.target_col,
       s.unit,
       s.confidence,
       ROUND(s.value::numeric, 4) AS display_value
FROM silver.corn_products s
JOIN m ON s.product = m.product
WHERE s.value IS NOT NULL;
