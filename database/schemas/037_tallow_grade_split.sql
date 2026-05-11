-- ============================================================================
-- Tallow Grade Split: Edible (EBFT) / Inedible (IBFT)
-- Round Lakes Companies
--
-- Splits the single BFT feedstock code into edible and inedible tallow grades
-- with separate pricing, CI scores, and economic pull logic.
--
-- The EIA Form 819 monthly total tallow consumption is the binding constraint.
-- EBFT + IBFT must reconcile to EIA total tallow for each period.
--
-- Usage: psql -U postgres -d rlc_commodities -f 037_tallow_grade_split.sql
-- ============================================================================

-- ═══════════════════════════════════════════════════════════════════════════
-- 1. ADD EBFT / IBFT TO FEEDSTOCK PROPERTIES
-- ═══════════════════════════════════════════════════════════════════════════

-- Edible tallow (bleachable fancy / packer grade)
INSERT INTO reference.feedstock_properties
    (feedstock_code, feedstock_name, category,
     lbs_per_gal_hefa, lbs_per_gal_transester, lbs_per_gal_coprocessing,
     ci_score_default, sort_order)
VALUES
    ('EBFT', 'Edible Tallow', 'animal_fat',
     9.38, 7.75, 7.75,
     26.0,   -- gCO2e/MJ — slightly higher CI than inedible (food-grade processing)
     4)      -- sort between DCO and CWG
ON CONFLICT (feedstock_code) DO UPDATE SET
    feedstock_name = EXCLUDED.feedstock_name,
    category = EXCLUDED.category,
    lbs_per_gal_hefa = EXCLUDED.lbs_per_gal_hefa,
    lbs_per_gal_transester = EXCLUDED.lbs_per_gal_transester,
    lbs_per_gal_coprocessing = EXCLUDED.lbs_per_gal_coprocessing,
    ci_score_default = EXCLUDED.ci_score_default,
    sort_order = EXCLUDED.sort_order,
    updated_at = NOW();

-- Inedible tallow (technical / renderer grade)
INSERT INTO reference.feedstock_properties
    (feedstock_code, feedstock_name, category,
     lbs_per_gal_hefa, lbs_per_gal_transester, lbs_per_gal_coprocessing,
     ci_score_default, sort_order)
VALUES
    ('IBFT', 'Inedible Tallow', 'animal_fat',
     9.50, 7.85, 7.85,
     22.0,   -- gCO2e/MJ — lower CI (waste-derived, no food-system allocation)
     5)      -- right after edible tallow
ON CONFLICT (feedstock_code) DO UPDATE SET
    feedstock_name = EXCLUDED.feedstock_name,
    category = EXCLUDED.category,
    lbs_per_gal_hefa = EXCLUDED.lbs_per_gal_hefa,
    lbs_per_gal_transester = EXCLUDED.lbs_per_gal_transester,
    lbs_per_gal_coprocessing = EXCLUDED.lbs_per_gal_coprocessing,
    ci_score_default = EXCLUDED.ci_score_default,
    sort_order = EXCLUDED.sort_order,
    updated_at = NOW();

-- Mark legacy BFT as parent/total code
UPDATE reference.feedstock_properties
SET feedstock_name = 'Tallow (Combined — legacy)',
    sort_order = 99
WHERE feedstock_code = 'BFT';


-- ═══════════════════════════════════════════════════════════════════════════
-- 2. TALLOW GRADE PARAMETERS — Configurable per grade × pathway
-- ═══════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS reference.tallow_grade_params (
    id                  SERIAL PRIMARY KEY,
    grade_code          VARCHAR(10) NOT NULL,     -- EBFT, IBFT
    pathway             VARCHAR(30) NOT NULL,     -- hefa, transesterification, coprocessing

    -- CI score for this grade in this pathway (gCO2e/MJ)
    ci_score_gco2e_mj   NUMERIC(6,2) NOT NULL,

    -- Conversion rate (lbs feedstock per gallon product) — cross-check vs feedstock_properties
    lbs_per_gal         NUMERIC(6,3),

    -- Supply constraints (million lbs / month, US total)
    -- Non-BBD demand that constrains availability
    food_export_floor_mil_lbs   NUMERIC(10,2),   -- Min committed to food/oleo/export
    max_bbd_share_pct           NUMERIC(5,2),    -- Max % of total grade production going to BBD

    -- Default allocation split when economics are ambiguous
    default_bbd_split_pct       NUMERIC(5,2),    -- % of total EIA tallow assigned to this grade

    notes               TEXT,
    updated_at          TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (grade_code, pathway)
);

COMMENT ON TABLE reference.tallow_grade_params IS
    'Configurable parameters for edible/inedible tallow allocation. CI scores, supply constraints, and default splits.';

-- Seed with initial estimates
-- US edible tallow: ~3.5B lbs/yr production, ~40% available for BBD after food/export
-- US inedible tallow: ~3.0B lbs/yr production, ~65% available for BBD
INSERT INTO reference.tallow_grade_params
    (grade_code, pathway, ci_score_gco2e_mj, lbs_per_gal,
     food_export_floor_mil_lbs, max_bbd_share_pct, default_bbd_split_pct, notes)
VALUES
    -- HEFA (renewable diesel)
    ('EBFT', 'hefa', 26.0, 9.38,
     175.0,   -- ~2.1B lbs/yr ÷ 12 committed to food/oleo/export
     0.45,    -- max 45% of edible tallow production to BBD
     0.35,    -- default: 35% of EIA total tallow is edible
     'Edible tallow in RD. Higher CI due to food-system allocation. Competes with food/oleo/export demand.'),

    ('IBFT', 'hefa', 22.0, 9.50,
     65.0,    -- ~780M lbs/yr ÷ 12 committed to soap/pet food/industrial
     0.75,    -- max 75% of inedible production to BBD
     0.65,    -- default: 65% of EIA total tallow is inedible
     'Inedible tallow in RD. Lower CI = better credits. Less competition from non-BBD uses.'),

    -- Transesterification (biodiesel)
    ('EBFT', 'transesterification', 26.0, 7.75,
     175.0, 0.45, 0.35,
     'Edible tallow in BD. Same food/export competition as HEFA pathway.'),

    ('IBFT', 'transesterification', 22.0, 7.85,
     65.0, 0.75, 0.65,
     'Inedible tallow in BD.'),

    -- Co-processing
    ('EBFT', 'coprocessing', 26.0, 7.75,
     175.0, 0.45, 0.35,
     'Edible tallow in co-processing.'),

    ('IBFT', 'coprocessing', 22.0, 7.85,
     65.0, 0.75, 0.65,
     'Inedible tallow in co-processing.')
ON CONFLICT (grade_code, pathway) DO UPDATE SET
    ci_score_gco2e_mj = EXCLUDED.ci_score_gco2e_mj,
    lbs_per_gal = EXCLUDED.lbs_per_gal,
    food_export_floor_mil_lbs = EXCLUDED.food_export_floor_mil_lbs,
    max_bbd_share_pct = EXCLUDED.max_bbd_share_pct,
    default_bbd_split_pct = EXCLUDED.default_bbd_split_pct,
    notes = EXCLUDED.notes,
    updated_at = NOW();


-- ═══════════════════════════════════════════════════════════════════════════
-- 3. TALLOW IMPLIED VALUE — Monthly economic calculation
-- ═══════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS silver.tallow_implied_value (
    id                  SERIAL PRIMARY KEY,
    period              DATE NOT NULL,            -- First of month
    grade_code          VARCHAR(10) NOT NULL,     -- EBFT, IBFT
    pathway             VARCHAR(30) NOT NULL,     -- hefa, transesterification, coprocessing
    fuel_type           VARCHAR(20) NOT NULL,     -- biodiesel, renewable_diesel, saf, coprocessing

    -- Revenue stack ($/gal)
    fuel_price_per_gal      NUMERIC(10,4),
    rin_value_per_gal       NUMERIC(10,4),
    lcfs_value_per_gal      NUMERIC(10,4),        -- CI-dependent: lower CI → higher credit
    btc_45z_per_gal         NUMERIC(10,4),        -- CI-dependent for 45Z
    state_credit_per_gal    NUMERIC(10,4),
    total_revenue_per_gal   NUMERIC(10,4),

    -- Cost ($/gal, excluding feedstock)
    processing_cost_per_gal NUMERIC(10,4),

    -- The implied feedstock value calculation
    lbs_per_gal             NUMERIC(6,3),
    implied_value_per_lb    NUMERIC(10,6),        -- Max price a BBD producer can pay
    market_price_per_lb     NUMERIC(10,6),        -- Actual market price for this grade

    -- Economic pull signal
    margin_spread_per_lb    NUMERIC(10,6),        -- implied - market (>0 = pull, <0 = priced out)
    pull_flag               VARCHAR(20),          -- 'economic', 'priced_out', 'marginal'

    -- CI score used in this calculation
    ci_score_used           NUMERIC(6,2),

    run_id                  UUID,
    created_at              TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (period, grade_code, pathway, fuel_type, run_id)
);

CREATE INDEX IF NOT EXISTS idx_tiv_period ON silver.tallow_implied_value(period);
CREATE INDEX IF NOT EXISTS idx_tiv_grade ON silver.tallow_implied_value(grade_code);

COMMENT ON TABLE silver.tallow_implied_value IS
    'Monthly implied max feedstock value for each tallow grade. Drives the edible/inedible allocation split.';


-- ═══════════════════════════════════════════════════════════════════════════
-- 4. TALLOW ALLOCATION RESULT — Monthly split reconciled to EIA
-- ═══════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS gold.tallow_allocation_detail (
    id                  SERIAL PRIMARY KEY,
    period              DATE NOT NULL,            -- First of month
    run_id              UUID,

    -- EIA guardrail
    eia_total_tallow_mil_lbs    NUMERIC(12,4),    -- From EIA Form 819 (the constraint)

    -- Allocation result
    grade_code          VARCHAR(10) NOT NULL,     -- EBFT, IBFT
    allocated_mil_lbs   NUMERIC(12,4),           -- Volume allocated to this grade
    allocated_pct       NUMERIC(5,2),            -- % of EIA total

    -- Economics that drove the allocation (best pathway for this grade)
    best_pathway        VARCHAR(30),
    implied_value_per_lb    NUMERIC(10,6),
    market_price_per_lb     NUMERIC(10,6),
    margin_spread_per_lb    NUMERIC(10,6),

    -- Constraint info
    allocation_method   VARCHAR(30),             -- 'economic_pull', 'supply_constrained', 'default_split'
    constraint_notes    TEXT,                     -- Human-readable explanation of what drove the split

    created_at          TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (period, run_id, grade_code)
);

CREATE INDEX IF NOT EXISTS idx_tad_period ON gold.tallow_allocation_detail(period);

COMMENT ON TABLE gold.tallow_allocation_detail IS
    'Monthly tallow allocation split between edible and inedible, reconciled to EIA Form 819 total.';


-- ═══════════════════════════════════════════════════════════════════════════
-- 5. GOLD VIEW — Tallow allocation economics summary
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW gold.tallow_allocation_economics AS
WITH latest_run AS (
    SELECT run_id
    FROM gold.tallow_allocation_detail
    ORDER BY created_at DESC
    LIMIT 1
),
detail AS (
    SELECT *
    FROM gold.tallow_allocation_detail
    WHERE run_id = (SELECT run_id FROM latest_run)
),
edible AS (
    SELECT period, allocated_mil_lbs, allocated_pct,
           implied_value_per_lb, market_price_per_lb, margin_spread_per_lb,
           best_pathway, allocation_method, constraint_notes
    FROM detail WHERE grade_code = 'EBFT'
),
inedible AS (
    SELECT period, allocated_mil_lbs, allocated_pct,
           implied_value_per_lb, market_price_per_lb, margin_spread_per_lb,
           best_pathway, allocation_method, constraint_notes
    FROM detail WHERE grade_code = 'IBFT'
)
SELECT
    COALESCE(e.period, i.period)                        AS period,
    d2.eia_total_tallow_mil_lbs                         AS eia_total_mil_lbs,

    -- Edible tallow
    e.allocated_mil_lbs                                 AS ebft_mil_lbs,
    e.allocated_pct                                     AS ebft_pct,
    e.implied_value_per_lb                              AS ebft_implied_value_lb,
    e.market_price_per_lb                               AS ebft_market_price_lb,
    e.margin_spread_per_lb                              AS ebft_spread_lb,
    e.best_pathway                                      AS ebft_pathway,
    e.allocation_method                                 AS ebft_method,

    -- Inedible tallow
    i.allocated_mil_lbs                                 AS ibft_mil_lbs,
    i.allocated_pct                                     AS ibft_pct,
    i.implied_value_per_lb                              AS ibft_implied_value_lb,
    i.market_price_per_lb                               AS ibft_market_price_lb,
    i.margin_spread_per_lb                              AS ibft_spread_lb,
    i.best_pathway                                      AS ibft_pathway,
    i.allocation_method                                 AS ibft_method,

    -- Check: do they sum to EIA total?
    ROUND(COALESCE(e.allocated_mil_lbs, 0) + COALESCE(i.allocated_mil_lbs, 0), 4)
                                                        AS check_sum_mil_lbs,
    e.constraint_notes                                  AS ebft_notes,
    i.constraint_notes                                  AS ibft_notes

FROM edible e
FULL OUTER JOIN inedible i ON e.period = i.period
LEFT JOIN LATERAL (
    SELECT eia_total_tallow_mil_lbs
    FROM detail d3
    WHERE d3.period = COALESCE(e.period, i.period)
    LIMIT 1
) d2 ON TRUE
ORDER BY COALESCE(e.period, i.period);

COMMENT ON VIEW gold.tallow_allocation_economics IS
    'Monthly tallow allocation summary: EIA total, EBFT/IBFT split, implied values, spreads. EIA total is binding constraint.';


-- ═══════════════════════════════════════════════════════════════════════════
-- 6. GRANTS
-- ═══════════════════════════════════════════════════════════════════════════

GRANT ALL ON ALL TABLES IN SCHEMA reference TO postgres;
GRANT ALL ON ALL TABLES IN SCHEMA silver TO postgres;
GRANT ALL ON ALL TABLES IN SCHEMA gold TO postgres;
GRANT ALL ON ALL SEQUENCES IN SCHEMA reference TO postgres;
GRANT ALL ON ALL SEQUENCES IN SCHEMA silver TO postgres;
GRANT ALL ON ALL SEQUENCES IN SCHEMA gold TO postgres;
