-- ============================================================================
-- Migration 036: Iowa Crush Agent System tables (per spec §12 step 1)
-- ============================================================================
-- Four tables that wire the Phase 2 facility-agent system:
--   silver.facility_state         — mutable per-facility daily state
--   silver.strategic_plan         — quarterly strategic plan output
--   bronze.daily_decisions        — append-only decision log
--   gold.monthly_crush_estimates  — aggregated monthly output for validation
--
-- All FK to reference.oilseed_crush_facilities(facility_id), which mirrors
-- core.kg_node.node_key for facilities in the KG.
--
-- Spec: docs/iowa_crush_agent_spec.md §6
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 6.2 silver.facility_state
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.facility_state (
    facility_id              TEXT PRIMARY KEY
                             REFERENCES reference.oilseed_crush_facilities(facility_id)
                             ON DELETE CASCADE,
    as_of_date               DATE NOT NULL,

    -- Inventory (bushels for soy, lbs for oil, tons for meal)
    bean_inventory_bu            NUMERIC(14,2) NOT NULL DEFAULT 0,
    oil_inventory_lbs            NUMERIC(14,2) NOT NULL DEFAULT 0,
    meal_inventory_tons          NUMERIC(14,2) NOT NULL DEFAULT 0,

    -- Forward book (committed but not yet delivered/realized)
    bean_purchases_committed_bu  NUMERIC(14,2) NOT NULL DEFAULT 0,
    oil_sold_forward_lbs         NUMERIC(14,2) NOT NULL DEFAULT 0,
    meal_sold_forward_tons       NUMERIC(14,2) NOT NULL DEFAULT 0,

    -- Operating
    days_of_coverage             NUMERIC(6,2),
    current_crush_rate_tpd       NUMERIC(8,2),
    last_30d_crush_bu            NUMERIC(14,2),

    -- Bookkeeping
    last_decision_id             BIGINT,
    backtest_run_id              TEXT,
    updated_at                   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_facility_state_as_of   ON silver.facility_state (as_of_date);
CREATE INDEX IF NOT EXISTS idx_facility_state_backtest ON silver.facility_state (backtest_run_id)
    WHERE backtest_run_id IS NOT NULL;

COMMENT ON TABLE silver.facility_state IS
'Mutable per-facility state, updated daily by the buyer agent. Reset between backtest runs via backtest_run_id partitioning.';


-- ----------------------------------------------------------------------------
-- 6.3 silver.strategic_plan
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.strategic_plan (
    plan_id                  BIGSERIAL PRIMARY KEY,
    facility_id              TEXT NOT NULL
                             REFERENCES reference.oilseed_crush_facilities(facility_id)
                             ON DELETE CASCADE,
    plan_period_start        DATE NOT NULL,
    plan_period_end          DATE NOT NULL,

    -- Throughput targets
    target_weekly_intake_bu      NUMERIC(14,2),
    target_weekly_crush_bu       NUMERIC(14,2),
    target_daily_crush_bu        NUMERIC(14,2),

    -- Pricing / coverage envelope
    bid_ceiling_basis            NUMERIC(8,4),
    coverage_target_days         INTEGER,
    coverage_target_min_days     INTEGER,
    coverage_target_max_days     INTEGER,

    -- Hedge ratios (0.0 to 1.0)
    hedge_ratio_oil              NUMERIC(4,3),
    hedge_ratio_meal             NUMERIC(4,3),

    -- Per-month hedge targets (per IFV review feedback — supersede generic ratios)
    target_oil_hedge_pct_by_month   JSONB,    -- {"2026-05": 0.65, "2026-06": 0.70, ...}
    target_meal_hedge_pct_by_month  JSONB,
    forward_sell_oil_floor_per_lb   NUMERIC(8,4),
    forward_sell_meal_floor_per_ton NUMERIC(8,2),

    -- Spreadsheet-derived constraints
    full_cost_breakeven_bu       NUMERIC(8,4),
    marginal_breakeven_bu        NUMERIC(8,4),

    -- Reasoning + provenance
    strategic_memo               TEXT,
    spreadsheet_snapshot_path    TEXT,
    spreadsheet_snapshot_sha256  TEXT,        -- content hash for deterministic reads

    created_at                   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by                   TEXT NOT NULL DEFAULT 'strategic_agent_v1'
);

CREATE INDEX IF NOT EXISTS idx_strategic_plan_facility_period
    ON silver.strategic_plan (facility_id, plan_period_start);

COMMENT ON TABLE silver.strategic_plan IS
'Output of Layer 1 strategic agent (Claude Opus, quarterly). Per-month hedge targets supersede single hedge ratios per spec review feedback. spreadsheet_snapshot_sha256 enforces deterministic read of versioned XLSX.';


-- ----------------------------------------------------------------------------
-- 6.4 bronze.daily_decisions
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.daily_decisions (
    decision_id              BIGSERIAL PRIMARY KEY,
    facility_id              TEXT NOT NULL,    -- references reference.oilseed_crush_facilities; FK skipped to allow facility deletion without losing audit trail
    decision_date            DATE NOT NULL,
    decision_timestamp       TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Market context at decision time
    cme_settle               NUMERIC(8,4),
    local_basis              NUMERIC(8,4),
    bean_offer_total         NUMERIC(8,4),
    meal_price               NUMERIC(8,2),
    oil_cash_price           NUMERIC(8,4),
    d4_rin_price             NUMERIC(8,4),
    lcfs_credit_price        NUMERIC(8,2),
    ptc_45z                  NUMERIC(8,4),

    -- Computed values
    implied_value_marginal_bu    NUMERIC(8,4),
    implied_value_full_cost_bu   NUMERIC(8,4),
    crush_margin_bu              NUMERIC(8,4),
    days_of_coverage_at_decision NUMERIC(6,2),

    -- Buy decisions
    buy_bushels              NUMERIC(14,2) NOT NULL DEFAULT 0,
    buy_basis_paid           NUMERIC(8,4),
    buy_delivery_window_start DATE,
    buy_delivery_window_end  DATE,

    -- Crush decisions
    crush_today_bushels      NUMERIC(14,2) NOT NULL DEFAULT 0,
    crush_rate_tpd_today     NUMERIC(8,2),

    -- Forward sell decisions
    forward_sell_oil_lbs     NUMERIC(14,2) NOT NULL DEFAULT 0,
    forward_sell_oil_price   NUMERIC(8,4),
    forward_sell_meal_tons   NUMERIC(14,2) NOT NULL DEFAULT 0,
    forward_sell_meal_price  NUMERIC(8,2),

    -- Reasoning
    decision_rule_triggered  TEXT,
    kg_signals_active        JSONB,
    llm_reasoning            TEXT,
    used_llm                 BOOLEAN NOT NULL DEFAULT FALSE,

    -- Backtest reproducibility
    backtest_run_id          TEXT,

    CONSTRAINT chk_buy_consistency CHECK (
        (buy_bushels = 0 AND buy_basis_paid IS NULL)
        OR (buy_bushels > 0 AND buy_basis_paid IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS idx_daily_decisions_facility_date
    ON bronze.daily_decisions (facility_id, decision_date);
CREATE INDEX IF NOT EXISTS idx_daily_decisions_backtest
    ON bronze.daily_decisions (backtest_run_id)
    WHERE backtest_run_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_daily_decisions_rule
    ON bronze.daily_decisions (decision_rule_triggered);

COMMENT ON TABLE bronze.daily_decisions IS
'Append-only daily decision log per facility. Includes no-ops (buy_bushels=0, crush_today_bushels=0) so the absence of a decision is itself observable. backtest_run_id partitions live vs replay runs.';


-- ----------------------------------------------------------------------------
-- 6.5 gold.monthly_crush_estimates
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.monthly_crush_estimates (
    estimate_id              BIGSERIAL PRIMARY KEY,
    facility_id              TEXT NOT NULL,
    year_month               DATE NOT NULL,    -- first of month, e.g., 2026-03-01

    -- Estimates from agent decisions
    estimated_crush_bu       NUMERIC(14,2) NOT NULL,
    estimated_oil_lbs        NUMERIC(14,2) NOT NULL,
    estimated_meal_tons      NUMERIC(14,2) NOT NULL,
    estimated_utilization    NUMERIC(4,3),

    -- Validation against NASS state-level
    nass_state_total_crush_bu  NUMERIC(14,2),
    facility_share_estimated   NUMERIC(5,4),
    error_pct                  NUMERIC(6,3) GENERATED ALWAYS AS
                               (CASE WHEN nass_state_total_crush_bu > 0
                                     THEN 100.0 * (estimated_crush_bu - facility_share_estimated * nass_state_total_crush_bu)
                                          / NULLIF(facility_share_estimated * nass_state_total_crush_bu, 0)
                                     ELSE NULL END) STORED,

    -- Status
    is_finalized             BOOLEAN NOT NULL DEFAULT FALSE,
    backtest_run_id          TEXT,

    created_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (facility_id, year_month, backtest_run_id)
);

CREATE INDEX IF NOT EXISTS idx_monthly_estimates_year_month
    ON gold.monthly_crush_estimates (year_month);
CREATE INDEX IF NOT EXISTS idx_monthly_estimates_backtest
    ON gold.monthly_crush_estimates (backtest_run_id)
    WHERE backtest_run_id IS NOT NULL;

COMMENT ON TABLE gold.monthly_crush_estimates IS
'Aggregated monthly crush estimates per facility. error_pct GENERATED column auto-computes facility error vs NASS-implied share once nass_state_total_crush_bu is populated.';


-- ----------------------------------------------------------------------------
-- Permissions
-- ----------------------------------------------------------------------------
GRANT SELECT, INSERT, UPDATE ON silver.facility_state         TO PUBLIC;
GRANT SELECT, INSERT, UPDATE ON silver.strategic_plan         TO PUBLIC;
GRANT SELECT, INSERT, UPDATE ON bronze.daily_decisions        TO PUBLIC;
GRANT SELECT, INSERT, UPDATE ON gold.monthly_crush_estimates  TO PUBLIC;
GRANT USAGE, SELECT ON SEQUENCE silver.strategic_plan_plan_id_seq         TO PUBLIC;
GRANT USAGE, SELECT ON SEQUENCE bronze.daily_decisions_decision_id_seq    TO PUBLIC;
GRANT USAGE, SELECT ON SEQUENCE gold.monthly_crush_estimates_estimate_id_seq TO PUBLIC;
