-- ============================================================================
-- Feedstock Allocation Engine — Database Schema
-- Round Lakes Companies
--
-- Bottom-up, plant-level model for estimating US biofuel feedstock consumption
-- across biodiesel, renewable diesel, SAF, and co-processing.
--
-- Usage: psql -U postgres -d rlc_commodities -f 035_feedstock_allocation_engine.sql
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS reference;
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- ═══════════════════════════════════════════════════════════════════════════
-- REFERENCE LAYER — Static/semi-static configuration tables
-- ═══════════════════════════════════════════════════════════════════════════

-- ---------------------------------------------------------------------------
-- Feedstock properties: conversion rates, CI scores, categories
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS reference.feedstock_properties (
    feedstock_code  VARCHAR(10) PRIMARY KEY,  -- SBO, CO, DCO, BFT, YG, UCO, CWG, PF, PALM, OTHER
    feedstock_name  VARCHAR(100) NOT NULL,
    category        VARCHAR(30) NOT NULL,     -- 'vegetable_oil', 'animal_fat', 'waste_oil', 'other'

    -- Conversion rates (lbs per gallon) by technology
    lbs_per_gal_hefa          NUMERIC(6,3),   -- Renewable diesel (HEFA)
    lbs_per_gal_transester    NUMERIC(6,3),   -- Biodiesel (transesterification)
    lbs_per_gal_coprocessing  NUMERIC(6,3),   -- Co-processing

    -- Default CI scores (gCO2e/MJ) — pathway-specific overrides in facility table
    ci_score_default          NUMERIC(6,2),   -- Default carbon intensity

    -- Display order for reports
    sort_order      INTEGER DEFAULT 99,

    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE reference.feedstock_properties IS
    'Feedstock conversion rates and properties. Source: Fastmarkets/EIA reference tables.';

-- Seed the feedstock codes
INSERT INTO reference.feedstock_properties (feedstock_code, feedstock_name, category, lbs_per_gal_hefa, lbs_per_gal_transester, lbs_per_gal_coprocessing, sort_order)
VALUES
    ('SBO',   'Soybean Oil',           'vegetable_oil', 7.50,  7.50,  7.50,  1),
    ('CO',    'Canola Oil',            'vegetable_oil', 7.55,  7.45,  7.45,  2),
    ('DCO',   'Distillers Corn Oil',   'vegetable_oil', 9.20,  8.20,  NULL,  3),
    ('BFT',   'Tallow',               'animal_fat',    9.38,  7.75,  7.75,  4),
    ('CWG',   'Choice White Grease',  'animal_fat',    9.375, 7.858, NULL,  5),
    ('PF',    'Poultry Fat',          'animal_fat',    8.12,  7.45,  NULL,  6),
    ('YG',    'Yellow Grease',        'waste_oil',     8.50,  8.23,  NULL,  7),
    ('UCO',   'Used Cooking Oil',     'waste_oil',     8.01,  NULL,  NULL,  8),
    ('PALM',  'Palm Oil',             'vegetable_oil', 7.45,  7.45,  NULL,  9),
    ('CSO',   'Cottonseed Oil',       'vegetable_oil', NULL,  7.45,  NULL, 10),
    ('SFO',   'Sunflower Oil',        'vegetable_oil', NULL,  7.45,  NULL, 11),
    ('LARD',  'Lard',                 'animal_fat',    NULL,  7.80,  NULL, 12),
    ('OTHER', 'Other/Unspecified',    'other',         8.40,  8.40,  8.00, 99)
ON CONFLICT (feedstock_code) DO UPDATE SET
    feedstock_name = EXCLUDED.feedstock_name,
    category = EXCLUDED.category,
    lbs_per_gal_hefa = EXCLUDED.lbs_per_gal_hefa,
    lbs_per_gal_transester = EXCLUDED.lbs_per_gal_transester,
    lbs_per_gal_coprocessing = EXCLUDED.lbs_per_gal_coprocessing,
    sort_order = EXCLUDED.sort_order,
    updated_at = NOW();


-- ---------------------------------------------------------------------------
-- Master facility registry
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS reference.biofuel_facilities (
    facility_id     SERIAL PRIMARY KEY,
    company         VARCHAR(200) NOT NULL,
    facility_name   VARCHAR(200) NOT NULL,
    city            VARCHAR(100),
    state           VARCHAR(5),
    padd            VARCHAR(10),
    latitude        NUMERIC(9,6),
    longitude       NUMERIC(9,6),

    -- Classification
    fuel_type       VARCHAR(20) NOT NULL,     -- 'biodiesel', 'renewable_diesel', 'saf', 'coprocessing'
    technology      VARCHAR(30),              -- 'transesterification', 'hefa', 'coprocessing', 'esterification'

    -- Capacity (million gallons per year)
    nameplate_mmgy      NUMERIC(10,1),
    expansion_mmgy      NUMERIC(10,1),        -- Announced/under-construction expansion
    operating_mmgy      NUMERIC(10,1),        -- Current effective operating capacity

    -- Status and timeline
    status          VARCHAR(30) DEFAULT 'operating',  -- 'operating', 'idle', 'under_construction', 'announced', 'closed', 'removed'
    year_online     INTEGER,
    year_offline    INTEGER,
    start_date      DATE,                     -- More precise than year_online

    -- Feedstock configuration
    feedstock_mix   TEXT,                      -- Human-readable, e.g. "23% YG - 10% CWG - 33% DCO - 33% UCO"
    eligible_feedstocks VARCHAR(100)[],        -- Array of feedstock_codes this plant can process
    primary_feedstock   VARCHAR(10),           -- Default/primary feedstock code

    -- EPA/regulatory cross-references
    epa_pathway_ids     TEXT[],               -- EPA pathway determination IDs
    d_codes             VARCHAR(5)[],          -- RIN D-codes plant can generate (D4, D5, D6, D7)
    frs_registry_id     VARCHAR(20),           -- EPA FRS cross-reference
    eia_plant_id        VARCHAR(50),           -- EIA identifier if available

    -- Source tracking
    data_source     VARCHAR(50),              -- 'fastmarkets', 'eia', 'epa_echo', 'epa_pathway', 'manual'
    notes           TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (company, facility_name, fuel_type)
);

CREATE INDEX IF NOT EXISTS idx_fac_fuel_type ON reference.biofuel_facilities(fuel_type);
CREATE INDEX IF NOT EXISTS idx_fac_state ON reference.biofuel_facilities(state);
CREATE INDEX IF NOT EXISTS idx_fac_status ON reference.biofuel_facilities(status);
CREATE INDEX IF NOT EXISTS idx_fac_padd ON reference.biofuel_facilities(padd);

COMMENT ON TABLE reference.biofuel_facilities IS
    'Master registry of US biofuel production facilities. Merged from EIA, EPA ECHO, EPA pathways, and Fastmarkets data.';


-- ---------------------------------------------------------------------------
-- Facility capacity history — track changes over time
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS reference.facility_capacity_history (
    id              SERIAL PRIMARY KEY,
    facility_id     INTEGER REFERENCES reference.biofuel_facilities(facility_id),
    effective_date  DATE NOT NULL,
    nameplate_mmgy  NUMERIC(10,1),
    operating_mmgy  NUMERIC(10,1),
    status          VARCHAR(30),
    change_reason   TEXT,                     -- 'expansion', 'debottleneck', 'idled', 'reopened', 'initial'
    source          VARCHAR(100),
    created_at      TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (facility_id, effective_date)
);

COMMENT ON TABLE reference.facility_capacity_history IS
    'Tracks capacity changes at each facility over time for historical modeling.';


-- ---------------------------------------------------------------------------
-- Biofuel policy timeline — drives credit availability in margin model
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS reference.biofuel_policy_timeline (
    id              SERIAL PRIMARY KEY,
    policy_name     VARCHAR(100) NOT NULL,    -- 'RFS2', 'BTC', '45Z', 'LCFS_CA', 'CFP_OR', 'CFS_WA', 'IL_SAF'
    policy_type     VARCHAR(30) NOT NULL,     -- 'federal_mandate', 'federal_credit', 'state_credit', 'state_mandate'
    jurisdiction    VARCHAR(30),              -- 'US', 'CA', 'OR', 'WA', 'IL', 'MN'
    effective_date  DATE NOT NULL,
    expiry_date     DATE,
    d_code          VARCHAR(5),               -- For RFS mandates
    year            INTEGER,                  -- For annual mandates/RVOs

    -- Mandate volumes (billion gallons or million RINs)
    mandate_volume      NUMERIC(12,2),
    mandate_unit        VARCHAR(30),          -- 'billion_gal', 'million_rins'

    -- Credit values (for fixed credits like BTC/45Z)
    credit_value        NUMERIC(8,4),         -- $/gallon
    credit_unit         VARCHAR(30),          -- 'usd_per_gal', 'usd_per_mt_co2e'
    ci_based            BOOLEAN DEFAULT FALSE,

    description     TEXT,
    source          VARCHAR(200),
    created_at      TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (policy_name, effective_date, d_code, year)
);

COMMENT ON TABLE reference.biofuel_policy_timeline IS
    'US biofuel policy evolution: RFS mandates, tax credits, state programs. Drives the margin model credit availability.';


-- ---------------------------------------------------------------------------
-- PADD region definitions
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS reference.padd_regions (
    padd_code   VARCHAR(10) PRIMARY KEY,
    padd_name   VARCHAR(100) NOT NULL,
    states      VARCHAR(5)[] NOT NULL
);

INSERT INTO reference.padd_regions (padd_code, padd_name, states) VALUES
    ('PADD1', 'East Coast',   ARRAY['CT','DC','DE','FL','GA','MA','MD','ME','NC','NH','NJ','NY','PA','RI','SC','VA','VT','WV']),
    ('PADD2', 'Midwest',      ARRAY['IA','IL','IN','KS','KY','MI','MN','MO','ND','NE','OH','OK','SD','TN','WI']),
    ('PADD3', 'Gulf Coast',   ARRAY['AL','AR','LA','MS','NM','TX']),
    ('PADD4', 'Rocky Mountain', ARRAY['CO','ID','MT','UT','WY']),
    ('PADD5', 'West Coast',   ARRAY['AK','AZ','CA','HI','NV','OR','WA'])
ON CONFLICT (padd_code) DO NOTHING;


-- ═══════════════════════════════════════════════════════════════════════════
-- BRONZE LAYER — Historical data ingested from spreadsheets & collectors
-- ═══════════════════════════════════════════════════════════════════════════

-- ---------------------------------------------------------------------------
-- Historical plant-level feedstock allocation (from Fastmarkets spreadsheets)
-- TRAINING DATA ONLY — never publish
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.historical_feedstock_allocation (
    id              SERIAL PRIMARY KEY,
    period          DATE NOT NULL,            -- First of month
    facility_id     INTEGER REFERENCES reference.biofuel_facilities(facility_id),
    facility_name   VARCHAR(200),             -- Denormalized for loading before facility_id resolution
    fuel_type       VARCHAR(20) NOT NULL,     -- 'biodiesel', 'renewable_diesel', 'saf', 'coprocessing'
    feedstock_code  VARCHAR(10) NOT NULL,     -- FK to reference.feedstock_properties
    quantity_mil_lbs NUMERIC(12,4),           -- Million pounds consumed
    quantity_mil_gal NUMERIC(12,4),           -- Million gallons produced (derived)
    pct_of_total    NUMERIC(5,2),            -- Percentage of facility's total feedstock
    source          VARCHAR(50) DEFAULT 'fastmarkets',
    created_at      TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (period, facility_name, fuel_type, feedstock_code)
);

CREATE INDEX IF NOT EXISTS idx_hfa_period ON bronze.historical_feedstock_allocation(period);
CREATE INDEX IF NOT EXISTS idx_hfa_fuel_type ON bronze.historical_feedstock_allocation(fuel_type);
CREATE INDEX IF NOT EXISTS idx_hfa_feedstock ON bronze.historical_feedstock_allocation(feedstock_code);

COMMENT ON TABLE bronze.historical_feedstock_allocation IS
    'Plant-level feedstock consumption from Fastmarkets models. PROPRIETARY — training/calibration only, never publish.';


-- ---------------------------------------------------------------------------
-- BBD capacity history (monthly from US BBD and Fuel Balance Sheets)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.bbd_capacity_history (
    id              SERIAL PRIMARY KEY,
    period          DATE NOT NULL,            -- First of month
    bd_capacity_mmgy    NUMERIC(10,2),        -- Biodiesel nameplate capacity
    rd_capacity_mmgy    NUMERIC(10,2),        -- Renewable diesel nameplate capacity
    combined_capacity_mmgy NUMERIC(10,2),     -- Total
    bd_production_mmgal NUMERIC(10,2),        -- Monthly BD production (million gallons)
    rd_production_mmgal NUMERIC(10,2),        -- Monthly RD production
    combined_production_mmgal NUMERIC(10,2),
    bd_utilization_pct  NUMERIC(5,1),
    rd_utilization_pct  NUMERIC(5,1),
    combined_utilization_pct NUMERIC(5,1),
    source          VARCHAR(50) DEFAULT 'fastmarkets',
    created_at      TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (period)
);

COMMENT ON TABLE bronze.bbd_capacity_history IS
    'Monthly US BBD industry capacity and utilization from EIA/Fastmarkets balance sheets.';


-- ---------------------------------------------------------------------------
-- BBD balance sheet (annual S&D by fuel type)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.bbd_balance_sheet (
    id              SERIAL PRIMARY KEY,
    year            INTEGER NOT NULL,
    fuel_type       VARCHAR(20) NOT NULL,     -- 'biodiesel', 'renewable_diesel', 'saf'
    is_forecast     BOOLEAN DEFAULT FALSE,    -- TRUE for estimate/forecast years

    -- Balance sheet components (million gallons)
    beginning_stocks    NUMERIC(12,2),
    production          NUMERIC(12,2),
    imports             NUMERIC(12,2),
    total_supply        NUMERIC(12,2),
    domestic_consumption NUMERIC(12,2),
    exports             NUMERIC(12,2),
    total_use           NUMERIC(12,2),
    ending_stocks       NUMERIC(12,2),

    -- Derived
    capacity_mmgy       NUMERIC(12,2),
    operating_rate_pct  NUMERIC(5,1),
    stocks_use_ratio    NUMERIC(5,2),

    source          VARCHAR(50) DEFAULT 'fastmarkets',
    created_at      TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (year, fuel_type)
);

COMMENT ON TABLE bronze.bbd_balance_sheet IS
    'Annual US BBD S&D balance sheets by fuel type. Source: EIA/Fastmarkets.';


-- ---------------------------------------------------------------------------
-- Credit prices (RINs, LCFS, CFP, 45Z)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.credit_prices (
    id              SERIAL PRIMARY KEY,
    price_date      DATE NOT NULL,
    frequency       VARCHAR(10) NOT NULL,     -- 'daily', 'weekly', 'monthly'

    -- RIN prices (cents per RIN)
    d3_rin          NUMERIC(10,4),
    d4_rin          NUMERIC(10,4),
    d5_rin          NUMERIC(10,4),
    d6_rin          NUMERIC(10,4),

    -- Carbon credits ($/MT CO2e)
    lcfs_ca         NUMERIC(10,4),            -- California LCFS
    cfp_or          NUMERIC(10,4),            -- Oregon Clean Fuels Program
    cfs_wa          NUMERIC(10,4),            -- Washington Clean Fuel Standard

    -- Spreads
    d4_d6_spread    NUMERIC(10,4),
    d4_d5_spread    NUMERIC(10,4),

    source          VARCHAR(50),              -- 'fastmarkets', 'opis', 'epa_emts', 'carb'
    is_proprietary  BOOLEAN DEFAULT FALSE,    -- TRUE = Jacobsen/OPIS data, never publish
    created_at      TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (price_date, frequency)
);

CREATE INDEX IF NOT EXISTS idx_credit_date ON bronze.credit_prices(price_date);

COMMENT ON TABLE bronze.credit_prices IS
    'RIN and carbon credit prices. Some proprietary (is_proprietary=TRUE), never publish raw.';


-- ---------------------------------------------------------------------------
-- Feedstock prices (FOB and delivered, by region)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.feedstock_prices (
    id              SERIAL PRIMARY KEY,
    price_date      DATE NOT NULL,
    frequency       VARCHAR(10) NOT NULL,     -- 'daily', 'weekly', 'monthly'
    feedstock_code  VARCHAR(10) NOT NULL,      -- FK to reference.feedstock_properties
    region          VARCHAR(30),              -- 'central_il', 'gulf', 'west_coast', 'socal', etc.

    price_per_lb    NUMERIC(10,6),            -- $/lb FOB
    price_per_gal   NUMERIC(10,4),            -- $/gal (derived: price_per_lb × lbs_per_gal)

    source          VARCHAR(50),              -- 'usda_ams', 'jacobsen', 'cbot', 'manual'
    is_proprietary  BOOLEAN DEFAULT FALSE,
    created_at      TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (price_date, frequency, feedstock_code, region)
);

CREATE INDEX IF NOT EXISTS idx_fsprice_date ON bronze.feedstock_prices(price_date);
CREATE INDEX IF NOT EXISTS idx_fsprice_feedstock ON bronze.feedstock_prices(feedstock_code);

COMMENT ON TABLE bronze.feedstock_prices IS
    'Feedstock commodity prices by region. Mix of public (USDA AMS, CBOT) and proprietary (Jacobsen).';


-- ---------------------------------------------------------------------------
-- Fuel prices (ULSD, B100, jet fuel — for revenue stack)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.fuel_prices (
    id              SERIAL PRIMARY KEY,
    price_date      DATE NOT NULL,
    frequency       VARCHAR(10) NOT NULL,

    -- $/gallon prices
    ulsd_gulf       NUMERIC(10,4),            -- ULSD Gulf Coast
    ulsd_nyharbor   NUMERIC(10,4),            -- ULSD NY Harbor
    b100_national   NUMERIC(10,4),            -- B100 national average
    b100_northeast  NUMERIC(10,4),
    b100_southeast  NUMERIC(10,4),
    b100_upper_midwest NUMERIC(10,4),
    b100_lower_midwest NUMERIC(10,4),
    b100_south_central NUMERIC(10,4),
    b100_rocky_mountain NUMERIC(10,4),
    rd_california   NUMERIC(10,4),            -- RD rack California
    jet_a_spot      NUMERIC(10,4),            -- Jet fuel spot

    -- Reference prices
    heating_oil_futures NUMERIC(10,4),        -- NYMEX HO ($/gal)
    wti_crude       NUMERIC(10,4),            -- WTI ($/bbl)

    source          VARCHAR(50),
    is_proprietary  BOOLEAN DEFAULT FALSE,
    created_at      TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (price_date, frequency)
);

COMMENT ON TABLE bronze.fuel_prices IS
    'Fuel selling prices for revenue stack calculation. EIA + proprietary sources.';


-- ---------------------------------------------------------------------------
-- Freight rates (feedstock transport cost by route)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.freight_rates (
    id              SERIAL PRIMARY KEY,
    price_date      DATE NOT NULL,
    frequency       VARCHAR(10) NOT NULL,     -- 'weekly', 'monthly'

    origin_region       VARCHAR(30) NOT NULL,  -- 'central_il', 'gulf', 'wcb', 'ecb'
    destination_region  VARCHAR(30) NOT NULL,  -- 'socal', 'gulf', 'pnw', etc.
    mode                VARCHAR(20),           -- 'rail', 'truck', 'barge'

    rate_per_cwt    NUMERIC(10,4),            -- $/cwt
    rate_per_lb     NUMERIC(10,6),            -- $/lb (derived: rate_per_cwt / 100)

    index_value     NUMERIC(10,4),            -- Rail fuel index if applicable
    index_multiplier NUMERIC(6,4),            -- Quarterly index adjustment

    source          VARCHAR(50),
    is_proprietary  BOOLEAN DEFAULT FALSE,
    created_at      TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (price_date, frequency, origin_region, destination_region, mode)
);

COMMENT ON TABLE bronze.freight_rates IS
    'Feedstock transportation costs by route. Currently based on proportional differentials.';


-- ---------------------------------------------------------------------------
-- Feedstock profitability (weekly margin by feedstock — from Fastmarkets model)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.feedstock_profitability (
    id              SERIAL PRIMARY KEY,
    price_date      DATE NOT NULL,
    feedstock_code  VARCHAR(10) NOT NULL,
    fuel_type       VARCHAR(20) NOT NULL,     -- 'biodiesel', 'renewable_diesel'

    -- Revenue components ($/gal)
    fuel_value          NUMERIC(10,4),
    rin_credit_value    NUMERIC(10,4),
    lcfs_credit_value   NUMERIC(10,4),
    btc_45z_value       NUMERIC(10,4),
    total_revenue       NUMERIC(10,4),

    -- Cost components ($/gal)
    feedstock_cost_gal  NUMERIC(10,4),
    rail_cost           NUMERIC(10,4),

    -- Margins ($/gal and $/lb)
    profit_per_gal      NUMERIC(10,4),
    profit_per_lb       NUMERIC(10,4),

    -- Regional margins ($/gal)
    margin_gulf         NUMERIC(10,4),
    margin_southern_plains NUMERIC(10,4),
    margin_mountain_west NUMERIC(10,4),
    margin_eastern_cb   NUMERIC(10,4),
    margin_western_cb   NUMERIC(10,4),
    margin_southeast    NUMERIC(10,4),
    margin_canada       NUMERIC(10,4),

    source          VARCHAR(50) DEFAULT 'fastmarkets',
    is_proprietary  BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (price_date, feedstock_code, fuel_type)
);

CREATE INDEX IF NOT EXISTS idx_fprof_date ON bronze.feedstock_profitability(price_date);

COMMENT ON TABLE bronze.feedstock_profitability IS
    'Weekly feedstock-by-feedstock margin calculations. PROPRIETARY — training/calibration only.';


-- ═══════════════════════════════════════════════════════════════════════════
-- SILVER LAYER — Cleaned, standardized, model-ready
-- ═══════════════════════════════════════════════════════════════════════════

-- ---------------------------------------------------------------------------
-- Feedstock supply by region (monthly)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.feedstock_supply (
    id              SERIAL PRIMARY KEY,
    period          DATE NOT NULL,            -- First of month
    feedstock_code  VARCHAR(10) NOT NULL,
    region          VARCHAR(30) NOT NULL,     -- PADD or custom region

    -- Supply (million lbs)
    domestic_production NUMERIC(12,2),
    imports             NUMERIC(12,2),
    total_available     NUMERIC(12,2),

    -- Non-biofuel demand that reduces availability (million lbs)
    food_industrial_use NUMERIC(12,2),
    export_use          NUMERIC(12,2),
    other_use           NUMERIC(12,2),
    net_available_biofuel NUMERIC(12,2),      -- Available for biofuel plants

    -- Price
    avg_price_per_lb    NUMERIC(10,6),
    freight_to_plant    NUMERIC(10,6),        -- Average freight within region
    delivered_cost_lb   NUMERIC(10,6),

    source          VARCHAR(50),
    created_at      TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (period, feedstock_code, region)
);

COMMENT ON TABLE silver.feedstock_supply IS
    'Monthly feedstock availability and pricing by region. Feeds the allocation engine.';


-- ═══════════════════════════════════════════════════════════════════════════
-- GOLD LAYER — Allocation engine output
-- ═══════════════════════════════════════════════════════════════════════════

-- ---------------------------------------------------------------------------
-- Plant-level feedstock allocation (engine output)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.feedstock_allocation (
    id              SERIAL PRIMARY KEY,
    period          DATE NOT NULL,            -- First of month
    run_id          UUID,                     -- Engine run identifier
    scenario        VARCHAR(30) DEFAULT 'base', -- 'base', 'high_sbo', 'rin_collapse', etc.

    facility_id     INTEGER REFERENCES reference.biofuel_facilities(facility_id),
    fuel_type       VARCHAR(20) NOT NULL,
    feedstock_code  VARCHAR(10) NOT NULL,

    -- Allocation (million lbs / million gallons)
    allocated_mil_lbs   NUMERIC(12,4),
    allocated_mil_gal   NUMERIC(12,4),
    pct_of_facility     NUMERIC(5,2),         -- % of this facility's total feedstock

    -- Economics that drove the allocation
    feedstock_cost_lb   NUMERIC(10,6),        -- Delivered cost $/lb
    margin_per_gal      NUMERIC(10,4),        -- Gross margin $/gal for this feedstock
    margin_rank         INTEGER,              -- 1 = best margin feedstock for this plant

    -- Constraints that limited allocation
    constraint_binding  VARCHAR(30),          -- 'none', 'supply', 'pathway', 'diversification', 'switching_cost'

    created_at      TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (period, run_id, facility_id, feedstock_code, scenario)
);

CREATE INDEX IF NOT EXISTS idx_alloc_period ON gold.feedstock_allocation(period);
CREATE INDEX IF NOT EXISTS idx_alloc_run ON gold.feedstock_allocation(run_id);
CREATE INDEX IF NOT EXISTS idx_alloc_scenario ON gold.feedstock_allocation(scenario);

COMMENT ON TABLE gold.feedstock_allocation IS
    'Allocation engine output: monthly plant-level feedstock consumption estimates.';


-- ---------------------------------------------------------------------------
-- Regional aggregation view (PADD level — publishable)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.feedstock_allocation_by_padd AS
SELECT
    fa.period,
    fa.scenario,
    bf.padd,
    fa.fuel_type,
    fa.feedstock_code,
    fp.feedstock_name,
    fp.category AS feedstock_category,
    SUM(fa.allocated_mil_lbs)  AS total_mil_lbs,
    SUM(fa.allocated_mil_gal)  AS total_mil_gal,
    COUNT(DISTINCT fa.facility_id) AS facility_count,
    AVG(fa.feedstock_cost_lb)  AS avg_cost_per_lb,
    AVG(fa.margin_per_gal)     AS avg_margin_per_gal
FROM gold.feedstock_allocation fa
JOIN reference.biofuel_facilities bf ON fa.facility_id = bf.facility_id
JOIN reference.feedstock_properties fp ON fa.feedstock_code = fp.feedstock_code
WHERE fa.run_id = (
    SELECT run_id FROM gold.feedstock_allocation
    WHERE scenario = fa.scenario
    ORDER BY created_at DESC LIMIT 1
)
GROUP BY fa.period, fa.scenario, bf.padd, fa.fuel_type, fa.feedstock_code,
         fp.feedstock_name, fp.category, fp.sort_order
ORDER BY fa.period, bf.padd, fa.fuel_type, fp.sort_order;

COMMENT ON VIEW gold.feedstock_allocation_by_padd IS
    'Regional feedstock consumption estimates aggregated to PADD level. Safe for publication.';


-- ---------------------------------------------------------------------------
-- National summary view (publishable)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.feedstock_allocation_national AS
SELECT
    fa.period,
    fa.scenario,
    fa.fuel_type,
    fa.feedstock_code,
    fp.feedstock_name,
    fp.category AS feedstock_category,
    SUM(fa.allocated_mil_lbs)  AS total_mil_lbs,
    SUM(fa.allocated_mil_gal)  AS total_mil_gal,
    COUNT(DISTINCT fa.facility_id) AS facility_count
FROM gold.feedstock_allocation fa
JOIN reference.feedstock_properties fp ON fa.feedstock_code = fp.feedstock_code
WHERE fa.run_id = (
    SELECT run_id FROM gold.feedstock_allocation
    WHERE scenario = fa.scenario
    ORDER BY created_at DESC LIMIT 1
)
GROUP BY fa.period, fa.scenario, fa.fuel_type, fa.feedstock_code,
         fp.feedstock_name, fp.category, fp.sort_order
ORDER BY fa.period, fa.fuel_type, fp.sort_order;


-- ---------------------------------------------------------------------------
-- Model accuracy tracking (compare engine output to EIA actuals)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.allocation_accuracy (
    id              SERIAL PRIMARY KEY,
    period          DATE NOT NULL,
    run_id          UUID,
    comparison_level VARCHAR(20) NOT NULL,    -- 'national', 'padd', 'fuel_type'
    fuel_type       VARCHAR(20),
    feedstock_code  VARCHAR(10),
    region          VARCHAR(30),

    -- Model vs actual
    model_mil_lbs   NUMERIC(12,4),
    actual_mil_lbs  NUMERIC(12,4),           -- From EIA Form 819
    error_mil_lbs   NUMERIC(12,4),
    error_pct       NUMERIC(8,4),
    abs_error_pct   NUMERIC(8,4),

    created_at      TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (period, run_id, comparison_level, fuel_type, feedstock_code, region)
);

COMMENT ON TABLE gold.allocation_accuracy IS
    'Tracks model accuracy against EIA Form 819 actuals for continuous calibration.';


-- ═══════════════════════════════════════════════════════════════════════════
-- GRANTS
-- ═══════════════════════════════════════════════════════════════════════════

-- Grant usage on schemas
GRANT USAGE ON SCHEMA reference TO postgres;
GRANT USAGE ON SCHEMA bronze TO postgres;
GRANT USAGE ON SCHEMA silver TO postgres;
GRANT USAGE ON SCHEMA gold TO postgres;

-- Grant full access on all tables in each schema
GRANT ALL ON ALL TABLES IN SCHEMA reference TO postgres;
GRANT ALL ON ALL TABLES IN SCHEMA bronze TO postgres;
GRANT ALL ON ALL TABLES IN SCHEMA silver TO postgres;
GRANT ALL ON ALL TABLES IN SCHEMA gold TO postgres;

-- Grant on sequences
GRANT ALL ON ALL SEQUENCES IN SCHEMA reference TO postgres;
GRANT ALL ON ALL SEQUENCES IN SCHEMA bronze TO postgres;
GRANT ALL ON ALL SEQUENCES IN SCHEMA silver TO postgres;
GRANT ALL ON ALL SEQUENCES IN SCHEMA gold TO postgres;
