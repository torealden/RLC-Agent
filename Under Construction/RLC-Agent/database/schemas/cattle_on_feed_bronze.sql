-- =============================================================================
-- CATTLE ON FEED - BRONZE SCHEMAS
-- =============================================================================
-- Source: USDA NASS Cattle on Feed Report
-- Release: Monthly, 3:00 PM ET (see agent_scheduler.py for specific dates)
-- Format: ZIP containing CSV files with structured row types
-- Encoding: Windows cp1252
--
-- CSV Format:
--   Column 1: table_id (numeric identifier)
--   Column 2: row_type (t=title, h=header, u=units, d=data, f=footnote, c=end)
--   Columns 3+: data values
--
-- Special Values:
--   (NA) = Not available
--   (D)  = Withheld to avoid disclosing individual operations
--   (X)  = Not applicable
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Table 82: US National Summary - Current Month
-- File: cofd_p02a_t082.csv
-- Content: On-feed inventory, placements, marketings, other disappearance
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.cofd_us_summary_current (
    report_date         DATE NOT NULL,           -- Report release date
    reference_date      DATE NOT NULL,           -- Data reference date (e.g., Dec 1)
    reference_month     VARCHAR(20) NOT NULL,    -- Month being reported (e.g., "November")
    item                VARCHAR(100) NOT NULL,   -- Metric name
    prior_year_value    INTEGER,                 -- Prior year value (1,000 head)
    current_year_value  INTEGER,                 -- Current year value (1,000 head)
    pct_of_prior_year   DECIMAL(5,1),           -- Percent of previous year
    raw_file            VARCHAR(100),            -- Source filename
    load_timestamp      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (report_date, reference_date, item)
);

COMMENT ON TABLE bronze.cofd_us_summary_current IS
'USDA Cattle on Feed - US national totals for current reporting month (Table 82)';

-- -----------------------------------------------------------------------------
-- Table 83: US National Summary - Previous Month
-- File: cofd_p02b_t083.csv
-- Content: Same as Table 82 but for the previous month
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.cofd_us_summary_previous (
    report_date         DATE NOT NULL,
    reference_date      DATE NOT NULL,
    reference_month     VARCHAR(20) NOT NULL,
    item                VARCHAR(100) NOT NULL,
    prior_year_value    INTEGER,
    current_year_value  INTEGER,
    pct_of_prior_year   DECIMAL(5,1),
    raw_file            VARCHAR(100),
    load_timestamp      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (report_date, reference_date, item)
);

COMMENT ON TABLE bronze.cofd_us_summary_previous IS
'USDA Cattle on Feed - US national totals for previous month (Table 83)';

-- -----------------------------------------------------------------------------
-- Table 86: Inventory by State
-- File: cofd_p03_t086.csv
-- Content: On-feed inventory by state with YoY and MoM comparisons
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.cofd_inventory_by_state (
    report_date             DATE NOT NULL,
    reference_date          DATE NOT NULL,       -- Current month reference (e.g., Dec 1, 2025)
    state                   VARCHAR(50) NOT NULL,
    prior_year_inventory    INTEGER,             -- Dec 1 prior year (1,000 head)
    prior_month_inventory   INTEGER,             -- Nov 1 current year (1,000 head)
    current_inventory       INTEGER,             -- Dec 1 current year (1,000 head)
    pct_of_prior_year       DECIMAL(5,1),       -- YoY percent
    pct_of_prior_month      DECIMAL(5,1),       -- MoM percent
    raw_file                VARCHAR(100),
    load_timestamp          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (report_date, reference_date, state)
);

COMMENT ON TABLE bronze.cofd_inventory_by_state IS
'USDA Cattle on Feed - On-feed inventory by state (Table 86). States: AZ, CA, CO, ID, IA, KS, MN, NE, OK, SD, TX, WA, Other States, United States';

-- -----------------------------------------------------------------------------
-- Table 87: Placements by State
-- File: cofd_p04_t087.csv
-- Content: Monthly placements by state with YoY and MoM comparisons
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.cofd_placements_by_state (
    report_date             DATE NOT NULL,
    reference_month         VARCHAR(20) NOT NULL,  -- Month of placements (e.g., "November")
    reference_year          INTEGER NOT NULL,
    state                   VARCHAR(50) NOT NULL,
    prior_year_placements   INTEGER,               -- Same month prior year (1,000 head)
    prior_month_placements  INTEGER,               -- Prior month current year (1,000 head)
    current_placements      INTEGER,               -- Current month (1,000 head)
    pct_of_prior_year       DECIMAL(5,1),
    pct_of_prior_month      DECIMAL(5,1),
    raw_file                VARCHAR(100),
    load_timestamp          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (report_date, reference_year, reference_month, state)
);

COMMENT ON TABLE bronze.cofd_placements_by_state IS
'USDA Cattle on Feed - Monthly placements by state (Table 87)';

-- -----------------------------------------------------------------------------
-- Table 88: Placements by Weight Group
-- File: cofd_p05a_t088.csv
-- Content: Placements broken down by weight category and state
-- Weight groups: <600, 600-699, 700-799, 800-899, 900-999, 1000+ lbs
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.cofd_placements_by_weight (
    report_date             DATE NOT NULL,
    reference_month         VARCHAR(20) NOT NULL,
    reference_year          INTEGER NOT NULL,
    state                   VARCHAR(50) NOT NULL,
    weight_under_600        INTEGER,               -- <600 lbs (1,000 head)
    weight_600_699          INTEGER,               -- 600-699 lbs
    weight_700_799          INTEGER,               -- 700-799 lbs
    weight_800_899          INTEGER,               -- 800-899 lbs
    weight_900_plus         INTEGER,               -- 900+ lbs (combined)
    weight_900_999          INTEGER,               -- 900-999 lbs (may be withheld)
    weight_1000_plus        INTEGER,               -- 1000+ lbs (may be withheld)
    total_placements        INTEGER,               -- Total
    raw_file                VARCHAR(100),
    load_timestamp          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (report_date, reference_year, reference_month, state)
);

COMMENT ON TABLE bronze.cofd_placements_by_weight IS
'USDA Cattle on Feed - Placements by weight group and state (Table 88). Note: 900-999 and 1000+ often withheld (D) at state level to protect individual operations';

-- -----------------------------------------------------------------------------
-- Table 91: Marketings by State
-- File: cofd_p05b_t091.csv
-- Content: Monthly marketings (cattle shipped to slaughter) by state
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.cofd_marketings_by_state (
    report_date             DATE NOT NULL,
    reference_month         VARCHAR(20) NOT NULL,
    reference_year          INTEGER NOT NULL,
    state                   VARCHAR(50) NOT NULL,
    prior_year_marketings   INTEGER,               -- Same month prior year (1,000 head)
    prior_month_marketings  INTEGER,               -- Prior month current year (1,000 head)
    current_marketings      INTEGER,               -- Current month (1,000 head)
    pct_of_prior_year       DECIMAL(5,1),
    pct_of_prior_month      DECIMAL(5,1),
    raw_file                VARCHAR(100),
    load_timestamp          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (report_date, reference_year, reference_month, state)
);

COMMENT ON TABLE bronze.cofd_marketings_by_state IS
'USDA Cattle on Feed - Monthly marketings by state (Table 91). Marketings = cattle shipped to slaughter market';

-- -----------------------------------------------------------------------------
-- Table 92: Other Disappearance by State
-- File: cofd_p06_t092.csv
-- Content: Death loss, transfers to pasture, shipments to other feedlots
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.cofd_other_disappearance_by_state (
    report_date                 DATE NOT NULL,
    reference_month             VARCHAR(20) NOT NULL,
    reference_year              INTEGER NOT NULL,
    state                       VARCHAR(50) NOT NULL,
    prior_year_disappearance    INTEGER,           -- Same month prior year (1,000 head)
    prior_month_disappearance   INTEGER,           -- Prior month current year (1,000 head)
    current_disappearance       INTEGER,           -- Current month (1,000 head)
    pct_of_prior_year           DECIMAL(5,1),
    pct_of_prior_month          DECIMAL(5,1),
    raw_file                    VARCHAR(100),
    load_timestamp              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (report_date, reference_year, reference_month, state)
);

COMMENT ON TABLE bronze.cofd_other_disappearance_by_state IS
'USDA Cattle on Feed - Other disappearance by state (Table 92). Includes death loss, movement to pasture, transfers to other feedlots';

-- -----------------------------------------------------------------------------
-- Table 52: Reliability Statistics
-- File: cofd_p08_t052.csv
-- Content: Statistical reliability measures for the estimates
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.cofd_reliability_stats (
    report_date             DATE NOT NULL,
    item                    VARCHAR(50) NOT NULL,  -- On feed, Placements, Marketings
    root_mean_square_error  DECIMAL(5,2),          -- RMSE (percent)
    confidence_90_pct       DECIMAL(5,2),          -- 90% confidence level (percent)
    avg_difference          INTEGER,                -- Average diff first vs latest (1,000 head)
    smallest_difference     INTEGER,                -- Smallest diff (1,000 head)
    largest_difference      INTEGER,                -- Largest diff (1,000 head)
    months_below_latest     INTEGER,                -- Count of months below latest
    months_above_latest     INTEGER,                -- Count of months above latest
    raw_file                VARCHAR(100),
    load_timestamp          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (report_date, item)
);

COMMENT ON TABLE bronze.cofd_reliability_stats IS
'USDA Cattle on Feed - Statistical reliability of estimates (Table 52). Based on 24-month comparison of first vs latest estimates';

-- -----------------------------------------------------------------------------
-- Raw Archive Table (optional - stores complete CSV content for audit)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.cofd_raw_archive (
    report_date         DATE NOT NULL,
    filename            VARCHAR(100) NOT NULL,
    table_id            INTEGER,
    row_type            CHAR(1),                   -- t, h, u, d, f, c
    row_number          INTEGER,
    raw_content         TEXT,                      -- Full CSV row
    load_timestamp      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (report_date, filename, row_number)
);

COMMENT ON TABLE bronze.cofd_raw_archive IS
'Raw CSV content archive for audit and reprocessing. Preserves original NASS format';

-- -----------------------------------------------------------------------------
-- Index suggestions for common queries
-- -----------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_cofd_inventory_state ON bronze.cofd_inventory_by_state(state, reference_date);
CREATE INDEX IF NOT EXISTS idx_cofd_placements_state ON bronze.cofd_placements_by_state(state, reference_year, reference_month);
CREATE INDEX IF NOT EXISTS idx_cofd_marketings_state ON bronze.cofd_marketings_by_state(state, reference_year, reference_month);
CREATE INDEX IF NOT EXISTS idx_cofd_weight_state ON bronze.cofd_placements_by_weight(state, reference_year, reference_month);

-- -----------------------------------------------------------------------------
-- Reference: State codes used in reports
-- -----------------------------------------------------------------------------
-- AZ  - Arizona
-- CA  - California
-- CO  - Colorado
-- ID  - Idaho
-- IA  - Iowa
-- KS  - Kansas
-- MN  - Minnesota (added to "Other States" in 2025)
-- NE  - Nebraska
-- OK  - Oklahoma
-- SD  - South Dakota
-- TX  - Texas
-- WA  - Washington
-- Other States - IL, NM, OR, WY, MN (varies by year)
-- United States - National total
