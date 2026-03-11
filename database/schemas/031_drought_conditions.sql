-- =====================================================
-- US Drought Monitor - bronze.drought_conditions
-- Weekly state-level drought severity by area percent
-- Source: https://droughtmonitor.unl.edu/
-- =====================================================

CREATE TABLE IF NOT EXISTS bronze.drought_conditions (
    id                  SERIAL PRIMARY KEY,
    map_date            DATE NOT NULL,              -- USDM map date (Tuesday)
    state               TEXT NOT NULL,              -- State abbreviation (IA, IL, etc.) or 'US' for national
    valid_start         DATE,                       -- Week start date
    valid_end           DATE,                       -- Week end date

    -- Percentage of area in each drought category
    none_pct            NUMERIC,                    -- % area not in drought
    d0_pct              NUMERIC NOT NULL DEFAULT 0, -- % Abnormally Dry
    d1_pct              NUMERIC NOT NULL DEFAULT 0, -- % Moderate Drought
    d2_pct              NUMERIC NOT NULL DEFAULT 0, -- % Severe Drought
    d3_pct              NUMERIC NOT NULL DEFAULT 0, -- % Extreme Drought
    d4_pct              NUMERIC NOT NULL DEFAULT 0, -- % Exceptional Drought

    -- Computed aggregates
    drought_pct         NUMERIC,                    -- D0+D1+D2+D3+D4 (any drought)
    severe_drought_pct  NUMERIC,                    -- D2+D3+D4 (severe+)

    source              TEXT DEFAULT 'USDM',
    collected_at        TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (map_date, state)
);

CREATE INDEX IF NOT EXISTS idx_drought_state_date
    ON bronze.drought_conditions (state, map_date);

CREATE INDEX IF NOT EXISTS idx_drought_date
    ON bronze.drought_conditions (map_date DESC);

CREATE INDEX IF NOT EXISTS idx_drought_severe
    ON bronze.drought_conditions (state, map_date)
    WHERE severe_drought_pct > 0;

-- =====================================================
-- Gold view: latest drought conditions by state
-- =====================================================
CREATE OR REPLACE VIEW gold.drought_latest AS
SELECT
    dc.state,
    dc.map_date,
    dc.none_pct,
    dc.d0_pct,
    dc.d1_pct,
    dc.d2_pct,
    dc.d3_pct,
    dc.d4_pct,
    dc.drought_pct,
    dc.severe_drought_pct
FROM bronze.drought_conditions dc
INNER JOIN (
    SELECT state, MAX(map_date) AS max_date
    FROM bronze.drought_conditions
    GROUP BY state
) latest ON dc.state = latest.state AND dc.map_date = latest.max_date
ORDER BY dc.state;

-- =====================================================
-- Gold view: week-over-week drought change by state
-- =====================================================
CREATE OR REPLACE VIEW gold.drought_weekly_change AS
SELECT
    curr.state,
    curr.map_date AS current_date,
    prev.map_date AS prior_date,
    curr.drought_pct AS current_drought_pct,
    prev.drought_pct AS prior_drought_pct,
    ROUND(curr.drought_pct - prev.drought_pct, 2) AS drought_change,
    curr.severe_drought_pct AS current_severe_pct,
    prev.severe_drought_pct AS prior_severe_pct,
    ROUND(curr.severe_drought_pct - prev.severe_drought_pct, 2) AS severe_change
FROM bronze.drought_conditions curr
JOIN bronze.drought_conditions prev
    ON curr.state = prev.state
    AND prev.map_date = (
        SELECT MAX(map_date)
        FROM bronze.drought_conditions
        WHERE state = curr.state
          AND map_date < curr.map_date
    )
WHERE curr.map_date = (
    SELECT MAX(map_date)
    FROM bronze.drought_conditions
    WHERE state = curr.state
)
ORDER BY curr.state;
