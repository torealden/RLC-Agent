-- 142_bronze_abiove_soy_complex.sql
-- Abiove (Associação Brasileira das Indústrias de Óleos Vegetais) Brazilian
-- soy-complex statistics: monthly crush, meal/oil production, and bean/meal/oil
-- stocks, plus the annual complex balance.
--
-- Source access: the six target series live only inside Abiove's Power BI
-- "publish to web" report (no API, no downloadable file for these tabs). Data is
-- ingested from operator-extracted workbooks (Desktop copied the Power BI pages),
-- not a live scrape. See docs/runbooks/abiove_update_runbook.md.
--
-- Units: THOUSAND METRIC TONS ("1.000 t") natively, per Abiove convention and the
-- project rule "thousand tonnes for all non-US commodities unless otherwise noted".
--
-- Long-format bronze so heterogeneous Abiove tabs (monthly processing-sector
-- balance, annual whole-complex balance, monthly crush history, monthly stocks)
-- all land in one table. Silver picks the canonical tab per series.

CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.abiove_soy_complex (
    id             BIGSERIAL PRIMARY KEY,
    period         DATE    NOT NULL,           -- month-start (monthly) or year-start (annual)
    year           INT     NOT NULL,
    month          INT,                        -- NULL for annual rows
    frequency      TEXT    NOT NULL,           -- 'monthly' | 'annual'
    scope          TEXT    NOT NULL,           -- 'processing_sector' (Setor Processador) | 'complex' (Complexo)
    commodity      TEXT    NOT NULL,           -- 'soybeans' | 'meal' | 'oil'
    attribute      TEXT    NOT NULL,           -- generic line: initial_stock, crush, production,
                                               --   imports, exports, domestic_consumption, final_stock,
                                               --   grain_acquisition, seeds_other
    item_code      TEXT,                       -- Abiove line code, e.g. '1.3', '2.2'
    item_label_pt  TEXT,                       -- original Portuguese label
    value_1000t    NUMERIC,                    -- value in thousand metric tons
    is_projection  BOOLEAN DEFAULT FALSE,      -- Abiove "(P)" projected column
    source_tab     TEXT    NOT NULL,           -- workbook sheet the value came from
    source_file    TEXT    NOT NULL,
    collected_at   TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT abiove_soy_complex_uq
        UNIQUE (period, frequency, scope, commodity, attribute, source_tab)
);

CREATE INDEX IF NOT EXISTS ix_abiove_soy_complex_lookup
    ON bronze.abiove_soy_complex (commodity, attribute, frequency, period);

COMMENT ON TABLE bronze.abiove_soy_complex IS
    'Abiove Brazilian soy-complex statistics (crush/meal/oil production + stocks + annual complex balance). Units: thousand metric tons. Loaded from operator-extracted Power BI workbooks via scripts/load_abiove_crushing_data.py.';
