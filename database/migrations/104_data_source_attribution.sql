-- Migration 104: reference.data_source_attribution
-- Maps each source-tag stored on bronze rows to the publisher attribution
-- text that downstream republishing code should emit (workbook _meta tabs,
-- generated reports, dashboards, etc.).
--
-- Pattern: any row in bronze.* with a 'source' column carries the source-tag.
-- Republish code joins on this table to get the citation. New collectors
-- add a row here when they're built so attribution is automatic.

CREATE SCHEMA IF NOT EXISTS reference;

CREATE TABLE IF NOT EXISTS reference.data_source_attribution (
    source_tag           TEXT PRIMARY KEY,
    publisher            TEXT NOT NULL,
    short_citation       TEXT NOT NULL,
    full_citation        TEXT,
    license_type         TEXT,
    license_url          TEXT,
    attribution_required BOOLEAN NOT NULL DEFAULT TRUE,
    notes                TEXT,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO reference.data_source_attribution
    (source_tag, publisher, short_citation, full_citation, license_type, license_url, attribution_required, notes)
VALUES
    -- US Census Bureau
    ('CENSUS_API', 'US Census Bureau',
        'Source: US Census Bureau',
        'US Census Bureau, USA Trade Online / Foreign Trade Statistics, accessed via api.census.gov.',
        'Public domain (US Gov)', 'https://www.census.gov/data/developers/about/terms-of-service.html',
        TRUE,
        'Monthly HS-10 detail 2013-present.'),
    ('CENSUS_FILE', 'US Census Bureau',
        'Source: US Census Bureau',
        'US Census Bureau, Foreign Trade historical files, accessed via FTP archives.',
        'Public domain (US Gov)', 'https://www.census.gov/foreign-trade/',
        TRUE,
        'Annual HS files 1992+.'),
    ('CENSUS_TRADE', 'US Census Bureau',
        'Source: US Census Bureau',
        'US Census Bureau, Foreign Trade Division.',
        'Public domain (US Gov)', 'https://www.census.gov/foreign-trade/',
        TRUE, NULL),

    -- USITC DataWeb (1994-2012 backfill source)
    ('USITC_DATAWEB', 'US International Trade Commission',
        'Source: USITC DataWeb',
        'US International Trade Commission, Interactive Tariff and Trade DataWeb (DataWeb), accessed via the v2 API at dataweb.usitc.gov.',
        'Public; attribution required',
        'https://dataweb.usitc.gov/',
        TRUE,
        'HS-10 monthly trade 1989-present. Attribution required by USITC terms when redistributing.'),

    -- USDA family
    ('FAS_PSD', 'USDA Foreign Agricultural Service',
        'Source: USDA FAS PSD',
        'USDA Foreign Agricultural Service, Production, Supply and Distribution (PSD) database.',
        'Public domain (US Gov)', 'https://apps.fas.usda.gov/psdonline/',
        TRUE, 'Global S&D balance sheets.'),
    ('FAS_ESR_V2', 'USDA Foreign Agricultural Service',
        'Source: USDA FAS Export Sales',
        'USDA Foreign Agricultural Service, Export Sales Reporting (ESR) v2 API.',
        'Public domain (US Gov)', 'https://www.fas.usda.gov/data',
        TRUE, 'Weekly US export sales by commodity.'),
    ('NASS_API', 'USDA National Agricultural Statistics Service',
        'Source: USDA NASS',
        'USDA National Agricultural Statistics Service, Quick Stats database.',
        'Public domain (US Gov)', 'https://quickstats.nass.usda.gov/',
        TRUE, NULL),
    ('USDA_AMS', 'USDA Agricultural Marketing Service',
        'Source: USDA AMS',
        'USDA Agricultural Marketing Service, MyMarketNews API.',
        'Public domain (US Gov)', 'https://mymarketnews.ams.usda.gov/', TRUE, NULL),
    ('ERS_OIL_CROPS', 'USDA Economic Research Service',
        'Source: USDA ERS',
        'USDA Economic Research Service, Oil Crops Yearbook.',
        'Public domain (US Gov)', 'https://www.ers.usda.gov/data-products/oil-crops-yearbook/',
        TRUE, NULL),

    -- EIA / EPA
    ('EIA_API', 'US Energy Information Administration',
        'Source: US EIA',
        'US Energy Information Administration, Open Data API.',
        'Public domain (US Gov)', 'https://www.eia.gov/opendata/', TRUE, NULL),
    ('EPA_RFS', 'US Environmental Protection Agency',
        'Source: US EPA',
        'US Environmental Protection Agency, Renewable Fuel Standard data.',
        'Public domain (US Gov)', 'https://www.epa.gov/fuels-registration-reporting-and-compliance-help', TRUE, NULL),

    -- CFTC
    ('CFTC_COT', 'US Commodity Futures Trading Commission',
        'Source: CFTC Commitments of Traders',
        'US Commodity Futures Trading Commission, Commitments of Traders reports.',
        'Public domain (US Gov)', 'https://www.cftc.gov/MarketReports/CommitmentsofTraders/index.htm', TRUE, NULL),

    -- Brazil
    ('CONAB', 'Companhia Nacional de Abastecimento (Brazil)',
        'Source: CONAB',
        'Companhia Nacional de Abastecimento (CONAB), Brazilian National Supply Company.',
        'Public domain (BR Gov)', 'https://portaldeinformacoes.conab.gov.br/', TRUE, NULL),
    ('CONAB_DIRECT', 'Companhia Nacional de Abastecimento (Brazil)',
        'Source: CONAB',
        'Companhia Nacional de Abastecimento (CONAB), public download endpoints.',
        'Public domain (BR Gov)', 'https://portaldeinformacoes.conab.gov.br/downloads/', TRUE, NULL),

    -- Internal / placeholder
    ('LEGACY_PRIOR_EMPLOYMENT', 'Internal — prior employment source files',
        'Source: internal historical files',
        'Internal historical files predating public-source ingestion. Replace with public-sourced data when available.',
        'Internal only', NULL, FALSE,
        'Use for the gap years 1994-2012 only if/until USITC DataWeb backfill completes.')

ON CONFLICT (source_tag) DO UPDATE SET
    publisher = EXCLUDED.publisher,
    short_citation = EXCLUDED.short_citation,
    full_citation = EXCLUDED.full_citation,
    license_type = EXCLUDED.license_type,
    license_url = EXCLUDED.license_url,
    attribution_required = EXCLUDED.attribution_required,
    notes = EXCLUDED.notes,
    updated_at = NOW();

-- Helper view: count of bronze.census_trade rows per source for republish UIs
CREATE OR REPLACE VIEW reference.census_trade_source_summary AS
SELECT
    ct.source AS source_tag,
    a.publisher,
    a.short_citation,
    a.attribution_required,
    COUNT(*) AS row_count,
    MIN(ct.year || '-' || LPAD(ct.month::text, 2, '0')) AS first_period,
    MAX(ct.year || '-' || LPAD(ct.month::text, 2, '0')) AS last_period
FROM bronze.census_trade ct
LEFT JOIN reference.data_source_attribution a ON a.source_tag = ct.source
GROUP BY ct.source, a.publisher, a.short_citation, a.attribution_required;
