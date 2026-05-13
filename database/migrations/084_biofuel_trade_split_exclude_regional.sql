-- =============================================================================
-- Migration 084: Exclude regional aggregates from gold.biofuel_trade_split
-- =============================================================================
-- Bug found in mig 082/083: Census country codes like '0022' (OECD), '0003'
-- (EUROPEAN UNION), '0023' (NATO), '1XXX/2XXX/...' are regional aggregates
-- that double-count the actual country rows below them. The original filter
-- `country_code NOT LIKE '%XXX'` missed the 4-digit '00XX' codes.
--
-- Fix: join to silver.trade_country_reference and filter where
-- is_regional_total = false. This is the same pattern gold.corn_oil_trade_split
-- already uses.
-- =============================================================================

DROP VIEW IF EXISTS gold.biofuel_trade_split;

CREATE VIEW gold.biofuel_trade_split AS
WITH base AS (
    SELECT
        ct.id,
        ct.year,
        ct.month,
        ct.flow,
        ct.hs_code,
        ct.country_code,
        COALESCE(tcr.country_name, ct.country_name) AS country_name,
        tcr.region,
        ct.quantity * COALESCE(bcf.blend_content_factor, 1.0) AS quantity_net_kg,
        ct.quantity                                             AS quantity_gross_kg,
        ct.value_usd,
        bcf.blend_content_factor,
        bcf.confidence AS blend_confidence,
        (
            SELECT btj.rule_id
            FROM reference.biofuel_trade_split btj
            WHERE btj.hs_code = ct.hs_code
              AND btj.flow    = ct.flow
              AND ct.year BETWEEN btj.year_from AND btj.year_to
              AND (btj.origin = ct.country_code OR btj.origin IS NULL)
            ORDER BY (btj.origin IS NOT NULL) DESC, btj.year_from DESC
            LIMIT 1
        ) AS rule_id
    FROM bronze.census_trade ct
    LEFT JOIN silver.trade_country_reference tcr
        ON ct.country_code = tcr.country_code
    LEFT JOIN reference.biofuel_hs_blend_content bcf
        ON bcf.hs_code = ct.hs_code AND bcf.flow = ct.flow
    WHERE ct.hs_code LIKE '3826%'
      AND ct.country_code <> '-'
      AND COALESCE(tcr.is_regional_total, false) = false
)
SELECT
    b.year,
    b.month,
    make_date(b.year, b.month, 1)  AS period_date,
    b.flow,
    b.hs_code,
    b.country_code,
    b.country_name,
    b.region,
    'BIODIESEL'::TEXT              AS commodity_split,
    b.quantity_net_kg   * COALESCE(r.bd_share, 1.0)            AS quantity_kg,
    b.quantity_net_kg   * COALESCE(r.bd_share, 1.0) * 0.301    AS quantity_gal,
    b.value_usd         * COALESCE(r.bd_share, 1.0)            AS value_usd,
    b.quantity_gross_kg * COALESCE(r.bd_share, 1.0)            AS quantity_gross_kg,
    b.blend_content_factor,
    b.blend_confidence,
    COALESCE(r.confidence, 'low')  AS split_confidence,
    COALESCE(r.bd_share, 1.0)      AS share_used
FROM base b
LEFT JOIN reference.biofuel_trade_split r ON r.rule_id = b.rule_id

UNION ALL

SELECT
    b.year,
    b.month,
    make_date(b.year, b.month, 1)  AS period_date,
    b.flow,
    b.hs_code,
    b.country_code,
    b.country_name,
    b.region,
    'RENEWABLE_DIESEL'::TEXT       AS commodity_split,
    b.quantity_net_kg   * COALESCE(r.rd_share, 0.0)            AS quantity_kg,
    b.quantity_net_kg   * COALESCE(r.rd_share, 0.0) * 0.301    AS quantity_gal,
    b.value_usd         * COALESCE(r.rd_share, 0.0)            AS value_usd,
    b.quantity_gross_kg * COALESCE(r.rd_share, 0.0)            AS quantity_gross_kg,
    b.blend_content_factor,
    b.blend_confidence,
    COALESCE(r.confidence, 'low')  AS split_confidence,
    COALESCE(r.rd_share, 0.0)      AS share_used
FROM base b
LEFT JOIN reference.biofuel_trade_split r ON r.rule_id = b.rule_id
WHERE COALESCE(r.rd_share, 0.0) > 0;

COMMENT ON VIEW gold.biofuel_trade_split IS
'Heuristic split of HS 3826 Census trade between biodiesel and renewable diesel. '
'Regional aggregates (OECD, EU, etc.) excluded via silver.trade_country_reference. '
'quantity_net_kg = quantity_gross_kg × blend_content_factor (3826.00.10≈0.85, '
'3826.00.30≈0.20, 3826000000≈0.95). Then split by reference.biofuel_trade_split.';
