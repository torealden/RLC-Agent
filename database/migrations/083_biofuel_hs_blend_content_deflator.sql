-- =============================================================================
-- Migration 083: HTSUS 3826 blend-content deflator
-- =============================================================================
-- Problem: Census `quantity` is gross product weight including petroleum
-- diluent. HTSUS 3826.00.30 is "biodiesel and mixtures, < 50% biodiesel
-- content" — most of the mass is petroleum. EIA's biodiesel trade series
-- counts net biodiesel content only. Without a deflator the split is 4×
-- too high vs the EIA anchor.
--
-- This migration adds a per-HS-code deflator table and updates
-- gold.biofuel_trade_split to multiply quantity by the deflator before the
-- BD/RD split.
-- =============================================================================

CREATE TABLE IF NOT EXISTS reference.biofuel_hs_blend_content (
    hs_code              TEXT NOT NULL,
    flow                 TEXT NOT NULL CHECK (flow IN ('imports','exports')),
    blend_content_factor NUMERIC NOT NULL CHECK (blend_content_factor > 0 AND blend_content_factor <= 1),
    confidence           TEXT NOT NULL CHECK (confidence IN ('high','medium','low')),
    source               TEXT,
    notes                TEXT,
    PRIMARY KEY (hs_code, flow)
);

DELETE FROM reference.biofuel_hs_blend_content;

INSERT INTO reference.biofuel_hs_blend_content (hs_code, flow, blend_content_factor, confidence, source, notes) VALUES
  ('3826001000','imports', 0.85, 'medium', 'HTSUS chapter notes',
   'HTSUS 3826.00.10: "biodiesel and mixtures, containing 50% or more biodiesel". Assume avg ~85% net content (covers B50-B100 with B100 typical).'),
  ('3826003000','imports', 0.20, 'medium', 'HTSUS chapter notes',
   'HTSUS 3826.00.30: "biodiesel and mixtures, containing less than 50% biodiesel". Assume avg ~20% net (B5-B20 typical; petroleum dominates mass).'),
  ('3826000000','exports', 0.95, 'high',   'Schedule B; merchant grade convention',
   'Schedule B single code for biodiesel exports. US exports primarily B100 merchant grade for downstream blending. Deflator near 1 because mass is mostly biodiesel.');

-- =============================================================================
-- Update gold.biofuel_trade_split to apply the deflator
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
        ct.country_name,
        -- Apply HS-level deflator BEFORE the BD/RD split. quantity_net_kg is the
        -- estimated net-biodiesel-content mass; quantity (gross) is preserved
        -- for traceability.
        ct.quantity * COALESCE(bcf.blend_content_factor, 1.0) AS quantity_net_kg,
        ct.quantity AS quantity_gross_kg,
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
    LEFT JOIN reference.biofuel_hs_blend_content bcf
      ON bcf.hs_code = ct.hs_code AND bcf.flow = ct.flow
    WHERE ct.hs_code LIKE '3826%'
      AND ct.country_code <> '-'
      AND ct.country_code NOT LIKE '%XXX'
)
SELECT
    b.year,
    b.month,
    make_date(b.year, b.month, 1)  AS period_date,
    b.flow,
    b.hs_code,
    b.country_code,
    b.country_name,
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
'quantity_net_kg = quantity_gross_kg × blend_content_factor (HTSUS 3826.00.10 ≈ 0.85, '
'3826.00.30 ≈ 0.20, Schedule B ≈ 0.95). BD/RD split then applied to net mass. '
'quantity_gal uses 0.301 gal/kg ≈ FAME-ish density.';
