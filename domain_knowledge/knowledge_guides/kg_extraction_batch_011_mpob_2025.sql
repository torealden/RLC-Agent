-- ============================================================================
-- RLC Knowledge Graph: Extraction Batch 011 (MPOB 2025 incremental)
-- Source: MPOB Annual Overview of the Malaysian Oil Palm Industry, 2025
-- File:   G:/My Drive/google_docs_to_add/MPOB_Overview_of_Industry_2025.docx
-- Extracted: 2026-04-16
-- Scope:  Incremental 1-year update reinforcing/extending batch_010 (MPOB 2016-2024)
-- ============================================================================

-- 2025 KEY FINDINGS:
--   * CPO production record 20.28M tonnes (+4.9%) -- first above 20M in dataset,
--     beating prior peak 19.86M (2019). Lifts assumed structural ceiling.
--   * Planted area REVERSED decline: +1.6% to 5.70M ha (from 5.61 in 2024) -- first
--     expansion since 2019 peak. Replanting trough appears ended.
--   * FFB yield 17.77 t/ha (+6.4%) -- near 2017 record 17.89. Labour recovery fully
--     reflected; second consecutive recovery year.
--   * Stocks 3.05M tonnes (+78.6%) -- 2nd highest in dataset (2018: 3.22M).
--   * NOTABLE DEVIATION: CPO annual average ROSE 2.7% to RM4,292.50 DESPITE stock
--     build. Inverse stocks-to-price rule overridden by (a) B50 anticipation, (b)
--     firm SBO, (c) lauric tightness. Rule preserved as default but 2025 logged as
--     historical_analog override event.
--   * Kenya overtook EU and China to become #2 Malaysian palm oil export market
--     (1.21M tonnes, 7.9%) -- structural East African hub upgrade.
--   * Indonesia B50 mandate (2026 target) cited explicitly as 2025 price driver.
--   * Imports tripled to 0.76M tonnes (from 0.25M) despite record domestic output.
--   * Biodiesel exports +38.4% volume / +48.1% revenue.
--   * CPKO annual average RM7,329.50 (+33.9%) -- new 10-year high.
-- ============================================================================


-- ============================================================================
-- 1. NODES: UPDATE existing data_series with 2025 datapoints (via ON CONFLICT)
-- ============================================================================

INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('data_series', 'mpob.cpo_production', 'Malaysia CPO Production',
 '{"source": "MPOB", "frequency": "monthly_annual", "units": "million_tonnes", "breakdown": ["Peninsular Malaysia", "Sabah", "Sarawak"], "historical_range_mmt": {"low": 17.32, "low_year": 2016, "high": 20.28, "high_year": 2025}, "annual_mmt": {"2016": 17.32, "2017": 19.92, "2018": 19.52, "2019": 19.86, "2020": 19.14, "2021": 18.12, "2022": 18.45, "2023": 18.55, "2024": 19.34, "2025": 20.28}, "release": "MPOB monthly supply and demand report (typically mid-following-month)", "reinforced_by_2025": "2025 production record 20.28M tonnes beat the 2019 peak (19.86M) -- first time above 20M in the MPOB dataset. Structural production ceiling revised upward. Drivers: labour recovery, FFB yield +6.4%, OER +0.4%, planted area +1.6%. Sarawak led regional growth at +7.8%."}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('data_series', 'mpob.planted_area', 'Malaysia Oil Palm Planted Area',
 '{"source": "MPOB", "frequency": "annual", "units": "million_hectares", "breakdown": ["Peninsular Malaysia", "Sabah", "Sarawak"], "historical_range_mha": {"low": 5.61, "low_year": 2024, "high": 5.90, "high_year": 2019}, "annual_mha": {"2016": 5.74, "2017": 5.81, "2018": 5.85, "2019": 5.90, "2020": 5.87, "2021": 5.74, "2022": 5.67, "2023": 5.65, "2024": 5.61, "2025": 5.70}, "trend": "Peaked 2019 (5.90M ha), declined steadily to 2024 trough (5.61M ha), REVERSED in 2025 (+1.6% to 5.70M ha). All three regions grew: Sarawak +2.2%, Peninsular +1.6%, Sabah +0.9%.", "reinforced_by_2025": "2024 was a cyclical low, not a structural endpoint. Replanting cycle emerged from its trough. Sarawak remains fastest-growing region."}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('data_series', 'mpob.ffb_yield', 'Malaysia FFB Yield',
 '{"source": "MPOB", "frequency": "annual", "units": "tonnes_per_hectare", "breakdown": ["Peninsular Malaysia", "Sabah", "Sarawak"], "historical_range": {"low_tha": 15.47, "low_year": 2021, "high_tha": 17.89, "high_year": 2017}, "annual_tha": {"2016": 15.91, "2017": 17.89, "2018": 17.16, "2019": 17.19, "2020": 16.73, "2021": 15.47, "2022": 15.76, "2023": 15.92, "2024": 16.70, "2025": 17.77}, "note": "High inter-year variability driven by El Nino, labour availability, biological rest year cycles.", "reinforced_by_2025": "2025 yield 17.77 t/ha is 2nd-highest in dataset, just below 2017 record (17.89). Second consecutive recovery year. Peninsular 19.49 (+5.8%), Sabah 16.76 (+6.5%), Sarawak 15.98 (+7.3%)."}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('data_series', 'mpob.oer', 'Malaysia National Oil Extraction Rate (OER)',
 '{"source": "MPOB", "frequency": "monthly_annual", "units": "percent", "breakdown": ["Peninsular Malaysia", "Sabah", "Sarawak"], "national_range_pct": {"low": 19.58, "low_year": 2023, "high": 20.21, "high_year": 2019}, "annual_pct": {"2016": 20.18, "2017": 19.99, "2018": 19.95, "2019": 20.21, "2020": 19.92, "2021": 19.89, "2022": 19.70, "2023": 19.58, "2024": 19.67, "2025": 19.74}, "note": "Sabah consistently highest. Peninsular and Sarawak typically 19.5-20.0%. OER below 19.70% warns of harvesting quality problems.", "reinforced_by_2025": "2025 OER 19.74% (+0.4%) -- third consecutive year below 20%. Peninsular +0.8% and Sarawak +0.7% rose; Sabah FELL to 20.31 (-1.1%) -- first Sabah decline in dataset, worth monitoring."}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('data_series', 'mpob.palm_oil_stocks', 'Malaysia Palm Oil Closing Stocks',
 '{"source": "MPOB", "frequency": "monthly_annual", "units": "million_tonnes", "breakdown": ["crude_palm_oil", "processed_palm_oil"], "historical_range_mmt": {"low": 1.27, "low_year": 2020, "high": 3.22, "high_year": 2018}, "annual_mmt": {"2016": 1.67, "2017": 2.73, "2018": 3.22, "2019": 2.01, "2020": 1.27, "2021": 1.61, "2022": 2.20, "2023": 2.29, "2024": 1.71, "2025": 3.05}, "2025_composition_tonnes": {"crude_palm_oil": 1821781, "processed_palm_oil": 1229366, "total": 3051147}, "note": "December year-end stocks are the primary annual benchmark. Strong inverse correlation with CPO prices -- with documented 2025 override.", "reinforced_by_2025": "2025 year-end 3.05M tonnes is 2nd-highest on record (after 2018: 3.22M). Build of +78.6% driven by record production outpacing 9.6% export decline plus 3-fold import surge. Crude fraction 1.82M tonnes (record). NOTABLE: CPO price rose 2.7% DESPITE this stock build."}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('data_series', 'mpob.palm_oil_exports', 'Malaysia Palm Oil Exports',
 '{"source": "MPOB/DOS", "frequency": "monthly_annual", "units": "million_tonnes", "historical_range_mmt": {"low": 15.13, "low_year": 2023, "high": 18.47, "high_year": 2019}, "annual_mmt": {"2016": 16.05, "2017": 16.56, "2018": 16.49, "2019": 18.47, "2020": 17.37, "2021": 15.56, "2022": 15.72, "2023": 15.13, "2024": 16.90, "2025": 15.27}, "reinforced_by_2025": "2025 exports 15.27M tonnes (-9.6% YoY), close to 2023 trough. Decline paradoxical given record production -- driven by strong domestic processing demand, higher domestic stocks, and competitive Indonesian supply. Top 7 markets: India 2.66M, Kenya 1.21M, EU 1.03M, China 0.90M, Turkey 0.75M, Philippines 0.72M, Japan 0.59M = 7.86M tonnes (51.5%)."}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('data_series', 'mpob.palm_oil_imports', 'Malaysia Palm Oil Imports',
 '{"source": "MPOB/DOS", "frequency": "monthly_annual", "units": "million_tonnes", "primary_source": "Indonesia (nearly 100% share)", "range_mmt": {"low": 0.25, "low_year": 2024, "high": 1.18, "high_year": 2021}, "annual_mmt": {"2024": 0.25, "2025": 0.76}, "reinforced_by_2025": "2025 imports 764,863 tonnes -- 3x the 2024 level. Counterintuitive given record domestic production. Reflects Malaysian refining/processing demand pulling Indonesian CPO for price, logistics, and grade reasons even with ample domestic supply."}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('data_series', 'mpob.cpo_price', 'Malaysia CPO Annual Average Price',
 '{"source": "MPOB", "frequency": "daily_monthly_annual", "units": "MYR/tonne", "pricing_basis": "local_delivered_Malaysia", "historical_annual_averages": {"2016": 2653, "2017": 2783, "2018": 2232.50, "2019": 2079, "2020": 2685.50, "2021": 4407, "2022": 5087.50, "2023": 3809.50, "2024": 4179.50, "2025": 4292.50}, "key_drivers": ["palm_oil_stocks", "soybean_oil_price", "brent_crude_oil", "MYR_USD_exchange_rate", "Indonesia_export_policy", "Indonesia_biodiesel_mandate_expansion"], "reinforced_by_2025": "2025 annual average RM4,292.50 (+2.7% YoY). Price rose despite +78.6% stock build -- first time in 10-year dataset where rising stocks coincided with rising annual price. MPOB cites (1) firmer SBO, (2) B50 mandate sentiment, (3) lauric tightness. Forward demand expectations overrode bearish stock signal."}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('data_series', 'mpob.pk_price', 'Malaysia Palm Kernel Annual Average Price',
 '{"source": "MPOB", "frequency": "monthly_annual", "units": "MYR/tonne", "pricing_basis": "ex_mill_Malaysia", "historical_annual_averages": {"2016": 2611, "2017": 2536, "2018": 1827.50, "2019": 1214, "2020": 1532, "2021": 2773, "2022": 3118, "2023": 2016, "2024": 2645.50, "2025": 3424.50}, "linkage": "Tracks CPKO and coconut oil (lauric complex), diverges from CPO", "reinforced_by_2025": "2025 PK average RM3,424.50 (+29.4%) -- strongest PK rally since 2021. PK outpaced CPO by 26.7 percentage points. Tight global coconut/CPKO supply."}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('data_series', 'mpob.cpko_price', 'Malaysia CPKO Annual Average Price',
 '{"source": "MPOB", "frequency": "monthly_annual", "units": "MYR/tonne", "pricing_basis": "local_delivered_Malaysia", "historical_annual_averages": {"2016": 5492.50, "2017": 5325, "2018": 3734.50, "2019": 2626.50, "2020": 3247, "2021": 5674.50, "2022": 6327, "2023": 3896, "2024": 5475.50, "2025": 7329.50}, "reinforced_by_2025": "2025 CPKO RM7,329.50 -- NEW 10-YEAR HIGH (+33.9% YoY, beats 2022 peak of 6,327 by 15.8%). CPKO/CPO ratio 1.71x -- sharpest lauric premium in dataset (vs 1.31x in 2024). Tight coconut supply plus oleochemical demand."}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('data_series', 'mpob.pfad_price', 'Malaysia PFAD Annual Average Price',
 '{"source": "MPOB", "frequency": "monthly_annual", "units": "MYR/tonne", "pricing_basis": "FOB_Malaysia", "historical_annual_averages": {"2016": 2462.50, "2017": 2733, "2018": 1922, "2019": 1807, "2020": 2546, "2021": 4233, "2022": 4313.50, "2023": 3424, "2024": 3802, "2025": 4175}, "reinforced_by_2025": "2025 PFAD RM4,175 (+9.8%) outpaced CPO (+2.7%). PFAD-CPO discount narrowed to RM117.50 (from RM377.50 in 2024). Strong biofuel feedstock demand and tight supply narrowed discount -- consistent with EU RED low-CI feedstock channel."}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('data_series', 'mpob.rbd_olein_price', 'Malaysia RBD Palm Olein Annual Average Price',
 '{"source": "MPOB", "frequency": "daily_monthly_annual", "units": "MYR/tonne", "pricing_basis": "FOB_Malaysia", "historical_annual_averages": {"2016": 2769.50, "2017": 2953.50, "2018": 2328.50, "2019": 2236.50, "2020": 2844, "2021": 4764.50, "2022": 5366.50, "2023": 4018, "2024": 4417, "2025": 4471.50}, "reinforced_by_2025": "2025 RBD olein RM4,471.50 (+1.2%) tracked CPO closely, refining margin premium RM179. RBD palm stearin fell -1.6% to RM4,353.50 -- notable first decline in a key palm product while CPO rose."}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('data_series', 'mpob.export_revenue', 'Malaysia Total Palm Oil Products Export Revenue',
 '{"source": "MPOB/DOS", "frequency": "annual", "units": "RM_billion", "historical_annual_RM_bn": {"2016": 64.59, "2017": 77.85, "2018": 67.49, "2019": 64.84, "2020": 73.25, "2021": 108.52, "2022": 137.89, "2023": 94.95, "2024": 109.39, "2025": 112.43}, "reinforced_by_2025": "2025 total revenue RM112.43bn (+2.8%) -- 3rd highest on record after 2022 (137.89) and 2024 (109.39). Palm oil volume -9.6% but palm oil revenue only -1.8%. Biodiesel revenue +48.1% on +38.4% volume. PKO revenue +35.7% on +33.9% price despite -5.9% volume -- lauric windfall."}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;


-- ============================================================================
-- 2. PRICE_LEVEL HISTORY NODES: Extend to 2025 (keep node_key stable)
-- ============================================================================

INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('price_level', 'cpo.historical_range_2016_2024', 'CPO Annual Average Price Range 2016-2025',
 '{"annual_averages_myr_tonne": {"2016": 2653, "2017": 2783, "2018": 2232.50, "2019": 2079, "2020": 2685.50, "2021": 4407, "2022": 5087.50, "2023": 3809.50, "2024": 4179.50, "2025": 4292.50}, "ten_year_range": {"low": 2079, "low_year": 2019, "high": 5087.50, "high_year": 2022}, "cycle_narrative": "El Nino 2016 -> supply recovery 2018-19 -> COVID 2020 -> labour shortage/SBO 2021 -> Ukraine/Indonesia ban 2022 -> correction 2023 -> B40 recovery 2024 -> second recovery leg on B50 anticipation 2025 despite record stocks build", "2025_note": "2025 firmness despite +78.6% stock build is the dataset anomaly.", "label_updated": "2025 extension added 2026-04-16"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties, label = EXCLUDED.label;

INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('price_level', 'malaysia.palm_oil_stocks.history_2016_2024', 'Malaysian Palm Oil Year-End Closing Stocks 2016-2025',
 '{"year_end_stocks_mmt": {"2016": 1.67, "2017": 2.73, "2018": 3.22, "2019": 2.01, "2020": 1.27, "2021": 1.61, "2022": 2.20, "2023": 2.29, "2024": 1.71, "2025": 3.05}, "inverse_correlation_with_price": "Strong 2016-2024. 2018 stocks peak (3.22M) coincided with CPO trough (RM2,232). 2020 stocks trough (1.27M) preceded 2021 record (RM4,407). 2025 is first documented override: stocks 3.05M + CPO +2.7%.", "bullish_threshold_mmt": 1.5, "bearish_threshold_mmt": 3.0, "2025_override_event": "Stocks rose from 1.71 to 3.05M tonnes (+78.6%, 2nd highest on record) but CPO annual average still gained 2.7% to RM4,292.50. MPOB attributes strength to B50 anticipation, firm SBO, lauric tightness.", "label_updated": "2025 extension added 2026-04-16"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties, label = EXCLUDED.label;


-- ============================================================================
-- 3. NEW NODE: Indonesia B50 Mandate (2026 implementation target)
-- ============================================================================

INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('policy', 'indonesia.b50_mandate_2026', 'Indonesia B50 Biodiesel Mandate (2026)',
 '{"context": "Indonesian government announced planned B50 biodiesel blending mandate for 2026, stepping up from B40. MPOB 2025 foreword explicitly identifies B50 sentiment as primary 2025 CPO price support factor. If implemented on announced pace, B50 adds approximately 1.5-2.5M tonnes incremental Indonesian CPO absorption vs B40.", "announcement_year": 2025, "implementation_year_target": 2026, "step_from": "B40", "step_to": "B50", "estimated_incremental_cpo_absorption_mmt": "1.5-2.5", "market_impact_2025": "Forward-looking bullish signal strong enough to lift CPO annual average 2.7% despite record stocks build -- best evidence in dataset of anticipatory policy pricing.", "risk_factors": ["Feedstock availability at B50 level", "Engine compatibility concerns", "Implementation slippage (historical B40 pattern)", "Fiscal cost of subsidies"], "source_document": "MPOB 2025 Annual Overview"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;


-- ============================================================================
-- 4. UPDATE existing country and policy nodes with 2025 properties
-- ============================================================================

UPDATE core.kg_node
SET properties = properties || jsonb_build_object(
    '2025_rank', 2,
    '2025_volume_mmt', 1.21,
    '2025_share_pct', 7.9,
    'reinforced_by_2025', 'Kenya became #2 Malaysian palm oil export market in 2025 at 1.21M tonnes (7.9% share), surpassing both EU and China for first time. Confirms East African distribution hub thesis -- imports CPO for local refining and re-export to Uganda, Rwanda, Congo, Burundi.'
),
last_reinforced = NOW()
WHERE node_key = 'kenya';

UPDATE core.kg_node
SET properties = properties || jsonb_build_object(
    '2025_rank', 1,
    '2025_volume_mmt', 2.66,
    '2025_share_pct', 17.4,
    'reinforced_by_2025', 'India retained #1 status for 12th consecutive year since 2014, at 2.66M tonnes (17.4% share). Share within 12-24% historical band near midpoint -- no MICECA surge nor restriction event in 2025.'
),
last_reinforced = NOW()
WHERE node_key = 'india';

UPDATE core.kg_node
SET properties = properties || jsonb_build_object(
    '2025_volume_mmt', 1.03,
    '2025_share_pct', 6.8,
    'reinforced_by_2025', 'EU fell to 3rd place in 2025 at 1.03M tonnes (6.8% share) -- surpassed by Kenya. Continued erosion from EUDR compliance and domestic oilseed availability. Low end of historical 7-13% band.'
),
last_reinforced = NOW()
WHERE node_key = 'eu';

UPDATE core.kg_node
SET properties = properties || jsonb_build_object(
    '2025_volume_mmt', 0.75,
    '2025_share_pct', 4.9,
    'reinforced_by_2025', 'Turkey share 4.9% in 2025 (0.75M tonnes) -- within historical 3.8-5.2% band.'
),
last_reinforced = NOW()
WHERE node_key = 'turkey';

UPDATE core.kg_node
SET properties = properties || jsonb_build_object(
    '2025_volume_mmt', 0.72,
    '2025_share_pct', 4.7,
    'reinforced_by_2025', 'Philippines share 4.7% in 2025 (0.72M tonnes) -- modest uptick, within historical 3.4-4.7% band.'
),
last_reinforced = NOW()
WHERE node_key = 'philippines';

UPDATE core.kg_node
SET properties = properties || jsonb_build_object(
    '2025_volume_mmt', 0.59,
    '2025_share_pct', 3.9,
    'reinforced_by_2025', 'Japan stable at 3.9% (0.59M tonnes) in 2025 -- consistent with 3.3-3.6% historical band.'
),
last_reinforced = NOW()
WHERE node_key = 'japan';

UPDATE core.kg_node
SET properties = properties || jsonb_build_object(
    'mandate_progression', jsonb_build_object(
        'B30', '2020-2022',
        'B35', '2023-2024',
        'B40', '2025',
        'B50_target', '2026 (announced 2025)'
    ),
    'reinforced_by_2025', 'MPOB 2025 explicitly cites B50 mandate anticipation as primary CPO price support factor. B50 (2026) would add ~1.5-2.5M tonnes of incremental Indonesian CPO absorption vs B40. See node indonesia.b50_mandate_2026 for detail.'
),
last_reinforced = NOW()
WHERE node_key = 'indonesia.biodiesel_mandate';


-- ============================================================================
-- 5. EDGES: REINFORCE existing edges with 2025 evidence
-- ============================================================================

UPDATE core.kg_edge
SET properties = properties || jsonb_build_object(
    'reinforced_by_2025', '2025 FFB yield +6.4% (16.70 to 17.77 t/ha) drove CPO production +4.9% to record 20.28M tonnes. Planted area also expanded +1.6% but yield was dominant contributor.'
),
weight = 0.98,
last_reinforced = NOW()
WHERE source_node_id = (SELECT id FROM core.kg_node WHERE node_key = 'mpob.ffb_yield')
  AND target_node_id = (SELECT id FROM core.kg_node WHERE node_key = 'mpob.cpo_production');

UPDATE core.kg_edge
SET properties = properties || jsonb_build_object(
    'reinforced_by_2025', '2025 OER +0.4% (19.67 to 19.74%) contributed marginally to record production.'
),
last_reinforced = NOW()
WHERE source_node_id = (SELECT id FROM core.kg_node WHERE node_key = 'mpob.oer')
  AND target_node_id = (SELECT id FROM core.kg_node WHERE node_key = 'mpob.cpo_production');

UPDATE core.kg_edge
SET properties = properties || jsonb_build_object(
    'reinforced_by_2025', '2025 textbook reinforcement: production +0.95M tonnes plus imports +0.51M tonnes outpaced exports -1.63M tonnes, driving stocks +1.34M tonnes to 3.05M (+78.6%). Classic supply-push stock build.'
),
last_reinforced = NOW()
WHERE source_node_id = (SELECT id FROM core.kg_node WHERE node_key = 'mpob.cpo_production')
  AND target_node_id = (SELECT id FROM core.kg_node WHERE node_key = 'mpob.palm_oil_stocks');

UPDATE core.kg_edge
SET properties = properties || jsonb_build_object(
    'reinforced_by_2025', '2025 reinforcement: exports -9.6% (-1.63M tonnes) was largest single contributor to stock build alongside record production.'
),
last_reinforced = NOW()
WHERE source_node_id = (SELECT id FROM core.kg_node WHERE node_key = 'mpob.palm_oil_exports')
  AND target_node_id = (SELECT id FROM core.kg_node WHERE node_key = 'mpob.palm_oil_stocks');

UPDATE core.kg_edge
SET properties = properties || jsonb_build_object(
    'reinforced_by_2025', '2025 imports tripled to 0.76M tonnes (+0.51M YoY). Despite record domestic production, import surge added meaningfully to stock build -- confirms imports as independent stock driver.'
),
weight = 0.80,
last_reinforced = NOW()
WHERE source_node_id = (SELECT id FROM core.kg_node WHERE node_key = 'mpob.palm_oil_imports')
  AND target_node_id = (SELECT id FROM core.kg_node WHERE node_key = 'mpob.palm_oil_stocks');

-- CRITICAL: Stocks -> CPO price edge. 2025 was the first override.
UPDATE core.kg_edge
SET properties = properties || jsonb_build_object(
    'reinforced_by_2025_override', '2025 is first year in 10-year dataset where inverse stocks-price rule did NOT hold on annual-average basis. Stocks rose +78.6% to 3.05M tonnes yet CPO annual average rose +2.7% to RM4,292.50. Override drivers: (1) Indonesia B50 mandate anticipation, (2) firm global SBO, (3) lauric complex tightness. Rule not abandoned -- remains default baseline -- but forward-looking policy expectations can dominate stock signal when significant mandate step imminent.',
    'edge_still_valid', true,
    'override_mechanism', 'forward_policy_expectations_dominate_when_large_mandate_step_within_6_to_12_months'
),
last_reinforced = NOW()
WHERE source_node_id = (SELECT id FROM core.kg_node WHERE node_key = 'mpob.palm_oil_stocks')
  AND target_node_id = (SELECT id FROM core.kg_node WHERE node_key = 'mpob.cpo_price');

UPDATE core.kg_edge
SET properties = properties || jsonb_build_object(
    'reinforced_by_2025', '2025 MPOB foreword cites firmer global SBO prices as PRIMARY CPO price support factor. Consistent with framework where SBO is #2 CPO driver after domestic stocks.'
),
last_reinforced = NOW()
WHERE source_node_id = (SELECT id FROM core.kg_node WHERE node_key = 'soybean_oil')
  AND target_node_id = (SELECT id FROM core.kg_node WHERE node_key = 'cpo');

UPDATE core.kg_edge
SET properties = properties || jsonb_build_object(
    'reinforced_by_2025', '2025 evidence strengthens edge. Indonesia B50 mandate (2026 announced) explicitly named by MPOB as primary CPO price support factor in 2025 -- powerful enough to override stocks-price inverse rule. Each mandate step continues as structural bullish catalyst.'
),
weight = 0.90,
last_reinforced = NOW()
WHERE source_node_id = (SELECT id FROM core.kg_node WHERE node_key = 'indonesia.biodiesel_mandate')
  AND target_node_id = (SELECT id FROM core.kg_node WHERE node_key = 'cpo');

UPDATE core.kg_edge
SET properties = properties || jsonb_build_object(
    'reinforced_by_2025', '2025 FFB mill gate RM930 (+6.3%) vs CPO +2.7%. FFB at 1% OER RM47.39 (+5.4%). FFB slightly outpaced CPO because of strong PK contribution to pricing formula (PK +29.4%).'
),
last_reinforced = NOW()
WHERE source_node_id = (SELECT id FROM core.kg_node WHERE node_key = 'cpo')
  AND target_node_id = (SELECT id FROM core.kg_node WHERE node_key = 'ffb');

UPDATE core.kg_edge
SET properties = properties || jsonb_build_object(
    'reinforced_by_2025', '2025 lauric rally: CPKO +33.9%, PK +29.4% -- sharply outpaced CPO +2.7%. CPKO/CPO ratio 1.71x (from 1.31x in 2024) -- strongest lauric divergence in 10-year dataset. MPOB cites stronger global lauric oil prices as PK/CPKO driver.'
),
last_reinforced = NOW()
WHERE source_node_id = (SELECT id FROM core.kg_node WHERE node_key = 'coconut_oil')
  AND target_node_id = (SELECT id FROM core.kg_node WHERE node_key = 'cpko');

UPDATE core.kg_edge
SET properties = properties || jsonb_build_object(
    'reinforced_by_2025', '2025 planted area +1.6% contributed to record production alongside yield and OER. Ends 2019-2024 declining-area narrative. Replanting cycle emerging from trough; medium-term production ceiling may lift if area growth persists.'
),
last_reinforced = NOW()
WHERE source_node_id = (SELECT id FROM core.kg_node WHERE node_key = 'mpob.planted_area')
  AND target_node_id = (SELECT id FROM core.kg_node WHERE node_key = 'mpob.cpo_production');


-- ============================================================================
-- 6. NEW EDGES: 2025-specific additions
-- ============================================================================

INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'indonesia.b50_mandate_2026'),
 (SELECT id FROM core.kg_node WHERE node_key = 'mpob.cpo_price'),
 'CAUSES', 0.85,
 '{"mechanism": "Indonesia B50 mandate (2026) creates forward-looking bullish expectation -- incremental 1.5-2.5M tonnes Indonesian CPO absorbed domestically vs B40, tightening global export availability. MPOB 2025 foreword explicitly names B50 anticipation as primary CPO price support factor. Forward demand expectation strong enough to override normally-dominant stocks-price inverse rule in 2025.", "direction": "positive_price_support_forward_looking", "magnitude_qualitative": "sufficient_to_offset_a_record_stocks_build_in_2025", "lead_time": "0-12 months before implementation", "evidence_2025": "CPO +2.7% despite +78.6% stocks build -- mandate anticipation identified by MPOB as causal mechanism."}'::jsonb,
 'extracted', 0.85);

INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'indonesia.b50_mandate_2026'),
 (SELECT id FROM core.kg_node WHERE node_key = 'indonesia.biodiesel_mandate'),
 'SUPPLIES', 1.0,
 '{"mechanism": "B50 is announced successor step to B40 in Indonesia national biodiesel mandate progression (B30 -> B35 -> B40 -> B50). Compositional/successor relationship, not causal.", "progression": "B30 (2020-2022) -> B35 (2023-2024) -> B40 (2025) -> B50 (2026 announced)"}'::jsonb,
 'extracted', 1.0);

INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'kenya'),
 (SELECT id FROM core.kg_node WHERE node_key = 'mpob.palm_oil_exports'),
 'CONSUMES', 0.80,
 '{"mechanism": "Kenya emerged in 2025 as #2 Malaysian palm oil export destination (1.21M tonnes, 7.9% share), surpassing EU and China for first time. Imports primarily CPO for local refining and re-export to landlocked East African countries. Structural East African distribution hub model.", "direction": "positive_demand_pull", "2025_rank_shift": "From #4 in 2024 to #2 in 2025", "2025_volume_mmt": 1.21, "market_type": "re_export_hub"}'::jsonb,
 'extracted', 0.80);


-- ============================================================================
-- 7. CONTEXTS: REINFORCE existing contexts with 2025 datapoints
-- ============================================================================

UPDATE core.kg_context
SET context_value = context_value || jsonb_build_object(
    'updated_2025', jsonb_build_object(
        '2025_observation', 'Driver hierarchy tested by unusual 2025 configuration: stocks BEARISH (+78.6% to 3.05M tonnes) but CPO gained +2.7%. Forward-looking B50 mandate expectation plus firm SBO dominated.',
        'rule_refinement', 'When large Indonesian mandate step (B40 -> B50 class) is 6-12 months away, forward policy expectations can temporarily outrank domestic stocks. Default hierarchy reasserts once policy realized or delayed.',
        '2025_snapshot', jsonb_build_object(
            'stocks_mmt', 3.05,
            'sbo_direction', 'firmer',
            'indonesia_policy_mode', 'B50_anticipation_bullish',
            'brent', 'stable',
            'cpo_price_result_myr', 4292.50
        )
    )
),
last_reinforced = NOW()
WHERE node_id = (SELECT id FROM core.kg_node WHERE node_key = 'mpob.cpo_price')
  AND context_key = 'cpo_price_driver_hierarchy';

UPDATE core.kg_context
SET context_value = context_value || jsonb_build_object(
    'ten_year_history_mmt', jsonb_build_object(
        '2016', 1.67, '2017', 2.73, '2018', 3.22, '2019', 2.01, '2020', 1.27,
        '2021', 1.61, '2022', 2.20, '2023', 2.29, '2024', 1.71, '2025', 3.05
    ),
    'updated_2025', jsonb_build_object(
        '2025_override', 'First documented override: stocks 3.05M tonnes (very bearish threshold >3.0M) but CPO annual average +2.7%. Standard thresholds still apply as baseline, but forward policy signals can temporarily invert the rule.',
        'rule_with_caveat', 'The >3.0M tonnes very-bearish signal is default bearish -- override only when (a) significant Indonesia mandate step 6-12 months out, OR (b) SBO in strong rally, OR (c) lauric complex in acute tightness pulling whole veg oil stack up.'
    )
),
last_reinforced = NOW()
WHERE node_id = (SELECT id FROM core.kg_node WHERE node_key = 'mpob.palm_oil_stocks')
  AND context_key = 'stocks_to_price_signal_levels';

UPDATE core.kg_context
SET context_value = context_value || jsonb_build_object(
    'ten_year_range_tha', jsonb_build_object(
        'trough', 15.47, 'trough_year', 2021,
        'peak', 17.89, 'peak_year', 2017,
        'recent_recovery', 17.77, 'recent_recovery_year', 2025
    ),
    'updated_2025', jsonb_build_object(
        '2025_observation', 'FFB yield 17.77 t/ha (+6.4%), 2nd-highest in dataset behind 2017 record (17.89). Confirms post-COVID labour recovery is multi-year, not single-year.',
        'recovery_trajectory_2023_2025', jsonb_build_object('2023', 15.92, '2024', 16.70, '2025', 17.77),
        'rule_refinement', 'After labour-induced yield trough (2021: 15.47), recovery takes 3-4 years to approach prior peak. Different from El Nino recovery (2016: 15.91 -> 2017: 17.89 single-year).'
    )
),
last_reinforced = NOW()
WHERE node_id = (SELECT id FROM core.kg_node WHERE node_key = 'mpob.ffb_yield')
  AND context_key = 'yield_shock_pattern_playbook';

UPDATE core.kg_context
SET context_value = context_value || jsonb_build_object(
    'updated_2025', jsonb_build_object(
        '2025_volume_mmt', 2.66,
        '2025_share_pct', 17.4,
        'observation', 'India share 17.4% in 2025 within 12-24% historical band near midpoint. No MICECA surge or restriction event. India-Malaysia in neutral policy mode.',
        'consecutive_years_as_number_one', '12 (since 2014)'
    )
),
last_reinforced = NOW()
WHERE node_id = (SELECT id FROM core.kg_node WHERE node_key = 'india')
  AND context_key = 'india_palm_oil_demand_framework';

UPDATE core.kg_context
SET context_value = context_value || jsonb_build_object(
    'updated_2025', jsonb_build_object(
        '2025_national_pct', 19.74,
        '2025_regional_pct', jsonb_build_object('peninsular', 19.62, 'sabah', 20.31, 'sarawak', 19.51),
        'observation_2025', 'National OER +0.4% to 19.74% -- third consecutive year below 20%. Peninsular and Sarawak rose, but Sabah FELL -1.1% to 20.31 -- first Sabah decline in dataset.',
        'rule_refinement', 'OER between 19.5-19.8% with simultaneous Sabah decline may signal harvesting quality issues specific to East Malaysia. Flag for 2026.'
    )
),
last_reinforced = NOW()
WHERE node_id = (SELECT id FROM core.kg_node WHERE node_key = 'mpob.oer')
  AND context_key = 'oer_quality_and_warning_signals';

UPDATE core.kg_context
SET context_value = context_value || jsonb_build_object(
    'updated_2025', jsonb_build_object(
        '2025_cpko_myr', 7329.50,
        '2025_cpo_myr', 4292.50,
        '2025_cpko_cpo_ratio', 1.71,
        '2025_observation', 'CPKO/CPO ratio 1.71x in 2025 -- sharpest lauric premium in 10-year dataset. CPKO new 10-year annual high RM7,329.50. Driver: tight global coconut oil supply plus oleochemical demand pull.',
        'rule_refinement_2025', 'Ratio above 1.7x -> lauric tightness dominant signal for CPKO/PK pricing. Watch coconut harvest in Philippines and Indonesia as primary reversion catalyst.'
    ),
    'cpko_cpo_ratio_history_updated', jsonb_build_object(
        '2016', '2.07x', '2019', '1.26x', '2021', '1.29x', '2022', '1.24x', '2024', '1.31x', '2025', '1.71x'
    )
),
last_reinforced = NOW()
WHERE node_id = (SELECT id FROM core.kg_node WHERE node_key = 'cpko')
  AND context_key = 'lauric_oil_pricing_framework';

UPDATE core.kg_context
SET context_value = context_value || jsonb_build_object(
    'updated_2025', jsonb_build_object(
        'year_2025', 'Palm oil volume -9.6% but revenue only -1.8% (price absorbed most of volume decline). Total products revenue +2.8% (biodiesel +48.1%, PKO +35.7%, oleochemicals +11.3% offset palm oil revenue decline). Mixed-divergence year driven by product-mix shift toward lauric and biodiesel.',
        'rule_refinement', 'When volume declines while biodiesel / PKO / oleochemical revenue rises, total export value story is structurally positive even if headline palm oil volumes weak. Monitor product-level revenue, not just palm oil headline.'
    )
),
last_reinforced = NOW()
WHERE node_id = (SELECT id FROM core.kg_node WHERE node_key = 'mpob.export_revenue')
  AND context_key = 'volume_revenue_divergence_rule';

UPDATE core.kg_context
SET context_value = context_value || jsonb_build_object(
    'updated_2025', jsonb_build_object(
        '2025_area_mha', 5.70,
        '2025_change_pct', 1.6,
        '2025_observation', 'Planted area REVERSED 5-year decline in 2025. All three regions expanded: Sarawak +2.2%, Peninsular +1.6%, Sabah +0.9%. MPOB attributes to immature palm areas from accelerated replanting. 2024 (5.61M ha) was cyclical low, not structural endpoint.',
        'rule_refinement', 'The structural area decline narrative from 2019-2024 requires qualification post-2025. Medium-term production ceiling may lift toward 21-22M tonnes if area growth persists with yield above 17 t/ha.'
    )
),
last_reinforced = NOW()
WHERE node_id = (SELECT id FROM core.kg_node WHERE node_key = 'mpob.planted_area')
  AND context_key = 'planted_area_trend_and_implication';

UPDATE core.kg_context
SET context_value = context_value || jsonb_build_object(
    'updated_2025', jsonb_build_object(
        'mode_4_mandate_step_anticipation', 'Indonesia mandate step announcements (e.g. B40 -> B50 for 2026) create forward-looking bullish expectation in palm oil markets 6-12 months ahead. This anticipatory pricing is NEW in 2025 -- strong enough to override bearish stock fundamentals.',
        '2025_evidence', 'MPOB 2025 foreword explicitly names B50 anticipation as primary CPO support factor. CPO +2.7% despite stocks +78.6% to 3.05M tonnes.',
        'monitoring_implication', 'Track Indonesian biodiesel mandate roadmap announcements, budget allocations for subsidies, and APROBI implementation readiness. B50 slippage risk: historical B40 implementation slipped ~12 months from announcement to effective date.'
    )
),
last_reinforced = NOW()
WHERE node_id = (SELECT id FROM core.kg_node WHERE node_key = 'indonesia.export_policy')
  AND context_key = 'indonesia_policy_market_impact_framework';


-- ============================================================================
-- 8. NEW CONTEXTS: Genuine 2025-only framework additions
-- ============================================================================

INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'mpob.cpo_price'),
 'historical_analog', 'stocks_price_override_2025',
 '{"rule": "2025 is the first documented year in the MPOB 10-year dataset where rising stocks and rising CPO price coincided on an annual-average basis.", "setup": {"stocks_change_pct": 78.6, "stocks_level_mmt": 3.05, "stocks_rank_in_dataset": "2nd highest (after 2018: 3.22M)", "cpo_price_change_pct": 2.7, "cpo_price_myr": 4292.50}, "override_factors": ["Indonesia B50 mandate (2026) anticipation -- explicitly cited by MPOB", "Firm global soybean oil prices -- explicitly cited", "Lauric complex tightness (CPKO +33.9%) dragging entire veg oil stack up"], "diagnostic_question_set": "When encountering bearish stock level with firm prices, ask: (1) Is there large Indonesian mandate step announced for next 12 months? (2) Is SBO in rally phase? (3) Is lauric complex in acute tightness? If yes to any, stocks-price inverse rule may be temporarily overridden.", "reversion_signal": "Override reverses when (a) mandate implementation realized or delayed, (b) SBO corrects, or (c) lauric supply recovers."}'::jsonb,
 'stocks_high_prices_rising', 'extracted');

INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'indonesia.b50_mandate_2026'),
 'expert_rule', 'indonesia_b50_implementation_framework',
 '{"rule": "Indonesia B50 biodiesel mandate (announced 2025, target implementation 2026) is next structural step in domestic Indonesian CPO absorption.", "incremental_absorption_mmt": "1.5-2.5 vs B40", "total_b50_cpo_absorption_estimate_mmt": "13-15 (vs B40 estimate 11-12)", "global_export_impact": "Reduces Indonesian CPO export availability by 1.5-2.5M tonnes annually at full implementation. Malaysian market share beneficiary.", "implementation_risks": ["Historical B40 slippage (announced 2024, phased through 2025) suggests B50 full implementation may slip into H2 2026 or 2027", "Feedstock availability -- requires 20%+ of Indonesian CPO output domestically", "Engine compatibility in commercial/heavy-duty fleet", "Fiscal subsidy cost dependent on CPO vs diesel spread"], "monitoring_signals": ["Indonesian Ministry of Energy announcements on phased implementation dates", "APROBI monthly production/blending reports", "Pertamina procurement tenders for FAME", "Indonesian government budget line for biofuel subsidies"], "market_pricing_behavior": "2025 evidence shows markets price mandate expectation 6-12 months ahead. CPO rallied into 2025 on B50 anticipation despite record stocks build."}'::jsonb,
 'always', 'extracted');

INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'kenya'),
 'expert_rule', 'kenya_east_africa_hub_framework',
 '{"rule": "Kenya has structurally upgraded from a minor top-10 market to #2 Malaysian palm oil export destination, surpassing both EU and China in 2025.", "growth_trajectory_mmt": {"2020": 0.3, "2021": 0.67, "2022": 0.95, "2023": 1.15, "2024": 1.18, "2025": 1.21}, "structural_drivers": ["East African regional refining hub -- Kenya imports CPO, refines domestically, re-exports to Uganda, Rwanda, Burundi, Congo (DRC)", "Mombasa port infrastructure advantage for landlocked East African markets", "Consumer edible oil demand growth across East African population centers", "Policy stability relative to Nigerian/other African palm oil markets"], "import_profile": "Primarily CPO (not refined olein) -- consistent with refining hub model. Food-grade application dominates.", "market_share_trajectory_pct": {"2021": 4.3, "2024": 7.0, "2025": 7.9}, "monitoring": "Track Kenya-Malaysia trade agreement developments, Mombasa refining capacity expansions, EAC tariff harmonization.", "rule_implication": "East Africa is now a genuine third pole of Malaysian palm oil demand alongside South Asia and East Asia. Structural, not cyclical."}'::jsonb,
 'always', 'extracted');

INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'mpob.palm_oil_imports'),
 'expert_rule', 'imports_despite_domestic_surplus_2025',
 '{"rule": "In 2025, Malaysia recorded record domestic CPO production AND tripled imports simultaneously -- unusual configuration requiring explanation.", "2025_facts": {"domestic_production_mmt": 20.28, "imports_mmt": 0.76, "exports_mmt": 15.27, "closing_stocks_mmt": 3.05}, "mechanistic_explanation": "Malaysian refining/oleochemical processing demand pulled in Indonesian CPO for reasons independent of physical supply shortage: (a) logistics -- certain refiners closer to Indonesian origin, (b) price arbitrage -- Indonesian CPO at discount when Indonesia was exporting aggressively, (c) product-specific sourcing -- particular feedstock grades preferred from Indonesia.", "broken_prior_rule": "Prior mental model: imports surge ONLY when domestic production low. 2025 shows imports can rise even with record production -- Malaysian processing sector is price/logistics-driven importer, not just shortage-driven.", "diagnostic_value": "Imports now a two-factor signal: (1) domestic production shortfall (traditional driver), (2) Indonesian price/logistics pull (new factor). Both may operate independently.", "monitoring_implication": "Track Malaysia-Indonesia CPO price spread and relative export levies."}'::jsonb,
 'always', 'extracted');


-- ============================================================================
-- 9. SOURCE REGISTRY: Register MPOB 2025
-- ============================================================================

INSERT INTO core.kg_source (
    source_key, source_type, title, location_uri, document_date, document_type,
    commodities, topics, status, first_processed, last_processed,
    nodes_extracted, edges_extracted, contexts_extracted
) VALUES
('docx_mpob_2025', 'local_file', 'MPOB_Overview_of_Industry_2025',
 'G:/My Drive/google_docs_to_add/MPOB_Overview_of_Industry_2025.docx',
 '2026-01-01', 'annual_report',
 '{cpo,ffb,palm_kernel,cpko,pkc,palm_oleochemicals,palm_biodiesel_my,rbd_palm_olein,pfad}',
 '{record_cpo_production_20.28M,planted_area_reversal,ffb_yield_recovery_17.77,stocks_build_3.05M,cpo_price_firm_despite_stocks,kenya_number_2_market,indonesia_b50_anticipation,cpko_record_7329,imports_tripled,biodiesel_export_surge,lauric_divergence_1.71x,stocks_price_override_event}',
 'completed', NOW(), NOW(), 1, 3, 4)
ON CONFLICT (source_key) DO UPDATE SET
    status = 'completed',
    last_processed = NOW(),
    title = EXCLUDED.title,
    location_uri = EXCLUDED.location_uri,
    document_date = EXCLUDED.document_date,
    commodities = EXCLUDED.commodities,
    topics = EXCLUDED.topics;


-- ============================================================================
-- END OF BATCH 011 (MPOB 2025 incremental)
-- ============================================================================
-- Touch summary:
--   NODES:
--     * 13 existing data_series / price_level nodes updated via ON CONFLICT
--     * 1 NEW node created: indonesia.b50_mandate_2026
--     * 7 region/country/policy nodes updated via UPDATE
--       (india, kenya, eu, turkey, philippines, japan, indonesia.biodiesel_mandate)
--   EDGES:
--     * 10 existing edges reinforced via UPDATE
--     * 3 NEW edges:
--         indonesia.b50_mandate_2026 -> mpob.cpo_price (CAUSES)
--         indonesia.b50_mandate_2026 -> indonesia.biodiesel_mandate (SUPPLIES)
--         kenya -> mpob.palm_oil_exports (CONSUMES)
--   CONTEXTS:
--     * 9 existing contexts reinforced via UPDATE
--     * 4 NEW contexts:
--         stocks_price_override_2025 (historical_analog)
--         indonesia_b50_implementation_framework (expert_rule)
--         kenya_east_africa_hub_framework (expert_rule)
--         imports_despite_domestic_surplus_2025 (expert_rule)
--   SOURCES:
--     * 1 new source registered: docx_mpob_2025
-- ============================================================================
