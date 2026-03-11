-- ============================================================
-- Knowledge Graph Extraction: Batch 009a
-- Source: HOBO Renewable Fuels Landscape and Feedstock Availability Study - Executive Summary
-- Date: 2026-02-14
-- Extraction by: Claude (KG Pipeline)
-- ============================================================

-- Source Registration
INSERT INTO kg_source (source_key, source_type, title, url, report_date, category, commodities, themes, status, first_seen, last_processed, node_count, edge_count, context_count)
VALUES
('gdoc_hobo_exec_summary', 'gdrive_doc', 'HOBO Study - Executive Summary', 'https://docs.google.com/document/d/1_if_QrZ1Kn34JLAH9xdiY3go5T-bpD2fFdIH2B55LuQ/edit', '2025-06-19', 'consulting_report', '{renewable_diesel,saf,soybean_oil,uco,tallow,dcor,choice_white_grease}', '{feasibility_study,feedstock_availability,policy_drivers,logistics,economic_competitiveness,permitting}', 'completed', NOW(), NOW(), 18, 14, 10)
ON CONFLICT (source_key) DO UPDATE SET status = 'completed', last_processed = NOW();

-- ============================================================
-- NODES
-- ============================================================

-- Entity: HOBO Renewables (the project company)
INSERT INTO kg_node (node_key, node_type, label, description, metadata)
VALUES
('hobo_renewables', 'company', 'HOBO Renewable Diesel LLC',
 'Development-stage renewable fuels company planning a 125+ MMgy HEFA RD/SAF facility in Clinton County, Iowa. Name reflects the Heating Oil-to-Bean Oil (HOBO) spread that underpins RD economics.',
 '{"location": "Clinton County, Iowa", "capacity_mgy": 125, "capacity_bpd": 9300, "technology": "HEFA", "products": ["renewable_diesel", "saf"], "status": "development_stage"}')
ON CONFLICT (node_key) DO UPDATE SET description = EXCLUDED.description, metadata = EXCLUDED.metadata;

-- Entity: HEFA Technology
INSERT INTO kg_node (node_key, node_type, label, description, metadata)
VALUES
('hefa_technology', 'technology', 'HEFA (Hydroprocessed Esters and Fatty Acids)',
 'Commercially dominant technology for RD/SAF production. Hydrotreats lipid feedstocks at high pressure with hydrogen to produce drop-in hydrocarbon fuels. 100+ plants worldwide operating, under construction, or announced. Can be tuned to produce variable RD/SAF ratios.',
 '{"maturity": "commercial", "plants_worldwide": "100+", "typical_rd_saf_ratio": "80/20 adjustable", "feedstock_type": "lipids"}')
ON CONFLICT (node_key) DO UPDATE SET description = EXCLUDED.description, metadata = EXCLUDED.metadata;

-- Entity: US RD Capacity
INSERT INTO kg_node (node_key, node_type, label, description, metadata)
VALUES
('us_rd_capacity', 'metric', 'US Renewable Diesel Capacity',
 'US renewable diesel production capacity expanded from ~1 billion gallons in 2020 to ~5 billion gallons expected by 2025. Growth is entirely policy-driven: RD costs more than fossil diesel without mandates and credits.',
 '{"capacity_2020_bgal": 1.0, "capacity_2025_bgal": 5.0, "growth_driver": "policy"}')
ON CONFLICT (node_key) DO UPDATE SET description = EXCLUDED.description, metadata = EXCLUDED.metadata;

-- Entity: US Feedstock Consumption for RD
INSERT INTO kg_node (node_key, node_type, label, description, metadata)
VALUES
('us_rd_feedstock_consumption', 'metric', 'US RD Feedstock Consumption',
 'US renewable diesel production consumed over 30 billion lbs of fats and oils in 2024, up from ~4 billion lbs in 2017. Demand is outpacing domestic supply (~32.5 billion lbs), forcing record imports.',
 '{"consumption_2017_blbs": 4, "consumption_2024_blbs": 30, "domestic_supply_blbs": 32.5, "import_growth_2020_2024": "10x"}')
ON CONFLICT (node_key) DO UPDATE SET description = EXCLUDED.description, metadata = EXCLUDED.metadata;

-- Entity: HOBO Feedstock Catchment
INSERT INTO kg_node (node_key, node_type, label, description, metadata)
VALUES
('hobo_feedstock_catchment', 'metric', 'HOBO 250-Mile Feedstock Catchment',
 'HOBO has potential access to ~6 billion lbs of feedstock within 250 miles (tallow, CWG, UCO, DCO, soybean oil) — roughly 6x its annual requirement of 1-1.1 billion lbs. Extends to 21 billion lbs at 500 miles.',
 '{"radius_250mi_blbs": 6, "radius_500mi_blbs": 21, "annual_requirement_blbs": "1.0-1.1", "coverage_ratio_250mi": "6x", "uco_access_blbs": 2.6}')
ON CONFLICT (node_key) DO UPDATE SET description = EXCLUDED.description, metadata = EXCLUDED.metadata;

-- Entity: IEA Feedstock Crunch Warning
INSERT INTO kg_node (node_key, node_type, label, description, metadata)
VALUES
('iea_feedstock_crunch', 'event', 'IEA Feedstock Crunch Warning (2027)',
 'IEA warns of potential feedstock crunch by 2027 as worldwide demand for vegetable oils and waste fats is expected to jump ~56% to 174 billion lbs under current trends.',
 '{"projected_demand_blbs": 174, "demand_increase_pct": 56, "year": 2027}')
ON CONFLICT (node_key) DO UPDATE SET description = EXCLUDED.description, metadata = EXCLUDED.metadata;

-- Entity: US Feedstock Imports
INSERT INTO kg_node (node_key, node_type, label, description, metadata)
VALUES
('us_feedstock_imports', 'metric', 'US Biofuel Feedstock Imports',
 'US imports of UCO, tallow, and other feedstocks surged 10-fold from 2020 to 2024 (738 million lbs to ~7.5 billion lbs) to meet RD demand. Import dependency creates tariff and policy risk.',
 '{"imports_2020_mlbs": 738, "imports_2024_blbs": 7.5, "growth_factor": "10x", "risk_factors": ["tariffs", "45Z_restriction", "china_trade_war"]}')
ON CONFLICT (node_key) DO UPDATE SET description = EXCLUDED.description, metadata = EXCLUDED.metadata;

-- Entity: ReFuelEU Aviation Mandate
INSERT INTO kg_node (node_key, node_type, label, description, metadata)
VALUES
('refueleu_mandate', 'policy', 'ReFuelEU Aviation Mandate',
 'EU regulation requiring minimum SAF in jet fuel: 2% by 2025, 6% by 2030, 20% by 2035. Equates to ~0.3B gal (2025), ~1.1B gal (2030), ~3.9B gal (2035). Virtually guarantees a robust SAF market.',
 '{"pct_2025": 2, "pct_2030": 6, "pct_2035": 20, "vol_2025_bgal": 0.3, "vol_2030_bgal": 1.1, "vol_2035_bgal": 3.9}')
ON CONFLICT (node_key) DO UPDATE SET description = EXCLUDED.description, metadata = EXCLUDED.metadata;

-- Entity: Global SAF Offtake Agreements
INSERT INTO kg_node (node_key, node_type, label, description, metadata)
VALUES
('global_saf_offtake', 'metric', 'Global SAF Offtake Agreements',
 'By late 2023, major airlines worldwide signed offtake agreements for over 14 billion gallons of SAF supply. These are multi-year agreements reflecting confidence production will scale.',
 '{"total_bgal": 14, "as_of": "late_2023", "type": "multi_year_offtake"}')
ON CONFLICT (node_key) DO UPDATE SET description = EXCLUDED.description, metadata = EXCLUDED.metadata;

-- Entity: Clean Fuel Production Credit (45Z)
INSERT INTO kg_node (node_key, node_type, label, description, metadata)
VALUES
('cfpc_45z', 'policy', 'Clean Fuel Production Credit (45Z)',
 'IRA tax credit replacing Blenders Tax Credit from 2025. Base $0.20/gal RD ($1.00 with prevailing wage/apprenticeship), $0.35/gal SAF ($1.75 enhanced). Scales linearly with CI below 50kgCO2/MMBTU. Set to expire 2027 but House Ways & Means drafting extension to 2031 with domestic feedstock requirement and ILUC exclusion.',
 '{"rd_base_per_gal": 0.20, "rd_enhanced_per_gal": 1.00, "saf_base_per_gal": 0.35, "saf_enhanced_per_gal": 1.75, "ci_threshold_kgco2_mmbtu": 50, "expiry": 2027, "proposed_extension": 2031}')
ON CONFLICT (node_key) DO UPDATE SET description = EXCLUDED.description, metadata = EXCLUDED.metadata;

-- Entity: HOBO CI Advantage
INSERT INTO kg_node (node_key, node_type, label, description, metadata)
VALUES
('hobo_ci_advantage', 'metric', 'HOBO Carbon Intensity Advantage',
 'HOBO targets CI in the low 20s gCO2/MJ vs industry average in the upper 30s. 75% of hydrogen feedstock produced from recycling process off-gases, reducing CI significantly. Lower CI = more credits per gallon.',
 '{"hobo_ci_gco2mj": "low_20s", "industry_avg_ci": "upper_30s", "h2_recycling_pct": 75}')
ON CONFLICT (node_key) DO UPDATE SET description = EXCLUDED.description, metadata = EXCLUDED.metadata;

-- Additional nodes for completeness
INSERT INTO kg_node (node_key, node_type, label, description, metadata)
VALUES
('rd_market', 'market', 'Renewable Diesel Market',
 'US RD market driven by RFS D4 RIN mandate, LCFS credits, and 45Z tax credit. California is dominant consumption market. Capacity ~5B gal by 2025.',
 '{"primary_driver": "RFS", "key_market": "California", "capacity_2025_bgal": 5}'),
('saf_market', 'market', 'Sustainable Aviation Fuel Market',
 'Nascent but rapidly growing market driven by EU mandates (ReFuelEU), IRA 45Z credits, state incentives, and corporate sustainability commitments. Airlines signed 14B+ gal in offtake agreements.',
 '{"stage": "nascent_rapid_growth", "key_drivers": ["ReFuelEU", "45Z", "corporate_commitments"]}'),
('california_lcfs', 'policy', 'California LCFS',
 'State LCFS awarding credits for fuels with lower CI than fossil baseline. Currently targeting 20% reduction by 2030, amendment processing for 30% by 2030 and 90% by 2045. Single biggest driver of RD growth historically.',
 '{"target_2030_current_pct": 20, "target_2030_amended_pct": 30, "target_2045_pct": 90}'),
('rfs_program', 'policy', 'Renewable Fuel Standard (RFS)',
 'Federal mandate administered by EPA establishing annual biofuel volume obligations. Uses tradeable RIN credits. D4 (biomass-based diesel) RVO has grown strongly while corn ethanol D6 is flat due to E10 blendwall.',
 '{"administrator": "EPA", "key_rin": "D4", "trend": "BBD_RVO_increasing"}'),
('uco_feedstock', 'commodity', 'Used Cooking Oil (UCO)',
 'Preferred HEFA SAF feedstock due to favorable carbon chain length vs other waste oils. US reliant on imports (esp. from China). HOBO location provides advantaged access to US domestic UCO supplies.',
 '{"ci_advantage": "very_low", "import_risk": "high", "china_dependency": "significant"}'),
('soybean_oil_feedstock', 'commodity', 'Soybean Oil (for biofuels)',
 'Largest single feedstock for US biomass-based diesel at ~49% share but declining from 53%. Higher CI than waste oils but abundant domestic supply. 20+ new crush plants announced since RD boom.',
 '{"bbd_share_pct": 49, "trend": "declining_share", "new_crush_plants": "20+", "ci": "higher_than_waste"}'),
('tallow_feedstock', 'commodity', 'Tallow (for biofuels)',
 'Animal fat feedstock. Was 80% of US RD feedstock in 2016, declined to 28% by 2024 as feedstock slate diversified. Low CI waste feedstock generating more credits.',
 '{"bbd_share_2016_pct": 80, "bbd_share_2024_pct": 28, "ci": "low"}'),
('dcor_feedstock', 'commodity', 'Distillers Corn Oil (DCO)',
 'Byproduct of ethanol production, extracted from distillers grains. Combined with UCO represents ~32% of BBD feedstock by 2024. Low CI waste feedstock.',
 '{"bbd_share_combined_with_uco_pct": 32, "source": "ethanol_byproduct", "ci": "low"}')
ON CONFLICT (node_key) DO UPDATE SET description = EXCLUDED.description, metadata = EXCLUDED.metadata;

-- ============================================================
-- EDGES
-- ============================================================

INSERT INTO kg_edge (source_node, target_node, edge_type, label, weight, metadata)
VALUES
-- Policy drives demand
('rfs_program', 'rd_market', 'DRIVES', 'RFS D4 RVO is main demand driver for US renewable diesel', 0.95,
 '{"mechanism": "D4_RIN_obligation", "confidence": "very_high", "source_count": 1}'),
('california_lcfs', 'rd_market', 'DRIVES', 'LCFS has been single biggest driver of RD growth, pulling most US RD into California', 0.95,
 '{"mechanism": "CI_credit_value", "confidence": "very_high", "source_count": 1}'),
('cfpc_45z', 'saf_market', 'ENABLES', '45Z SAF credit of up to $1.75/gal bridges cost gap to fossil jet, enabling SAF market development', 0.90,
 '{"mechanism": "production_tax_credit", "saf_credit_max": 1.75, "confidence": "very_high"}'),
('refueleu_mandate', 'saf_market', 'DRIVES', 'EU SAF mandate virtually guarantees robust SAF demand: 2%→6%→20% by 2025/2030/2035', 0.95,
 '{"mechanism": "blending_mandate", "confidence": "very_high"}'),

-- Feedstock dynamics
('us_rd_feedstock_consumption', 'iea_feedstock_crunch', 'CONTRIBUTES_TO', 'US RD feedstock demand (30B+ lbs) contributing to projected global feedstock crunch by 2027', 0.85,
 '{"mechanism": "demand_outpacing_supply", "confidence": "high"}'),
('us_rd_feedstock_consumption', 'us_feedstock_imports', 'DRIVES', 'Domestic demand outpacing domestic supply (~32.5B lbs) forces record imports (10x growth 2020-2024)', 0.90,
 '{"mechanism": "supply_deficit", "confidence": "very_high"}'),

-- HOBO advantages
('hobo_feedstock_catchment', 'hobo_renewables', 'ADVANTAGES', 'HOBO has 6x coverage ratio on feedstock within 250mi, providing significant supply security vs coastal competitors', 0.90,
 '{"coverage_ratio": "6x", "radius_miles": 250, "confidence": "high"}'),
('hobo_ci_advantage', 'hobo_renewables', 'ADVANTAGES', 'HOBO CI in low 20s vs industry upper 30s means more valuable product via higher credit generation per gallon', 0.85,
 '{"ci_delta_gco2mj": "~15", "mechanism": "higher_credits", "confidence": "high"}'),

-- Feedstock competition
('uco_feedstock', 'soybean_oil_feedstock', 'COMPETES_WITH', 'UCO and SBO compete as HEFA feedstocks; UCO preferred for low CI but supply-constrained, SBO abundant but higher CI', 0.85,
 '{"differentiation": "CI_vs_availability", "confidence": "high"}'),

-- Technology
('hefa_technology', 'rd_market', 'ENABLES', 'HEFA is commercially dominant technology for RD production with 100+ plants worldwide', 0.95,
 '{"confidence": "very_high"}'),
('hefa_technology', 'saf_market', 'ENABLES', 'HEFA also produces SAF via adjusted hydrocracking/fractionation; same plants can swing between RD and SAF', 0.90,
 '{"mechanism": "product_slate_flexibility", "confidence": "very_high"}'),

-- Market structure
('rd_market', 'saf_market', 'COMPETES_WITH', 'RD and SAF compete for same HEFA feedstock and production capacity; plants can swing output based on relative economics', 0.85,
 '{"mechanism": "shared_feedstock_shared_capacity", "confidence": "high"}'),

-- Strategic pillar
('hobo_renewables', 'saf_market', 'TARGETS', 'HOBO targets SAF market via flexible product slate, Chicago airport proximity ($1.50/gal IL incentive), and Canadian aviation demand', 0.80,
 '{"key_markets": ["Illinois_airports", "Canada", "Europe_export"], "confidence": "high"}')
ON CONFLICT DO NOTHING;

-- ============================================================
-- CONTEXTS (Analyst Brain enrichment)
-- ============================================================

INSERT INTO kg_context (context_key, node_or_edge_ref, context_type, content, confidence, metadata)
VALUES
-- Strategic Framework: Four Pillars
('hobo_four_pillars', 'hobo_renewables', 'strategic_framework',
 'HOBO study recommends four strategic pillars: (1) Feedstock Security via long-term contracts, partnerships, and multi-feedstock flexibility; (2) Production Flexibility between RD and SAF to respond to market signals; (3) Market and Policy Alignment by registering for all credit programs and securing airline/distributor offtake pre-startup; (4) Phased Growth starting with proven HEFA/established feedstocks then exploring novel feedstocks and CI improvement pathways.',
 0.90, '{"source": "hobo_exec_summary", "type": "recommendation"}'),

-- Critical Insight: Policy Dependence
('rd_saf_policy_dependence', 'rd_market', 'expert_rule',
 'RD/SAF are margin-sensitive commodity businesses despite green premium. Production costs exceed fossil equivalents. Profitability HINGES on policy incentives and feedstock pricing. Feedstock = 70-80% of HEFA operating costs. Without credits, RD carries hefty premium over diesel and struggles to compete.',
 0.95, '{"source": "hobo_exec_summary", "confidence": "very_high", "critical": true}'),

-- Feedstock Shift Pattern
('feedstock_slate_shift', 'us_rd_feedstock_consumption', 'historical_pattern',
 'US RD feedstock slate shifted dramatically: tallow fell from 80% (2016) to 28% (2024). UCO+DCO grew to 32% combined. SBO declined from 53% to 49% but remains #1. Canola oil surged from near-zero to significant share. Trend driven by CI regulations rewarding waste feedstocks with more credits.',
 0.90, '{"source": "hobo_exec_summary", "time_range": "2016-2024"}'),

-- Import Vulnerability
('import_vulnerability', 'us_feedstock_imports', 'risk_assessment',
 'US feedstock import dependency creates three risks: (1) US/Chinese tariff disputes threatening UCO supply (China is key importer); (2) Likely removal of imported feedstocks from 45Z eligibility; (3) Global competition for limited waste oil supply. Coastal/import-dependent producers face greater exposure than Midwest-located HOBO.',
 0.90, '{"source": "hobo_exec_summary", "risk_level": "high"}'),

-- Logistics Competitive Advantage
('hobo_logistics_advantage', 'hobo_renewables', 'competitive_advantage',
 'Most RD/SAF projects repurposed existing oil refineries located 500-1500 miles from primary feedstock basin. While quick to execute, they are not built-for-purpose (limits feedstock flexibility and CI) and have long supply chains. HOBO as greenfield in feedstock heartland has short supply chains (majority within 250mi by truck), built-for-purpose design, and multi-directional outbound access (rail, barge, highway) to 45% of US population centers plus Canadian demand.',
 0.90, '{"source": "hobo_exec_summary", "key_differentiator": true}'),

-- Airline SAF Commitment Scale
('airline_saf_commitments', 'saf_market', 'market_data',
 'By late 2023, major airlines had signed offtake agreements for 14+ billion gallons of SAF. This is multi-year supply reflecting confidence in production scale-up. Combined with EU ReFuelEU mandates (2%→6%→20% by 2025/2030/2035), demand signals are very strong.',
 0.85, '{"source": "hobo_exec_summary", "data_point": "14B_gal_offtake"}'),

-- Permitting Precedent Warning
('permitting_risk', 'hobo_renewables', 'risk_assessment',
 'Permitting can kill projects: a 250 MMgy RD plant in Washington State was cancelled after years due to permitting delays and uncertainties. HOBO reports all major permits secured, but Fastmarkets has not independently validated this claim.',
 0.75, '{"source": "hobo_exec_summary", "caveat": "unverified_by_fastmarkets"}'),

-- HOBO UCO Access
('hobo_uco_access', 'hobo_feedstock_catchment', 'competitive_advantage',
 'HOBO has potential access to 2.6 billion lbs of UCO. UCO is preferred HEFA SAF feedstock due to carbon chain length. US UCO import dependency (especially from China) is at risk from tariffs and 45Z restrictions. HOBO domestic access is a significant differentiator vs import-dependent coastal producers.',
 0.85, '{"source": "hobo_exec_summary", "feedstock": "UCO", "quantity_blbs": 2.6}'),

-- New Crush Capacity Signal
('crush_capacity_buildout', 'soybean_oil_feedstock', 'market_signal',
 'Over 20 new soy crushing plant projects announced since the RD boom began, versus ~70 currently operating. This massive capacity buildout signals structural shift: biofuel demand for soybean oil is reshaping the entire US oilseed processing industry.',
 0.85, '{"source": "hobo_exec_summary", "new_plants": "20+", "existing_plants": "~70"}'),

-- Capacity Growth Rate
('rd_capacity_growth', 'us_rd_capacity', 'historical_pattern',
 'US RD capacity grew from ~1B gal (2020) to ~5B gal (2025) - a 5x expansion in 5 years. This is the fastest infrastructure buildout in US biofuels history, driven entirely by policy incentives (RFS, LCFS, BTC/45Z).',
 0.90, '{"source": "hobo_exec_summary", "growth_factor": "5x", "period": "2020-2025"}')
ON CONFLICT (context_key) DO UPDATE SET content = EXCLUDED.content, confidence = EXCLUDED.confidence;
