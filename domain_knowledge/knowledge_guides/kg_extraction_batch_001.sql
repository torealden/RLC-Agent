-- ============================================================================
-- RLC Knowledge Graph: Extraction Batch 001
-- Source: 18 reports (8 CO weekly, 10 RIN Forecast) spanning 2019-2022
-- Extracted: 2026-02-14
-- ============================================================================

-- ============================================================================
-- NODES: Entities in the canola oil / biofuels analytical universe
-- ============================================================================

-- Commodities
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('commodity', 'canola_oil', 'Canola Oil', '{"markets": ["Los Angeles cash", "Chicago"], "units": "cents/lb", "ticker_proxy": "ICE canola futures"}'),
('commodity', 'soybean_oil', 'Soybean Oil', '{"markets": ["CBOT futures", "Central Illinois CDG"], "units": "cents/lb", "ticker": "ZL"}'),
('commodity', 'canola_seed', 'Canola Seed', '{"markets": ["ICE Winnipeg", "cash PNW"], "units": "CAD/tonne", "ticker": "RS"}'),
('commodity', 'palm_oil', 'Palm Oil', '{"markets": ["BMD Malaysia"], "units": "MYR/tonne"}'),
('commodity', 'corn_oil', 'Corn Oil', '{"context": "Competing vegetable oil, substitute in food and biodiesel"}'),
('commodity', 'heating_oil', 'Heating Oil', '{"markets": ["NYMEX"], "units": "USD/gallon", "ticker": "HO"}'),
('commodity', 'crude_oil', 'Crude Oil', '{"markets": ["NYMEX WTI", "ICE Brent"], "units": "USD/barrel"}'),
('commodity', 'd4_rin', 'D4 RIN (Biomass-Based Diesel)', '{"units": "cents", "context": "Renewable Identification Number for biomass-based diesel compliance"}'),
('commodity', 'd6_rin', 'D6 RIN (Ethanol)', '{"units": "cents", "context": "Renewable Identification Number for conventional biofuel (ethanol)"}')
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Data Series
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('data_series', 'copa.canola_crush', 'COPA Canadian Canola Crushing Volume', '{"source": "Canadian Oilseed Processors Association", "frequency": "monthly_official_weekly_estimate", "units": "tonnes", "release_lag": "~4 weeks"}'),
('data_series', 'copa.canola_oil_production', 'COPA Canadian Canola Oil Production', '{"source": "COPA", "frequency": "monthly", "units": "tonnes"}'),
('data_series', 'nass.fats_oils.canola_crush', 'NASS U.S. Canola Crushing Volume', '{"source": "USDA NASS Fats and Oils report", "frequency": "monthly", "units": "million_lbs"}'),
('data_series', 'nass.fats_oils.canola_oil_stocks', 'NASS U.S. Canola Oil Inventories', '{"source": "USDA NASS Fats and Oils report", "frequency": "monthly", "units": "million_lbs", "breakdown": ["crude", "refined"]}'),
('data_series', 'census.canola_oil_imports', 'Census Bureau U.S. Canola Oil Imports', '{"source": "U.S. Census Bureau trade data", "frequency": "monthly", "units": "million_lbs"}'),
('data_series', 'canada.canola_seed_exports', 'Canadian Canola Seed Exports', '{"source": "Statistics Canada / Trade data", "frequency": "monthly", "units": "tonnes"}'),
('data_series', 'canada.canola_oil_exports', 'Canadian Canola Oil Exports', '{"source": "Statistics Canada / Trade data", "frequency": "monthly", "units": "tonnes", "key_destinations": ["United States", "China"]}'),
('data_series', 'eia.biodiesel_feedstock', 'EIA Monthly Biofuels Capacity and Feedstocks', '{"source": "Energy Information Administration", "frequency": "monthly", "context": "Reports canola oil used in biodiesel production"}'),
('data_series', 'epa.emts', 'EPA EMTS RIN Generation Data', '{"source": "EPA Moderated Transaction System", "frequency": "monthly", "context": "Tracks RIN generation by fuel type and D-code"}'),
('data_series', 'hobo_spread', 'HOBO Spread (Soybean Oil minus Heating Oil)', '{"calculation": "CBOT soybean oil front month minus NYMEX heating oil equivalent in cents/lb", "units": "cents/lb", "context": "Key indicator of biodiesel production economics"}'),
('data_series', 'canola_oil_basis_la', 'Canola Oil Basis (LA to SBO)', '{"calculation": "RBD canola oil Los Angeles minus CDG soybean oil Central Illinois", "units": "cents/lb"}'),
('data_series', 'us.canola_oil.domestic_use', 'U.S. Canola Oil Domestic Disappearance', '{"derived_from": "NASS stocks, trade, production data", "breakdown": ["biofuel_use", "non_biofuel_use"], "units": "million_lbs"}'),
('data_series', 'us.canola_oil.ending_stocks', 'U.S. Canola Oil Ending Stocks', '{"source": "USDA/Jacobsen balance sheet", "units": "million_lbs"}'),
('data_series', 'canada.canola.ending_stocks', 'Canadian Canola Ending Stocks', '{"source": "USDA/StatsCan/Jacobsen", "units": "million_tonnes"}'),
('data_series', 'usda.crop_progress.canola_planting', 'USDA Canola Planting Progress (North Dakota)', '{"source": "USDA NASS Crop Progress", "frequency": "weekly_apr_jun", "units": "percent_planted"}'),
('data_series', 'stats_can.prospective_plantings', 'Statistics Canada Canola Prospective Plantings', '{"source": "Statistics Canada", "frequency": "annual_spring", "units": "million_hectares"}')
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Reports
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('report', 'usda.wasde', 'USDA WASDE Report', '{"frequency": "monthly", "context": "World Agricultural Supply and Demand Estimates - primary government balance sheet"}'),
('report', 'usda.fats_oils', 'USDA NASS Fats and Oils Report', '{"frequency": "monthly", "context": "Domestic crushing, production, stocks, disappearance for oilseeds"}'),
('report', 'eia.monthly_biofuels', 'EIA Monthly Biofuels Capacity and Feedstocks Update', '{"frequency": "monthly", "context": "Feedstock usage in biodiesel and renewable diesel by oil type"}')
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Regions
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('region', 'canada.prairies', 'Canadian Prairies', '{"provinces": ["Saskatchewan", "Alberta", "Manitoba"], "context": "Primary canola growing region globally"}'),
('region', 'us.northern_plains', 'U.S. Northern Plains', '{"states": ["North Dakota", "Montana", "Minnesota", "South Dakota"], "context": "Primary U.S. canola growing region"}'),
('region', 'china', 'China', '{"context": "Major canola seed and canola oil importer from Canada"}')
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Market Participants
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('market_participant', 'copa', 'Canadian Oilseed Processors Association (COPA)', '{"context": "Reports weekly and monthly Canadian canola crushing data"}'),
('market_participant', 'biodiesel_producers', 'U.S. Biodiesel Producers', '{"context": "Demand source for canola oil, soybean oil, and other feedstocks"}'),
('market_participant', 'food_manufacturers', 'Food Manufacturers', '{"context": "Major non-biofuel demand source for canola oil, consumer preference driver"}')
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Policy Mechanisms
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('policy', 'rfs2', 'Renewable Fuel Standard (RFS2)', '{"context": "Federal mandate for renewable fuel blending volumes, administered by EPA"}'),
('policy', 'rvo', 'Renewable Volume Obligations (RVO)', '{"context": "Annual blending requirements set under RFS2, finalized by November each year"}'),
('policy', 'sre', 'Small Refinery Exemptions (SRE)', '{"context": "EPA waivers that reduce effective RFS demand obligations, historically bearish for D4 RINs"}'),
('policy', 'btc', 'Blender Tax Credit (BTC)', '{"context": "$1/gallon tax credit for biodiesel blending, on-again-off-again policy with major market impact"}'),
('policy', 'lcfs', 'California Low Carbon Fuel Standard (LCFS)', '{"context": "State-level low carbon fuel credit program, favors low CI feedstocks like canola oil and UCO"}')
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Seasonal Events
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('seasonal_event', 'canola_planting_na', 'North American Canola Planting Season', '{"typical_window": "May-mid June", "regions": ["Canadian Prairies", "North Dakota"]}'),
('seasonal_event', 'canola_growing_na', 'North American Canola Growing Season', '{"typical_window": "June-August", "critical_period": "Flowering in July"}'),
('seasonal_event', 'canola_harvest_na', 'North American Canola Harvest', '{"typical_window": "September-October"}')
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Balance Sheet Lines
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('balance_sheet_line', 'us.canola_oil.stu', 'U.S. Canola Oil Stocks-to-Use Ratio', '{"units": "percent"}'),
('balance_sheet_line', 'canada.canola.carryout', 'Canadian Canola Carryout', '{"units": "million_tonnes"}'),
('balance_sheet_line', 'us.canola_oil.biofuel_use', 'U.S. Canola Oil Usage in Biofuel Production', '{"units": "million_lbs", "breakdown": ["biodiesel", "renewable_diesel"]}'),
('balance_sheet_line', 'us.canola_oil.non_biofuel_use', 'U.S. Canola Oil Non-Biofuel (Food/Industrial) Use', '{"units": "million_lbs"}')
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Price Levels / Spreads
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('price_level', 'canola_oil_la_1dollar', 'Canola Oil $1.00/lb Floor (Los Angeles)', '{"context": "Psychologically significant price level referenced as support in tight markets"}'),
('price_level', 'canada_canola_700_tonne', 'Canadian Canola CAD $700/tonne', '{"context": "Psychologically critical support level for canola futures"}')
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;


-- ============================================================================
-- EDGES: Relationships — the analyst's mental model
-- ============================================================================

-- SUPPLY CHAIN: Crush → Oil Production → Price
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'copa.canola_crush'),
 (SELECT id FROM core.kg_node WHERE node_key = 'copa.canola_oil_production'),
 'CAUSES', 0.95,
 '{"mechanism": "Higher crushing volumes directly increase canola oil production/supply", "direction": "positive_correlation", "lag": "immediate", "note": "Near 1:1 relationship with extraction rate ~44%"}',
 'extracted', 0.95),

((SELECT id FROM core.kg_node WHERE node_key = 'copa.canola_oil_production'),
 (SELECT id FROM core.kg_node WHERE node_key = 'canola_oil'),
 'CAUSES', 0.85,
 '{"mechanism": "Increased Canadian canola oil supply weighs on prices, especially LA cash market", "direction": "inverse", "lag": "weeks", "note": "Effect moderated by export demand and US import pipeline timing"}',
 'extracted', 0.90),

-- SUPPLY CHAIN: Canadian stocks → crush capacity → price
((SELECT id FROM core.kg_node WHERE node_key = 'canada.canola.ending_stocks'),
 (SELECT id FROM core.kg_node WHERE node_key = 'copa.canola_crush'),
 'CAUSES', 0.90,
 '{"mechanism": "Tight Canadian canola stocks constrain crushing volumes. When carryout is historically low, weekly crush declines sharply in final months of MY", "direction": "positive_correlation", "lag": "progressive through MY", "evidence": "2021/22 crush fell from 900K+/month to 525K in May as stocks tightened"}',
 'extracted', 0.95),

-- CRUSH MARGINS: Canola oil price → crush incentive
((SELECT id FROM core.kg_node WHERE node_key = 'canola_oil'),
 (SELECT id FROM core.kg_node WHERE node_key = 'copa.canola_crush'),
 'CAUSES', 0.80,
 '{"mechanism": "Rising canola oil prices improve crush margins, incentivizing processors to maintain or increase volumes. Falling margins slow crush.", "direction": "positive_correlation", "lag": "weeks", "note": "Canadian crush margins respond faster than US soy crush margins to oil price recovery"}',
 'extracted', 0.90),

-- SUBSTITUTION: Soybean oil ↔ Canola oil
((SELECT id FROM core.kg_node WHERE node_key = 'soybean_oil'),
 (SELECT id FROM core.kg_node WHERE node_key = 'canola_oil'),
 'COMPETES_WITH', 0.90,
 '{"mechanism": "Substitutable in food manufacturing and biodiesel feedstock. When SBO prices rise sharply, food manufacturers substitute canola oil, supporting canola demand.", "direction": "bidirectional", "normal_premium": "canola oil premium of ~10 cents/lb historically, expanded to 30+ cents in 2021-2022", "switching_context": "Food manufacturers switch at narrower spreads than biodiesel producers"}',
 'extracted', 0.95),

-- SUBSTITUTION: Canola oil ↔ Corn oil in biodiesel
((SELECT id FROM core.kg_node WHERE node_key = 'canola_oil'),
 (SELECT id FROM core.kg_node WHERE node_key = 'corn_oil'),
 'COMPETES_WITH', 0.70,
 '{"mechanism": "In biodiesel feedstock, corn oil can substitute for canola oil. During production slowdowns, canola oil may be substituted for competing vegetable oils including corn oil.", "direction": "bidirectional", "context": "More relevant during periods of vegetable oil oversupply"}',
 'extracted', 0.75),

-- CROSS-MARKET: HOBO spread → biodiesel economics → D4 RIN
((SELECT id FROM core.kg_node WHERE node_key = 'hobo_spread'),
 (SELECT id FROM core.kg_node WHERE node_key = 'd4_rin'),
 'CAUSES', 0.90,
 '{"mechanism": "HOBO spread is the primary indicator of biodiesel production economics. Widening HOBO (SBO rising vs HO) squeezes biodiesel margins, reducing production and RIN generation. D4 RINs track HOBO closely as the best lead indicator.", "direction": "generally_positive_but_complex", "note": "HOBO leads the structure — look to this corner as the best lead indicator (direct analyst quote from reports)"}',
 'extracted', 0.95),

-- CROSS-MARKET: HOBO spread → biodiesel production → canola oil demand
((SELECT id FROM core.kg_node WHERE node_key = 'hobo_spread'),
 (SELECT id FROM core.kg_node WHERE node_key = 'us.canola_oil.biofuel_use'),
 'CAUSES', 0.85,
 '{"mechanism": "Sharp increase in HOBO spread reduces biodiesel production incentive, reducing canola oil demand from biodiesel producers", "direction": "inverse_when_widening", "lag": "1-3 months for production response to show in data", "evidence": "2021: sharp HOBO widening in Apr-May reduced summer biodiesel production forecast"}',
 'extracted', 0.90),

-- POLICY → MARKET: SRE → D4 RIN
((SELECT id FROM core.kg_node WHERE node_key = 'sre'),
 (SELECT id FROM core.kg_node WHERE node_key = 'd4_rin'),
 'CAUSES', 0.95,
 '{"mechanism": "Each SRE waiver reduces effective RFS demand obligation, putting length back into the RIN market. EPA SRE announcements cause immediate D4 price declines.", "direction": "inverse_SRE_bearish_RIN", "magnitude": "31 waivers in Aug 2019 sent D4 tumbling, settled at 41 cents", "recovery": "Market recovers if White House signals policy correction"}',
 'extracted', 0.95),

-- POLICY → MARKET: BTC → D4 RIN
((SELECT id FROM core.kg_node WHERE node_key = 'btc'),
 (SELECT id FROM core.kg_node WHERE node_key = 'd4_rin'),
 'CAUSES', 0.85,
 '{"mechanism": "When BTC prospects fade, D4 RINs should perform because RINs must absorb the value that BTC would otherwise provide. BTC and RINs are alternative sources of margin for biodiesel producers — loss of one requires the other to compensate.", "direction": "inverse_BTC_decline_bullish_RIN", "note": "This is a D4-specific dynamic that separates D4 from D6 behavior", "breakaway_point": "When market significantly discounts BTC value, D4 diverges from broader RIN complex"}',
 'extracted', 0.90),

-- POLICY → MARKET: RVO → D4 RIN
((SELECT id FROM core.kg_node WHERE node_key = 'rvo'),
 (SELECT id FROM core.kg_node WHERE node_key = 'd4_rin'),
 'CAUSES', 0.85,
 '{"mechanism": "Higher RVO mandates increase demand for RINs, supporting D4 prices. RVO finalization deadline in November creates anticipatory trading.", "direction": "positive", "lag": "anticipatory — market moves on expectations before finalization"}',
 'extracted', 0.85),

-- POLICY → FEEDSTOCK: LCFS → low CI feedstock demand
((SELECT id FROM core.kg_node WHERE node_key = 'lcfs'),
 (SELECT id FROM core.kg_node WHERE node_key = 'canola_oil'),
 'CAUSES', 0.75,
 '{"mechanism": "LCFS credits favor low carbon intensity feedstocks. Canola oil has favorable CI score, making it attractive for California biodiesel/renewable diesel producers despite higher cost.", "direction": "positive_demand_support"}',
 'extracted', 0.80),

-- WEATHER → PRODUCTION: Prairie drought → canola production
((SELECT id FROM core.kg_node WHERE node_key = 'canada.prairies'),
 (SELECT id FROM core.kg_node WHERE node_key = 'canola_seed'),
 'CAUSES', 0.90,
 '{"mechanism": "Drought conditions in Canadian Prairies reduce canola yields and production, tightening stocks. Subsoil moisture levels are critical for longer-term crop development even if surface conditions improve.", "direction": "drought_bearish_production", "critical_timing": "May-July growing season, especially flowering in July", "note": "Even with planting delays, soil moisture is more important than timing for yield"}',
 'extracted', 0.95),

-- WEATHER → PRODUCTION: US Northern Plains → canola
((SELECT id FROM core.kg_node WHERE node_key = 'us.northern_plains'),
 (SELECT id FROM core.kg_node WHERE node_key = 'canola_seed'),
 'CAUSES', 0.75,
 '{"mechanism": "North Dakota growing conditions affect US canola production. Farmers can plant canola through mid-late June without significant yield impact. Soil moisture more critical than planting pace.", "direction": "drought_bearish_production"}',
 'extracted', 0.85),

-- TRADE: China → Canadian canola exports
((SELECT id FROM core.kg_node WHERE node_key = 'china'),
 (SELECT id FROM core.kg_node WHERE node_key = 'canada.canola_seed_exports'),
 'CAUSES', 0.85,
 '{"mechanism": "China is the largest buyer of Canadian canola seed. Surges in Chinese demand (e.g., March 2020: 387K tonnes vs 111K avg) dramatically affect export pace and Canadian stock levels.", "direction": "positive_demand", "political_risk": "Canada-China diplomatic tensions have historically disrupted canola trade flows"}',
 'extracted', 0.85),

-- IMPORTS → US SUPPLY: Canadian canola oil exports → US canola oil supply
((SELECT id FROM core.kg_node WHERE node_key = 'canada.canola_oil_exports'),
 (SELECT id FROM core.kg_node WHERE node_key = 'census.canola_oil_imports'),
 'CAUSES', 0.90,
 '{"mechanism": "Canada is the dominant source of US canola oil imports. US accounts for ~48% of Canadian canola oil exports. Changes in Canadian export pace directly impact US supply.", "direction": "positive_correlation"}',
 'extracted', 0.90),

-- RENEWABLE DIESEL DEMAND (emerging 2021+)
((SELECT id FROM core.kg_node WHERE node_key = 'us.canola_oil.biofuel_use'),
 (SELECT id FROM core.kg_node WHERE node_key = 'canola_oil'),
 'CAUSES', 0.85,
 '{"mechanism": "Canola oil usage in biofuel production is a growing demand source. Biodiesel usage began at ~1 lb/gal in 2017, dropped to 0.5 lb/gal by 2018-19, recovered to 0.75 lb/gal by 2020. Renewable diesel demand expected to begin Q4 2021/22 (Jul 2022).", "direction": "positive_demand_supports_price", "evolution": "Pre-2022 biodiesel only; post-2022 biodiesel + renewable diesel"}',
 'extracted', 0.90),

-- CRUDE OIL → VEGETABLE OILS (macro)
((SELECT id FROM core.kg_node WHERE node_key = 'crude_oil'),
 (SELECT id FROM core.kg_node WHERE node_key = 'canola_oil'),
 'CAUSES', 0.70,
 '{"mechanism": "Depressed crude oil prices weigh on all vegetable oil values through biofuel economics channel. When crude falls, biodiesel/renewable diesel margins compress, reducing feedstock demand.", "direction": "positive_correlation_on_price", "lag": "concurrent to weeks", "strength": "stronger during periods of high biofuel feedstock demand"}',
 'extracted', 0.80);


-- ============================================================================
-- CONTEXTS: The analyst's brain — enrichment data
-- ============================================================================

-- Canola Oil Basis: Normal Ranges and Interpretation
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'canola_oil_basis_la'),
 'risk_threshold', 'basis_ranges',
 '{"normal_range_cents": {"min": 8, "max": 15, "label": "historical average ~10 cents"},
   "elevated_range_cents": {"min": 15, "max": 35, "label": "tight supply or strong food demand"},
   "extreme_range_cents": {"min": 35, "max": 50, "label": "supply crisis, 2021 peak was 44 cents"},
   "interpretation": "Basis above long-term average of 10 cents indicates canola-specific tightness beyond broad vegetable oil moves. Basis expanded to 3x historical average (30+ cents) in 2021-2022 due to North American drought.",
   "caution": "Even at historically high basis, can remain below prior year same week, suggesting further upside potential"}',
 'always', 'extracted'),

-- U.S. Canola Oil Stocks-to-Use: Levels and Price Implications
((SELECT id FROM core.kg_node WHERE node_key = 'us.canola_oil.stu'),
 'risk_threshold', 'stu_levels',
 '{"comfortable": {"min_pct": 4.5, "context": "Near or above 10-year average of 4.8%, adequate pipeline"},
   "tight": {"min_pct": 3.0, "max_pct": 4.5, "context": "Below average, price-supportive"},
   "critical": {"max_pct": 3.0, "context": "2019/20 hit 2.9%, lowest in recent history, preceded sharp price rally"},
   "ten_year_avg_pct": 4.8,
   "recent_low": {"year": "2019/20", "value_pct": 2.9}}',
 'always', 'extracted'),

-- Canadian Canola Carryout: Tightness Thresholds
((SELECT id FROM core.kg_node WHERE node_key = 'canada.canola.carryout'),
 'risk_threshold', 'carryout_levels',
 '{"comfortable": {"min_mmt": 2.0, "context": "Adequate pipeline for crushing and export demand"},
   "tight": {"min_mmt": 1.0, "max_mmt": 2.0, "context": "Crush volumes likely to decline in final months of MY"},
   "critical": {"max_mmt": 1.0, "context": "2020/21: Jacobsen predicted 700K tonnes vs USDA 1.18M. Tight stocks forced weekly crush to lowest since Aug 2019, supporting prices above $1/lb LA"},
   "note": "When Jacobsen and USDA forecasts diverge significantly on Canadian carryout (480K+ tonnes gap in 2021), market tends to eventually move toward the tighter Jacobsen estimate"}',
 'always', 'extracted'),

-- COPA Weekly Crush: Interpretation Framework
((SELECT id FROM core.kg_node WHERE node_key = 'copa.canola_crush'),
 'expert_rule', 'weekly_crush_interpretation',
 '{"rule": "COPA weekly crush data is the earliest signal of Canadian canola oil supply direction. Sharp week-over-week declines signal supply constraints, especially when occurring in final quarter of marketing year.",
   "signal_strong_supply": "Weekly crush above 5-year average, rising trend",
   "signal_tightening": "Back-to-back weekly declines of 10%+ from peak, dropping to levels not seen since previous summer/fall",
   "signal_crisis": "Weekly volumes at lowest since late August imply severe pipeline constraints (May 2022 example)",
   "monthly_conversion": "Sum weekly COPA data to estimate monthly crush, but watch for end-of-month adjustments when COPA releases official monthly numbers",
   "surprise_impact": "Crush 20%+ above prior year for multiple months signals exceptional demand or supply that will need balance sheet revision"}',
 'always', 'extracted'),

-- Canola Oil Usage in Biodiesel: Per-Gallon Metric
((SELECT id FROM core.kg_node WHERE node_key = 'us.canola_oil.biofuel_use'),
 'seasonal_norm', 'usage_per_gallon_trends',
 '{"metric": "canola oil pounds per gallon of total biodiesel output",
   "high_usage": {"value_lbs": 1.0, "when": "late 2017", "context": "Peak canola oil share of feedstock"},
   "low_usage": {"value_lbs": 0.5, "when": "late 2018 - early 2019", "context": "Canola lost share to competing feedstocks"},
   "recovery": {"value_lbs": 0.75, "when": "2020", "context": "Partial recovery in canola share"},
   "interpretation": "This per-gallon metric is the key to translating biodiesel production forecasts into canola oil demand. Changes in feedstock share can offset or amplify total production changes."}',
 'always', 'extracted'),

-- Non-Biofuel Use: Baseline and Growth
((SELECT id FROM core.kg_node WHERE node_key = 'us.canola_oil.non_biofuel_use'),
 'seasonal_norm', 'baseline_and_growth',
 '{"five_year_baseline_annual_lbs": "4.25 billion",
   "growth_trend": "Generally flat over 5 years with small year-over-year increases",
   "derivation": "Calculated as total domestic disappearance minus estimated biofuel use — inherits noise from both estimates",
   "sensitivity": "Monthly implied non-biofuel use is volatile (ranged from 308M to 438M lbs/month in 2020). Look at cumulative YTD vs prior year for trend, not individual months.",
   "consumer_preference_driver": "Health-conscious consumer preference for canola oil in food manufacturing provides structural floor for non-biofuel demand"}',
 'always', 'extracted'),

-- HOBO Spread: Lead Indicator Framework
((SELECT id FROM core.kg_node WHERE node_key = 'hobo_spread'),
 'expert_rule', 'lead_indicator_framework',
 '{"rule": "HOBO spread is the single best lead indicator for D4 RIN price direction and biodiesel production economics. When analyzing RIN markets, always check HOBO first.",
   "positive_hobo": "Positive HOBO (SBO > HO equivalent) means biodiesel production is economically viable without subsidy support. Higher positive = more margin = more production = more RIN supply.",
   "negative_or_narrow": "Narrow or negative HOBO squeezes biodiesel margins, slowing production. D4 RINs must rise to compensate.",
   "disruption_events": "Geopolitical events (Saudi drone attack 2019) can cause sudden HOBO moves via energy side, creating RIN volatility",
   "btc_interaction": "When BTC is uncertain, HOBO becomes even more critical because it is the only reliable margin component for biodiesel producers"}',
 'always', 'extracted'),

-- D4 vs D6 RIN Divergence
((SELECT id FROM core.kg_node WHERE node_key = 'd4_rin'),
 'expert_rule', 'd4_d6_divergence',
 '{"rule": "D4 and D6 RINs can diverge based on distinct fundamentals. D6 drifts with EPA whispers and SRE threats across the broader RIN complex. D4 has unique territory driven by BTC status and biodiesel-specific economics.",
   "divergence_trigger": "When BTC prospects fade, D4 must absorb the lost subsidy value — this is the breakaway point where D4 separates from D6",
   "convergence_trigger": "Broad EPA policy actions (SRE batches, RVO announcements) affect both D4 and D6 together",
   "vintage_dynamics": "Older vintage RINs (e.g., B18) may react differently to SRE announcements — often overreact and then stabilize closer to fundamental value faster than current vintage"}',
 'always', 'extracted'),

-- SRE Impact: Market Mechanics
((SELECT id FROM core.kg_node WHERE node_key = 'sre'),
 'expert_rule', 'market_impact_mechanics',
 '{"rule": "SRE announcements create demand destruction for RINs with less than 5 months in the year for supply adjustment. The latent nature of data means the full production impact takes months to appear in EIA/EMTS statistics.",
   "immediate_impact": "D4 RIN prices drop on announcement day, then stabilize at new lower equilibrium",
   "political_cycle": "SRE risk rises during election cycles when refinery-state and farm-state political considerations compete. White House signals of farmer support can rapidly reverse SRE-driven declines.",
   "magnitude_reference": "31 waivers in Aug 2019 reduced D4 from ~50s to 41 cents, D6 to 11 cents",
   "recovery_catalyst": "Presidential signals of policy correction (reallocating waived gallons, stronger RVO) are the primary recovery catalyst"}',
 'always', 'extracted'),

-- Planting Progress: Canola-Specific Rules
((SELECT id FROM core.kg_node WHERE node_key = 'usda.crop_progress.canola_planting'),
 'expert_rule', 'planting_delay_impact',
 '{"rule": "Canola planting delays in North Dakota are less yield-critical than for corn or soybeans. Farmers can plant through mid-to-late June without significant yield concerns. Soil moisture is more important than planting pace.",
   "comparison_metric": "Compare percent planted to 5-year average. Even large gaps (13% vs 59% 5yr avg, as in May 2022) do not necessarily imply yield loss.",
   "what_matters_more": "Topsoil AND subsoil moisture ratings. Subsoil moisture is critical for longer-term development — if adequate, can withstand drier-than-normal growing season.",
   "pasture_proxy": "USDA pasture and range condition ratings in North Dakota serve as proxy for early-season crop conditions when canola-specific ratings are not yet available"}',
 'always', 'extracted'),

-- NASS Fats and Oils: Data Interpretation Rules
((SELECT id FROM core.kg_node WHERE node_key = 'usda.fats_oils'),
 'expert_rule', 'data_interpretation',
 '{"rule": "The NASS Fats and Oils report is the primary source for U.S. canola oil balance sheet estimation. Domestic disappearance is calculated as a residual (production + imports - exports - stock change), so individual monthly figures can be noisy.",
   "negative_residual_signal": "A significant negative residual (implied use exceeds identified supply) typically means last year''s crop and ending stocks were larger than officially reported. The 2015/16 precedent showed a negative residual of 246M lbs.",
   "stock_split_matters": "Crude vs refined stock split provides signal: rising refined stocks = demand slowdown; rising crude stocks = processing bottleneck or production surge",
   "monthly_noise": "Individual monthly implied use can swing 100M+ lbs. Always use cumulative YTD comparison to prior year and 5-year average for trend assessment"}',
 'always', 'extracted'),

-- Economic Cycle Impact on Vegetable Oils
((SELECT id FROM core.kg_node WHERE node_key = 'canola_oil'),
 'expert_rule', 'economic_cycle_impact',
 '{"rule": "During economic slowdowns, food demand shifts from foodservice to retail/processed foods. This increases vegetable oil demand from food manufacturers while reducing foodservice channel demand. Net effect is roughly offsetting, so recovery in foodservice demand provides marginal uplift.",
   "covid_lesson": "During 2020 COVID shutdowns, consumer shift to processed foods with longer shelf life raised food manufacturer demand for vegetable oils, partially offsetting foodservice decline",
   "recovery_dynamic": "If consumer processed food preferences persist while restaurants reopen, the combination creates net demand growth — happened in 2020/21",
   "crude_oil_overlay": "Even during demand recovery, depressed crude oil prices can continue to weigh on vegetable oil values through biofuel economics channel"}',
 'always', 'extracted');


-- ============================================================================
-- SOURCE REGISTRY: Log which documents were processed
-- ============================================================================

INSERT INTO core.kg_source (source_key, source_type, title, location_uri, document_date, document_type, commodities, topics, status, first_processed, last_processed, nodes_extracted, edges_extracted, contexts_extracted) VALUES
('gdoc_1pP1mqnsnZsNqvlEBynf03Nq_R2SZJKTgsp019GyYfro', 'gdrive_doc', 'CO - 04272020', 'https://docs.google.com/document/d/1pP1mqnsnZsNqvlEBynf03Nq_R2SZJKTgsp019GyYfro/edit', '2020-04-27', 'weekly_report', '{canola_oil,biodiesel,renewable_diesel}', '{biodiesel_feedstock,production_forecast,usage_per_gallon}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_1mr0wBhUoIfwxxVxYVogUAr7Ty-su-N38gGSV41QxcxE', 'gdrive_doc', 'CO - 04202020', 'https://docs.google.com/document/d/1mr0wBhUoIfwxxVxYVogUAr7Ty-su-N38gGSV41QxcxE/edit', '2020-04-20', 'weekly_report', '{canola_oil,canola_seed}', '{copa_crush,prospective_plantings,crush_margins,canada_supply}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_1JrT2m6IzJ1D8nnOHxL3nICh_0xPsfN7KdUqcy8DjZIw', 'gdrive_doc', 'CO - 05042020', 'https://docs.google.com/document/d/1JrT2m6IzJ1D8nnOHxL3nICh_0xPsfN7KdUqcy8DjZIw/edit', '2020-05-04', 'weekly_report', '{canola_oil,canola_seed,soybean_oil}', '{nass_fats_oils,crush_volume,stocks,domestic_use,covid_impact}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_1G7oA-svpf-GIyC3FlcBcuYcrSGmXItobB57Uoqj5swo', 'gdrive_doc', 'CO - 05112020', 'https://docs.google.com/document/d/1G7oA-svpf-GIyC3FlcBcuYcrSGmXItobB57Uoqj5swo/edit', '2020-05-11', 'weekly_report', '{canola_oil,canola_seed}', '{copa_crush,trade_data,china_demand,domestic_use,balance_sheet,stocks_to_use}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_1ZtueBkkAMN6dtKEDcITvu1EgEaC2a629oN66z_BXRdk', 'gdrive_doc', 'CO - 05182020', 'https://docs.google.com/document/d/1ZtueBkkAMN6dtKEDcITvu1EgEaC2a629oN66z_BXRdk/edit', '2020-05-18', 'weekly_report', '{canola_oil}', '{weekly_report}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_17xE6DCBq9aLfP2q47BsCVMAb_6GfP49Csoq8UtGcfJc', 'gdrive_doc', 'CO - 05262020', 'https://docs.google.com/document/d/17xE6DCBq9aLfP2q47BsCVMAb_6GfP49Csoq8UtGcfJc/edit', '2020-05-26', 'weekly_report', '{canola_oil}', '{weekly_report}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_1HPQh1tHIis5nXof-Vf67T9NBCfj5WF8tykpo0kli910', 'gdrive_doc', 'CO - 06012020', 'https://docs.google.com/document/d/1HPQh1tHIis5nXof-Vf67T9NBCfj5WF8tykpo0kli910/edit', '2020-06-01', 'weekly_report', '{canola_oil}', '{weekly_report}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_1Ltywyrk7sIv1rppS5E2NP9YVLXjGyuvxLcz2ruACj98', 'gdrive_doc', 'CO - 05242021', 'https://docs.google.com/document/d/1Ltywyrk7sIv1rppS5E2NP9YVLXjGyuvxLcz2ruACj98/edit', '2021-05-24', 'weekly_report', '{canola_oil,canola_seed}', '{copa_crush,epa_emts,hobo_spread,biodiesel,growing_conditions,prairie_weather,balance_sheet}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_19D9hcEPHjIR7sYL2f4Wovh1Ihw3SWNIMz0uVJBl4CTw', 'gdrive_doc', 'CO - 06012021', 'https://docs.google.com/document/d/19D9hcEPHjIR7sYL2f4Wovh1Ihw3SWNIMz0uVJBl4CTw/edit', '2021-06-01', 'weekly_report', '{canola_oil,canola_seed}', '{copa_crush,nass_fats_oils,eia_feedstock,stocks,domestic_use,prairie_weather}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_1eDP06PxusF2VHe3mQjySpHgmkCZDFedlf6LVoWtg-5E', 'gdrive_doc', 'CO - 06072021', 'https://docs.google.com/document/d/1eDP06PxusF2VHe3mQjySpHgmkCZDFedlf6LVoWtg-5E/edit', '2021-06-07', 'weekly_report', '{canola_oil,canola_seed,soybean_oil}', '{copa_crush,wasde_preview,canada_balance_sheet,substitution,food_demand}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_1V7hee1a3fyRZMCnIx3A3usYT7FKMQE5a7QP0zRWUv68', 'gdrive_doc', 'CO - 04192022', 'https://docs.google.com/document/d/1V7hee1a3fyRZMCnIx3A3usYT7FKMQE5a7QP0zRWUv68/edit', '2022-04-19', 'weekly_report', '{canola_oil,soybean_oil}', '{basis,copa_crush,vegetable_oil_rally,spread_analysis}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_1KI6DnGEgr_EghKxL1Peu91tcGkbbseWNbOtdurCc2ew', 'gdrive_doc', 'CO - 05242022', 'https://docs.google.com/document/d/1KI6DnGEgr_EghKxL1Peu91tcGkbbseWNbOtdurCc2ew/edit', '2022-05-24', 'weekly_report', '{canola_oil,canola_seed}', '{basis,planting_progress,soil_moisture,copa_crush,supply_constraints}', 'completed', NOW(), NOW(), 0, 0, 0),
-- RIN Reports
('gdoc_1uQ5fa9KGD5MIvL-BXN7cwasElHe6G1Rs57lnpi3UUaI', 'gdrive_doc', 'RIN Forecast - 07292019', 'https://docs.google.com/document/d/1uQ5fa9KGD5MIvL-BXN7cwasElHe6G1Rs57lnpi3UUaI/edit', '2019-07-29', 'weekly_report', '{d4_rin,d6_rin,soybean_oil,heating_oil}', '{hobo_spread,btc,d4_d6_divergence,rin_structure}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_1lPYOv6y-BR49CSaS5Re2ew9cuzdlecnJvNBoFVzXDNM', 'gdrive_doc', 'RIN Forecast - 08052019', 'https://docs.google.com/document/d/1lPYOv6y-BR49CSaS5Re2ew9cuzdlecnJvNBoFVzXDNM/edit', '2019-08-05', 'weekly_report', '{d4_rin,soybean_oil,biodiesel}', '{eia_feedstock,lcfs,low_ci_feedstock,hobo_spread}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_1l81PrJNz6aE3lihj8H28qWzodZ5-2M-D-_WxdfZpyJk', 'gdrive_doc', 'RIN Forecast - 08122019', 'https://docs.google.com/document/d/1l81PrJNz6aE3lihj8H28qWzodZ5-2M-D-_WxdfZpyJk/edit', '2019-08-12', 'weekly_report', '{d4_rin,d6_rin}', '{sre_impact,rin_vintages,demand_destruction}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_1SsYHom2XHQZE28j2kj1LjQC5L5e28pAvKb6lIR44IDw', 'gdrive_doc', 'RIN Forecast - 08192019', 'https://docs.google.com/document/d/1SsYHom2XHQZE28j2kj1LjQC5L5e28pAvKb6lIR44IDw/edit', '2019-08-19', 'weekly_report', '{d4_rin}', '{sre,btc,hobo_lead_indicator,market_structure}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_1ItOzpmrvMxgGYe4xsw6V_TeeR-9lOD6c0jgB2cNR0kU', 'gdrive_doc', 'RIN Forecast - 08262019', 'https://docs.google.com/document/d/1ItOzpmrvMxgGYe4xsw6V_TeeR-9lOD6c0jgB2cNR0kU/edit', '2019-08-26', 'weekly_report', '{d4_rin}', '{sre,political_cycle,white_house,election_dynamics}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_1JrlavMSzB5go_d7WYsSSxOXKzmMdtLmH--S2zbadq0A', 'gdrive_doc', 'RIN Forecast - 09032019', 'https://docs.google.com/document/d/1JrlavMSzB5go_d7WYsSSxOXKzmMdtLmH--S2zbadq0A/edit', '2019-09-03', 'weekly_report', '{d4_rin}', '{sre,rvo,btc,policy_options}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_1pg2ucQKGkqCRY_Zs-IPRuFFsOhk_b-o4GOUndwYdHqw', 'gdrive_doc', 'RIN Forecast - 09092019', 'https://docs.google.com/document/d/1pg2ucQKGkqCRY_Zs-IPRuFFsOhk_b-o4GOUndwYdHqw/edit', '2019-09-09', 'weekly_report', '{d4_rin}', '{sre,rfs2,political_pressure,farmer_support}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_1PYmyKM8RLgsB5FF76ogDvzuPhMKEp23g3x_3FkYgcz0', 'gdrive_doc', 'RIN Forecast - 09162019', 'https://docs.google.com/document/d/1PYmyKM8RLgsB5FF76ogDvzuPhMKEp23g3x_3FkYgcz0/edit', '2019-09-16', 'weekly_report', '{d4_rin,soybean_oil,heating_oil,crude_oil}', '{hobo_spread,sre_reallocation,saudi_attack,geopolitical_risk}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_1tNapdVlgT2mVJJq9cE0TDWwlouEvZlEL3bAEit5L64M', 'gdrive_doc', 'RIN Forecast - 09232019', 'https://docs.google.com/document/d/1tNapdVlgT2mVJJq9cE0TDWwlouEvZlEL3bAEit5L64M/edit', '2019-09-23', 'weekly_report', '{d4_rin}', '{sre_reallocation,rvo_finalization,political_timeline}', 'completed', NOW(), NOW(), 0, 0, 0)
ON CONFLICT (source_key) DO UPDATE SET status = 'completed', last_processed = NOW();
