-- ============================================================================
-- RLC Knowledge Graph: Extraction Batch 002
-- Source: 2 HigbyBarrett Weekly Reports (Oct 30, Sep 24, 2025) — Tore sections
-- Extracted: 2026-02-14
-- ============================================================================

-- ============================================================================
-- NEW NODES: Expanding into row crop universe
-- ============================================================================

-- Commodities (new)
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('commodity', 'corn', 'Corn', '{"markets": ["CBOT futures", "CIF Gulf", "interior cash"], "units": "cents/bu", "ticker": "ZC", "front_month_convention": "Dec for new crop, Mar for deferred"}'),
('commodity', 'soybeans', 'Soybeans', '{"markets": ["CBOT futures", "CIF Gulf", "PNW FOB"], "units": "cents/bu", "ticker": "ZS"}'),
('commodity', 'soybean_meal', 'Soybean Meal', '{"markets": ["CBOT futures", "delivered cash"], "units": "USD/short_ton", "ticker": "ZM"}'),
('commodity', 'wheat_srw', 'Wheat (SRW)', '{"markets": ["CBOT futures", "CIF Gulf"], "units": "cents/bu", "ticker": "ZW", "protein": "soft red winter"}'),
('commodity', 'wheat_hrw', 'Wheat (HRW)', '{"markets": ["KCBT futures", "FOB Gulf"], "units": "cents/bu", "ticker": "KE", "protein": "hard red winter"}'),
('commodity', 'wheat_hrs', 'Wheat (HRS)', '{"markets": ["MGEX futures", "PNW FOB"], "units": "cents/bu", "ticker": "MWE", "protein": "hard red spring"}'),
('commodity', 'ethanol', 'Ethanol', '{"markets": ["CBOT futures", "rack"], "units": "USD/gallon"}')
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Data Series (new)
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('data_series', 'nopa.crush', 'NOPA Monthly Crush Report', '{"source": "National Oilseed Processors Association", "frequency": "monthly_mid_month", "context": "U.S. soybean crush volumes, oil stocks, meal production"}'),
('data_series', 'cftc.cot', 'CFTC Commitments of Traders', '{"source": "CFTC", "frequency": "weekly_friday", "context": "Managed money, commercial, and swap dealer positioning in futures"}'),
('data_series', 'usda.fgis', 'USDA FGIS Export Inspections', '{"source": "USDA Federal Grain Inspection Service", "frequency": "weekly_monday", "context": "Physical grain inspections at ports, confirms actual shipments"}'),
('data_series', 'usda.export_sales', 'USDA Weekly Export Sales', '{"source": "USDA FAS", "frequency": "weekly_thursday", "context": "New sales and shipments by commodity and destination"}'),
('data_series', 'usda.crop_progress', 'USDA Weekly Crop Progress', '{"source": "USDA NASS", "frequency": "weekly_monday_growing_season", "context": "Planting, development, condition, and harvest progress"}'),
('data_series', 'usda.grain_stocks', 'USDA Quarterly Grain Stocks', '{"source": "USDA NASS", "frequency": "quarterly_end_month", "context": "As-of stocks for corn, soybeans, wheat — finalizes old-crop carryout"}'),
('data_series', 'eia.ethanol', 'EIA Weekly Ethanol Report', '{"source": "EIA", "frequency": "weekly_wednesday", "context": "Ethanol production, stocks, implied corn grind"}'),
('data_series', 'usda.flash_sales', 'USDA Flash Sales (Daily Export Sales)', '{"source": "USDA", "frequency": "daily_as_occur", "context": "Large single-day export sales reported next business day"}'),
('data_series', 'brazil.anec', 'ANEC Brazil Export Forecasts', '{"source": "ANEC (Brazilian grain exporters association)", "frequency": "weekly", "context": "Forward-looking Brazilian soy and corn export projections"}'),
('data_series', 'calendar_spread.pct_carry', 'Calendar Spread % of Full Carry', '{"calculation": "spread / (interest_cost + storage_cost) over spread interval", "assumptions": "7% annual interest + $0.05/bu/month storage/insurance", "context": "Key market structure indicator — % of carry tells you who owns grain and why"}'),
('data_series', 'mississippi_river.stage', 'Mississippi River Stage Levels', '{"source": "USACE / NOAA river gauges", "context": "Low water restricts barge drafts, raises freight, weakens interior basis"}'),
('data_series', 'baltic_dry_index', 'Baltic Dry Index', '{"source": "Baltic Exchange", "frequency": "daily", "context": "Global dry bulk freight rate indicator, impacts export competitiveness"}')
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Regions (new)
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('region', 'brazil.mato_grosso', 'Mato Grosso', '{"context": "Largest Brazilian soybean producing state, first to plant, determines safrinha corn window"}'),
('region', 'brazil.goias', 'Goiás', '{"context": "Key central Brazil soybean state, similar planting window to MT"}'),
('region', 'brazil.south', 'Southern Brazil (PR/RS)', '{"context": "Paraná and Rio Grande do Sul — later planting, different weather patterns from central"}'),
('region', 'argentina.pampas', 'Argentine Pampas', '{"context": "Primary corn and soybean growing region — Córdoba, Santa Fe, Buenos Aires"}'),
('region', 'us.gulf', 'U.S. Gulf Export Region', '{"context": "Primary grain export channel — CIF basis is key indicator"}'),
('region', 'us.pnw', 'U.S. Pacific Northwest', '{"context": "Secondary grain export channel, rail-dependent, key for wheat and some soy"}'),
('region', 'us.corn_belt', 'U.S. Corn Belt', '{"context": "IA, IL, IN, OH, MN, NE — primary production region"}'),
('region', 'russia', 'Russia', '{"context": "Dominant global wheat exporter, FOB offers set world price floor"}'),
('region', 'black_sea', 'Black Sea Region', '{"context": "Russia + Ukraine — combined wheat/corn export dominance"}')
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Market Participants (new)
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('market_participant', 'managed_money', 'Managed Money Funds', '{"context": "Speculative traders in futures — positioning drives short-covering rallies and selling pressure"}'),
('market_participant', 'commercial_hedgers', 'Commercial Hedgers (Elevators/End Users)', '{"context": "Physical market participants hedging in futures — their basis bids reveal real demand"}'),
('market_participant', 'china_crushers', 'Chinese Soybean Crushers', '{"context": "State and private crushers — largest source of world soybean import demand"}')
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Reports (new)
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('report', 'usda.grain_stocks_report', 'USDA Quarterly Grain Stocks Report', '{"frequency": "quarterly", "release": "end of March, June, Sep, Jan", "context": "Finalizes old-crop ending stocks — often causes sharp price reactions (stocks shock)"}'),
('report', 'nopa.monthly', 'NOPA Monthly Crush Report', '{"frequency": "monthly_mid_month", "context": "Member crush volumes, soyoil stocks — key demand indicator"}')
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;


-- ============================================================================
-- EDGES: Row crop analytical framework
-- ============================================================================

-- MARKET STRUCTURE: Calendar spread % carry → market signal
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'calendar_spread.pct_carry'),
 (SELECT id FROM core.kg_node WHERE node_key = 'corn'),
 'LEADS', 0.90,
 '{"mechanism": "% of full carry indicates who owns grain and their incentive. Near full carry (>75%) = abundant supply, strong storage incentive, no urgency to move. Narrowing carry (<50%) = someone wants nearby grain badly (crusher, exporter). Inversion = supply emergency.", "interpretation_framework": {"full_carry_signal": "bearish — market paying you to store, no one needs it now", "narrowing_signal": "bullish — demand pulling nearby, processors or exporters bidding aggressively", "inversion_signal": "very bullish — demand exceeds nearby supply, rationing required"}, "assumptions": "7% annual interest + $0.05/bu/month storage/insurance"}',
 'extracted', 0.95),

-- POSITIONING: Fund short concentration → squeeze risk
((SELECT id FROM core.kg_node WHERE node_key = 'managed_money'),
 (SELECT id FROM core.kg_node WHERE node_key = 'soybeans'),
 'CAUSES', 0.80,
 '{"mechanism": "When managed money is heavily net short (>100K contracts), the market is vulnerable to sharp short-covering rallies on any bullish catalyst. The position itself becomes a coiled spring. Mid-August 2025 saw shorts above 125K contracts (~59% of short-side OI), which kept futures suppressed but created explosion risk.", "direction": "asymmetric_upside_risk_when_heavily_short", "catalyst_triggers": ["yield disappointment", "Chinese buying burst", "weather event", "policy shift"], "applies_to_all_grains": true}',
 'extracted', 0.90),

-- LOGISTICS: Mississippi River → basis → export competitiveness
((SELECT id FROM core.kg_node WHERE node_key = 'mississippi_river.stage'),
 (SELECT id FROM core.kg_node WHERE node_key = 'us.gulf'),
 'CAUSES', 0.90,
 '{"mechanism": "Low Mississippi River levels restrict barge drafts, raise freight costs (400%+ of tariff in Oct 2025), and weaken interior basis. Creates a wedge between interior and Gulf values. CIF basis can firm even as river basis weakens because barge supply tightens at the Gulf end.", "direction": "low_water_bearish_interior_basis", "historical_reference": "2022 was precedent case — affected CME spreads and even flat price", "freight_indicator": "Cairo-Memphis barge tariff — above $19/ton signals stress"}',
 'extracted', 0.90),

-- TRADE COMPETITION: Brazil FOB → US export displacement
((SELECT id FROM core.kg_node WHERE node_key = 'brazil.anec'),
 (SELECT id FROM core.kg_node WHERE node_key = 'soybeans'),
 'CAUSES', 0.85,
 '{"mechanism": "Brazil FOB discount of $0.30-0.40/bu to US Gulf (as of Q4 2025) keeps Chinese crushers sourcing Brazil. Until FOB spreads compress, US soybean exports remain structurally capped. Brazil controls the export narrative.", "direction": "brazil_discount_bearish_us_exports", "magnitude": "FOB Santos soybeans $437-442/MT vs US Gulf ~$480/MT in Oct 2025", "corn_also": "Brazil corn FOB $203-205/MT vs US Gulf $222/MT — $25/MT advantage into Asia"}',
 'extracted', 0.90),

-- CHINA: Trade policy → soybean demand
((SELECT id FROM core.kg_node WHERE node_key = 'china'),
 (SELECT id FROM core.kg_node WHERE node_key = 'soybeans'),
 'CAUSES', 0.90,
 '{"mechanism": "China is the worlds largest soybean importer. Tariff status, diplomatic signals (Trump-Xi summits), and state crusher buying patterns drive US export pace. Token purchases ahead of summits are common but do not indicate sustained demand shift.", "direction": "policy_driven_demand", "key_indicators": ["flash sales", "CFR spread to Brazil", "Dalian crush margins", "diplomatic calendar"], "2025_context": "China bypassing US despite 80-90c/bu FOB discount, covering 95% from Brazil", "tariff_status": "145% tariff on US ag still in effect"}',
 'extracted', 0.95),

-- CRUSH: Board crush margins → processor basis bids → soybean demand
((SELECT id FROM core.kg_node WHERE node_key = 'nopa.crush'),
 (SELECT id FROM core.kg_node WHERE node_key = 'soybeans'),
 'CAUSES', 0.90,
 '{"mechanism": "When board crush margins are above $1.45-2.00/bu, processors bid aggressively on basis to secure coverage. Processor bids become the demand anchor for soybeans when exports are weak. In Q4 2025, crush absorbed record volumes even as exports lagged.", "direction": "positive_margins_support_basis", "split_market": "This creates the classic split: processors strong, exports weak. Cash market tells the sharper story than futures.", "record_pace": "Sep 2025 NOPA crush 197.9M bu, Oct on pace to exceed YoY"}',
 'extracted', 0.95),

-- SUPPLY: Harvest progress → basis behavior → spread dynamics
((SELECT id FROM core.kg_node WHERE node_key = 'usda.crop_progress'),
 (SELECT id FROM core.kg_node WHERE node_key = 'corn'),
 'CAUSES', 0.75,
 '{"mechanism": "Harvest pace drives seasonal basis pressure. Fast harvest = localized cash gluts if elevator space maxes out, wider carries. Slow harvest = delayed pressure. First 2-3 weeks of harvest yield reports shape the narrative for the rest of the season.", "direction": "faster_harvest_bearish_basis_initially", "storage_test": "On-farm storage capacity tested by big crops — when elevators full, cash basis collapses locally"}',
 'extracted', 0.85),

-- GLOBAL: Russian export policy → world wheat price floor
((SELECT id FROM core.kg_node WHERE node_key = 'russia'),
 (SELECT id FROM core.kg_node WHERE node_key = 'wheat_srw'),
 'CAUSES', 0.90,
 '{"mechanism": "Russia is the dominant global wheat exporter. Russian FOB 12.5% protein sets the world price floor. When Russia raises export taxes, FOB prices rise and competitors gain margin. When Russia exports aggressively, it undercuts everyone. Russian FOB at $228-235/MT in Q3-Q4 2025 kept US wheat $20-25/MT uncompetitive.", "direction": "russian_fob_sets_global_floor", "policy_tools": ["floating export tax", "export quota", "unofficial guidance"], "us_impact": "US wheat plays supporting actor role — FOB uncompetitive, exports capped"}',
 'extracted', 0.95),

-- WEATHER: Brazil monsoon → planting pace → safrinha impact
((SELECT id FROM core.kg_node WHERE node_key = 'brazil.mato_grosso'),
 (SELECT id FROM core.kg_node WHERE node_key = 'corn'),
 'CAUSES', 0.80,
 '{"mechanism": "Mato Grosso soybean planting window determines the safrinha corn crop window. Late monsoon = late soy planting = compressed safrinha window = lower corn yield potential. This is a cascading effect: soybean weather risk in Oct-Nov becomes corn supply risk for the following Jun-Sep.", "direction": "late_monsoon_bullish_deferred_corn", "critical_timing": "Monsoon onset Oct-Nov. If not by early Nov, market adds corn premium."}',
 'extracted', 0.85),

-- CRUDE OIL → SOYBEAN OIL (expanded from batch 1)
((SELECT id FROM core.kg_node WHERE node_key = 'crude_oil'),
 (SELECT id FROM core.kg_node WHERE node_key = 'soybean_oil'),
 'CAUSES', 0.80,
 '{"mechanism": "Crude oil prices drive biofuel margin expectations. Lower crude weakens biodiesel/renewable diesel economics, reducing soybean oil demand. Soybean oil is the direct energy-linked vegetable oil in the US market.", "direction": "positive_correlation", "2025_note": "Crude recovery to $60-65/bbl supported ethanol and soy oil product margins"}',
 'extracted', 0.85),

-- CROSS-MARKET: Meal demand → crush → oil supply → oil price
((SELECT id FROM core.kg_node WHERE node_key = 'soybean_meal'),
 (SELECT id FROM core.kg_node WHERE node_key = 'soybean_oil'),
 'CAUSES', 0.85,
 '{"mechanism": "Strong meal demand drives record crush volumes, which simultaneously produces more soybean oil. If meal is the crush driver, oil supply increases as a byproduct, capping oil rallies unless biofuel demand absorbs the excess. This is the meal-oil ratio dynamic.", "direction": "strong_meal_demand_increases_oil_supply", "2025_evidence": "Domestic meal demand averaging 115K MT/day (up from 107K record in 2023/24) drove 5% YoY increase in soy oil supply"}',
 'extracted', 0.90),

-- EU CORN DEFICIT → import demand → US export opportunity
((SELECT id FROM core.kg_node WHERE node_key = 'corn'),
 (SELECT id FROM core.kg_node WHERE node_key = 'corn'),
 'CROSS_MARKET', 0.70,
 '{"mechanism": "EU drought cut corn production to 57-58 MMT (lowest since 2018), creating import needs of 18-21 MMT — the largest in 3 years. The EU shifted from exporter to buyer, creating potential for US and Brazilian corn to capture share.", "direction": "eu_deficit_bullish_us_exports", "2025_context": "Opportunity opens once Brazil logistical logjam eases", "scope": "EU as worlds second-largest corn importer in 2025/26"}',
 'extracted', 0.75);


-- ============================================================================
-- CONTEXTS: The HB Weekly analytical framework
-- ============================================================================

-- The "So What?" Synthesis Framework
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'corn'),
 'expert_rule', 'synthesis_framework',
 '{"rule": "Every analysis section ends with a So What that synthesizes the data into an actionable market view. The framework is: state the dominant force → identify the constraint → name the catalyst that could change it → specify the price range implied.",
   "example_corn": "Corn is still a carry market, but recent price and spread action signal improving spot demand. Until either export pace broadens or Chinese demand returns, corn futures likely drift inside $4.20-$4.40 range, with basis and spreads steering price tone.",
   "example_soy": "Soybeans persist in a split-market dynamic: crushers are bidding actively on meal and board margins, while export demand remains patchy and largely Brazil-facing.",
   "principle": "Cash tells the sharper story than futures. Physical market signals (basis, spreads, processor bids) reveal real demand better than flat price."}',
 'always', 'extracted'),

-- Calendar Spread Interpretation: % of Full Carry
((SELECT id FROM core.kg_node WHERE node_key = 'calendar_spread.pct_carry'),
 'expert_rule', 'carry_interpretation',
 '{"rule": "Calculate % of full carry using 7% annual interest + $0.05/bu/month storage/insurance. This single number tells you the market structure.",
   "benchmarks": {
     "90-100%": "Full carry — market says store it, no nearby demand, bearish",
     "60-75%": "Commercial carry — normal storage incentive, balanced",
     "40-60%": "Narrowing — someone pulling nearby, watch processor/exporter bids",
     "below_40%": "Tightening — demand exceeding pipeline capacity",
     "inversion": "Supply emergency or extreme demand — very bullish"
   },
   "corn_example_oct2025": "Dec/Mar at 65% of full carry, firmest in 5 weeks — Gulf demand firming vs interior harvest flow",
   "soy_example_oct2025": "Jan/Mar at 50% of full carry — crush pull holding January, export still Brazil-facing",
   "practical_note": "Always state the % when discussing spreads — it standardizes comparison across commodities and time periods"}',
 'always', 'extracted'),

-- Commodity Recommendation Framework
((SELECT id FROM core.kg_node WHERE node_key = 'corn'),
 'expert_rule', 'recommendation_framework',
 '{"rule": "Each commodity gets: (1) Fundamental Recommendation with specific trade level, (2) 2 Bullish Factors, (3) 2 Bearish Factors, (4) 2 Swing Factors, (5) Technical Recommendation with 3 bullish and 3 bearish technicals.",
   "recommendation_style": "Directional with specific price triggers: Sell any contract above $4.75, Roll Jan short to May and add above $10.75, Sell if any contract breaches 55 cents",
   "swing_factor_principle": "Swing factors are things that could go either way — they define the uncertainty, not the direction",
   "technical_weight": "Technical factors are always listed but clearly secondary to fundamentals in the recommendation hierarchy",
   "volatility_trade": "For sophisticated traders, buying cheap springtime volatility is an alternative to directional positions when conviction is moderate"}',
 'always', 'extracted'),

-- Synthesis & Outlook Structure
((SELECT id FROM core.kg_node WHERE node_key = 'corn'),
 'expert_rule', 'outlook_structure',
 '{"rule": "Weekly outlook follows: Base Case → Bullish Triggers → Bearish Triggers → Watchlist. This structures the probability distribution explicitly.",
   "base_case": "Most likely outcome with specific price ranges — this is the central tendency",
   "bullish_triggers": "Named catalysts with specific magnitude (e.g., 5-10 bpa lighter for corn could push Dec toward $4.40)",
   "bearish_triggers": "Named catalysts with specific magnitude (e.g., corn could slip to $3.80-3.90)",
   "watchlist": "5-6 specific items for the coming week with why each matters",
   "principle": "The purpose is decision-relevant forecasting, not market commentary. Every element should help someone decide what to do."}',
 'always', 'extracted'),

-- Data Vacuum Trading (Government Shutdown Context)
((SELECT id FROM core.kg_node WHERE node_key = 'usda.export_sales'),
 'expert_rule', 'data_vacuum_adaptation',
 '{"rule": "When government data is suspended (shutdown, holiday), markets rely on physical flow indicators: (1) FGIS inspections (fee-funded, still published), (2) basis and barge signals, (3) ANEC and private export estimates, (4) vessel lineups, (5) EIA (independent agency). CME open interest changes become positioning proxy when CFTC COT is unavailable.",
   "2025_precedent": "29+ day government shutdown in Oct 2025 — USDA export sales, crop progress, and CFTC COT all suspended. Trade direction relied on physical flows, barge logistics, and processor-led basis signals for 5+ weeks.",
   "key_insight": "When data goes dark, basis and spreads become the only truth. Cash-led support remained clear even without official statistics.",
   "resume_impact": "When USDA resumes, expect backlogged multi-week data dumps and publication lags — market may overshoot on catch-up"}',
 'when_usda_dark', 'extracted'),

-- Farmland Value Bubble Thesis
((SELECT id FROM core.kg_node WHERE node_key = 'corn'),
 'expert_rule', 'farmland_bubble_thesis',
 '{"rule": "Rural land values are experiencing a price bubble driven by government land-based farm programs and ad hoc payments flowing to landowners. The disconnect between cropland values and cash rents is the key indicator.",
   "evidence": {"illinois_cropland_value": "$9,850/acre", "illinois_cash_rent": "$264/acre", "real_agricultural_value_estimate": "$2,660/acre (based on $133 breakeven rent at 5% return)", "gap_ratio": "3.7x bubble premium"},
   "mechanism": "Government incentives to buy land have diminished the link between land values and real agricultural value. Result: farmers who rent land are increasingly in financial distress.",
   "risk_factors": ["political shift away from farm vote", "investor landowners lack emotional attachment (no put under market)", "beginning farmers face steep barriers"],
   "floor_thesis": "If rural land prices start declining, the floor is real agricultural value — potentially 60-70% below current levels in prime areas"}',
 'always', 'extracted'),

-- Energy-Inflation Contrarian View
((SELECT id FROM core.kg_node WHERE node_key = 'crude_oil'),
 'expert_rule', 'energy_inflation_framework',
 '{"rule": "Stagflation fears are overblown because crude oil is ~half the May 2022 price. 1970s stagflation required 1,100% oil price increase in 7 years. Current energy dynamics are disinflationary.",
   "mechanism": "Energy impacts inflation directly (fuel, petrochemicals) and indirectly (input costs at every production stage). Cumulative impact tends to be understated by macro models.",
   "lower_energy_outcomes": ["PPI decline with retail prices staying firm = retailer margin expansion", "Discretionary income increases = more consumer spending", "Economic downturn would force retailers to lower prices"],
   "tariff_precedent": "During the last trade war, the rate of inflation decreased. Not a foregone conclusion that tariffs are inflationary.",
   "housing_overlay": "508K more sellers than buyers — hard to have runaway inflation with weak housing market"}',
 'always', 'extracted'),

-- Split Market Dynamic (Processors vs Exporters)
((SELECT id FROM core.kg_node WHERE node_key = 'soybeans'),
 'expert_rule', 'split_market_dynamic',
 '{"rule": "When exports are weak but domestic crush is strong, the soybean market splits: processor bids anchor interior prices while export basis sags. This is the defining market structure of 2025.",
   "indicator": "CIF Gulf soy at +50-55c vs Nov (weak) while processor bids at +60-75c (strong) — divergence signals crush-led demand",
   "why_it_matters": "In a split market, flat price understates domestic demand strength. Basis and spreads do the heavy lifting beneath the board.",
   "trade_implication": "For merchandisers: soybeans tilt toward prompt movement into processors to monetize strong nearby bids. Corn and wheat favor storage to capture carry.",
   "historical_parallel": "Similar dynamic in canola oil markets when biofuel demand supports basis while global competition caps flat price"}',
 'always', 'extracted');


-- ============================================================================
-- SOURCE REGISTRY
-- ============================================================================

INSERT INTO core.kg_source (source_key, source_type, title, location_uri, document_date, document_type, commodities, topics, status, first_processed, last_processed, nodes_extracted, edges_extracted, contexts_extracted) VALUES
('gdoc_1CNsvkrN4EPvcXnpLjKTXTFkmZGHA__9eNnvbW0DCwio', 'gdrive_doc', 'HigbyBarrett Weekly Report Oct 30, 2025 - Tore', 'https://docs.google.com/document/d/1CNsvkrN4EPvcXnpLjKTXTFkmZGHA__9eNnvbW0DCwio/edit', '2025-10-30', 'weekly_comprehensive', '{corn,soybeans,wheat,soybean_meal,soybean_oil}', '{wasde,export_inspections,basis,spreads,carry_structure,brazil_competition,china_demand,river_logistics,government_shutdown,fund_positioning,trade_policy,farmland_values,stagflation}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_121wPBMzmnxwaqEXgOO7i03K1C1Q7TfxAT-EPTMjNgzo', 'gdrive_doc', 'HigbyBarrett Weekly Report Sep 24, 2025 - Tore', 'https://docs.google.com/document/d/121wPBMzmnxwaqEXgOO7i03K1C1Q7TfxAT-EPTMjNgzo/edit', '2025-09-24', 'weekly_comprehensive', '{corn,soybeans,wheat,soybean_meal,soybean_oil}', '{wasde,harvest_progress,yield_trends,fund_short_base,brazil_fob,russia_wheat,calendar_spreads,basis,eu_corn_deficit,argentina_politics}', 'completed', NOW(), NOW(), 0, 0, 0)
ON CONFLICT (source_key) DO UPDATE SET status = 'completed', last_processed = NOW();
