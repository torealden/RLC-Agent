-- ============================================================================
-- RLC Knowledge Graph: Extraction Batch 003
-- Source: 12 YG/UCO reports (2018-2022) + 1 Balance Sheet Commentary (2021)
-- Extracted: 2026-02-14
-- ============================================================================

-- ============================================================================
-- NEW NODES: Waste oil / rendered fats universe
-- ============================================================================

INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('commodity', 'yellow_grease', 'Yellow Grease', '{"markets": ["Illinois cash", "Southeast", "Midwest", "West Coast"], "units": "cents/lb", "context": "Low-grade rendered fat from animal processing; also functions as feed ingredient and biodiesel feedstock"}'),
('commodity', 'uco', 'Used Cooking Oil (UCO)', '{"markets": ["Illinois cash", "West Coast/SoCal"], "units": "cents/lb", "context": "Collected waste oil from restaurants/food service; highest CI benefit among waste feedstocks globally"}'),
('commodity', 'distillers_corn_oil', 'Distillers Corn Oil (DCO)', '{"markets": ["Illinois cash"], "units": "cents/lb", "context": "Byproduct of ethanol production; low CI score makes it attractive for renewable diesel"}'),
('commodity', 'animal_fats', 'Animal Fats (Tallow/Lard/Choice White Grease)', '{"markets": ["various regional cash"], "units": "cents/lb", "context": "Rendered animal fats — tallow, lard, choice white grease; compete with YG in feed and biofuel"}'),
('commodity', 'camelina_oil', 'Camelina Oil', '{"context": "Alternative oilseed crop with very low CI score; potential future competitor to UCO/YG in renewable diesel"}'),
('commodity', 'renewable_diesel', 'Renewable Diesel', '{"markets": ["California rack", "Gulf Coast"], "units": "USD/gallon", "context": "Drop-in hydrocarbon fuel produced by hydrotreating vegetable oils and fats; different from biodiesel (FAME)"}')
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('data_series', 'eia.rd_feedstock', 'EIA Monthly Renewable Diesel Feedstock Report', '{"source": "EIA", "frequency": "monthly", "context": "Reports feedstock usage in renewable diesel by oil type — key for tracking YG/UCO demand shift from biodiesel to RD"}'),
('data_series', 'usda.food_spending', 'USDA Monthly Food Spending Report', '{"source": "USDA ERS", "frequency": "monthly", "context": "Food at home vs food away from home spending — proxy for UCO supply (restaurants = primary UCO source)"}'),
('data_series', 'yg_relative_price_sbo', 'Yellow Grease Relative Price to Soybean Oil', '{"calculation": "YG price / SBO price * 100", "units": "percent", "context": "Key substitution signal — when YG is cheap relative to SBO, biodiesel producers shift to YG"}'),
('data_series', 'yg_relative_price_corn', 'Yellow Grease Relative Price to Corn', '{"calculation": "YG price / corn price equivalent * 100", "units": "percent", "context": "Determines whether YG flows into feed (energy supplement) or biofuel"}'),
('data_series', 'uco_yg_spread', 'UCO-YG Spread', '{"calculation": "UCO price minus YG price", "units": "cents/lb", "context": "Measures the CI premium — wider spread = stronger renewable diesel pull on UCO specifically"}'),
('data_series', 'lcfs_credit_price', 'LCFS Credit Price', '{"source": "California Air Resources Board / market", "units": "USD/tonne CO2e", "context": "Drives the economics of low CI feedstock premiums"}')
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('market_participant', 'rd_producers', 'Renewable Diesel Producers', '{"context": "Large-scale hydroprocessing facilities (Marathon, Phillips 66, Diamond Green, Neste, etc.) — primary demand driver for UCO/YG post-2020", "key_dynamic": "Can outbid biodiesel producers due to better economics from LCFS + RIN + BTC stack"}'),
('market_participant', 'uco_collectors', 'UCO Collection Companies', '{"context": "Firms that collect used cooking oil from restaurants — logistics and seasonal collection patterns drive supply"}'),
('market_participant', 'renderers', 'Rendering Companies', '{"context": "Process animal byproducts into yellow grease, tallow, etc. — supply driven by meat packing volumes"}'),
('market_participant', 'feed_buyers', 'Animal Feed Buyers', '{"context": "Compete with biofuel for YG — use as energy supplement in feed rations, especially winter formulations"}')
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('policy', 'eu_red', 'EU Renewable Energy Directive (RED)', '{"context": "UCO qualifies as waste-based feedstock generating double credits under EU RED — drives global UCO trade flows"}'),
('seasonal_event', 'winter_fat_demand', 'Winter Fat Inclusion in Feed Rations', '{"typical_window": "November-February", "context": "Cold weather increases caloric density needs in animal feed, pulling YG into feed channel and reducing biofuel availability"}'),
('seasonal_event', 'summer_uco_peak', 'Summer UCO Collection Peak', '{"typical_window": "May-August", "context": "Restaurant activity peaks, UCO collection volumes increase; partially offset by higher biofuel demand"}')
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;


-- ============================================================================
-- EDGES: Waste oil analytical framework
-- ============================================================================

-- CORE: LCFS credit price → UCO premium
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'lcfs_credit_price'),
 (SELECT id FROM core.kg_node WHERE node_key = 'uco'),
 'CAUSES', 0.95,
 '{"mechanism": "LCFS credit price directly determines how much UCO can trade above soybean oil and still generate equivalent or better economics for renewable diesel producers. At $190/tonne LCFS, UCO can trade up to 9 cents/lb ABOVE soybean oil and still match SBO credit value. This is the price ceiling formula.", "direction": "higher_LCFS_supports_higher_UCO_premium", "formula": "UCO max premium to SBO = f(LCFS credit price, CI score differential)", "note": "Of all feedstocks, UCO has the greatest low-carbon fungibility globally — at minimum equal and more often highest value in credit generation across all carbon offset schemes"}',
 'extracted', 0.95),

-- CORE: Renewable diesel capacity → UCO/YG demand shift
((SELECT id FROM core.kg_node WHERE node_key = 'rd_producers'),
 (SELECT id FROM core.kg_node WHERE node_key = 'uco'),
 'CAUSES', 0.95,
 '{"mechanism": "Growth in renewable diesel capacity is the dominant demand driver for UCO and YG. RD producers can outbid biodiesel producers due to superior economics from the LCFS + RIN + BTC credit stack. This is causing a structural shift: YG/UCO usage in biodiesel is forecast to DECLINE 60% over the forecast period as RD producers capture the supply.", "direction": "positive_demand", "structural_shift": "Pre-2020: biodiesel was primary biofuel demand. Post-2020: renewable diesel rapidly replacing biodiesel as primary buyer of low CI feedstocks.", "magnitude": "RD capacity additions 2021-2024 expected to add 3-4 billion gallons"}',
 'extracted', 0.95),

-- SUPPLY: Restaurant activity → UCO collection
((SELECT id FROM core.kg_node WHERE node_key = 'usda.food_spending'),
 (SELECT id FROM core.kg_node WHERE node_key = 'uco'),
 'CAUSES', 0.85,
 '{"mechanism": "UCO supply is directly linked to restaurant activity (food away from home spending). COVID-19 crushed restaurant traffic, reducing UCO collection. Recovery to 2019 levels expected by 2022. UCO production model is based on combination of informal industry surveys and USDA food spending data.", "direction": "positive_correlation", "covid_impact": "2020 UCO production dropped significantly due to restaurant closures", "seasonal_pattern": "UCO collection declines in winter (logistics + lower restaurant traffic) and peaks in summer", "production_estimate_2022": "2.08 billion lbs UCO + 1.05 billion lbs YG = ~3.1 billion lbs total"}',
 'extracted', 0.90),

-- SUPPLY: Animal processing → YG supply
((SELECT id FROM core.kg_node WHERE node_key = 'renderers'),
 (SELECT id FROM core.kg_node WHERE node_key = 'yellow_grease'),
 'CAUSES', 0.85,
 '{"mechanism": "YG supply driven by meat packing and rendering volumes. In winter, colder temps maintain low FFA levels in animal fat, LIMITING the amount sold as yellow grease (quality improves to choice white grease/tallow). Holiday season slows animal fat production.", "direction": "positive_correlation_with_processing_volume", "seasonal_note": "Winter: lower YG supply (better quality = upgraded to tallow) + higher feed demand = seasonal tightening"}',
 'extracted', 0.85),

-- COMPETITION: YG vs feed (seasonal switching)
((SELECT id FROM core.kg_node WHERE node_key = 'feed_buyers'),
 (SELECT id FROM core.kg_node WHERE node_key = 'yellow_grease'),
 'COMPETES_WITH', 0.85,
 '{"mechanism": "Feed buyers compete with biofuel producers for YG. The relative price of YG to corn determines which channel captures supply. When YG is cheap vs corn (below 3-year average of ~320% relative price), feed buyers pull YG. Vegetarian-fed diet premium in poultry drives DCO into feed at up to 32 cents/lb, indirectly affecting the YG-DCO-UCO complex.", "direction": "bidirectional", "key_metric": "YG relative price to corn — below 260% = strong feed pull (as in Oct 2018)", "winter_effect": "Winter rations need higher caloric density → fat inclusion rates increase → more YG to feed"}',
 'extracted', 0.85),

-- SUBSTITUTION: UCO ↔ YG ↔ SBO in biofuel
((SELECT id FROM core.kg_node WHERE node_key = 'uco'),
 (SELECT id FROM core.kg_node WHERE node_key = 'yellow_grease'),
 'COMPETES_WITH', 0.90,
 '{"mechanism": "UCO and YG compete for the same biofuel demand but UCO commands a CI premium. The UCO-YG spread widened from historical ~3-5 cents to 10-16 cents in 2021-2022 as renewable diesel producers specifically targeted UCO for its superior CI score. Spread behavior signals whether RD demand is broad-based (narrow spread) or UCO-specific (wide spread).", "normal_spread_cents": {"min": 3, "max": 5}, "elevated_spread_cents": {"min": 8, "max": 16, "when": "2021-2022 renewable diesel buildout"}, "record_spread": "16 cents in Dec 2021"}',
 'extracted', 0.95),

-- SUBSTITUTION: UCO ↔ SBO in biodiesel/RD
((SELECT id FROM core.kg_node WHERE node_key = 'uco'),
 (SELECT id FROM core.kg_node WHERE node_key = 'soybean_oil'),
 'COMPETES_WITH', 0.85,
 '{"mechanism": "UCO relative price to SBO determines substitution. At 3-year avg of ~82% of SBO price, UCO is competitive feedstock. BUT when LCFS credits are high, UCO can trade ABOVE SBO (up to 9 cents over at $190/tonne LCFS) and still be preferred due to credit generation. This breaks the normal substitution logic — UCO is not just a cheap alternative, it is a premium product in carbon markets.", "normal_relative_price_pct": 82, "inverted_premium_threshold": "LCFS > $150/tonne", "key_insight": "UCO pricing is driven by carbon credit economics, not by traditional vegetable oil substitution"}',
 'extracted', 0.90),

-- CROSS-MARKET: DCO ↔ UCO (CI competition)
((SELECT id FROM core.kg_node WHERE node_key = 'distillers_corn_oil'),
 (SELECT id FROM core.kg_node WHERE node_key = 'uco'),
 'COMPETES_WITH', 0.80,
 '{"mechanism": "DCO has similar low CI benefits to UCO. At $190/tonne LCFS, DCO generates 6 cents/lb more than SBO but 3 cents/lb less than UCO. This 3-cent differential should theoretically cap the DCO-UCO spread. However, vegetarian-fed diet demand pulls DCO into feed at premiums (32 cents/lb delivered), disrupting the biofuel substitution economics.", "lcfs_spread_limit_cents": 3, "feed_channel_disruption": "When feed buyers pay 32c for DCO, it is no longer a biofuel feedstock competitor"}',
 'extracted', 0.85),

-- STRUCTURAL: Biodiesel → Renewable diesel transition
((SELECT id FROM core.kg_node WHERE node_key = 'biodiesel_producers'),
 (SELECT id FROM core.kg_node WHERE node_key = 'rd_producers'),
 'COMPETES_WITH', 0.90,
 '{"mechanism": "Renewable diesel producers are structurally displacing biodiesel producers for low CI feedstock supply. RD economics (LCFS + RIN + BTC) allow them to outbid biodiesel. Jacobsen forecasts biodiesel YG/UCO usage to decline ~60% over 5-year forecast period as RD captures share. This will force rationalization in the biodiesel industry.", "direction": "rd_displacing_biodiesel", "timeline": "2021-2026 transition period", "rationalization": "Smaller biodiesel plants unable to compete on feedstock costs will shut down"}',
 'extracted', 0.90),

-- SUPPLY CONSTRAINT: Refining specifications
((SELECT id FROM core.kg_node WHERE node_key = 'uco'),
 (SELECT id FROM core.kg_node WHERE node_key = 'rd_producers'),
 'CAUSES', 0.75,
 '{"mechanism": "Unlike vegetable oils, a substantial portion of YG/UCO may NOT meet renewable diesel specifications without further refining. This creates a quality bottleneck: even if biofuel demand surges, some YG/UCO supply will remain in non-biofuel channels because it cannot be economically refined to RD spec. This limits the demand ceiling and keeps exports and non-biofuel use higher for YG/UCO than for competing feedstocks.", "direction": "supply_quality_constraint", "implication": "YG/UCO exports will remain a larger % of total demand than for vegetable oils where the refining hurdle is lower"}',
 'extracted', 0.85),

-- TRADE: Global CI competition → UCO trade flows
((SELECT id FROM core.kg_node WHERE node_key = 'eu_red'),
 (SELECT id FROM core.kg_node WHERE node_key = 'uco'),
 'CAUSES', 0.80,
 '{"mechanism": "EU RED double-counting for waste-based feedstocks creates global competition for UCO supply. UCO has the greatest low-carbon fungibility of any feedstock — it generates premium credits in California LCFS, EU RED, and other carbon schemes worldwide. This global demand competition supports prices and can redirect trade flows based on which jurisdiction offers the highest carbon credit value.", "direction": "global_demand_competition", "developing_country_risk": "Countries building domestic renewable fuel programs will compete with export market for their own UCO supply"}',
 'extracted', 0.85),

-- VOLATILITY: Fixed supply vs elastic demand
((SELECT id FROM core.kg_node WHERE node_key = 'uco'),
 (SELECT id FROM core.kg_node WHERE node_key = 'uco'),
 'RISK_THRESHOLD', 0.80,
 '{"mechanism": "YG/UCO supply is relatively fixed (tied to food consumption and animal processing) while biofuel demand is elastic (driven by capacity additions and policy). This inelastic supply / elastic demand combination means that when demand shifts occur, prices must move sharply to ration supply. Low speculative participation in physical markets reduces the dampening effect of speculators, but also means price moves tend to be stickier once established.", "volatility_forecast": "Elevated through RD capacity buildout (2021-2025), may moderate once supply chains mature", "risk": "If advanced feedstocks (camelina) or alternative oilseed crops underperform, there is significant upside price risk at end of forecast period"}',
 'extracted', 0.85);


-- ============================================================================
-- CONTEXTS: Waste oil analytical framework
-- ============================================================================

INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES

-- UCO Price Ceiling Formula
((SELECT id FROM core.kg_node WHERE node_key = 'uco'),
 'expert_rule', 'uco_price_ceiling_formula',
 '{"rule": "UCO price ceiling is determined by LCFS credit economics relative to competing feedstocks. At a given LCFS price, calculate the credit value differential between UCO and SBO (based on CI scores), and that is the maximum premium UCO can sustain over SBO while still being the preferred feedstock.",
   "example_190_lcfs": {"lcfs_price_per_tonne": 190, "uco_max_premium_to_sbo_cents": 9, "dco_premium_to_sbo_cents": 6, "implied_uco_dco_spread_cents": 3},
   "key_insight": "This formula breaks the traditional vegetable oil substitution logic. UCO is not priced as a cheap alternative — it is priced as a carbon credit generation tool. The price driver is the credit market, not the oil market."}',
 'always', 'extracted'),

-- YG Relative Price Framework
((SELECT id FROM core.kg_node WHERE node_key = 'yellow_grease'),
 'expert_rule', 'relative_price_framework',
 '{"rule": "Track YG relative price to BOTH corn and soybean oil. These two ratios determine where YG flows.",
   "relative_to_corn": {"3yr_avg_pct": 320, "below_avg_signal": "YG cheap vs corn → feed buyers pull aggressively → biofuel supply tightens", "above_avg_signal": "YG expensive vs corn → feed buyers substitute corn → more YG available for biofuel"},
   "relative_to_sbo": {"3yr_avg_pct": 60, "below_avg_signal": "YG cheap vs SBO → biodiesel producers switch to YG from SBO → supports YG price", "above_avg_signal": "YG expensive vs SBO → biodiesel producers switch to SBO → caps YG upside"},
   "geographic_note": "Relative prices vary by geography — Missouri River, Southeast, West Coast each have distinct patterns. Always check regional data, not just Illinois."}',
 'always', 'extracted'),

-- UCO-YG Spread as Demand Signal
((SELECT id FROM core.kg_node WHERE node_key = 'uco_yg_spread'),
 'expert_rule', 'spread_interpretation',
 '{"rule": "The UCO-YG spread is the clearest signal of how strongly renewable diesel producers are targeting low CI feedstocks specifically.",
   "normal_range_cents": {"min": 3, "max": 5, "context": "Biodiesel era — both feedstocks treated similarly"},
   "elevated_range_cents": {"min": 8, "max": 12, "context": "RD pull specifically targeting UCO for CI advantage"},
   "extreme_range_cents": {"min": 12, "max": 16, "context": "Record levels in Dec 2021 — maximum CI premium"},
   "divergence_signal": "When UCO prices hold firm but YG declines, it confirms RD-specific demand vs broad feedstock demand. The April 2022 episode (YG fell 4 cents, UCO unchanged) was a textbook example."}',
 'always', 'extracted'),

-- Seasonal Supply Model
((SELECT id FROM core.kg_node WHERE node_key = 'uco'),
 'seasonal_norm', 'supply_seasonality',
 '{"rule": "UCO and YG have distinct seasonal supply patterns driven by restaurant activity, weather, and animal processing cycles.",
   "q1_jan_mar": {"uco": "declining — post-holiday restaurant slowdown, winter logistics difficulties", "yg": "declining — holiday season ends, meat processing slows"},
   "q2_apr_jun": {"uco": "rising — spring restaurant recovery, warmer weather improves collection", "yg": "steady to rising — spring processing ramp"},
   "q3_jul_sep": {"uco": "peak — maximum restaurant activity, best collection conditions", "yg": "steady — normal processing, good quality"},
   "q4_oct_dec": {"uco": "declining — cooler weather slows collection, holiday period mixed", "yg": "declining — cold temps improve fat quality (upgrades to tallow), holiday processing slowdown"},
   "production_split_2022": {"uco_annual_lbs": "2.08 billion", "yg_annual_lbs": "1.05 billion", "total": "~3.1 billion lbs"}}',
 'always', 'extracted'),

-- Biodiesel-to-Renewable-Diesel Structural Transition
((SELECT id FROM core.kg_node WHERE node_key = 'renewable_diesel'),
 'expert_rule', 'structural_transition_thesis',
 '{"rule": "The biofuels industry is undergoing a structural transition from biodiesel (FAME) to renewable diesel (HVO/HEFA). This transition is the dominant force in waste oil/fat markets for the 2021-2026 period.",
   "mechanism": "RD plants are larger, better capitalized, and can stack LCFS + RIN + BTC credits more efficiently. They will outbid smaller biodiesel plants for feedstock.",
   "biodiesel_impact": "YG/UCO usage in biodiesel forecast to decline ~60% over 5 years. Industry rationalization inevitable — smaller plants shut down.",
   "rd_demand_growth": "RD capacity additions of 3-4 billion gallons/year will absorb substantially more YG/UCO",
   "supply_constraint": "YG/UCO supply is relatively fixed — tied to food consumption and animal processing. Demand growth will significantly outpace supply growth, supporting prices.",
   "advanced_feedstock_wildcard": "Camelina oil and advanced feedstocks could add supply late in forecast period — if they underperform, significant upside price risk remains"}',
 'always', 'extracted'),

-- Volatility Regime in Waste Oil Markets
((SELECT id FROM core.kg_node WHERE node_key = 'yellow_grease'),
 'expert_rule', 'volatility_characteristics',
 '{"rule": "YG/UCO markets exhibit different volatility characteristics than exchange-traded commodities. Limited speculative participation moderates volatility, but also means prices tend to be stickier once established.",
   "observation_2022": "Despite being near record highs (UCO 69c, YG 60.5c), price volatility was surprisingly low — prices held steady for weeks. This is unusual for commodities near all-time highs.",
   "explanation": "Lack of speculative participation and the physical nature of trading (truck/rail lots, no futures market) means price discovery is slower. But once a new level is established, it persists.",
   "risk": "When a supply-demand imbalance does force adjustment, the move can be abrupt because there are no speculators to absorb the flow."}',
 'always', 'extracted'),

-- Bulk vs Truck Market Premium
((SELECT id FROM core.kg_node WHERE node_key = 'uco'),
 'expert_rule', 'market_structure_premium',
 '{"rule": "Large rail car volume (10+ car minimum lots) trades at a delivered premium to local truck markets. This premium reflects: (1) logistics efficiency for large RD consumers, (2) consistent quality from aggregation, (3) long-term contract structures.",
   "example": "In Nov 2018, bulk UCO traded above 30 cents/lb delivered for Dec period while local truck markets lagged",
   "implication": "When analyzing UCO prices, the bulk rail premium vs truck market spread indicates how aggressively large RD producers are competing for supply"}',
 'always', 'extracted');


-- ============================================================================
-- SOURCE REGISTRY
-- ============================================================================

INSERT INTO core.kg_source (source_key, source_type, title, location_uri, document_date, document_type, commodities, topics, status, first_processed, last_processed, nodes_extracted, edges_extracted, contexts_extracted) VALUES
('gdoc_1FwywWcaZz3Mu7lSzNWAiZs-7yuGd2cqJVWd4h6zQZU0', 'gdrive_doc', 'YG Outlook 11.5.18', 'https://docs.google.com/document/d/1FwywWcaZz3Mu7lSzNWAiZs-7yuGd2cqJVWd4h6zQZU0/edit', '2018-11-05', 'weekly_report', '{yellow_grease,uco,dco,soybean_oil}', '{lcfs_economics,relative_pricing,ci_premium,uco_price_ceiling,feed_competition,seasonal_supply}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_1Ysx9AioDT-XWst_FFS9uD9jgyCcLqbVVG9cOMmPwkkg', 'gdrive_doc', 'YG-UCO Balance Sheet Commentary - 02212021', 'https://docs.google.com/document/d/1Ysx9AioDT-XWst_FFS9uD9jgyCcLqbVVG9cOMmPwkkg/edit', '2021-02-21', 'balance_sheet_commentary', '{yellow_grease,uco}', '{structural_transition,biodiesel_rd_shift,supply_forecast,demand_forecast,advanced_feedstocks,export_decline,price_volatility}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_1EE2li1ltyY07E3VsEgYOA7zS4gFbom6PpPTjbjfXI6w', 'gdrive_doc', 'UCO and YG - 01042022', 'https://docs.google.com/document/d/1EE2li1ltyY07E3VsEgYOA7zS4gFbom6PpPTjbjfXI6w/edit', '2022-01-04', 'weekly_report', '{yellow_grease,uco,dco}', '{rd_demand,pandemic_impact,seasonal_supply,price_forecast}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_1yFZ6jSe0D9xzYiTIi3AlYTIG05scd1WIxxr_SAoHasA', 'gdrive_doc', 'UCO and YG - 01312022', 'https://docs.google.com/document/d/1yFZ6jSe0D9xzYiTIi3AlYTIG05scd1WIxxr_SAoHasA/edit', '2022-01-31', 'weekly_report', '{yellow_grease,uco}', '{rd_demand,food_spending,record_prices,uco_premium}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_1cXOv42Sd8bmakR0uvB_S1oZr9tzch7cV_zjxrnSXsoA', 'gdrive_doc', 'UCO and YG - 03012022', 'https://docs.google.com/document/d/1cXOv42Sd8bmakR0uvB_S1oZr9tzch7cV_zjxrnSXsoA/edit', '2022-03-01', 'weekly_report', '{yellow_grease,uco}', '{eia_feedstock,record_prices,spread_analysis,rd_usage}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_147apVxhgxG3Ijnw2XWimA1kOkTtloGIsXRufxnXs7KQ', 'gdrive_doc', 'UCO-YG - 04042022', 'https://docs.google.com/document/d/147apVxhgxG3Ijnw2XWimA1kOkTtloGIsXRufxnXs7KQ/edit', '2022-04-04', 'weekly_report', '{yellow_grease,uco}', '{uco_yg_divergence,biodiesel_demand,fats_oils_data,production_estimates}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_1M_24kfwnJzECasUrc5A6sox2TIi_rwLF8kcDU4Ukg4w', 'gdrive_doc', 'UCO-YG - 05022022', 'https://docs.google.com/document/d/1M_24kfwnJzECasUrc5A6sox2TIi_rwLF8kcDU4Ukg4w/edit', '2022-05-02', 'weekly_report', '{yellow_grease,uco}', '{record_prices,spread_widening,muted_volatility,price_forecast}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_1idoKDAoDwbCj0NkoAMweoC3534b_TrSvSanXbXsejMU', 'gdrive_doc', 'UCO-YG - 05312022', 'https://docs.google.com/document/d/1idoKDAoDwbCj0NkoAMweoC3534b_TrSvSanXbXsejMU/edit', '2022-05-31', 'weekly_report', '{yellow_grease,uco}', '{production_estimates,biofuel_economics,price_ceiling,demand_balance}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_15wwcdPSdVGVAJgM7v0cZgllpjoHSXU8vw5uft2Mv_5o', 'gdrive_doc', 'UCO-YG - 06272022', 'https://docs.google.com/document/d/15wwcdPSdVGVAJgM7v0cZgllpjoHSXU8vw5uft2Mv_5o/edit', '2022-06-27', 'weekly_report', '{yellow_grease,uco}', '{record_prices,production_data,california_premium,rd_demand}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_1qCtJ80akUTvFAqB5RqlQwA5c0iHxwnJcsGVqNwvWd1I', 'gdrive_doc', 'UCO-YG - 08022022', 'https://docs.google.com/document/d/1qCtJ80akUTvFAqB5RqlQwA5c0iHxwnJcsGVqNwvWd1I/edit', '2022-08-02', 'weekly_report', '{yellow_grease,uco}', '{production_data,food_spending,weather_risk,sbo_stabilization}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_1lg2KOut8jdkhB3wRArKt5WwzJ1-gRx-2U9tq6UWQoC4', 'gdrive_doc', 'UCO-YG - 09062022', 'https://docs.google.com/document/d/1lg2KOut8jdkhB3wRArKt5WwzJ1-gRx-2U9tq6UWQoC4/edit', '2022-09-06', 'weekly_report', '{yellow_grease,uco}', '{price_stability_near_records,rd_production_weakness,vegetable_oil_profitability_competition}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_1sSxj4Ydqj1ZqY-bwW6HVh7qW28OfGQUEey64cElM_Q4', 'gdrive_doc', 'UCO-YG - 10042022', 'https://docs.google.com/document/d/1sSxj4Ydqj1ZqY-bwW6HVh7qW28OfGQUEey64cElM_Q4/edit', '2022-10-04', 'weekly_report', '{yellow_grease,uco}', '{price_decline_from_peaks,rd_production_ramp,vegetable_oil_competition,2023_recovery_forecast}', 'completed', NOW(), NOW(), 0, 0, 0)
ON CONFLICT (source_key) DO UPDATE SET status = 'completed', last_processed = NOW();
