-- ============================================================================
-- RLC Knowledge Graph: Extraction Batch 007
-- Source: 8 HB Weekly Text (Feb-May 2021, Mar 2022 ×3, Mar-Apr 2023 ×3)
-- Focus: Outlook Forum, Prospective Plantings, March 31, balance sheet construction
-- Extracted: 2026-02-14
-- ============================================================================

-- NEW NODES
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('seasonal_event', 'usda_outlook_forum', 'USDA Agricultural Outlook Forum', '{"context": "Annual February conference. First official USDA new-crop balance sheet using long-term projections adjusted for latest WASDE. Starting point is October WASDE — critical to note USDA may not have updated key assumptions since then. Sets initial acreage, yield, and price forecasts that market trades off until Prospective Plantings."}'),
('seasonal_event', 'usda_prospective_plantings', 'USDA Prospective Plantings (March 31)', '{"context": "Survey-based assessment of farmer planting intentions, conducted first two weeks of March. Released last business day of March simultaneously with Grain Stocks. Together they create one of most volatile trading days of year. Provides first survey-based input for new-crop balance sheets."}'),
('analytical_model', 'prospective_plantings_framework', 'Prospective Plantings Analytical Framework', '{"context": "Multi-step methodology: (1) build independent acreage estimate from insurance price ratios, input costs, conversations with farmers, (2) compare to analyst range, (3) assess probability of surprise by range width as % of average — tighter range = higher surprise probability, (4) identify which crop will surprise most, (5) assess compounding effect if both acreage and stocks surprise same direction."}'),
('analytical_model', 'outlook_forum_adjustment_model', 'Outlook Forum → May WASDE Adjustment Framework', '{"context": "AOF projections based on October WASDE starting point. Between Feb and May, real-world changes (geopolitics, weather, demand shifts) create divergence. Key adjustment areas: export forecasts (Ukraine invasion 2022), biofuel demand (RD expansion), South American production revisions. May WASDE incorporates Prospective Plantings + AOF demand assumptions."}'),
('analytical_model', 'insurance_price_ratio_model', 'Revenue Protection Insurance Price Ratio → Acreage Model', '{"context": "RP insurance sets average price during February. Soy/corn price ratio during insurance-setting month predicts marginal acre allocation. 2023: ratio fell from 2.42 to 2.32 → favored corn over soy. Above 2.5 strongly favors soy, below 2.2 strongly favors corn. Market sets new-crop prices during Feb that lock in insurance guarantees — these become farmers actual decision inputs."}'),
('analytical_model', 'balance_sheet_construction', 'Independent Balance Sheet Construction Methodology', '{"context": "HB builds independent balance sheets from component estimates rather than adjusting USDA. Supply: production (own acreage × own yield), beginning stocks (prior year carryout), imports. Demand: exports (own trade flow model), crush/grind (NOPA/EIA), feed (GCAUs), FSI (seasonal patterns), seed (planting rate × acreage). Carryout = supply minus demand. When own carryout diverges >5% from USDA/consensus, that IS the trade."}'),
('data_series', 'usda.long_term_projections', 'USDA Long-Term Agricultural Projections to 2030', '{"context": "Released annually in Feb before Outlook Forum. Contains 10yr balance sheet forecasts. Starting point is October WASDE. Key limitation: does not reflect real-time market conditions between Oct and Feb. HB uses these as baseline to identify where USDA assumptions are stale or wrong."}'),
('geopolitical_event', 'ukraine_invasion_2022', 'Russia-Ukraine Invasion (Feb 2022)', '{"context": "Russia+Ukraine = 30% of world wheat exports, 80% of sunflower oil exports. Ukraine 3rd largest wheat exporter. Invasion impact: US wheat export forecast +150M bu, corn export forecast +400M bu, sunflower oil loss increased soybean oil demand globally. HB immediately revised balance sheets while market still processing headlines."}'),
('analytical_model', 'march1_stocks_methodology', 'March 1 Quarterly Stocks Estimation', '{"context": "March 1 = halfway through corn/soy marketing year. Provides second-quarter implied usage. Soy: known crush (NOPA) + exports (Census) + seed → residual implied. Corn: FSI (ethanol from EIA + other seasonal) + feed/residual implied. Wheat: food + exports known, feed/residual is variable. When range of analyst estimates as % of average is large (>10%), surprise probability increases."}')
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- EDGES
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES

-- Outlook Forum → Prospective Plantings pipeline
((SELECT id FROM core.kg_node WHERE node_key = 'usda_outlook_forum'),
 (SELECT id FROM core.kg_node WHERE node_key = 'usda_prospective_plantings'),
 'LEADS', 0.95,
 '{"mechanism": "AOF (Feb) sets initial benchmark → market trades off AOF acreage/yield until PP (Mar 31). Key analytical value: compare PP to AOF for direction of revision. 2022: AOF corn 92M, PP 89.4M (-2.6M, record soy took share). 2023: AOF corn 91M, PP 92M (+1M, ratio shift). When PP deviates >1M acres from AOF, that signals real economic shift in farmer behavior vs USDA assumptions. AOF yield assumption carries into May WASDE unless weather intervenes.",
   "timing": "Feb AOF → first 2 weeks of March (survey period) → Mar 31 release → Apr WASDE (no new-crop) → May WASDE (first official new-crop incorporating PP)."}',
 'extracted', 0.95),

-- Insurance ratio → acreage allocation
((SELECT id FROM core.kg_node WHERE node_key = 'insurance_price_ratio_model'),
 (SELECT id FROM core.kg_node WHERE node_key = 'soybeans'),
 'PREDICTS', 0.90,
 '{"mechanism": "RP insurance average price set during February. This IS the farmers decision framework — not spot prices, not futures at planting time. Soy/corn ratio: 2023 fell from 2.42 to 2.32 → favored corn, PP confirmed +2.4M corn. 2022: despite record soy oil prices and high fertilizer (narratives favoring soy), ratio at 2.44 was not extreme enough to prevent record soy area BUT June report reversed some of it. Key insight: ratio below 2.3 = corn strongly favored. Above 2.5 = soy strongly favored. Between 2.3-2.5 = contested, other factors (input costs, planting conditions) determine marginal acre."}',
 'extracted', 0.90),

-- Prospective Plantings framework
((SELECT id FROM core.kg_node WHERE node_key = 'prospective_plantings_framework'),
 (SELECT id FROM core.kg_node WHERE node_key = 'corn'),
 'PREDICTS', 0.95,
 '{"methodology": {
   "step1_independent_estimate": "Build from insurance ratios + input costs + farmer conversations + historical tendencies (farmers like corn)",
   "step2_range_analysis": "Analyst range width as % of average predicts surprise probability. Soy 2023: 2.5% range = HIGH surprise probability. Corn 2023: 4.8% = moderate. Wheat 2023: 8.9% = lower",
   "step3_identify_surprise_crop": "In most years when one crop surprises below, other is above. 2021 was RARE exception: both corn AND soy below, wheat took acres. Both below same direction → limit-up",
   "step4_compounding_assessment": "When BOTH acreage and stocks surprise same direction → amplified move. 2021: both below expectations → corn limit-up, soy +90c, wheat +30c",
   "step5_adjustment_cascade": "PP acreage → recalculate production → adjust demand to reach new carryout → new price forecast → feed back into export competitiveness",
   "validation": "2021: HB predicted corn 93M, USDA reported 91.1M — PP survey captured late-winter farmer hesitancy that conversations missed. 2022: HB predicted soy 88M, USDA reported 91M — record soy surprised to upside. 2023: HB predicted corn 91M, USDA reported 92M — ratio shift confirmed"
 }}',
 'extracted', 0.95),

-- Balance sheet construction → carryout
((SELECT id FROM core.kg_node WHERE node_key = 'balance_sheet_construction'),
 (SELECT id FROM core.kg_node WHERE node_key = 'soybeans'),
 'PREDICTS', 0.95,
 '{"methodology": "Build EVERY component independently rather than adjusting USDA. Supply side: own acreage (from insurance ratio + planting pace + conversations) × own yield (trendline adjusted for conditions) + beginning stocks (own prior year carryout) + imports. Demand side: crush (NOPA monthly data × marketing year pattern), exports (own trade flow model using FOB spreads + Chinese demand assessment per Batch 004), feed (minimal for soy), seed (PP acreage × seeding rate), residual (export inspection model per Batch 006). Carryout = supply - demand. When own carryout diverges >5% from consensus, that is highest-conviction trade. 2022: HB 233M vs USDA AOF 305M = bearish USDA by 72M = bullish soybeans. 2023: HB 160M vs consensus variable = tightest pipeline levels."}',
 'extracted', 0.95),

-- Ukraine → wheat/veg oil supply shock
((SELECT id FROM core.kg_node WHERE node_key = 'ukraine_invasion_2022'),
 (SELECT id FROM core.kg_node WHERE node_key = 'wheat_srw'),
 'CAUSES', 0.95,
 '{"mechanism": "Russia+Ukraine 30% of world wheat exports. Invasion immediately repriced: HB raised US wheat exports +150M bu, corn exports +400M bu. Sunflower oil loss (80% of world exports) → soy oil demand surge globally. HB raised soy oil demand for China/India. Key analytical principle: geopolitical events require IMMEDIATE balance sheet revision, not waiting for USDA. AOF projections set before invasion were instantly stale. Market took weeks to fully reprice while HB adjusted within days. This is where independent balance sheet construction provides maximum edge — speed of adjustment."}',
 'extracted', 0.95),

-- Long-term projections as baseline
((SELECT id FROM core.kg_node WHERE node_key = 'usda.long_term_projections'),
 (SELECT id FROM core.kg_node WHERE node_key = 'usda_outlook_forum'),
 'LEADS', 0.85,
 '{"mechanism": "Long-term projections (released Nov, updated Feb) provide baseline that AOF modifies. Critical flaw: starting point is October WASDE, which can be significantly stale by February. 2021: USDA long-term predicted corn $3.65, soy $10 — actual 2021 was corn $5.45, soy $13.30. Long-term projections systematically underestimate biofuel demand growth (8.1B lbs soy oil in biodiesel vs HB expectation of 10B+). Use as baseline to identify USDA blind spots, not as forecast."}',
 'extracted', 0.85),

-- March 1 stocks methodology
((SELECT id FROM core.kg_node WHERE node_key = 'march1_stocks_methodology'),
 (SELECT id FROM core.kg_node WHERE node_key = 'corn'),
 'PREDICTS', 0.90,
 '{"mechanism": "March 1 = midpoint of marketing year. Second-quarter feed/residual implied by stocks minus known FSI and exports. Corn second-quarter feed/residual: 2021 implied 1.41B (up 90M YoY), 2022 implied 1.4B (down 33M), 2023 implied 1.4B (down 139M). Key diagnostic: second-quarter as % of marketing-year total. Historical avg 27%. Below average suggests marketing-year forecast too high. Above suggests too low. But: wait for full FSI data before adjusting — premature changes based on one quarter lead to whipsaw. Soy simpler: second-quarter residual typically -30 to -50M. When outside that range, flag for production revision potential (2021: cumulative residual suggested production 120M bu above USDA → yield revision from 50.2 to potential 51.7 → raised trendline)."}',
 'extracted', 0.90),

-- Macro override on report day
((SELECT id FROM core.kg_node WHERE node_key = 'usda_prospective_plantings'),
 (SELECT id FROM core.kg_node WHERE node_key = 'soybeans'),
 'CAUSES', 0.90,
 '{"mechanism": "Fundamental surprise can be overridden by macro context on report day. 2023: bullish soy acreage (87.5M vs 88.2M expected) drove only +2.9% intraday before settling below highs — recession fears capped gains. This connects to Batch 005 contra-intuitive reaction framework: when market fails to fully price a fundamental surprise, the macro override eventually fades and fundamentals reassert. Timing: macro dominates day 1-5, fundamentals dominate week 2+."}',
 'extracted', 0.85);

-- CONTEXTS
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES

-- Annual calendar: Feb-Mar analytical sequence
((SELECT id FROM core.kg_node WHERE node_key = 'usda_outlook_forum'),
 'expert_rule', 'february_march_analytical_sequence',
 '{"sequence": {
   "early_feb": "USDA Long-Term Projections released. Review for baseline assumptions and identify stale inputs (Oct WASDE starting point). Note where USDA underestimates biofuel demand, overestimates export competition.",
   "mid_feb": "RP insurance average price finalized. Calculate soy/corn ratio. This locks in farmer economics for planting decisions. Below 2.3 = corn. Above 2.5 = soy. Between = contested.",
   "late_feb": "USDA Outlook Forum. First official new-crop balance sheets. Compare AOF to own independent estimate. Identify divergences. Key: AOF yield is trendline — weather has not happened yet.",
   "early_mar": "USDA survey period begins (first two weeks). Farmer conversations become critical — are they confirming or deviating from AOF? Build independent acreage estimate. Begin stocks estimation.",
   "mid_mar": "March WASDE (last old-crop only). Minor changes typical. Note any demand revisions that cascade to stocks estimates. Build final pre-report positions.",
   "late_mar": "March 31 Prospective Plantings + Grain Stocks. Most volatile trading day. Position based on independent estimates vs analyst range. Assess reaction proportionality."
 }}',
 'pre_planting', 'extracted'),

-- Prospective Plantings surprise probability model
((SELECT id FROM core.kg_node WHERE node_key = 'prospective_plantings_framework'),
 'expert_rule', 'surprise_probability_assessment',
 '{"method": "Range width of analyst estimates as % of average predicts surprise probability. Narrower range = analysts agree = HIGHER probability USDA differs. Wider range = uncertainty already priced = LOWER probability of meaningful surprise. Soy is typically highest surprise probability crop because: (1) range is narrowest as % of average, (2) farmers can switch late, (3) double-crop adds uncertainty. Examples: 2023 soy range 2.5% = high surprise prob (USDA came in below). 2023 corn range 4.8% = moderate (USDA came above). When both crops surprise same direction from consensus, that is RARE and extremely bullish/bearish. Historical frequency: same-direction surprise ~15% of years."}',
 'pre_report', 'extracted'),

-- Balance sheet adjustment cascade
((SELECT id FROM core.kg_node WHERE node_key = 'balance_sheet_construction'),
 'expert_rule', 'adjustment_cascade_methodology',
 '{"cascade": "When acreage changes from expectation, cascade adjustments through entire balance sheet: (1) Production = new acreage × yield (initially keep yield unchanged). (2) If production tighter, reduce export forecast FIRST (most elastic demand component). (3) If still tight, check if ethanol/crush margins support demand cut. (4) Feed/residual adjusts last (least controllable). (5) New carryout → new STU ratio → new price forecast. (6) New price feeds back into export competitiveness and feed substitution economics. Key: do NOT simply change carryout by production delta. Demand responds to price changes. HB typically adjusts exports by 40-60% of production change, feed/residual by 10-20%, and allows carryout to absorb remainder. This produces more realistic price forecasts than pure arithmetic."}',
 'always', 'extracted'),

-- Geopolitical event rapid response
((SELECT id FROM core.kg_node WHERE node_key = 'ukraine_invasion_2022'),
 'expert_rule', 'geopolitical_rapid_response',
 '{"framework": "When geopolitical shock occurs: (1) IMMEDIATELY identify supply displacement — which countries/commodities affected. (2) Quantify displaced volume in bu/tonnes/lbs. (3) Identify substitution pathways — who absorbs displaced demand. (4) Revise OWN balance sheets SAME DAY. (5) Compare revised estimates to USDA/consensus (which will take weeks to adjust). (6) Divergence between own revised estimate and stale consensus IS the trade. Ukraine example: HB revised within days. Market took 2-3 weeks. USDA took until May WASDE. Speed of independent analysis is maximum competitive advantage during geopolitical events."}',
 'always', 'extracted');

-- SOURCE REGISTRY
INSERT INTO core.kg_source (source_key, source_type, title, location_uri, document_date, document_type, commodities, topics, status, first_processed, last_processed, nodes_extracted, edges_extracted, contexts_extracted) VALUES
('gdoc_1VUj221s5W5FBvLT05EJJOm7s5WipCcUWdRzUnbaDMNg', 'gdrive_doc', 'HB Weekly Text - 03032022 (Outlook Forum Analysis)', 'https://docs.google.com/document/d/1VUj221s5W5FBvLT05EJJOm7s5WipCcUWdRzUnbaDMNg/edit', '2022-03-03', 'weekly_text', '{corn,soybeans,wheat}', '{outlook_forum,acreage_forecast,ukraine_invasion,balance_sheet}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_1CGMCpQ07hf8gof-UtWUlb5jLE66gkzBOpiOr3CPHk4U', 'gdrive_doc', 'HB Weekly Text - 03312021 (PP+Stocks Reaction)', 'https://docs.google.com/document/d/1CGMCpQ07hf8gof-UtWUlb5jLE66gkzBOpiOr3CPHk4U/edit', '2021-03-31', 'weekly_text', '{corn,soybeans,wheat}', '{prospective_plantings,grain_stocks,limit_up,residual,production_revision}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_1HDxUG8E_5imIfAreurSL95EoY1jo1RJ8uMiopYx8E3c', 'gdrive_doc', 'HB Weekly Text - 03312022 (PP+Stocks Reaction)', 'https://docs.google.com/document/d/1HDxUG8E_5imIfAreurSL95EoY1jo1RJ8uMiopYx8E3c/edit', '2022-03-31', 'weekly_text', '{corn,soybeans,wheat}', '{prospective_plantings,record_soy_area,corn_decline,ukraine,biofuel_acreage}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_1Oq650PuMezstoyAz516sJSN_szF8VVq5j3Kt_wpXGEQ', 'gdrive_doc', 'HB Weekly Text - 03302023 (PP+Stocks Preview)', 'https://docs.google.com/document/d/1Oq650PuMezstoyAz516sJSN_szF8VVq5j3Kt_wpXGEQ/edit', '2023-03-30', 'weekly_text', '{corn,soybeans,wheat}', '{plantings_preview,surprise_probability,range_analysis,insurance_ratio}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_1mhj9RHMZ_LA8JfFSOZZhdIOaexri0jJSrLMtSeomibc', 'gdrive_doc', 'HB Weekly Text - 04062023 (PP+Stocks Reaction)', 'https://docs.google.com/document/d/1mhj9RHMZ_LA8JfFSOZZhdIOaexri0jJSrLMtSeomibc/edit', '2023-04-06', 'weekly_text', '{corn,soybeans,wheat}', '{plantings_reaction,stocks_implied_usage,feed_residual,macro_override}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_1qg57pcX5YNy7okM_jmaEksXb2UQAZRb8kNVQAtZpnqA', 'gdrive_doc', 'HB Weekly Text - 02172021 (Long-Term Projections + AOF Preview)', 'https://docs.google.com/document/d/1qg57pcX5YNy7okM_jmaEksXb2UQAZRb8kNVQAtZpnqA/edit', '2021-02-17', 'weekly_text', '{corn,soybeans,wheat,soybean_oil}', '{long_term_projections,outlook_forum,biofuel_demand,acreage_trends,price_forecast}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_1LemPI2TgAaZ8H--U4ngq8f5XeeQ92d3DZy6CnSfW3ek', 'gdrive_doc', 'HB Weekly Text - 03242022 (PP Preview + Ukraine)', 'https://docs.google.com/document/d/1LemPI2TgAaZ8H--U4ngq8f5XeeQ92d3DZy6CnSfW3ek/edit', '2022-03-24', 'weekly_text', '{corn,soybeans,wheat}', '{plantings_preview,ukraine,corn_preference,insurance_prices}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_12TD0mTeJFKlqS3mgYL_CwpYGHKyITToMAH4r6zqb16Q', 'gdrive_doc', 'HB Weekly Text - 03232021 (PP+Stocks Preview)', 'https://docs.google.com/document/d/12TD0mTeJFKlqS3mgYL_CwpYGHKyITToMAH4r6zqb16Q/edit', '2021-03-23', 'weekly_text', '{corn,soybeans,wheat}', '{plantings_preview,stocks_preview,volatility_expectations}', 'completed', NOW(), NOW(), 0, 0, 0)
ON CONFLICT (source_key) DO UPDATE SET status = 'completed', last_processed = NOW();
