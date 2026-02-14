-- ============================================================================
-- RLC Knowledge Graph: Extraction Batch 003
-- Source: 4 HB Weekly Text reports (2020-2022) + 2 HB First Drafts (Jun 2025)
-- Extracted: 2026-02-14
-- ============================================================================

-- ============================================================================
-- NEW NODES
-- ============================================================================

INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('data_series', 'usda.grain_stocks.residual', 'USDA Quarterly Grain Stocks — Residual Use Methodology', '{"context": "Residual use is the balancing item in quarterly stocks. Fluctuates with grain-in-transit volumes. Negative residual = grain previously in transit found in current quarter. Positive = more in transit now than counted."}'),
('data_series', 'noaa.enso.oni', 'NOAA Oceanic Nino Index (ONI)', '{"source": "NOAA CPC", "calculation": "3-month running mean SST anomalies in Nino 3.4 region", "context": "Primary El Nino/La Nina indicator"}'),
('data_series', 'oil_share', 'Soybean Oil Share of Crush Value', '{"calculation": "oil_value / (oil_value + meal_value)", "context": "Above 50% signals structural shift toward crushing for oil"}'),
('data_series', 'crush_margin.board', 'Board Crush Margin', '{"calculation": "product_value(meal+oil) - soybean_cost - processing", "context": "Primary processor profitability indicator"}'),
('data_series', 'spread.cdgo_vs_rbd', 'Crude Degummed vs RBD Soybean Oil Spread', '{"context": "Refining margin. Wide spread = biofuel demand pulling RBD values above food-market CDG."}'),
('market_participant', 'china_state_buyers', 'Chinese State Grain Buyers', '{"context": "State-directed purchasing drives flash sales. Need-based vs agreement-based buying distinction is critical."}'),
('market_participant', 'argentine_crushers', 'Argentine Soybean Crushers', '{"context": "World largest meal exporters. Differential export tax incentivizes domestic processing."}'),
('policy', 'phase1_trade_deal', 'US-China Phase 1 Trade Agreement', '{"signed": "Jan 2020", "ag_targets": "$12.5B increase 2020, $19.5B 2021 from $24B 2017 baseline"}'),
('policy', 'argentina_export_tax', 'Argentine Differential Export Tax', '{"structure": "Lower tax on meal/oil than raw soybeans", "vulnerability": "If US meal prices collapse, Argentina may shift to exporting beans"}'),
('seasonal_event', 'la_nina_cycle', 'La Nina Multi-Year Cycle', '{"context": "Year 3 pattern tends to FLIP. Triple La Nina extremely rare since 1950."}'),
('seasonal_event', 'corn_pollination_window', 'Corn Pollination Window (July)', '{"context": "Make-or-break month for US corn yields. Drives more price volatility than any other crop stage."}'),
('seasonal_event', 'soybean_pod_fill_aug', 'Soybean Pod Fill (August)', '{"context": "August rainfall determines US soybean yields. Single largest yield driver."}'),
('seasonal_event', 'brazil_safrinha_window', 'Brazil Safrinha Corn Planting Window', '{"context": "Jan-Feb planting driven by soy harvest timing. Late soy = compressed safrinha = lower corn yield."}'),
('seasonal_event', 'usda_june30_reports', 'USDA June 30 Acreage and Grain Stocks', '{"context": "Most volatile USDA reports. Corn above estimate 7/10 years. Soybean below estimate 10 consecutive years."}')
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- ============================================================================
-- EDGES
-- ============================================================================

INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
-- Crush-for-oil thesis
((SELECT id FROM core.kg_node WHERE node_key = 'oil_share'),
 (SELECT id FROM core.kg_node WHERE node_key = 'soybean_meal'),
 'CAUSES', 0.95,
 '{"mechanism": "When oil share exceeds ~50% sustainably, crushers shift from matching volumes to meal demand to matching oil demand. Produces EXCESS meal. Meal prices fall to corn-competitive level. Displaces DDGS, canola meal, sunflower meal first. US meal exports surge. Argentine crush industry contracts. Cheap feed enables US meat expansion. China adapts by crushing more domestically. Brazil expands soy+safrinha acreage. Written Nov 2021, thesis directionally correct through 2025.", "structural": true}',
 'extracted', 0.95),

-- Residual use methodology
((SELECT id FROM core.kg_node WHERE node_key = 'usda.grain_stocks.residual'),
 (SELECT id FROM core.kg_node WHERE node_key = 'soybeans'),
 'LEADS', 0.85,
 '{"mechanism": "Build quarterly balance sheet from NOPA (crush), Census+inspections (exports), NASS ethanol grind (corn FSI). Residual driven by grain-in-transit changes. Declining shipments Q-over-Q = negative residual = stocks appear higher. Produces independent estimate before USDA report for positioning edge."}',
 'extracted', 0.90),

-- ENSO weather framework
((SELECT id FROM core.kg_node WHERE node_key = 'noaa.enso.oni'),
 (SELECT id FROM core.kg_node WHERE node_key = 'soybeans'),
 'CAUSES', 0.80,
 '{"mechanism": "La Nina: drier Southern Plains + Argentina, wetter Great Lakes/Ohio Valley. ONI strength modulates pattern. Multi-year events show year-3 pattern FLIP. Triple La Nina (2022) only 3rd since 1950. CPC provides 9-month ONI forecast lead. When ONI < -1.0 AND US stocks tight, apply above-trend weather risk premium."}',
 'extracted', 0.90),

-- Chinese demand surge
((SELECT id FROM core.kg_node WHERE node_key = 'china_state_buyers'),
 (SELECT id FROM core.kg_node WHERE node_key = 'corn'),
 'CAUSES', 0.90,
 '{"mechanism": "2020/21 Chinese corn imports surged 7.6M→potential 20-30M MT. USDA May-Dec revision from 7M to 16.5M (135%). ASF herd recovery + flooding + depleted reserves. Chinese veg oil imports grew 46% in 3 years. Canada-China canola dispute contributed. Price rally reduces volume needed for dollar-denominated targets."}',
 'extracted', 0.90),

-- RVO → soy oil demand (Jun 2025)
((SELECT id FROM core.kg_node WHERE node_key = 'rvo'),
 (SELECT id FROM core.kg_node WHERE node_key = 'soybean_oil'),
 'CAUSES', 0.95,
 '{"mechanism": "EPA proposed record RFS: 24.02B gal biofuel 2026, biomass-based diesel 3.35B→5.61B gal (+68%). Conventional ethanol 15.0B gal maintained. Rules to reduce RIN values for imported feedstocks (UCO, tallow). SRE gallons to be reallocated. Soy oil hit daily limit on announcement. Trump admin proposal with bipartisan support."}',
 'extracted', 0.95),

-- Fund short → explosion risk (Jun 2025)
((SELECT id FROM core.kg_node WHERE node_key = 'managed_money'),
 (SELECT id FROM core.kg_node WHERE node_key = 'corn'),
 'CAUSES', 0.90,
 '{"mechanism": "Managed money net short ~164K corn contracts ahead of June 30 reports. KC wheat shorts at all-time record. Corn acreage above estimate 7/10 years. Soybean below estimate 10 consecutive. Plantings outside pre-report range 4/6 Junes. These biases are tradeable: market consistently misprices acreage risk."}',
 'extracted', 0.90),

-- Brazil record → US displacement
((SELECT id FROM core.kg_node WHERE node_key = 'brazil.mato_grosso'),
 (SELECT id FROM core.kg_node WHERE node_key = 'soybeans'),
 'CAUSES', 0.85,
 '{"mechanism": "Record Brazil soy crops cap US exports. Jun 2025 Brazil soy shipments 14.37M MT (monthly record). US Gulf $20-60c/bu premium. China stays Brazil-facing. Safrinha corn 11% larger displaces US Jul-Aug. Record Brazilian prices drove sharp acreage expansion within one season."}',
 'extracted', 0.85),

-- Refining margin → crush economics
((SELECT id FROM core.kg_node WHERE node_key = 'spread.cdgo_vs_rbd'),
 (SELECT id FROM core.kg_node WHERE node_key = 'crush_margin.board'),
 'CAUSES', 0.85,
 '{"mechanism": "Wide CDGO-RBD spread from biofuel demand adds $0.03-0.08/lb to oil value for crushers who refine. Analysts using only CDGO or board oil understate crusher profitability. Sustained spread triggered investment in both crush and refining capacity."}',
 'extracted', 0.85);

-- ============================================================================
-- CONTEXTS
-- ============================================================================

INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'soybean_oil'),
 'expert_rule', 'crush_for_oil_thesis',
 '{"thesis": "Permanent structural shift from crushing for meal to crushing for oil, driven by renewable diesel. When cash oil share sustains above ~50%, crushers match volumes to oil demand. Excess meal cascades: displaces alternative proteins → US meal exports surge → Argentine crush contracts → cheap feed expands US meat → China adapts → Brazil expands acreage. Written Nov 2021, validated through 2025."}',
 'structural', 'extracted'),

((SELECT id FROM core.kg_node WHERE node_key = 'usda.grain_stocks.residual'),
 'expert_rule', 'stocks_forecasting_methodology',
 '{"methodology": "Build quarterly balance sheet independently: supply (prior stocks+imports), crush (NOPA 2mo+1mo est), exports (Census 2mo+inspections 1mo), FSI (NASS ethanol+seasonal patterns). Residual from grain-in-transit change. When HB estimate diverges from consensus, divergence direction is the trade."}',
 'pre_report', 'extracted'),

((SELECT id FROM core.kg_node WHERE node_key = 'noaa.enso.oni'),
 'expert_rule', 'enso_crop_impact_framework',
 '{"framework": "La Nina: drier S.Plains+Argentina, wetter Ohio Valley. ONI modulates strength. Year-3 flip in multi-year events. Triple La Nina extremely rare. CPC 9-month forecast lead. ONI < -1.0 + tight stocks = above-trend weather premium."}',
 'growing_season', 'extracted'),

((SELECT id FROM core.kg_node WHERE node_key = 'usda.crop_progress'),
 'expert_rule', 'acreage_report_biases',
 '{"biases": "Corn above estimate 7/10 years (last 4 consecutive). Soybean below estimate 10 consecutive years. Plantings outside pre-report range 4/6 Junes. Market consistently misprices in same direction. Tradeable when fund positioning aligns with wrong-direction consensus."}',
 'pre_report', 'extracted'),

((SELECT id FROM core.kg_node WHERE node_key = 'china'),
 'expert_rule', 'chinese_put_framework',
 '{"framework": "When Chinese needs are genuine (not policy compliance), they create price floor. Need indicators: USDA revision trajectory, flash sales frequency, Dalian premiums, reserve auctions, hog margins. Price rally reduces volume for dollar targets — compliance illusion. Veg oil imports grew 46% in 3yr partly from Canada-China canola dispute."}',
 'always', 'extracted'),

((SELECT id FROM core.kg_node WHERE node_key = 'corn'),
 'expert_rule', 'year_in_review_methodology',
 '{"framework": "Annual: identify 4-5 macro forces, trace USDA May→Dec revision trajectory (magnitude signals demand surprise), identify carry-forward vs resolving themes. 2020: corn stocks 3.3B→1.7B (-48%), soy 405M→175M (-57%). Revision magnitude itself is the signal."}',
 'year_end', 'extracted');

-- ============================================================================
-- SOURCE REGISTRY
-- ============================================================================

INSERT INTO core.kg_source (source_key, source_type, title, location_uri, document_date, document_type, commodities, topics, status, first_processed, last_processed, nodes_extracted, edges_extracted, contexts_extracted) VALUES
('gdoc_1ZeT0aWR78bFpBQE0U4mbtpNqO_IuPChxQftWzil4WhI', 'gdrive_doc', 'HB Weekly Text - 12302020 (Year in Review)', 'https://docs.google.com/document/d/1ZeT0aWR78bFpBQE0U4mbtpNqO_IuPChxQftWzil4WhI/edit', '2020-12-30', 'weekly_text', '{corn,soybeans,wheat,soybean_meal,soybean_oil}', '{year_in_review,china_demand,phase1_trade,la_nina,covid_impact}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_1o8nVnUCDeNa-eL7TzBawTP678kNpvcYROUIXcCg46tw', 'gdrive_doc', 'HB Weekly Text 2 - 03232021 (Grain Stocks Methodology)', 'https://docs.google.com/document/d/1o8nVnUCDeNa-eL7TzBawTP678kNpvcYROUIXcCg46tw/edit', '2021-03-23', 'weekly_text', '{corn,soybeans,wheat}', '{grain_stocks,residual_use,stocks_methodology}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_1DiGh3NUgaj_Q8mnuXn2lTbM58MpgeAccJ4o1OB_iGhM', 'gdrive_doc', 'HB Weekly Text - 11182021 (Crush for Oil Thesis)', 'https://docs.google.com/document/d/1DiGh3NUgaj_Q8mnuXn2lTbM58MpgeAccJ4o1OB_iGhM/edit', '2021-11-18', 'weekly_text', '{soybeans,soybean_meal,soybean_oil}', '{crush_for_oil,oil_share,structural_shift,renewable_diesel,argentina_impact}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_1kbGV6Opmim6_Y54nOH-zTrjhI4X1kSrVBufAKgw8xBw', 'gdrive_doc', 'HB Weekly Text - 12012022 (La Nina/ENSO Framework)', 'https://docs.google.com/document/d/1kbGV6Opmim6_Y54nOH-zTrjhI4X1kSrVBufAKgw8xBw/edit', '2022-12-01', 'weekly_text', '{corn,soybeans,wheat}', '{enso,la_nina,oni,weather_framework,triple_la_nina}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_1vGPBq_ZMk7Ozm8IFF3Ly1yfBYA-gGJVdLX0LCKhW1qg', 'gdrive_doc', 'HigbyBarrett First Draft - Jun 19 2025', 'https://docs.google.com/document/d/1vGPBq_ZMk7Ozm8IFF3Ly1yfBYA-gGJVdLX0LCKhW1qg/edit', '2025-06-19', 'weekly_first_draft', '{corn,soybeans,wheat,soybean_meal,soybean_oil}', '{rvo_proposal,rfs,biofuel_mandate,nopa_crush,fund_positioning}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_1HRk4wg5_fge8tLd2vbXm0f5FmgZsooC0q_0eM5XjDRQ', 'gdrive_doc', 'HigbyBarrett First Draft - Jun 25 2025', 'https://docs.google.com/document/d/1HRk4wg5_fge8tLd2vbXm0f5FmgZsooC0q_0eM5XjDRQ/edit', '2025-06-25', 'weekly_first_draft', '{corn,soybeans,wheat,soybean_meal,soybean_oil}', '{june30_reports,acreage_bias,stocks_preview,weather,fund_shorts}', 'completed', NOW(), NOW(), 0, 0, 0)
ON CONFLICT (source_key) DO UPDATE SET status = 'completed', last_processed = NOW();
