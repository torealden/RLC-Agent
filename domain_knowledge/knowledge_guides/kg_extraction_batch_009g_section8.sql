-- ============================================================
-- Knowledge Graph Extraction: Batch 009g
-- Source: HOBO Study - Section 8: RD and SAF Price Projections and HOBO Margin Outlook
-- Date: 2026-02-14
-- ============================================================

INSERT INTO kg_source (source_key, source_type, title, url, report_date, category, commodities, themes, status, first_seen, last_processed, node_count, edge_count, context_count)
VALUES
('gdoc_hobo_section8', 'gdrive_doc', 'HOBO Study - Section 8: Price Projections and Margin Outlook', 'https://docs.google.com/document/d/1tzMuI3_2Us4jKbtycC9a4KLs-TXvceQuJCGrFQKGqiA/edit', '2025-06-19', 'consulting_report', '{renewable_diesel,saf,d4_rin,ulsd,jet_fuel,uco,soybean_oil,tallow}', '{pricing_dynamics,credit_stacking,ci_value,margin_scenarios,competitor_benchmarking,product_flexibility,market_optimization}', 'completed', NOW(), NOW(), 10, 8, 10)
ON CONFLICT (source_key) DO UPDATE SET status = 'completed', last_processed = NOW();

-- NODES
INSERT INTO kg_node (node_key, node_type, label, description, metadata)
VALUES
('rd_price_stack', 'metric', 'RD Price Stack Construction',
 'RD revenue per gallon is a stack: Base ULSD price + D4 RINs (1.7 × RIN price) + LCFS credit + 45Z/BTC tax credit. In California 2025: ~$2.50 + $2.55 (RINs) + $0.50-0.75 (LCFS) + $0.65-1.00 (45Z) = $5.50-6.00+/gal effective price. Credits fill ~50% of total revenue.',
 '{"components": ["ulsd_base", "d4_rin", "lcfs", "45z"], "ca_2025_total": "$5.50-6.00+", "credit_share_pct": "~50"}'),
('saf_price_stack', 'metric', 'SAF Price Stack Construction',
 'SAF cost ~$8.00/gal without credits. Airlines pay ~$4.00/gal (jet parity). Gap bridged by: RINs ~$1.80/gal + LCFS ~$0.70/gal + SAF credit $1.50/gal = ~$4.00 in credits. Net: buyer pays $4, credits pay $4. Without credits, SAF is unmarketable at scale.',
 '{"total_cost": "$8.00", "buyer_pays": "$4.00", "credits": "$4.00", "credit_essential": true}'),
('regional_netback_hierarchy', 'metric', 'Regional RD/SAF Netback Hierarchy',
 'HOBO pricing hierarchy: California > Illinois > Alberta in base case. California advantage driven by LCFS + cap-and-trade. 2022 example: CA RD total value ~$1,028/ton vs IL ~$877/ton — CA 30% premium. Canada competitive but requires credit arbitrage optimization.',
 '{"hierarchy": "CA > IL > AB", "ca_premium_pct": 30, "ca_2022_per_ton": 1028, "il_2022_per_ton": 877}'),
('saf_vs_rd_economics', 'metric', 'SAF vs RD Relative Economics Over Time',
 'Pre-2023: SAF yielded $0.60-1.00/gal LESS than RD (no dedicated credit, jet<diesel). 2023 with SAF BTC: SAF became MORE valuable — +$288/ton vs RD in Midwest, +$55/ton in CA. 2025+ with 45Z: SAF and RD achieve comparable netbacks when credits strong. Without credits: RD outperforms by $0.20-0.30/gal.',
 '{"pre_2023_saf_penalty_per_gal": "$0.60-1.00", "2023_saf_premium_midwest_per_ton": 288, "no_credit_rd_advantage_per_gal": "$0.20-0.30"}'),
('ci_value_framework', 'metric', 'CI Value Per Point Framework',
 'Baseline CI 35: LCFS credit ~$0.50/gal, 45Z ~$0.65/gal RD ($1.40 SAF). Each 5 CI points reduction = ~$0.15/gal more. CI 35→20 = +$0.45/gal. CI 35→0 = +$0.80-1.00/gal. For HOBO 155 MMgy, each 5 CI points = $20-30M/year. CI improvement ROI is immediate.',
 '{"baseline_ci": 35, "per_5ci_per_gal": 0.15, "ci35_to_ci0_per_gal": "0.80-1.00", "per_5ci_annual_M": "20-30"}'),
('ci_table', 'reference', 'SAF CI by Pathway Reference Table',
 'CI benchmarks (gCO2e/MJ): UCO/Animal Fats HEFA ~18.2 (80% reduction); SBO HEFA ~64.9 (27%); Corn ethanol ATJ ~90.8 (~0%); Cellulosic ATJ ~24.6-39.7 (60-72%); MSW/Wood FT ~7.7-32.5 (63-91%); PtL ~7 (92%). Fossil jet baseline: 89.',
 '{"uco_hefa": 18.2, "sbo_hefa": 64.9, "corn_atj": 90.8, "cellulosic_atj": "24.6-39.7", "ft_msw": "7.7-32.5", "ptl": 7, "fossil_baseline": 89}'),
('base_case_margins', 'metric', 'HOBO Base Case Margins by Market',
 'Base case margins (first operating year): Illinois RD ~$130-150/ton ($0.40-0.50/gal), IL SAF ~$180/ton ($0.55/gal). California RD ~$220-230/ton ($0.65-0.70/gal), CA SAF ~$270-290/ton ($0.80-0.88/gal). Alberta RD ~$240-260/ton ($1.05-1.10/gal, includes credit arbitrage assumptions).',
 '{"il_rd_per_gal": "$0.40-0.50", "il_saf_per_gal": 0.55, "ca_rd_per_gal": "$0.65-0.70", "ca_saf_per_gal": "$0.80-0.88", "ab_rd_per_gal": "$1.05-1.10"}'),
('best_worst_margins', 'metric', 'HOBO Best/Worst Case Margin Scenarios',
 'Best case: margins could exceed base by 50-100%. Chicago RD approaching $0.80-0.90/gal, CA surpassing $1.00/gal (precedent in late 2021/early 2022). EBITDA margins mid-30%+. Worst case: Midwest RD near $0/gal, CA under $0.20/gal. Similar to industry 2024 downturn.',
 '{"best_case_il_rd_per_gal": "$0.80-0.90", "best_case_ca_per_gal": ">$1.00", "worst_case_il_per_gal": "~$0", "worst_case_ca_per_gal": "<$0.20"}'),
('product_slate_optimization', 'strategy', 'Product Slate Optimization Strategy',
 'HOBO should continuously optimize: (1) RD vs SAF production ratio based on relative credit economics; (2) Geographic routing (CA vs IL vs Canada) based on netback after freight/credits; (3) Contract vs spot mix (lock base margin, float remainder for upside). Value at stake from optimization is tens of millions annually.',
 '{"optimization_dimensions": ["product_ratio", "geographic_routing", "contract_vs_spot"], "value": "tens_of_millions_annually"}'),
('canada_credit_arbitrage', 'strategy', 'Canada Credit Arbitrage Opportunity',
 'Surprising finding: Alberta margins appear highest at $1.05-1.10/gal due to potential double-dip of US credits (RIN/45Z) + Canadian CFR credit. Practically cannot double-count on same gallon — must optimize per-gallon credit routing. But Canada optionality is valuable, especially post-2027 if US credits sunset.',
 '{"margin_per_gal": "$1.05-1.10", "caveat": "cannot_double_count", "post_2027_value": "high"}')
ON CONFLICT (node_key) DO UPDATE SET description = EXCLUDED.description, metadata = EXCLUDED.metadata;

-- EDGES
INSERT INTO kg_edge (source_node, target_node, edge_type, label, weight, metadata)
VALUES
('rd_price_stack', 'hobo_renewables', 'DETERMINES', 'RD revenue stack (ULSD + RINs + LCFS + 45Z) determines ~$5.50-6.00/gal effective price in CA. Credits = ~50% of revenue', 0.90,
 '{"confidence": "high"}'),
('saf_price_stack', 'hobo_renewables', 'DETERMINES', 'SAF requires ~$4.00/gal in credits to bridge $8.00 production cost to $4.00 buyer price. Credits are not optional — SAF is unmarketable without them', 0.95,
 '{"critical": true, "confidence": "very_high"}'),
('ci_value_framework', 'hobo_ci_advantage', 'QUANTIFIES', 'Each 5 CI points = $0.15/gal = $20-30M/year. HOBO CI 20s vs industry 30s = ~$40-60M/year advantage in credit revenue', 0.90,
 '{"mechanism": "credit_generation_differential", "confidence": "high"}'),
('saf_vs_rd_economics', 'credit_arbitrage_strategy', 'INFORMS', 'SAF more valuable than RD when credits strong (+$288/ton Midwest 2023), RD more valuable when credits weak ($0.20-0.30/gal advantage). Flexibility to switch is the hedge.', 0.85,
 '{"confidence": "high"}'),
('regional_netback_hierarchy', 'product_slate_optimization', 'INFORMS', 'CA>IL>AB netback hierarchy guides geographic routing. But rankings shift with credit price changes — continuous monitoring required', 0.85,
 '{"confidence": "high"}'),
('base_case_margins', 'diamond_green_diesel', 'BENCHMARKS_AGAINST', 'HOBO base case ($0.50-0.70/gal) sits middle of DGD range ($0.50-1.30) and matches Neste global average. Competitive positioning realistic', 0.85,
 '{"confidence": "high"}'),
('product_slate_optimization', 'hobo_renewables', 'ADVANTAGES', 'Dynamic optimization of product/market/contract mix worth tens of millions annually. Requires dedicated trading/optimization capability', 0.85,
 '{"confidence": "high"}'),
('canada_credit_arbitrage', 'hobo_renewables', 'ADVANTAGES', 'Canada provides both geographic diversification and potential credit arbitrage. Especially valuable post-2027 if US federal credits expire', 0.80,
 '{"confidence": "high"}')
ON CONFLICT DO NOTHING;

-- CONTEXTS
INSERT INTO kg_context (context_key, node_or_edge_ref, context_type, content, confidence, metadata)
VALUES
('price_stack_decomposition', 'rd_price_stack', 'quantitative_framework',
 'Complete RD price decomposition (California 2025 illustrative): Base ULSD $2.50 + D4 RINs $2.55 (1.7 × $1.50) + LCFS $0.50-0.75 (at $60-70/MT, 55g/MJ avoided) + 45Z $0.65-1.00 (CI-dependent) = effective selling price $5.50-6.50/gal. Production cost: feedstock $3.00-4.50 (varies by oil type) + OPEX $0.30-0.50 + overhead = $3.50-5.00/gal. Net margin $0.50-1.50/gal depending on all variables. Key insight: credits make up roughly HALF of total revenue. Without credits, RD would need to sell at $4.00+/gal — far above $2.50 ULSD.',
 0.90, '{"source": "hobo_section8", "type": "margin_build"}'),

('saf_economic_impossibility_without_credits', 'saf_price_stack', 'expert_rule',
 'SAF economics are absolutely credit-dependent. Production cost ~$8.00/gal. Airlines willing to pay ~$4.00 (jet parity, maybe slight premium). Gap = $4.00 MUST come from credits (RINs + LCFS + 45Z). The IRA SAF credit + RIN + LCFS was estimated to provide up to ~$3.78/gal in combined incentive. Even this barely closes the gap. Without any credits, SAF would carry $3-4/gal premium over jet — unmarketable at scale. This is THE central economic fact for any SAF project.',
 0.95, '{"source": "hobo_section8", "critical": true}'),

('saf_flipped_economics_2023', 'saf_vs_rd_economics', 'market_data',
 'SAF economics flipped in 2023 when dedicated SAF BTC ($1.25-1.75/gal) was introduced. Before 2023: SAF yielded $60-140/ton LESS than RD (jet fuel cheaper than diesel, no SAF-specific credit). After SAF credit: SAF yielded $288/ton MORE than RD in Midwest, $55/ton more in CA. This validates HOBO product flexibility strategy — without ability to swing, producers locked into wrong product at wrong time.',
 0.90, '{"source": "hobo_section8", "turning_point": 2023}'),

('margin_scenarios_spread', 'base_case_margins', 'scenario_analysis',
 'Margin range is enormous: Best case (high diesel, strong credits, cheap feedstock) = IL RD $0.80-0.90/gal, CA >$1.00/gal, EBITDA margins 35%+. Base case = IL $0.40-0.50/gal, CA $0.65-0.70/gal, EBITDA 25-30%. Worst case (low diesel, credit collapse, feedstock spike) = IL near $0/gal, CA <$0.20/gal. Worst case is NOT hypothetical — Neste experienced $242/ton ($0.18/gal) in Q4 2024. HOBO must be prepared for full range.',
 0.90, '{"source": "hobo_section8", "range_il_per_gal": "$0 to $0.90"}'),

('lcfs_credit_price_impact', 'california_lcfs', 'market_data',
 'LCFS credit price is highly volatile and directly impacts margin: $20/ton change in LCFS = ~$0.17/gal for typical RD. Credits ranged from $200 to $70/ton in recent years. At CI 35, HOBO earns ~$0.50/gal LCFS credit in CA. If LCFS credits fell from $100 to $50/ton, ~$0.50/gal of value lost. LCFS amendment to tighten from 20% to 30% by 2030 would likely support credit prices.',
 0.85, '{"source": "hobo_section8", "sensitivity": "$20/ton = $0.17/gal"}'),

('ci_pathway_economics', 'ci_table', 'reference_data',
 'CI determines credit value AND market access. UCO HEFA at CI 18.2 generates maximum credits in ALL programs. SBO HEFA at CI 64.9 barely qualifies for some programs (50% threshold = 44.5 gCO2/MJ). Corn ATJ at CI 90.8 fails 50% threshold entirely without CCS. PtL at CI 7 maximizes 45Z. HOBO target of CI low 20s positions it in the "premium credit" zone — matching UCO-level performance even with blended feedstock.',
 0.90, '{"source": "hobo_section8", "type": "pathway_comparison"}'),

('trading_desk_recommendation', 'product_slate_optimization', 'recommendation',
 'HOBO should establish a dedicated trading/optimization desk that daily evaluates: (1) Where should next railcar go (CA vs IL vs Canada)? (2) What product slate maximizes value (more RD or SAF this week)? (3) What credit prices are doing (RIN, LCFS, 45Z) and how to capture them? This kind of arbitrage is common in commodities and can add several million dollars annually at essentially no capital cost — just smart decision-making. HOBO team has decades of commodity risk experience to execute this.',
 0.85, '{"source": "hobo_section8", "value": "millions_annually_at_zero_capex"}'),

('80_20_split_optimization', 'product_slate_optimization', 'quantitative_framework',
 'Intermediate production splits analyzed: 80/20 RD/SAF in base case yields blended margin very close to 100% SAF case when SAF incentives are rich (because 20% SAF gets big credit uplift). Even small SAF proportion raises overall margin: 80% RD at $0.50 + 20% SAF at $1.00 = weighted average $0.60 vs pure RD $0.50. In downside scenario (weak SAF credits), 80/20 protects bulk of margin in RD. Flexibility is an embedded option with significant value.',
 0.85, '{"source": "hobo_section8", "type": "optimization_analysis"}'),

('all_saf_routes_above_fossil', 'saf_market', 'expert_rule',
 'Most analyses (IEA, McKinsey) find ALL SAF routes remain more expensive than fossil jet through 2050 absent continuous subsidies. Learning curves and hydrogen cost declines may narrow gaps, but policy support (mandates/credits) will remain essential for SAF viability for DECADES. Lenders should expect permanent policy dependence for any SAF investment.',
 0.90, '{"source": "hobo_section8", "time_horizon": "through_2050", "critical": true}'),

('strategic_recommendations_section8', 'hobo_renewables', 'recommendation_set',
 'Section 8 strategic recommendations: (1) Prioritize low-CI pathways — each CI point = direct dollars; (2) Maintain product flexibility RD/SAF for first years with mixed contract/spot; (3) Diversify markets across CA, OR, WA, IL, Canada, possibly EU export; (4) Feedstock cost management via long-term contracts, portfolio diversification, potential vertical integration; (5) Hedging program for commodity/credit exposure; (6) Policy engagement and advocacy for credit extensions; (7) Continuous improvement culture — treat margin optimization as ongoing exercise.',
 0.90, '{"source": "hobo_section8", "type": "recommendation_set"}')
ON CONFLICT (context_key) DO UPDATE SET content = EXCLUDED.content, confidence = EXCLUDED.confidence;
