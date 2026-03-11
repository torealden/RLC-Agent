-- ============================================================
-- Knowledge Graph Extraction: Batch 009f
-- Source: HOBO Study - Section 7: Economic Viability of a Clinton County HEFA Facility
-- Date: 2026-02-14
-- ============================================================

INSERT INTO kg_source (source_key, source_type, title, url, report_date, category, commodities, themes, status, first_seen, last_processed, node_count, edge_count, context_count)
VALUES
('gdoc_hobo_section7', 'gdrive_doc', 'HOBO Study - Section 7: Economic Viability', 'https://docs.google.com/document/d/1YMSUN3k4Q385uQAXruP2I56jhmQxjUATAlRTACvCd1g/edit', '2025-06-19', 'consulting_report', '{renewable_diesel,saf,uco,tallow,dcor,soybean_oil,choice_white_grease}', '{capex,opex,margins,sensitivity,feedstock_cost,competitor_benchmarking,diamond_green,execution_timeline}', 'completed', NOW(), NOW(), 8, 6, 8)
ON CONFLICT (source_key) DO UPDATE SET status = 'completed', last_processed = NOW();

-- NODES
INSERT INTO kg_node (node_key, node_type, label, description, metadata)
VALUES
('hefa_capex_benchmark', 'metric', 'HEFA CapEx Benchmarks',
 'A plant of HOBO scale (~10,000 bpd / ~150 MMgy) typically requires $1,200-1,500M in CapEx. Translates to ~$8-10 per gallon of annual capacity, mid-range by refining standards. Technology proven so can use established designs.',
 '{"capex_range_M": "1200-1500", "per_gal_capacity": "$8-10", "scale_bpd": 10000}'),
('hefa_opex_structure', 'metric', 'HEFA Operating Cost Structure',
 'Variable OPEX relatively low at $0.30-0.40/gal excluding feedstock. Key costs: hydrogen (from natural gas), utilities, catalysts, labor. Feedstock = 70-80% of cash cost per gallon. Iowa offers lower natural gas costs. Managing feedstock procurement is central to economic viability.',
 '{"variable_opex_per_gal": "$0.30-0.40", "feedstock_pct_of_cost": "70-80%", "h2_source": "natural_gas"}'),
('feedstock_sensitivity_rule', 'metric', 'Feedstock Price Sensitivity ($0.05/lb = ~$0.35-0.40/gal)',
 'A $0.05/lb change in feedstock price translates to ~$0.35-0.40 per gallon change in production cost (since ~7-8 lbs of feed yield a gallon). This means a jump from $0.40 to $0.50/lb in feedstock could erase ~$0.75/gal in margin.',
 '{"sensitivity": "$0.05_per_lb = $0.35-0.40_per_gal", "lbs_per_gallon": "7-8", "critical": true}'),
('diamond_green_diesel', 'company', 'Diamond Green Diesel (DGD)',
 'Joint venture of Valero and Darling Ingredients. Over 1B gal/year capacity — largest US RD producer. Integrated feedstock supply via Darling global fats/oils collection. DGD margins ranged $0.50-$1.30/gal in recent years, falling to $0.60/gal by Q3 2024.',
 '{"partners": ["Valero", "Darling"], "capacity_bgal": 1.0, "margin_range_per_gal": "$0.50-1.30", "margin_q3_2024": "$0.60"}'),
('neste_corporation', 'company', 'Neste Corporation',
 'World-leading renewable diesel producer. Sales margin fell from ~$813/ton Q4 2023 to $242/ton Q4 2024 — nearly 70% drop due to low diesel prices, oversupply, and weaker credits. $242/ton = ~$0.18/gal, very slim.',
 '{"margin_q4_2023_per_ton": 813, "margin_q4_2024_per_ton": 242, "margin_decline_pct": 70}'),
('hobo_execution_timeline', 'metric', 'HOBO Project Execution Timeline',
 'HOBO has secured permits and LSTK EPC contract — shovel ready. FID expected Q3 2025. Construction ~40 months. Target commissioning end 2028. 6+ month ramp-up to full capacity. Feedstock MOUs claimed for 200% of plant requirements.',
 '{"fid": "Q3_2025", "construction_months": 40, "commissioning": "end_2028", "feedstock_mou_coverage_pct": 200, "permits": "secured"}'),
('midwest_feedstock_cost_curve', 'metric', 'Midwest Feedstock Cost Curve',
 'Midwest waste fats trade at discount to coastal: CWG mid-$0.30s/lb (vs $0.40+ coastal). UCO/DCO low $0.40s (at times near $0.50). Local ethanol DCO and renderer animal fats form cheapest tier. Cost curve rises as HOBO taps more distant or premium sources. SBO at ~$0.50/lb is fallback but higher CI.',
 '{"cwg_midwest_per_lb": "mid_0.30s", "cwg_coastal_per_lb": "$0.40+", "uco_dco_per_lb": "low_0.40s", "sbo_per_lb": "~0.50"}'),
('ci_reduction_roi', 'metric', 'CI Reduction ROI',
 'Every 5 CI points improvement yields ~$0.15/gal extra margin via LCFS + 45Z. For 10,000 bpd plant (~155 MMgy), that is ~$20-30M/year per 5 CI points. Moving from CI 35 to CI 20 = ~$0.45/gal more. To CI 0 = ~$0.80-1.00/gal more. ROI on CI-reducing investments is immediate via credits.',
 '{"per_5ci_per_gal": 0.15, "per_5ci_annual_M": "20-30", "ci35_to_ci20_per_gal": 0.45, "ci35_to_ci0_per_gal": "0.80-1.00"}')
ON CONFLICT (node_key) DO UPDATE SET description = EXCLUDED.description, metadata = EXCLUDED.metadata;

-- EDGES
INSERT INTO kg_edge (source_node, target_node, edge_type, label, weight, metadata)
VALUES
('feedstock_sensitivity_rule', 'hobo_renewables', 'CONSTRAINS', '$0.05/lb feedstock swing = $0.35-0.40/gal margin impact — feedstock is THE dominant economic variable for HEFA', 0.95,
 '{"critical": true, "confidence": "very_high"}'),
('diamond_green_diesel', 'hobo_renewables', 'COMPETES_WITH', 'DGD is formidable competitor with 1B+ gal scale and integrated Darling feedstock supply. HOBO counters with Midwest location cost edge and SAF flexibility', 0.85,
 '{"competitive_dynamic": "scale_vs_location", "confidence": "high"}'),
('neste_corporation', 'hobo_renewables', 'COMPETES_WITH', 'Neste is global leader but margins collapsed 70% in 2024. HOBO projects similar margin range ($0.50-0.70/gal) but with lower logistics costs serving domestic markets', 0.80,
 '{"competitive_dynamic": "global_vs_domestic", "confidence": "high"}'),
('ci_reduction_roi', 'hobo_ci_advantage', 'REINFORCES', 'Each CI point reduction adds direct dollar value via credits. HOBO CI advantage (low 20s vs industry 30s) translates to $20-30M+ annually in credit revenue advantage', 0.90,
 '{"mechanism": "credit_generation", "confidence": "high"}'),
('midwest_feedstock_cost_curve', 'hobo_renewables', 'ADVANTAGES', 'Midwest waste fats trade at discount to coastal markets. HOBO local sourcing within 250mi by truck minimizes transport costs and middleman markup', 0.85,
 '{"mechanism": "proximity_discount", "confidence": "high"}'),
('hefa_opex_structure', 'feedstock_sensitivity_rule', 'EXPLAINS', 'Feedstock at 70-80% of HEFA cash cost means feedstock price changes dominate all other cost variables by a wide margin', 0.95,
 '{"confidence": "very_high"}')
ON CONFLICT DO NOTHING;

-- CONTEXTS
INSERT INTO kg_context (context_key, node_or_edge_ref, context_type, content, confidence, metadata)
VALUES
('margin_construction_rd_california', 'hobo_renewables', 'quantitative_framework',
 'Illustrative RD margin build (California, 2025): ULSD price ~$2.50/gal + D4 RINs ~$2.80/gal (1.7 RINs × ~$1.69) + LCFS credits ~$0.50-0.75/gal + BTC/45Z ~$1.00/gal = effective net selling price $5.50-6.00+/gal. Production cost ~$4.00-4.50/gal (feedstock $3.00+ plus ~$0.50 other). Margin potential >$1.00/gal in optimized California scenario.',
 0.85, '{"source": "hobo_section7", "type": "margin_build"}'),

('no_credit_breakeven', 'hobo_renewables', 'risk_assessment',
 'CRITICAL: Without policy incentives, HOBO needs to sell at ~$4.00+/gal to break even on variable costs — substantially above typical diesel/jet market prices of $2.50-3.00. This means credits are not "nice to have" — they are ESSENTIAL for viability. This risk is not unique to HOBO but applies to the entire RD/SAF industry.',
 0.95, '{"source": "hobo_section7", "critical": true}'),

('competitor_margin_collapse_2024', 'rd_market', 'market_data',
 'Industry margin compression 2024: Neste margins fell from $813/ton to $242/ton (70% decline). DGD margins fell from ~$0.95/gal to $0.60/gal. Causes: low diesel prices, new competition/oversupply, weaker credits. This is the downside scenario HOBO must be prepared for. Even industry leaders were severely impacted.',
 0.90, '{"source": "hobo_section7", "neste_decline_pct": 70, "dgd_margin_q3_2024": 0.60}'),

('feedstock_cost_is_everything', 'feedstock_sensitivity_rule', 'expert_rule',
 'Feedstock cost dominates all other economic variables in HEFA economics. At 70-80% of cash cost, a $0.05/lb feedstock change = $0.35-0.40/gal margin change. For comparison, variable OPEX is only $0.30-0.40/gal total. Every cent/lb saved on feedstock = ~$0.08/gal margin. HOBO Midwest location advantage of even $0.05/lb vs coastal = ~$0.35-0.40/gal structural margin edge. This is why feedstock strategy (Section 5) is THE most critical section of the business plan.',
 0.95, '{"source": "hobo_section7", "critical": true, "conversion": "1_cent_per_lb = 0.08_per_gal"}'),

('hobo_vs_dgd_strategy', 'diamond_green_diesel', 'competitive_analysis',
 'DGD advantages: massive scale (1B+ gal), integrated Darling feedstock supply (global collection), Valero refining/trading expertise. HOBO counters: Midwest location feedstock cost edge (sitting at the source vs coastal competitors), SAF flexibility (DGD primarily RD), lower transport costs for domestic feedstock, potentially lower CI via built-for-purpose design. HOBO cannot replicate DGD scale but can differentiate on cost position and flexibility.',
 0.85, '{"source": "hobo_section7", "strategy": "niche_vs_scale"}'),

('execution_readiness', 'hobo_execution_timeline', 'project_assessment',
 'HOBO claims strong execution readiness: all permits secured (unverified by Fastmarkets), LSTK EPC contract in place (shovel ready), feedstock MOUs for 200% of requirements (also unverified). FID Q3 2025, construction 40 months, commissioning end 2028. Timeline aggressive but feasible if financing closes. Key risk: financing without oil major backing requires strong feedstock/offtake contracts to satisfy lenders.',
 0.80, '{"source": "hobo_section7", "caveats": "permits_and_mous_unverified"}'),

('ci_as_investment_thesis', 'ci_reduction_roi', 'investment_framework',
 'CI reduction is the most powerful margin lever after feedstock cost. Each 5 CI points = ~$0.15/gal = ~$20-30M/year for a plant HOBO size. Moving from CI 35 to CI 20 = ~$0.45/gal = ~$60-70M/year. This means investments in CI reduction (renewable process energy, green H2, carbon capture, feedstock optimization) have immediate payback via credits. This makes the CI strategy effectively self-funding.',
 0.90, '{"source": "hobo_section7", "type": "investment_case"}'),

('feedstock_supply_curve', 'midwest_feedstock_cost_curve', 'market_structure',
 'HOBO feedstock supply curve has distinct tiers: Tier 1 (cheapest) = local ethanol plant DCO and renderer animal fats, minimal transport/middleman. Tier 2 = regional waste oils (UCO, CWG) at moderate cost. Tier 3 = more distant sources or premium feedstocks. Tier 4 = fallback soybean oil at ~$0.50/lb (higher cost, higher CI = fewer credits). Strategy: maximize Tier 1-2 to minimize cost and maximize CI performance, only use Tier 3-4 when needed. Risk: 1B+ lbs annual requirement may exhaust local cheap supply.',
 0.85, '{"source": "hobo_section7", "annual_requirement_blbs": "1.0-1.1"}')
ON CONFLICT (context_key) DO UPDATE SET content = EXCLUDED.content, confidence = EXCLUDED.confidence;
