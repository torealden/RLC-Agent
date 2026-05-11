-- ============================================================================
-- RLC Knowledge Graph: Extraction Batch 010
-- Source: Oilseed Crushing Plant Financial Model Template (eFinancialModels v1.10)
--         Generic 30-year DCF template for oilseed crushing plant feasibility
-- Focus: capex/opex structure, oilseed→meal+oil yield ratios, capacity
--        modeling (lines × utilization × operating days), funding stack
--        (debt/equity), returns benchmarks (IRR/payback), break-even price
-- Importance: structural template for the IFV cost defaults, the Iowa
--        crushing facility workbook benchmarks, and the future BBD facility-
--        agent simulation. Numbers are illustrative — what's permanent here
--        is the MODEL STRUCTURE.
-- Extracted: 2026-04-26
-- ============================================================================

-- BATCH REGISTRATION
INSERT INTO core.kg_source (source_key, source_type, title, location_uri, document_date, document_type, commodities, topics, status, first_processed, last_processed, nodes_extracted, edges_extracted, contexts_extracted) VALUES
('pdf_oilseed_crushing_plant_v1_1', 'pdf',
 'Oilseed Crushing Plant Financial Model v1.10',
 'C:/Users/torem/RLC Dropbox/Tore Alden/pdf files for kg/Oilseed Crushing Plant v1.1.pdf',
 '2024-04-12', 'financial_model_template',
 '{soybeans,sunflower,corn,rapeseed,copra,soybean_oil,canola_oil,sunflower_oil,soybean_meal,canola_meal}',
 '{capex,opex,yield_matrix,capacity_modeling,returns_template,break_even_analysis,funding_stack,facility_economics,crusher_feasibility}',
 'completed', NOW(), NOW(), 7, 7, 9)
ON CONFLICT (source_key) DO UPDATE SET status = 'completed', last_processed = NOW();

-- ============================================================================
-- NODES
-- ============================================================================
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES

('model', 'oilseed_crushing_plant_model', 'Oilseed Crushing Plant Financial Model (Template)', '{
  "context": "Generic 30-year DCF model template covering capex, opex, oilseed/meal/oil flows, debt+equity funding, returns, break-even, and sensitivity. Multi-oilseed (soybean/sunflower/corn/rapeseed/copra) blend with per-line capacity (4 crushing lines = 150 ton/day). Provides a structural baseline for any new crushing plant feasibility OR for back-validating an existing plant''s economics.",
  "model_horizon_years": 30,
  "operating_days_per_year": 250,
  "daily_capacity_factor_pct": 68.5,
  "outputs": ["meal", "oil"],
  "currency": "USD",
  "use_cases": ["greenfield_feasibility", "iowa_facility_benchmark", "ifv_cost_defaults", "facility_agent_baseline"]
}'),

('metric', 'crushing_plant_capacity_model', 'Crushing Plant Capacity Modeling', '{
  "context": "Plant capacity = SUM(line ton/day) × operating_days × daily_capacity_factor. Template uses 4 lines (20 + 30 + 50 + 50 = 150 ton/day) × 250 op days × 68.5% = ~25,700 ton/yr at modeled utilization (37,500 ton/yr nameplate). Capacity utilization ramps 85% Y2 → 100% Y10.",
  "lines_count": 4,
  "line_capacity_ton_per_day": [20, 30, 50, 50],
  "total_line_capacity_ton_per_day": 150,
  "operating_days_per_year": 250,
  "daily_capacity_factor_pct": 68.5,
  "nameplate_ton_per_year": 37500,
  "utilization_ramp": "85.3% Y2 → 100% Y10",
  "increase_in_production_pct_per_year": 2.0
}'),

('metric', 'crushing_plant_yield_matrix', 'Oilseed→Meal+Oil Yield Matrix (per ton oilseed)', '{
  "context": "Per-ton oilseed yield ratios used in the template. Critical for production estimation: oil_production = oilseed_throughput × oil_yield_per_ton. Template assumes uniform 7.5 lb/gal oil density. NOTE: yields here are the model''s template values — actual plant yields vary ±3-5%.",
  "oil_density_lb_per_gal": 7.5,
  "yields": {
    "soybean":   {"meal_ton_per_ton": 0.58, "oil_gal_per_ton": 34.59, "oil_lb_per_ton": 259.4},
    "sunflower": {"meal_ton_per_ton": 0.45, "oil_gal_per_ton": 45.70, "oil_lb_per_ton": 342.8},
    "corn":      {"meal_ton_per_ton": 0.45, "oil_gal_per_ton": 86.00, "oil_lb_per_ton": 645.0},
    "rapeseed":  {"meal_ton_per_ton": 0.45, "oil_gal_per_ton": 116.00, "oil_lb_per_ton": 870.0},
    "copra":     {"meal_ton_per_ton": 0.31, "oil_gal_per_ton": 71.94, "oil_lb_per_ton": 539.6}
  }
}'),

('metric', 'crushing_plant_capex_structure', 'Crushing Plant CAPEX Structure (Template)', '{
  "context": "CAPEX breakdown for a 150 ton/day plant. Total $10.25M split across four asset classes with distinct depreciation lives. Maintenance CAPEX runs 0.5%/yr of gross fixed assets. Spending phased Y0/Y1/Y2 by class.",
  "total_capex_usd": 10250000,
  "maintenance_pct_of_gross_per_year": 0.5,
  "components": {
    "crushing_lines":  {"usd": 6000000, "pct_total": 58.5, "depreciation_years": 30, "y0_pct": 40, "y1_pct": 60, "y2_pct": 0},
    "building":        {"usd": 1500000, "pct_total": 14.6, "depreciation_years": 30, "y0_pct": 80, "y1_pct": 20, "y2_pct": 0},
    "infrastructure":  {"usd": 1750000, "pct_total": 17.1, "depreciation_years": 10, "y0_pct": 30, "y1_pct": 70, "y2_pct": 0},
    "trucks":          {"usd": 1000000, "pct_total":  9.8, "depreciation_years": 15, "y0_pct":  0, "y1_pct": 80, "y2_pct": 20}
  },
  "capex_per_ton_day_usd": 68333,
  "implied_capex_per_ton_year_usd": 273
}'),

('metric', 'crushing_plant_opex_structure', 'Crushing Plant OPEX Structure (Template)', '{
  "context": "OPEX template benchmarks. Other Direct Costs $200/ton oilseed throughput (covers utilities, chemicals, labor allocations). Working capital cycle: 30 days receivables, 45 days inventory, 15 days payables. Net working capital ≈ 4-5% of CAPEX. Cost inflation built in at modest annual rate.",
  "other_direct_costs_usd_per_ton_oilseed": 200,
  "days_receivables": 30,
  "days_inventory": 45,
  "days_payables": 15,
  "net_working_capital_as_pct_of_capex": 4.6,
  "annual_cost_inflation_pct": 1.5,
  "headcount_employees": 5
}'),

('metric', 'crushing_plant_funding_structure', 'Crushing Plant Funding Stack (Template)', '{
  "context": "Capital structure for a $15.7M project. Total = CAPEX + OPEX bridge + working capital + cash reserve. Sources are dominated by senior debt (Loan A at 1.5%) plus subordinated debt (Loan B at 0%) and equity (~40%). Founders 0%, outside investors 20% stake. Dividend distribution starts Y10 at 30% payout once minimum cash position met.",
  "uses_of_funds": {
    "capex_usd":               10250000,
    "opex_bridge_usd":          4452173,
    "net_working_capital_usd":   722827,
    "cash_reserve_usd":          300000,
    "total_uses_usd":          15725000
  },
  "sources_of_funds": {
    "loan_a_usd":  {"amount": 7500000, "rate_pct": 1.5, "share_pct": 47.7},
    "loan_b_usd":  {"amount": 2000000, "rate_pct": 0.0, "share_pct": 12.7},
    "equity_usd":  {"amount": 6225000, "share_pct": 39.6}
  },
  "debt_to_equity_ratio": 1.53,
  "dividend_start_year": 10,
  "dividend_payout_ratio_pct": 30,
  "minimum_cash_position_usd": 7500000
}'),

('metric', 'crushing_plant_returns_template', 'Crushing Plant Returns Benchmark (Template)', '{
  "context": "Returns benchmark from 30-year template DCF. EBITDA margin grows from 7.4% (Y2) to 14.3% (lifetime) as capacity utilization ramps and depreciation rolls off. ROCE 16.5% Y2 → 33%+ by Y10. Use these as a SANITY CHECK for any new crushing plant proposal — actual returns can vary materially with feedstock pricing and product mix.",
  "discount_rate_pct": 12.0,
  "unlevered_irr_pct": 24.0,
  "levered_irr_pct": 45.0,
  "payback_years_unlevered": 4.0,
  "payback_years_levered": 3.0,
  "ebitda_margin_year_2_pct": 7.4,
  "ebitda_margin_lifetime_pct": 14.3,
  "roce_year_2_pct": 16.5,
  "roce_year_10_pct": 33.0,
  "lifetime_revenue_usd": 2453799497,
  "lifetime_ebitda_usd": 351786369,
  "exit_ev_ebitda_x": 6.0
}')
ON CONFLICT (node_key) DO UPDATE SET
    properties = EXCLUDED.properties,
    label = EXCLUDED.label;

-- ============================================================================
-- CONTEXTS — concrete analytical rules + benchmarks
-- ============================================================================
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, source) VALUES

((SELECT id FROM core.kg_node WHERE node_key = 'oilseed_crushing_plant_model'),
 'quantitative_framework', 'price_assumptions',
 '{"content": "Template price assumptions (per ton oilseed): Soybean $1,500 · Sunflower $1,450 · Corn $1,300 · Rapeseed $1,250 · Copra $1,000. Blended-mix average $1,300/ton. Output prices: Meal $2,500/ton, Oil $7.50/gal (× 7.5 lb/gal density = $1,000/ton oil). These are templates — replace with current cash market prices when validating an actual plant.",
   "blended_oilseed_price_usd_per_ton": 1300,
   "meal_price_usd_per_ton": 2500,
   "oil_price_usd_per_gal": 7.50,
   "oil_price_usd_per_ton": 1000,
   "confidence": 0.85,
   "source_doc": "oilseed_crushing_plant_v1_1_p6"
 }', 'pdf_oilseed_crushing_plant_v1_1'),

((SELECT id FROM core.kg_node WHERE node_key = 'oilseed_crushing_plant_model'),
 'expert_rule', 'multi_oilseed_blend',
 '{"content": "Template assumes a multi-oilseed blend rather than single-feedstock processing: Soybean 30% · Sunflower 20% · Corn 20% · Rapeseed 20% · Copra 10%. This implies a flexible/swing crushing operation, common in mid-size facilities serving multiple end-product markets. Single-feedstock plants (e.g., dedicated soy crushers) would replace blend with 100% allocation and use that crop''s yield row from the yield matrix.",
   "blend_pct": {"soybean": 30, "sunflower": 20, "corn": 20, "rapeseed": 20, "copra": 10},
   "implication": "flexible_crusher_design",
   "confidence": 0.9,
   "source_doc": "oilseed_crushing_plant_v1_1_p6"
 }', 'pdf_oilseed_crushing_plant_v1_1'),

((SELECT id FROM core.kg_node WHERE node_key = 'crushing_plant_capacity_model'),
 'expert_rule', 'capacity_throughput_formula',
 '{"content": "Annual oilseed throughput = total_line_capacity (ton/day) × operating_days (typ. 250) × daily_capacity_factor (typ. 68.5%) × utilization_ramp (85%→100% over years 2-10). Use this formula in reverse to estimate the number of crushing days required to process a given oilseed volume, OR to back into an implied utilization from reported throughput.",
   "formula": "throughput_ton_year = sum(line_ton_day) * op_days * daily_factor * utilization",
   "typical_op_days_per_year": 250,
   "typical_daily_factor_pct": 68.5,
   "max_realistic_utilization_pct": 100,
   "ramp_period_years": 8,
   "confidence": 0.95,
   "source_doc": "oilseed_crushing_plant_v1_1_p6"
 }', 'pdf_oilseed_crushing_plant_v1_1'),

((SELECT id FROM core.kg_node WHERE node_key = 'crushing_plant_yield_matrix'),
 'expert_rule', 'yield_anchor_for_validation',
 '{"content": "Yield matrix benchmarks per ton oilseed (template values): Soybean 11 lb oil + 0.58 ton meal · Canola/Rapeseed 17.4 lb oil + 0.45 ton meal · Sunflower 12.9 lb oil + 0.45 ton meal. Use these to VALIDATE NASS-reported crush. Example: NASS soy crush 214 mil bu × 60 lb/bu × 0.193 (template oil yield 11.6 lb/bu / 60 lb/bu) = 2,481 mil lbs oil — should match NASS soybean crude_oil_production within 1-2%. Mismatch >5% suggests data quality issue or unusual processing yield.",
   "validation_use_case": "cross_check_nass_crush_against_oil_production",
   "tolerance_pct": "1-2",
   "flag_threshold_pct": 5,
   "confidence": 0.9,
   "source_doc": "oilseed_crushing_plant_v1_1_p6"
 }', 'pdf_oilseed_crushing_plant_v1_1'),

((SELECT id FROM core.kg_node WHERE node_key = 'crushing_plant_capex_structure'),
 'expert_rule', 'capex_intensity_benchmark',
 '{"content": "Template implies CAPEX intensity of ~$68k per ton/day of nameplate capacity ($10.25M / 150 ton/day) for a multi-line oilseed crusher with associated building, infrastructure, and trucks. By comparison, modern dedicated soybean crushers run $80-150M for 100k+ ton/day plants → $800-1,500/ton-day intensity. The template''s $68k/ton-day reflects a smaller, less integrated facility. Use the ratio to scale-check any new plant proposal.",
   "template_capex_per_ton_day_usd": 68333,
   "modern_soy_crusher_capex_per_ton_day_usd_range": "800-1500",
   "scale_factor_caveat": "template is small-scale; large modern plants have higher per-ton capex but lower per-bushel-processed cost",
   "confidence": 0.85,
   "source_doc": "oilseed_crushing_plant_v1_1_p6_p9"
 }', 'pdf_oilseed_crushing_plant_v1_1'),

((SELECT id FROM core.kg_node WHERE node_key = 'crushing_plant_opex_structure'),
 'expert_rule', 'opex_per_ton_benchmark',
 '{"content": "Template ''Other Direct Costs'' of $200/ton oilseed covers utilities, chemicals, consumables, labor allocations (excluding the cost of the oilseed itself). For a soybean-only plant at 60 lb/bu, this is ~$6/bu of crushing margin (excluding oilseed cost). Modern Midwest soy crushers run $0.30-0.60/bu for processing — the template''s $6/bu is at the high end, which fits a small/multi-feedstock operation. Use $0.30-0.60/bu as the BENCHMARK for a competitive modern soy crusher; anything above $1.00/bu signals scale or efficiency issue.",
   "template_other_direct_usd_per_ton": 200,
   "implied_per_bushel_soy_usd": 6.00,
   "modern_soy_processing_usd_per_bu_range": "0.30-0.60",
   "competitive_threshold_usd_per_bu": 1.00,
   "confidence": 0.85,
   "source_doc": "oilseed_crushing_plant_v1_1_p6"
 }', 'pdf_oilseed_crushing_plant_v1_1'),

((SELECT id FROM core.kg_node WHERE node_key = 'crushing_plant_returns_template'),
 'expert_rule', 'irr_benchmark_for_proposals',
 '{"content": "Template returns: Unlevered IRR 24% / Levered IRR 45% over 30-year life at 12% discount rate. Payback ~4yr unlevered. EBITDA margin 7.4% Y2 ramping to 14.3% lifetime. Use as TRIAGE benchmark for new crushing plant proposals: <15% unlev IRR = uneconomic at template prices; 15-25% = realistic baseline; >25% = either superior siting/feedstock advantage OR optimistic price assumptions. Always check sensitivity to oilseed price ±10% (template shows 18.8-29.5% IRR range).",
   "triage_unlev_irr_pct": {"uneconomic_below": 15, "realistic_band": "15-25", "scrutinize_above": 25},
   "oilseed_price_sensitivity_pct_irr_per_10pct_price_change": "10-12",
   "capex_sensitivity_pct_irr_per_10pct_capex_change": "1-2",
   "opex_sensitivity_pct_irr_per_10pct_opex_change": "0.4-0.6",
   "confidence": 0.85,
   "source_doc": "oilseed_crushing_plant_v1_1_p5"
 }', 'pdf_oilseed_crushing_plant_v1_1'),

((SELECT id FROM core.kg_node WHERE node_key = 'crushing_plant_returns_template'),
 'scenario_analysis', 'break_even_analysis',
 '{"content": "Template Year-2 break-even analysis: at $1,724.65/ton oilseed price, plant breaks even at 6,923 tons throughput. At nameplate 26,667 tons throughput, break-even oilseed price is $1,612.56/ton. The gap between break-even price ($1,612) and template assumed price ($1,300) implies $312/ton of margin headroom — i.e., template assumes a comfortable margin even with unfavorable price moves. Use the same break-even logic for any plant: break_even_volume × oilseed_price = revenue ÷ marginal_yield ÷ output_price = volume threshold.",
   "year_2_breakeven_oilseed_price_usd_per_ton": 1612.56,
   "year_2_breakeven_volume_tons": 6923,
   "template_oilseed_price_usd_per_ton": 1300,
   "margin_headroom_usd_per_ton": 312,
   "confidence": 0.9,
   "source_doc": "oilseed_crushing_plant_v1_1_p5"
 }', 'pdf_oilseed_crushing_plant_v1_1'),

((SELECT id FROM core.kg_node WHERE node_key = 'crushing_plant_funding_structure'),
 'expert_rule', 'capital_stack_benchmark',
 '{"content": "Template capital stack: 60% debt + 40% equity, with debt split into senior secured (Loan A, 1.5% rate, 76% of debt) and subordinated/grant-like (Loan B, 0% rate, 24% of debt). Modern crushing plant projects (e.g., Shell Rock Soy Processing, Platinum Crush Alta — both 2024 IA plants) typically run 50-65% debt with senior rates of 5-7% in the current environment, plus equity from co-op members or PE. The template''s 1.5% rate is artificially low — replace with current market rates when validating real proposals.",
   "template_debt_share_pct": 60.4,
   "template_equity_share_pct": 39.6,
   "template_senior_debt_rate_pct": 1.5,
   "modern_market_senior_rate_pct_range": "5-7",
   "modern_debt_share_pct_range": "50-65",
   "confidence": 0.85,
   "source_doc": "oilseed_crushing_plant_v1_1_p6"
 }', 'pdf_oilseed_crushing_plant_v1_1');

-- ============================================================================
-- EDGES — connect to existing KG nodes
-- ============================================================================
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, properties) VALUES

-- The crushing-plant model SUPPORTS the existing crusher_feasibility_model (batch 008)
((SELECT id FROM core.kg_node WHERE node_key = 'oilseed_crushing_plant_model'),
 (SELECT id FROM core.kg_node WHERE node_key = 'crusher_feasibility_model'),
 'supports',
 '{"description": "Template provides the structural DCF baseline for any crusher_feasibility_model invocation — capex/opex/yield/funding defaults all start from this template and get overridden with plant-specific inputs.", "strength": "high"}'),

-- The yield matrix CALIBRATES the model
((SELECT id FROM core.kg_node WHERE node_key = 'crushing_plant_yield_matrix'),
 (SELECT id FROM core.kg_node WHERE node_key = 'oilseed_crushing_plant_model'),
 'calibrates',
 '{"description": "Per-ton oilseed → meal/oil yield ratios are the production-side calibration of the model. Changing yields directly drives revenue.", "calibration_lever": "yield_per_ton"}'),

-- CAPEX structure FEEDS the returns template
((SELECT id FROM core.kg_node WHERE node_key = 'crushing_plant_capex_structure'),
 (SELECT id FROM core.kg_node WHERE node_key = 'crushing_plant_returns_template'),
 'feeds',
 '{"description": "CAPEX intensity drives initial outflow + depreciation schedule, both of which materially affect IRR/NPV/payback.", "sensitivity": "1-2 pct IRR per 10pct capex change"}'),

-- The model FEEDS the IFV via cost defaults
((SELECT id FROM core.kg_node WHERE node_key = 'oilseed_crushing_plant_model'),
 (SELECT id FROM core.kg_node WHERE node_key = 'bbd_margin_model'),
 'feeds',
 '{"description": "Crushing-plant OPEX/CAPEX template provides default values for the upstream side of the BBD margin model when computing implied feedstock value (the IFV kg_callable). Specifically: per-bushel processing cost, effective oil yield, depreciation amortization.", "feeds_into": "ifv_cost_defaults"}'),

-- The model RELATES to HOBO (downstream consumer of crush output)
((SELECT id FROM core.kg_node WHERE node_key = 'oilseed_crushing_plant_model'),
 (SELECT id FROM core.kg_node WHERE node_key = 'hobo_renewables'),
 'relates_to',
 '{"description": "HOBO''s 250-mile feedstock catchment includes ~6 billion lbs of fats/oils, much of it sourced from oilseed crushing plants like the one this template models. Plant-level economics (this template) determine whether crushers will sell oil to biofuel buyers (HOBO) or food channels.", "relationship": "upstream_feedstock_supplier"}'),

-- The model SUPPORTS the future facility-agent simulation
((SELECT id FROM core.kg_node WHERE node_key = 'oilseed_crushing_plant_model'),
 (SELECT id FROM core.kg_node WHERE node_key = 'crusher_feasibility_model'),
 'extends',
 '{"description": "Template extends the consulting-style crusher_feasibility_model with a full 30-year DCF + multi-oilseed blend capability. Useful for stress-testing facility-agent decisions across the full plant lifecycle.", "extension_type": "lifecycle_dcf"}'),

-- Yield matrix RELATES to NASS crush data validation
((SELECT id FROM core.kg_node WHERE node_key = 'crushing_plant_yield_matrix'),
 (SELECT id FROM core.kg_node WHERE node_key = 'soybean_oil_feedstock'),
 'predicts',
 '{"description": "Yield matrix predicts oil production volume from reported NASS oilseed crush — should match within 1-2% for soy. Mismatch >5% flags a data quality issue.", "validation_metric": "soy_crush_mil_bu × 11.6 lb/bu = predicted_oil_mil_lbs"}');
