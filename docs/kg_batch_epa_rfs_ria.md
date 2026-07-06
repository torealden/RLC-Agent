# KG Batch — EPA RFS Regulatory Impact Analyses (2023-25 + 2026-27)

**Staged for KG ingestion** (core.kg_node / kg_context / kg_edge / kg_source). Extracted 2026-07-06 by
deep-dive subagent from EPA-420-R-23-015 (Set 1, 2023-25) and EPA-420-R-26-011 (Set 2, 2026-27).
Full extracted text: scratchpad `420r26011.txt`, `EPA2023_2025.txt`. **Honest caveat for any client use:**
RIA "Analyzed Volumes" are EPA's *modeled mix*, NOT the enforceable standards; CI figures are
literature-review *ranges*, not certified point CIs. Label accordingly.

## kg_source
- EPA-420-R-26-011 "RFS Standards for 2026 & 2027" Final Rule RIA (Set 2), docket EPA-HQ-OAR-2024-0505, ~Jun 2026, 438pp.
- EPA-420-R-23-015 "RFS Standards for 2023-2025" Final Rule RIA (Set 1), docket EPA-HQ-OAR-2021-0427, Jun 2023, 477pp.

## NEW NODES
**`rfs_rvo_2026_2027`** — EPA final standards + analyzed mix 2026-27. Total Renewable 25,820/25,980 M RIN; BBD(D4) 9,961/10,118 M RIN; BBD physical 6,074/6,445 M gal (RD 4,290/4,660, biodiesel flat 1,780). Binding constraint = **domestic BBD production capacity** (6,705/7,205 M gal @ ~90% util), NOT feedstock. 45Z/OBBB North-America feedstock screen baked in. Net societal cost ~$19.9B/yr. Claim: up to 20% soy oil diverted from food to fuel. → rfs2, rvo, cfpc_45z, renewable_diesel

**`rfs_rvo_2023_2025`** — Set 1. Total 20.94/21.54/22.33 bg; BBD(D4) 5,965/6,205/6,881 M RIN; BBD physical 3,710/3,846/4,239 M gal (RD rising, biodiesel declining). Feedstock called "central and critical" limiting factor (reversed by 2026-27). Canola RD pathway finalized Dec 2022. → rfs2, canola_oil

**`epa_fog_category`** — EPA "FOG" = UCO + animal fats + biogenic lipids, lumped. EMTS can't differentiate; EPA uses EIA feedstocks update → **~35% UCO / 65% tallow (2025)**. HS: UCO=1518, tallow=1502.10. 8 lb FOG/gal BBD. Incremental FOG growth is import-driven post-2014. RD-from-FOG 2026: UCO 458 + tallow 851 M gal. **[Directly relevant: EPA's 65% tallow FOG split traces to EIA — the over-count RLC corrects from slaughter.]** → used_cooking_oil, epa_feedstock_ci_values

**`epa_feedstock_demand_projection`** — projected annual feedstock increase to 2027 (M gal BBD-eq): dom FOG +50, dom soy +140, imp UCO +40, imp tallow +20, imp canola +120 → total +625/yr. Historical BBD-from-feedstock (M gal): FOG 869→1,395→1,970→1,890 (22-25); soy 1,159→1,418→1,680→1,370; DCO 325→332→520→560; canola 174→344→542→600. Soy growth adopted ~250 M gal/yr. → bbd_balance_sheet_model, soybean_oil

**`epa_import_dynamics`** — FOG imports (MMT): **UCO 0.40→1.41→2.45→2.11**; animal fats 0.55→0.79→0.88→1.11 (2022-25). **[UCO 2024 2.45MMT=5.40B lb ≈ RLC 5.43B; tallow 0.88MMT=1.94B ≈ RLC 1.95B — exact corroboration.]** 2025 decline = US tariffs on Asian UCO + China rebate removal. Mexico UCO newly advantaged (45Z NA-eligible). Canada canola crush → 15 MMT by end-2026. → canola_oil, used_cooking_oil, canada_cfr

**`epa_feedstock_ci_values`** — literature ranges (gCO2e/MJ): diesel 84-94; UCO BD 12-32/RD 12-37; tallow BD 16-58/RD 14-80; DCO BD 14-37/RD 12-46; soy RD 26-128 (LUC-dominated). Waste feedstocks far lower CI (no LUC, byproduct allocation). → cfpc_45z, lcfs_credit_framework

**`epa_bbd_economics`** (or merge → bbd_margin_model) — veg-oil prices 2026 ($/lb): soy 0.66, corn oil 0.55, FOG 0.50 (corn oil≈82.7% soy, FOG≈75.4% soy). RD prod cost 2026 ($/gal): soy 5.95, corn oil 5.07, FOG 4.70. Feedstock >80% of BBD cost. RD yield >100%; ~93.5 gal RD/100 gal oil. Canola costed as soy. → bbd_margin_model, diamond_green_diesel

**`epa_soy_crush_expansion`** (or merge → soybean_oil) — crush +39% (1,734→2,410 M bu, 13/14→24/25). Capacity +360 M gal-eq (2026, broken ground) up to +757 by 2027. Soy oil $0.31→$0.61/lb; ~2yr build lag. → crusher_feasibility_model, nopa.crush

## EXTEND EXISTING NODES
**`cfpc_45z`** (+OBBB): 45Z replaced $1/gal blenders + SAF credit (2025 tax yr); <50 kgCO2e/MMBtu. OBBB (PL 119-21, Jul-2025): extended to 2029; **North-America feedstocks only after 2025**; **LUC removed from emission calc** → neutralizes FOG's CI advantage over crop oils → EPA re-forecasts lower imported FOG, higher domestic soy + Canadian canola. Producer (not blender) credit, US-only.

**`rfs2`** (+): nesting — excess D4 fills advanced then conventional (analyzed BBD > applicable volume). D4 RIN ~$2 (2021)→$0.75 (summer 2023); priced on biodiesel-diesel spread + tax-credit presence; tracks soy oil. eRIN pathway removed in 2026-27 rule.

## CROSS-CUTTING CAUSAL CLAIMS (candidate edges)
1. cfpc_45z →(LUC removal neutralizes FOG CI edge)→ soybean_oil, canola_oil.
2. NA feedstock screen → import collapse offset by export decrease → net BBD imports ~flat 2027.
3. **Capacity (not feedstock) = 2026-27 binding constraint** (reversal from 2023-25).
4. Soy price → crush investment (2yr lag) → oil availability.
5. State LCFS + low CI pulled FOG imports; 45Z + tariffs now push back (22-24 boom, 25 bust both policy-driven).
6. 20% food-soy-oil diversion = EPA's assumed pressure valve, backfilled by LatAm soy.
