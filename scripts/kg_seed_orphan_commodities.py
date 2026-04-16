"""
Seed contexts on orphan commodity KG nodes using analytical content from
project memory files. Targets: fats/greases commodities that are the user's
current active work.

Each node gets:
  - structural_definition : what it is, how it differs from related items
  - market_structure       : who produces/consumes, price sources
  - analytical_rules       : how to reason about it for forecasts
"""

import json
import os
from pathlib import Path
import psycopg2
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / '.env')


def connect():
    return psycopg2.connect(
        host=os.environ['RLC_PG_HOST'],
        port=os.environ.get('RLC_PG_PORT', 5432),
        database=os.environ.get('RLC_PG_DATABASE', 'rlc_commodities'),
        user=os.environ['RLC_PG_USER'],
        password=os.environ['RLC_PG_PASSWORD'],
        sslmode='require',
    )


def insert_context(cur, node_id, ctx_type, ctx_key, body, source='extracted'):
    cur.execute("""
        INSERT INTO core.kg_context
            (node_id, context_type, context_key, context_value, applicable_when, source, source_count)
        VALUES (%s, %s, %s, %s, 'always', %s, 1)
        ON CONFLICT DO NOTHING
        RETURNING id
    """, (node_id, ctx_type, ctx_key, json.dumps(body, default=str), source))
    r = cur.fetchone()
    if r:
        return r[0]
    cur.execute("""SELECT id FROM core.kg_context WHERE node_id=%s AND context_key=%s""",
                (node_id, ctx_key))
    return cur.fetchone()[0]


def seed_bft(cur, node_id):
    ctx = []
    ctx.append(insert_context(cur, node_id, 'structural_definition', 'bft_grade_distinction', {
        'content': (
            'Bleachable Fancy Tallow (BFT) is a commercial grade of tallow, not a single '
            'product. The key split is edible vs inedible: EBFT (edible/packer grade, food-use '
            'certified) vs IBFT (inedible/renderer grade, industrial/biofuel use). Both are '
            'physically "bleachable fancy tallow"; the distinction is end-use certification. '
            '"Packer" vs "Renderer" labels refer to SOURCE, not grade — both can produce either.'
        ),
        'codes': {'EBFT': 'Edible BFT', 'IBFT': 'Inedible BFT', 'BFT': 'Legacy total'},
        'packer_renderer_spread': "~= renderer's processing/logistics margin",
        'confidence': 0.95,
    }))
    ctx.append(insert_context(cur, node_id, 'market_structure', 'bft_price_sources', {
        'content': (
            'Price references available: Public — USDA AMS NW_LS442 Tallow & Protein Report '
            '(weekly, packer_bleachable_tallow + renderer_bleachable_tallow + edible_tallow); '
            'USDA ERS OilCrops Table 36 (edible tallow wholesale Chicago monthly). '
            'Proprietary — Fastmarkets series AG-TLW-0001 through 0035; Dropbox US Tallow Prices.xlsx.'
        ),
        'public_series': ['AMS NW_LS442 weekly', 'ERS OilCrops Table 36 monthly'],
        'proprietary_series': ['Fastmarkets AG-TLW-0001..0035', 'BFT Packer Chicago/West Coast daily'],
        'no_public_inedible': 'No USDA spot price specifically for IBFT; renderer_bleachable is closest proxy',
        'confidence': 0.9,
    }))
    ctx.append(insert_context(cur, node_id, 'expert_rule', 'bft_eia_guardrail', {
        'content': (
            'EIA Form 819 monthly total tallow consumption is the BINDING constraint for any '
            'EBFT + IBFT modeled split: EBFT + IBFT must reconcile to EIA total. If model '
            'implies more tallow used than EIA reports, reconcile to EIA numbers. '
            'CI score differential between EBFT and IBFT drives the allocation split; '
            'calibrate CI differential against historical realized splits.'
        ),
        'confidence': 0.9,
    }))
    return ctx


def seed_dco(cur, node_id):
    ctx = []
    ctx.append(insert_context(cur, node_id, 'structural_definition', 'dco_hs_code_problem', {
        'content': (
            'Distillers Corn Oil (DCO) shares HS code 1515.21 with all other crude corn oil '
            '(including wet-mill food-grade). There is NO separate code. DCO-only trade '
            'flows cannot be identified from HS code alone — must split by trading partner.'
        ),
        'hs_code': '1515.21',
        'confidence': 0.95,
    }))
    ctx.append(insert_context(cur, node_id, 'expert_rule', 'dco_country_split_rule', {
        'content': (
            'DCO vs food-grade corn oil split rule based on trade partners. '
            'DCO imports: essentially only from Canada (corn ethanol + proximity + rail). '
            'DCO exports: biofuel-incentive countries — Netherlands, Spain, Germany, Belgium, UK '
            '(EU biodiesel), Singapore (Asian biofuel hub), South Korea (RFS-equivalent), '
            'Canada (Clean Fuel Regs), Scandinavia. Food-grade corn oil exports: Mexico '
            '(dominant), Central America, Caribbean, parts of Asia (price-sensitive food buyers).'
        ),
        'import_countries_dco': ['CA'],
        'export_countries_dco': ['NL', 'ES', 'DE', 'BE', 'GB', 'SG', 'KR', 'CA', 'SE', 'DK', 'FI', 'NO'],
        'export_countries_food': ['MX', 'CARIBBEAN', 'C_AMERICA'],
        'confidence': 0.9,
    }))
    ctx.append(insert_context(cur, node_id, 'market_structure', 'dco_biofuel_value', {
        'content': (
            'DCO is a biomass-based diesel feedstock with low CI score and high LCFS/45Z value. '
            'Price is anchored to biofuel credit stack, not food demand. When LCFS or 45Z '
            'credits shift, DCO price re-rates. Watch for (a) new RD plants coming online '
            'in unexpected geographies (e.g., Asia) shifting export destinations, and (b) '
            'ILUC policy changes that would affect DCO CI advantage vs virgin vegetable oils.'
        ),
        'ci_advantage_vs_soy_oil_gco2mj': '~20-30 lower',
        'confidence': 0.85,
    }))
    return ctx


def seed_uco(cur, node_id):
    ctx = []
    ctx.append(insert_context(cur, node_id, 'structural_definition', 'uco_yg_relationship', {
        'content': (
            'Used Cooking Oil (UCO) is a subset of yellow grease (YG). Balance sheet structure: '
            'Combined = NASS YG total = UCO + (YG ex-UCO, i.e., non-restaurant YG). '
            'UCO gets its own balance sheet because it has distinct CI scores, prices, '
            'trade flows (especially massive Chinese imports), and biofuel allocation behavior.'
        ),
        'confidence': 0.95,
    }))
    ctx.append(insert_context(cur, node_id, 'data_quality_issue', 'nass_yg_suppression_dec_2023', {
        'content': (
            'NASS stopped publishing yellow grease production starting December 2023 due to '
            'confidentiality (D) — too few reporters (Darling Ingredients dominance). Pre-suppression '
            'YG was running ~170-200M lbs/month. One published value: Jan 2026 = 130M lbs. '
            'Parser bug: NASS backfill parser fell through (D) to next numeric row, producing '
            'identical YG and "Other Grease" numbers post-Nov 2023 — fix by returning NULL for (D).'
        ),
        'suppression_start': '2023-12',
        'pre_suppression_monthly_avg_mlbs': 172,
        'confidence': 1.0,
    }))
    ctx.append(insert_context(cur, node_id, 'expert_rule', 'uco_collection_model', {
        'content': (
            'UCO production estimated by: Restaurant count (Census CBP NAICS 722) × per-site '
            'generation rate × collection rate, distributed monthly by ERS FAFH spending '
            'seasonal index. Calibrated rates: Full-service 25 gal/mo, QSR 55 gal/mo, Cafeteria '
            '20, Catering 8, Bars 3. Weighted avg ~32 gal/mo, collection rate 70%, 7.5 lbs/gal. '
            'Produces ~1B lbs/yr UCO (43-54% of NASS YG total). UCO share of YG has been '
            'growing: 43% in 2016 to 54% in 2023.'
        ),
        'generation_rates_gal_per_month': {'full_service': 25, 'qsr': 55, 'cafeteria': 20, 'catering': 8, 'bars': 3},
        'collection_rate': 0.70, 'density_lbs_per_gal': 7.5,
        'annual_production_blbs': 1.0,
        'model_location': 'src/models/uco_collection_model.py',
        'confidence': 0.85,
    }))
    return ctx


def seed_animal_fats(cur, node_id):
    ctx = []
    ctx.append(insert_context(cur, node_id, 'structural_definition', 'animal_fats_taxonomy', {
        'content': (
            'Animal fats is an umbrella category including: tallow (beef/mutton, split into '
            'EBFT/IBFT grades), lard (pork), choice white grease (CWG, pork trim/hog), '
            'poultry fat, yellow grease (includes UCO). Each has distinct: sources, price '
            'levels, CI scores, and biofuel demand pull. Total US production ~10B lbs/yr '
            'across all grades.'
        ),
        'sub_commodities': ['tallow_EBFT', 'tallow_IBFT', 'lard', 'cwg', 'poultry_fat', 'yellow_grease', 'uco'],
        'confidence': 0.9,
    }))
    ctx.append(insert_context(cur, node_id, 'market_structure', 'animal_fats_biofuel_pull', {
        'content': (
            'Low CI scores (20-40 gCO2/MJ) make animal fats premium feedstocks for RD/SAF. '
            'Demand pull from LCFS + 45Z + D4 RIN stack has bid prices up from 25 cts/lb to '
            '60 cts/lb range. Competition with virgin vegetable oils for the same RD capacity '
            'creates a soy oil / animal fat spread that governs crush economics for soy.'
        ),
        'ci_range_gco2mj': [20, 40],
        'historical_price_range_cts_per_lb': [25, 60],
        'confidence': 0.85,
    }))
    return ctx


def seed_canola_oil_feedstock(cur, node_id):
    ctx = []
    ctx.append(insert_context(cur, node_id, 'structural_definition', 'canola_oil_feedstock_vs_food', {
        'content': (
            'Canola oil used as BBD feedstock is compositionally the same as food-grade but '
            'typically undergoes less refining and has a slightly lower CI score than soy oil '
            '(~40 vs ~50 gCO2/MJ). Canada is the dominant producer (~20 MMT canola crushed/yr). '
            'In the BBD context, canola competes with soy oil on a CI-adjusted landed cost basis. '
            'Under LCFS / 45Z, canola often has ~$0.10-0.15/gal advantage over soy oil.'
        ),
        'ci_advantage_vs_soy_oil_gco2mj': 10,
        'price_advantage_usd_per_gal_rd': [0.10, 0.15],
        'confidence': 0.85,
    }))
    ctx.append(insert_context(cur, node_id, 'market_structure', 'canola_canada_supply', {
        'content': (
            'Canada is the #1 canola producer (~20 MMT crushed/yr). Canola oil exports to US '
            'for BBD use have grown rapidly with RD capacity buildout. Key pricing reference: '
            'ICE Canola futures (Winnipeg) plus crush margin + transport. $1.00/lb canola oil '
            'Los Angeles is the regional price floor historically (kg_node: canola_oil_la_1dollar).'
        ),
        'confidence': 0.85,
    }))
    return ctx


def main():
    conn = connect()
    cur = conn.cursor()

    seeders = {
        'bleachable_fancy_tallow': seed_bft,
        'dcor_feedstock':          seed_dco,
        'used_cooking_oil':        seed_uco,
        'uco':                     seed_uco,         # alias
        'animal_fats':             seed_animal_fats,
        'canola_oil_feedstock':    seed_canola_oil_feedstock,
    }

    cur.execute("""
        SELECT id, node_key FROM core.kg_node WHERE node_key = ANY(%s)
    """, (list(seeders.keys()),))
    nodes = {row[1]: row[0] for row in cur.fetchall()}
    print(f"Matched {len(nodes)} nodes: {list(nodes.keys())}")

    total_ctx = 0
    seen_ids = set()
    for key, seeder in seeders.items():
        if key not in nodes:
            print(f"  SKIP {key}: node not found")
            continue
        if nodes[key] in seen_ids:
            print(f"  SKIP {key}: aliased to already-seeded node id={nodes[key]}")
            continue
        seen_ids.add(nodes[key])
        print(f"\nSeeding {key} (id={nodes[key]})...")
        try:
            ctx_ids = seeder(cur, nodes[key])
            conn.commit()
            print(f"  Inserted {len(ctx_ids)} contexts: {ctx_ids}")
            total_ctx += len(ctx_ids)
        except Exception as e:
            conn.rollback()
            print(f"  ! {key} seeder failed, rolled back: {e}")

    print(f"\nTotal contexts inserted: {total_ctx}")

    # Final audit
    cur.execute("""
        SELECT n.node_key, COUNT(c.id) AS ctx_count
        FROM core.kg_node n
        LEFT JOIN core.kg_context c ON c.node_id = n.id
        WHERE n.node_key = ANY(%s)
        GROUP BY n.node_key
        ORDER BY n.node_key
    """, (list(seeders.keys()),))
    print("\nAudit after seeding:")
    for row in cur.fetchall():
        print(f"  {row[0]}: {row[1]} contexts")

    conn.close()


if __name__ == '__main__':
    main()
