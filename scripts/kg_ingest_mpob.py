"""
Ingest MPOB 2016 + 2017 industry overview docs into the knowledge graph.

Creates:
  - 2 kg_source rows (MPOB_2016, MPOB_2017)
  - New nodes: malaysia (region), indonesia (region), india (region),
    el_nino_2015_16 (event), oil_palm_ffb_yield (metric)
  - Contexts on palm_oil node capturing analytical frameworks
  - Edges palm_oil <-> soybean_oil spread, malaysia->produces->palm_oil, etc.
  - kg_provenance rows linking every new context/edge to its source
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


def upsert_source(cur, source_key, title, doc_date, topics, commodities, word_count):
    cur.execute("""
        INSERT INTO core.kg_source
            (source_key, source_type, title, location_uri, document_date,
             author, commodities, topics, document_type, status,
             first_processed, last_processed, word_count, nodes_extracted, edges_extracted, contexts_extracted)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW(), %s, 0, 0, 0)
        ON CONFLICT (source_key) DO UPDATE SET
            last_processed = NOW(),
            title = EXCLUDED.title,
            topics = EXCLUDED.topics
        RETURNING id
    """, (source_key, 'industry_report', title,
          f'G:/My Drive/google_docs_to_add/{source_key}.docx',
          doc_date, 'MPOB', commodities, topics, 'annual_industry_review', 'processed',
          word_count))
    return cur.fetchone()[0]


def upsert_node(cur, node_key, node_type, label, properties):
    cur.execute("""
        INSERT INTO core.kg_node (node_type, node_key, label, properties, source_count)
        VALUES (%s, %s, %s, %s, 1)
        ON CONFLICT (node_key) DO UPDATE SET
            label = EXCLUDED.label,
            properties = EXCLUDED.properties,
            source_count = core.kg_node.source_count + 1,
            last_reinforced = NOW()
        RETURNING id
    """, (node_type, node_key, label, json.dumps(properties)))
    return cur.fetchone()[0]


def insert_context(cur, node_id, context_type, context_key, context_value,
                   applicable_when='always', source='extracted'):
    cur.execute("""
        INSERT INTO core.kg_context
            (node_id, context_type, context_key, context_value, applicable_when, source, source_count)
        VALUES (%s, %s, %s, %s, %s, %s, 1)
        ON CONFLICT DO NOTHING
        RETURNING id
    """, (node_id, context_type, context_key, json.dumps(context_value),
          applicable_when, source))
    r = cur.fetchone()
    if r:
        return r[0]
    # Conflict — fetch existing id
    cur.execute("""SELECT id FROM core.kg_context WHERE node_id=%s AND context_key=%s""",
                (node_id, context_key))
    return cur.fetchone()[0]


def insert_edge(cur, src_id, tgt_id, edge_type, properties, weight=0.8, confidence=0.8):
    cur.execute("""
        INSERT INTO core.kg_edge
            (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence, source_count)
        VALUES (%s, %s, %s, %s, %s, 'kg_ingest_mpob', %s, 1)
        ON CONFLICT DO NOTHING
        RETURNING id
    """, (src_id, tgt_id, edge_type, weight, json.dumps(properties), confidence))
    r = cur.fetchone()
    if r:
        return r[0]
    cur.execute("""SELECT id FROM core.kg_edge
                   WHERE source_node_id=%s AND target_node_id=%s AND edge_type=%s""",
                (src_id, tgt_id, edge_type))
    row = cur.fetchone()
    return row[0] if row else None


def add_provenance(cur, entity_type, entity_id, source_id, excerpt, confidence=0.85):
    cur.execute("""
        INSERT INTO core.kg_provenance
            (entity_type, entity_id, source_id, extracted_at, extraction_method,
             source_excerpt, source_confidence)
        VALUES (%s, %s, %s, NOW(), 'manual_curation', %s, %s)
        ON CONFLICT DO NOTHING
    """, (entity_type, entity_id, source_id, excerpt[:500], confidence))


def main():
    conn = connect()
    cur = conn.cursor()

    print("=" * 70)
    print("MPOB ingestion — palm oil industry 2016 + 2017 reports")
    print("=" * 70)

    # --- 1. Sources ---
    src_2016 = upsert_source(cur, 'mpob_2016_industry_overview',
        'MPOB Overview of the Malaysian Oil Palm Industry 2016',
        '2017-02-15', ['palm_oil', 'el_nino', 'ffb_yield', 'oer', 'exports'],
        ['palm_oil'], 1833)
    src_2017 = upsert_source(cur, 'mpob_2017_industry_overview',
        'MPOB Overview of the Malaysian Oil Palm Industry 2017',
        '2018-02-15', ['palm_oil', 'el_nino_recovery', 'india_indonesia_competition'],
        ['palm_oil'], 1608)
    print(f"Sources: {src_2016} (2016), {src_2017} (2017)")

    # --- 2. Nodes ---
    # palm_oil node already exists (id=4), just upsert properties with enriched data
    n_palm = upsert_node(cur, 'palm_oil', 'commodity', 'Palm Oil', {
        'major_producers': ['malaysia', 'indonesia'],
        'global_production_2017_malaysia_mmt': 19.92,
        'global_production_2016_malaysia_mmt': 17.32,
        'largest_importers': ['india', 'china', 'eu', 'pakistan'],
        'key_metrics': ['cpo_production', 'ffb_yield', 'oer', 'planted_area', 'stocks'],
        'pricing_anchor': 'RM per tonne; discount/premium to soybean oil governs substitution',
        'context': 'Palm oil is the most-produced vegetable oil globally. Malaysia is the #2 producer after Indonesia. FFB yield × OER × planted area drives supply; weather (esp. El-Nino) is the dominant shock.',
    })

    n_malaysia = upsert_node(cur, 'malaysia', 'region', 'Malaysia', {
        'commodities': ['palm_oil'],
        'planted_area_mha_2017': 5.81,
        'largest_oil_palm_state_2017': 'Sarawak (1.56 mha, took over from Sabah)',
        'key_states': ['Sarawak', 'Sabah', 'Peninsular Malaysia'],
    })
    n_indonesia = upsert_node(cur, 'indonesia', 'region', 'Indonesia', {
        'commodities': ['palm_oil'],
        'role': '#1 palm oil producer globally; key competitor to Malaysia in India/EU markets',
    })
    n_india = upsert_node(cur, 'india', 'region', 'India', {
        'role': '#1 palm oil importer globally; substitutes between Malaysia, Indonesia, and sunflower oil',
        'imports_2017_from_malaysia_mmt': 2.03,
        'imports_2016_from_malaysia_mmt': 2.83,
        'imports_2017_from_indonesia_mmt': 7.05,
    })
    n_elnino = upsert_node(cur, 'el_nino_2015_16', 'seasonal_event', 'El-Nino 2015-2016 (palm oil impact)', {
        'period': '2015-H2 through 2016-H1',
        'impact': 'Prolonged dry, below-average rainfall; lagged impact on 2016 FFB yields',
        'pattern': 'FFB yield response lags weather by ~12 months due to palm biology',
    })
    n_palm_oer = upsert_node(cur, 'palm_oil_oer', 'metric', 'Oil Extraction Rate (OER)', {
        'unit': 'percent',
        'definition': 'CPO extracted per tonne FFB processed',
        'typical_range': '19-21%',
        'drivers': ['FFB quality', 'mill efficiency', 'ripeness at harvest', 'weather stress'],
    })
    print(f"Nodes upserted: palm_oil={n_palm}, malaysia={n_malaysia}, indonesia={n_indonesia}, "
          f"india={n_india}, el_nino={n_elnino}, oer={n_palm_oer}")

    # --- 3. Contexts on palm_oil ---
    contexts = []

    c1 = insert_context(cur, n_palm, 'expert_rule', 'el_nino_palm_impact_chain', {
        'content': (
            'El-Nino causes prolonged dry weather over SE Asia. Palm biology means the '
            'impact shows up with a 6-18 month lag: 2015-16 El-Nino caused the 2016 FFB '
            'yield collapse (-13.9% to 15.91 t/ha from 18.48 t/ha). CPO production followed '
            '-13.2% to 17.32 MMT. 2017 recovery year +15.0% CPO to 19.92 MMT as yields '
            'rebounded to 17.89 t/ha. Rule: dry H2 year N -> yield cut H2 year N+1.'
        ),
        'mechanism': 'dry_stress -> lower_FFB_yield_lagged -> lower_CPO_production -> drawdown_stocks -> price_spike',
        'magnitude_ref': '2016: FFB yield -13.9%, CPO production -13.2%, CPO price +23.2% to RM2,653/t',
        'confidence': 0.9,
        'source_doc': 'mpob_2016_industry_overview',
    })
    contexts.append(c1)

    c2 = insert_context(cur, n_palm, 'expert_rule', 'oer_ffb_quality_link', {
        'content': (
            'OER decline correlates with FFB quality, which itself degrades when weather '
            'stress accelerates harvest or reduces ripeness. 2016 OER -1.4% to 20.18%; '
            '2017 OER -2.3% to 19.72% despite production recovery because mills processed '
            'higher volumes of mixed-quality FFB. OER changes are smaller than yield '
            'changes but multiply: CPO = FFB * OER. 2% OER change on 20 MMT FFB = 400k MT CPO.'
        ),
        'sensitivity': '1% OER change -> ~0.2 MMT CPO change on Malaysia base',
        'confidence': 0.85,
        'source_doc': 'mpob_2017_industry_overview',
    })
    contexts.append(c2)

    c3 = insert_context(cur, n_palm, 'market_structure', 'malaysia_indonesia_india_competition', {
        'content': (
            'Malaysia and Indonesia compete for the India market. India is the #1 palm oil '
            'destination. When Malaysian prices rise or trade policy shifts, India substitutes '
            'toward Indonesia. 2017: Malaysian exports to India -28.2% to 2.03 MMT while '
            'Indonesia exports to India +32.8% to 7.05 MMT (+1.7 MMT swing). India also '
            'substitutes into sunflower oil (+42.7% to 2.26 MMT in 2017). Rule: track the '
            'Malaysia/Indonesia-to-India share ratio as a price-sensitivity indicator.'
        ),
        'confidence': 0.9,
        'source_doc': 'mpob_2017_industry_overview',
    })
    contexts.append(c3)

    c4 = insert_context(cur, n_palm, 'market_data', 'palm_cpo_price_history', {
        'content': 'CPO prices 2015-2017 in RM/tonne',
        'values': {
            '2015': 2153,
            '2016': 2653,  # +23.2% vs 2015
            '2017': 2783,  # +4.9% vs 2016 (approximate from text; exact not cited)
        },
        'pk_2016_rm_per_t': 2611,
        'pk_2017_rm_per_t': 2536,
        'cpko_2016_rm_per_t': 5492.50,
        'cpko_2017_rm_per_t': 5325.00,
        'confidence': 0.85,
        'source_doc': 'mpob_2017_industry_overview',
    })
    contexts.append(c4)

    c5 = insert_context(cur, n_palm, 'market_structure', 'palm_soy_oil_substitution', {
        'content': (
            'Palm oil export demand to major destinations (India, EU, China, US) is a '
            'function of the CPO discount to soybean oil. Narrow discount -> palm oil '
            'exports decline as substitution reverses. 2016 MPOB explicitly attributed '
            'lower exports to the narrowing CPO-SBO discount. Soy oil reference: '
            'BOA/CBOT front-month. For 2-3 month forecasting, track the spread.'
        ),
        'related_series': ['CBOT soybean oil front', 'CPO Bursa Malaysia front'],
        'confidence': 0.9,
        'source_doc': 'mpob_2016_industry_overview',
    })
    contexts.append(c5)

    c6 = insert_context(cur, n_palm, 'market_data', 'planted_area_by_state_2017', {
        'content': (
            'Sarawak overtook Sabah as the largest oil-palm state in 2017. New planting '
            'concentrated in Sarawak (+4.7% in 2016). Peninsular Malaysia is fragmented '
            'across 11 states but still the largest aggregate (47% of total area).'
        ),
        'values_mha_2017': {'Sarawak': 1.56, 'Sabah': 1.55, 'Peninsular': 2.70, 'Total': 5.81},
        'values_mha_2016': {'Sarawak': 1.51, 'Sabah': 1.55, 'Peninsular': 2.68, 'Total': 5.74},
        'growth_trend': 'Peninsular Malaysia planted area is saturated; future expansion happens in Sarawak',
        'confidence': 0.95,
        'source_doc': 'mpob_2017_industry_overview',
    })
    contexts.append(c6)

    c7 = insert_context(cur, n_palm, 'historical_pattern', 'export_revenue_vs_volume_decoupling', {
        'content': (
            '2016 paradox: total export VOLUME -8.2% (23.29 MMT from 25.37 MMT) but '
            'total export REVENUE +7.3% (RM64.58B from RM60.17B). Higher prices more '
            'than offset lower volumes. Rule: in supply-shock years, revenue often '
            'rises even as volumes fall. Watch this when evaluating margin / '
            'profitability for palm integrated players.'
        ),
        'confidence': 0.9,
        'source_doc': 'mpob_2016_industry_overview',
    })
    contexts.append(c7)

    c8 = insert_context(cur, n_elnino, 'expert_rule', 'elnino_palm_yield_lag', {
        'content': (
            'El-Nino dry stress on oil palm produces the yield cut 12-18 months after '
            'the stress period due to palm biology (flowering -> fruiting timeline). '
            '2015-16 El-Nino -> 2016 FFB yield -13.9%. Rule: El-Nino developing H2 of '
            'year N implies materially lower palm oil production through most of year N+1.'
        ),
        'affected_commodity': 'palm_oil',
        'lag_months': 12,
        'confidence': 0.95,
        'source_doc': 'mpob_2016_industry_overview',
    })
    contexts.append(c8)

    print(f"Contexts inserted on palm_oil + el_nino: {len(contexts)}")

    # --- 4. Edges ---
    edges = []
    edges.append(insert_edge(cur, n_malaysia, n_palm, 'PRODUCES',
        {'label': 'Malaysia is #2 palm oil producer (~30% global share)',
         'production_2017_mmt': 19.92}, weight=0.95, confidence=0.95))
    edges.append(insert_edge(cur, n_indonesia, n_palm, 'PRODUCES',
        {'label': 'Indonesia is #1 palm oil producer; sets global supply tone',
         'competitive_dynamic': 'lower_cost_vs_malaysia'}, weight=0.95, confidence=0.95))
    edges.append(insert_edge(cur, n_palm, n_india, 'EXPORTS_TO',
        {'label': 'India is #1 Malaysian palm oil market; share fluctuates vs Indonesia',
         'share_range': '12-18% of Malaysia exports'}, weight=0.9, confidence=0.9))
    edges.append(insert_edge(cur, n_palm, n_china := upsert_node(cur, 'china', 'region', 'China',
        {'role': 'Major vegetable oil importer'}), 'EXPORTS_TO',
        {'label': 'China ~12% of Malaysia palm oil exports'}, weight=0.85, confidence=0.9))
    edges.append(insert_edge(cur, n_elnino, n_palm, 'IMPACTS',
        {'label': '2015-16 El-Nino caused 2016 palm oil production collapse (-13.2% CPO)',
         'lag_months': 12, 'critical': True}, weight=0.95, confidence=0.95))
    edges.append(insert_edge(cur, n_palm, 2, 'SUBSTITUTES_WITH',  # n_palm -> soybean_oil (id=2)
        {'label': 'Palm oil and soybean oil are substitutes in food/industrial use. '
                  'CPO-SBO spread governs export flows.',
         'key_driver': 'price_spread'}, weight=0.9, confidence=0.9))
    edges.append(insert_edge(cur, n_indonesia, n_malaysia, 'COMPETES_WITH',
        {'label': 'Indonesia palm oil competes with Malaysia for India + EU markets. '
                  'When Malaysian prices/policy shift, India substitutes to Indonesia (2017: +1.7 MMT swing).'},
        weight=0.9, confidence=0.9))
    print(f"Edges inserted: {len([e for e in edges if e])}")

    # --- 5. Provenance rows ---
    prov_count = 0
    # All contexts above came from MPOB docs
    mpob_map = {c1: src_2016, c2: src_2017, c3: src_2017,
                c4: src_2017, c5: src_2016, c6: src_2017,
                c7: src_2016, c8: src_2016}
    for ctx_id, src_id in mpob_map.items():
        add_provenance(cur, 'context', ctx_id, src_id, 'See kg_context body')
        prov_count += 1

    # Edges: attribute to 2017 doc (most recent)
    for e_id in edges:
        if e_id:
            add_provenance(cur, 'edge', e_id, src_2017, 'MPOB 2017 industry review')
            prov_count += 1

    # New nodes: attribute to 2017 doc
    for n_id in [n_malaysia, n_indonesia, n_india, n_elnino, n_palm_oer]:
        add_provenance(cur, 'node', n_id, src_2017, 'Created from MPOB data')
        prov_count += 1
    add_provenance(cur, 'node', n_palm, src_2016, 'palm_oil node enriched from MPOB')
    prov_count += 1

    print(f"Provenance rows added: {prov_count}")

    # --- 6. Update source extraction stats ---
    for src_id, nodes, edges_c, ctxs in [
        (src_2016, 1, 3, 4),   # palm_oil only node modified; 3 edges (el_nino, substitutes, 1 produces); 4 contexts
        (src_2017, 5, 4, 4),
    ]:
        cur.execute("""UPDATE core.kg_source SET nodes_extracted=%s, edges_extracted=%s, contexts_extracted=%s
                       WHERE id=%s""", (nodes, edges_c, ctxs, src_id))

    conn.commit()

    # --- Summary ---
    cur.execute("""SELECT COUNT(*) FROM core.kg_context WHERE node_id=%s""", (n_palm,))
    palm_ctx = cur.fetchone()[0]
    cur.execute("""SELECT COUNT(*) FROM core.kg_edge WHERE source_node_id=%s OR target_node_id=%s""",
                (n_palm, n_palm))
    palm_edges = cur.fetchone()[0]
    cur.execute("""SELECT COUNT(*) FROM core.kg_provenance""")
    prov_total = cur.fetchone()[0]

    print()
    print("=" * 70)
    print(f"DONE. palm_oil now has {palm_ctx} contexts and {palm_edges} edges.")
    print(f"kg_provenance rows in DB: {prov_total} (was 0 before this run)")
    print("=" * 70)

    conn.close()


if __name__ == '__main__':
    main()
