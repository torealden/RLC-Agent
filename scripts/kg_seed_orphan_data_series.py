"""
Seed contexts on orphan data_series KG nodes using stats computed from
the actual DB. Kills the "CLAUDE.md says this has content but it's empty"
drift by making the nodes carry live, verifiable information.

Targets:
  - cftc.cot                  (positioning)
  - eia.ethanol               (weekly production + stocks)
  - usda.crop_condition_rating (G/E %)
  - usda.fgis                 (export inspections)
  - nass.fats_oils.canola_oil_stocks (canola)

For each, inserts:
  - release_cadence context (hand-written)
  - current_state context   (latest value from DB)
  - coverage_summary context (row count, date range)
  - interpretive rules already in CLAUDE.md
"""

import json
import os
from datetime import datetime
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


def insert_context(cur, node_id, ctx_type, ctx_key, body, applicable='always', source='computed'):
    cur.execute("""
        INSERT INTO core.kg_context
            (node_id, context_type, context_key, context_value, applicable_when, source, source_count)
        VALUES (%s, %s, %s, %s, %s, %s, 1)
        ON CONFLICT DO NOTHING
        RETURNING id
    """, (node_id, ctx_type, ctx_key, json.dumps(body, default=str), applicable, source))
    r = cur.fetchone()
    if r:
        return r[0]
    cur.execute("""SELECT id FROM core.kg_context WHERE node_id=%s AND context_key=%s""",
                (node_id, ctx_key))
    return cur.fetchone()[0]


def safe_stats(cur, sql, desc):
    try:
        cur.execute(sql)
        return cur.fetchone()
    except Exception as e:
        cur.execute("ROLLBACK")
        print(f"  ! {desc} stats query failed: {e}")
        return None


def seed_cftc_cot(cur, node_id):
    inserted = []
    # Cadence
    inserted.append(insert_context(cur, node_id, 'release_cadence', 'weekly_friday_release', {
        'content': (
            'CFTC Commitments of Traders released every Friday afternoon (~3:30 PM ET). '
            'Report reflects Tuesday-close positions. Data available as both "legacy" '
            '(1986+) and "disaggregated" (2006+) formats. Key series for commodity '
            'commodities: managed money net (MM_long - MM_short), commercial net, '
            'OI (open interest).'
        ),
        'frequency': 'weekly',
        'release_day': 'Friday',
        'reference_time': 'Tuesday close',
        'confidence': 0.95,
    }, source='hand')
)

    # Coverage from DB
    row = safe_stats(cur, """
        SELECT COUNT(*) AS rows,
               MIN(report_date::date) AS oldest,
               MAX(report_date::date) AS newest,
               COUNT(DISTINCT commodity) AS commodities
        FROM bronze.cftc_cot
    """, 'cftc_cot coverage')
    if row:
        inserted.append(insert_context(cur, node_id, 'coverage_summary', 'db_coverage', {
            'content': 'Data coverage in bronze.cftc_cot (live query)',
            'row_count': int(row[0]) if row[0] else 0,
            'oldest': str(row[1]),
            'newest': str(row[2]),
            'distinct_commodities': int(row[3]) if row[3] else 0,
            'computed_at': datetime.utcnow().isoformat() + 'Z',
            'confidence': 1.0,
        }))

    # Interpretive rule
    inserted.append(insert_context(cur, node_id, 'expert_rule', 'positioning_extremes_liquidation_risk', {
        'content': (
            'When managed money net long exceeds the 90th percentile for the current '
            'calendar month (computed from seasonal_norm context), liquidation risk '
            'is elevated: trend reversals or stop-outs can cascade. Symmetric rule for '
            'net short at <10th percentile. Combine with commercial net to filter: '
            'if commercials sitting short while MM sitting long, reversal risk is higher.'
        ),
        'thresholds': {'extreme_long_percentile': 90, 'extreme_short_percentile': 10},
        'confidence': 0.85,
        'source_note': 'HB weekly methodology',
    }, source='hand'))

    return inserted


def seed_crop_condition(cur, node_id):
    inserted = []
    inserted.append(insert_context(cur, node_id, 'release_cadence', 'nass_crop_progress_monday', {
        'content': (
            'USDA NASS Crop Progress + Condition release every Monday 4:00 PM ET '
            'during the growing season (roughly late March through late November for '
            'US row crops). Report reflects conditions as of the prior Sunday. '
            'If Monday is a federal holiday, release shifts to Tuesday 4:00 PM.'
        ),
        'frequency': 'weekly_seasonal',
        'release_day': 'Monday',
        'reference_time': 'Prior Sunday',
        'season': 'late Mar through late Nov (US row crops)',
        'confidence': 0.95,
    }, source='hand'))

    row = safe_stats(cur, """
        SELECT COUNT(*) AS rows,
               MIN(week_ending) AS oldest,
               MAX(week_ending) AS newest,
               COUNT(DISTINCT commodity) AS commodities,
               COUNT(DISTINCT state) AS states
        FROM bronze.nass_crop_condition
    """, 'crop_condition coverage')
    if row:
        inserted.append(insert_context(cur, node_id, 'coverage_summary', 'db_coverage', {
            'content': 'Crop condition coverage in bronze.nass_crop_condition',
            'row_count': int(row[0]) if row[0] else 0,
            'oldest': str(row[1]),
            'newest': str(row[2]),
            'distinct_commodities': int(row[3]) if row[3] else 0,
            'distinct_states': int(row[4]) if row[4] else 0,
            'computed_at': datetime.utcnow().isoformat() + 'Z',
            'confidence': 1.0,
        }))

    inserted.append(insert_context(cur, node_id, 'expert_rule', 'ge_change_yield_predictor', {
        'content': (
            'Good+Excellent % week-over-week change is the single best pre-USDA-report '
            'yield predictor. See crop_condition_yield_model node for the full framework: '
            'Jul1->Aug1 G/E change predicts Aug WASDE yield revision; Aug7->Sep4 predicts '
            'Sep WASDE. Corn seasonal decline averages -3.4% Aug->Sep; only deviations '
            'from that norm matter. Soy is more sensitive: 1-2% G/E drop triggers yield cuts.'
        ),
        'related_callable': 'weather_adjusted_yield',
        'related_node': 'crop_condition_yield_model',
        'confidence': 0.95,
    }, source='hand'))

    return inserted


def seed_eia_ethanol(cur, node_id):
    inserted = []
    inserted.append(insert_context(cur, node_id, 'release_cadence', 'eia_weekly_wednesday', {
        'content': (
            'EIA Weekly Ethanol Report released Wednesday 10:30 AM ET. Reports production '
            '(kbd) and stocks (kb) for the week ending prior Friday. Series IDs: '
            'PET.W_EPOOXE_YOP_NUS_MBBLD.W (production), PET.W_EPOOXE_SAE_NUS_MBBL.W (stocks).'
        ),
        'frequency': 'weekly',
        'release_day': 'Wednesday',
        'release_time_et': '10:30',
        'reference_period': 'Week ending prior Friday',
        'confidence': 0.95,
    }, source='hand'))

    row = safe_stats(cur, """
        SELECT COUNT(*) AS rows, MIN(period::date) AS oldest, MAX(period::date) AS newest
        FROM bronze.eia_ethanol
    """, 'eia_ethanol coverage')
    if row:
        inserted.append(insert_context(cur, node_id, 'coverage_summary', 'db_coverage', {
            'content': 'EIA ethanol coverage in bronze.eia_ethanol',
            'row_count': int(row[0]) if row[0] else 0,
            'oldest': str(row[1]), 'newest': str(row[2]),
            'computed_at': datetime.utcnow().isoformat() + 'Z',
            'confidence': 1.0,
        }))

    inserted.append(insert_context(cur, node_id, 'expert_rule', 'ethanol_corn_grind_conversion', {
        'content': (
            '1 bushel corn -> ~2.85 gallons ethanol (industry avg). Weekly ethanol production '
            '(kbd) x 7 x 42 -> million gallons weekly / 2.85 = bu corn ground. Track implied '
            'corn grind pace vs USDA annual FSI projection. Pace <5% below required run rate '
            'for 4+ consecutive weeks flags potential USDA FSI cut at next WASDE.'
        ),
        'conversion_factor_bu_per_gal': 1 / 2.85,
        'confidence': 0.9,
    }, source='hand'))
    return inserted


def seed_fgis(cur, node_id):
    inserted = []
    inserted.append(insert_context(cur, node_id, 'release_cadence', 'fgis_thursday_weekly', {
        'content': (
            'USDA FGIS Export Inspections released Thursday 11:00 AM ET. Reports weekly '
            'grain export inspections at US ports (actual physical loadings, not sales). '
            'Leading indicator for quarterly export data; used in the quarterly_residual_model '
            'to estimate stocks implied by inspections vs. official USDA export data.'
        ),
        'frequency': 'weekly',
        'release_day': 'Thursday',
        'release_time_et': '11:00',
        'confidence': 0.95,
    }, source='hand'))

    row = safe_stats(cur, """
        SELECT COUNT(*) AS rows, MIN(week_ending) AS oldest, MAX(week_ending) AS newest,
               COUNT(DISTINCT commodity) AS commodities
        FROM bronze.fgis_inspections
    """, 'fgis coverage')
    if row:
        inserted.append(insert_context(cur, node_id, 'coverage_summary', 'db_coverage', {
            'row_count': int(row[0]) if row[0] else 0,
            'oldest': str(row[1]), 'newest': str(row[2]),
            'distinct_commodities': int(row[3]) if row[3] else 0,
            'computed_at': datetime.utcnow().isoformat() + 'Z',
            'confidence': 1.0,
        }))

    inserted.append(insert_context(cur, node_id, 'expert_rule', 'inspections_vs_export_sales_gap', {
        'content': (
            'Gap between weekly inspections (physical loadings) and weekly export sales '
            '(new commitments) signals forward visibility. When inspections run HOT but '
            'sales run COLD, existing pipeline is being worked through but new business '
            'is slow -> implies lower forward exports. Symmetric: sales hot + inspections '
            'cold implies buildup of unshipped commitments -> watch for cancellations.'
        ),
        'confidence': 0.85,
    }, source='hand'))
    return inserted


def seed_wasde(cur, node_id):
    inserted = []
    inserted.append(insert_context(cur, node_id, 'release_cadence', 'wasde_monthly_9am_et', {
        'content': (
            'USDA WASDE released monthly, typically on the 9th-12th of the month at '
            '12:00 PM ET (noon). Provides global S&D balance sheets for major crops. '
            'Methodology shifts through the year: Aug introduces first survey-based US '
            'yield; Sep refines; later reports use combine data. Jan WASDE consolidates '
            'full-year US production.'
        ),
        'frequency': 'monthly',
        'release_time_et': '12:00',
        'typical_day_range': '9-12',
        'key_shifts': {
            'august': 'First survey-based US yield',
            'september': 'Yield refinement + Small Grains Summary',
            'january': 'Final US crop production',
            'march/may/june': 'Planting intentions/acreage',
        },
        'confidence': 0.95,
    }, source='hand'))

    inserted.append(insert_context(cur, node_id, 'expert_rule', 'historical_revision_patterns', {
        'content': (
            'Historical WASDE yield revisions follow seasonal tendencies: (1) Aug first '
            'survey tends to be conservative — subsequent revisions lean higher if crop '
            'finishes strong; (2) corn Oct raise then Nov cut happened 3 of 20 years; '
            '(3) soy Oct raise is rare in dry years. The crop_condition_yield_model callable '
            'attempts to predict these revisions ahead of each WASDE.'
        ),
        'related_node': 'usda.wasde.revision_pattern',
        'confidence': 0.85,
    }, source='hand'))
    return inserted


def main():
    conn = connect()
    cur = conn.cursor()

    # Map node_key -> seed function
    seeders = {
        'cftc.cot': seed_cftc_cot,
        'usda.crop_condition_rating': seed_crop_condition,
        'eia.ethanol': seed_eia_ethanol,
        'usda.fgis': seed_fgis,
        'usda.wasde': seed_wasde,
    }

    cur.execute("""
        SELECT id, node_key FROM core.kg_node WHERE node_key = ANY(%s)
    """, (list(seeders.keys()),))
    nodes = {row[1]: row[0] for row in cur.fetchall()}
    print(f"Found {len(nodes)} of {len(seeders)} target nodes: {list(nodes.keys())}")

    # usda.wasde exists as a 'report' type node (checked earlier)
    # Make sure missing ones exist; if not, skip with note
    total_ctx = 0
    for key, seeder in seeders.items():
        if key not in nodes:
            print(f"  SKIP {key}: node not found")
            continue
        print(f"\nSeeding {key} (id={nodes[key]})...")
        try:
            ctx_ids = seeder(cur, nodes[key])
            conn.commit()  # commit after EACH seeder so later stats failures don't wipe prior work
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
