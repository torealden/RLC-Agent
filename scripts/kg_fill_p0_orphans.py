"""
Phase A of KG gap-fill strategy: close the 11 P0 LIGHT_TOUCH orphan nodes.

Targets:
  usda.grain_stocks, usda.grain_stocks.feed_residual,
  corn_pollination_window, soybean_pod_fill_aug,
  usda_june30_reports, usda_june30_stocks,
  tallow_feedstock (merge with bleachable_fancy_tallow),
  rfs2, us.corn_belt, nopa.monthly, usda.grain_stocks_report.

Each gets a dense, analyst-grade context so the LLM has substance to cite and
reason with when producing monthly forecasts.
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


def insert_context(cur, node_id, ctx_type, ctx_key, body, source='hand'):
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


def lookup(cur, key):
    cur.execute("SELECT id FROM core.kg_node WHERE node_key=%s", (key,))
    r = cur.fetchone()
    return r[0] if r else None


def main():
    conn = connect()
    cur = conn.cursor()

    FILLS = []

    # =========================================================================
    # usda.grain_stocks (quarterly report data)
    # =========================================================================
    nid = lookup(cur, 'usda.grain_stocks')
    if nid:
        FILLS.append(insert_context(cur, nid, 'release_cadence', 'quarterly_dates', {
            'content': (
                'USDA NASS Grain Stocks reports quarterly: Sep 1 stocks (released late Sep), '
                'Dec 1 (released early Jan), Mar 1 (released late Mar), Jun 1 (released late Jun). '
                'Release time 12:00 PM ET. The June 30 release is the SAME DAY as the Acreage '
                'report — historically the most-volatile USDA trading day of the year.'
            ),
            'quarterly_effective_dates': {'Q1': 'Mar 1', 'Q2': 'Jun 1', 'Q3': 'Sep 1', 'Q4': 'Dec 1'},
            'release_time_et': '12:00',
            'confidence': 0.95,
        }))
        FILLS.append(insert_context(cur, nid, 'expert_rule', 'residual_calc', {
            'content': (
                'The implied feed + residual disappearance between Grain Stocks releases is '
                'a major yardstick for demand revisions. Quarterly residual = '
                '(prior stocks) + (production slice) + (imports) - (exports from FGIS+FAS) - '
                '(reported end-use crush/ethanol/food) - (ending stocks). Divergences from '
                'USDA monthly WASDE feed/residual imply a future WASDE adjustment.'
            ),
            'related_model': 'quarterly_residual_model',
            'confidence': 0.9,
        }))

    # =========================================================================
    # usda.grain_stocks.feed_residual (the computed residual itself)
    # =========================================================================
    nid = lookup(cur, 'usda.grain_stocks.feed_residual')
    if nid:
        FILLS.append(insert_context(cur, nid, 'expert_rule', 'feed_residual_identity', {
            'content': (
                'Feed & residual (F&R) = total_supply - (crush + ethanol + FSI + exports + '
                'ending_stocks). For corn, F&R is ~5000-5500 mbu/yr and gets revised substantially '
                'through the marketing year as NASS grain stocks pins down disappearance. '
                'When actual ending stocks from NASS surprise LOWER than WASDE ending stocks, '
                'USDA typically BUMPS F&R UP next WASDE (or lowers crush/exports). Magnitude '
                'of surprise maps to magnitude of revision.'
            ),
            'typical_range_mbu_corn': [5000, 5500],
            'revision_mechanics': 'Grain Stocks surprise → WASDE F&R adjustment 2-4 weeks later',
            'confidence': 0.9,
        }))

    # =========================================================================
    # corn_pollination_window (critical weather-adjusted yield callable input)
    # =========================================================================
    nid = lookup(cur, 'corn_pollination_window')
    if nid:
        FILLS.append(insert_context(cur, nid, 'seasonal_calendar', 'pollination_timing_corn_belt', {
            'content': (
                'Corn pollination (silking + early grain fill) is the single most yield-sensitive '
                'window. Peak silking in the US Corn Belt is typically 3rd-4th week of July, with '
                'Iowa 1-7 days ahead of Illinois/Indiana. Frost-to-silk window is ~55-65 days; '
                'earlier planting pulls silking earlier. Extreme heat (>95F) or drought during '
                'this 2-3 week window causes the biggest single-event yield hits '
                '(2012 drought: corn yield cut ~25% vs trend; 2023: partial cut). '
                'The weather_adjusted_yield callable uses "pollination" growth_stage with '
                'highest rain sensitivity (3.2 bpa/in) and highest temp penalty (1.5 bpa/deg over 86F).'
            ),
            'typical_peak_date_iowa': 'Jul 20-25',
            'typical_peak_date_il_in': 'Jul 25-31',
            'critical_weather_thresholds': {'heat_stress_f': 86, 'extreme_heat_f': 95, 'drought_inches_below_normal_30d': 1.5},
            'analog_events': [{'2012': 'drought + heat, yield -25% vs trend'},
                              {'2023': 'partial heat stress'},
                              {'2020': 'derecho (post-pollination but material)'}],
            'linked_callable': 'weather_adjusted_yield',
            'confidence': 0.95,
        }))

    # =========================================================================
    # soybean_pod_fill_aug
    # =========================================================================
    nid = lookup(cur, 'soybean_pod_fill_aug')
    if nid:
        FILLS.append(insert_context(cur, nid, 'seasonal_calendar', 'soy_pod_fill_timing', {
            'content': (
                'Soybean pod fill runs roughly August 10 through early September in the Corn '
                'Belt. This window is the single most yield-critical stage for soybeans — '
                'water stress here cuts bean count, bean size, and oil content. Unlike corn, '
                'soy yield is more forgiving of mid-summer weather and more sensitive to '
                'late-season water — "August rain makes beans." Drought during pod fill '
                'produces yield cuts 2-3x larger than same-magnitude drought in July.'
            ),
            'typical_dates': 'Aug 10 - Sep 7',
            'sensitivity_vs_corn': 'Later and more rain-critical; temp less dominant',
            'linked_callable': 'weather_adjusted_yield (growth_stage=pod_fill)',
            'confidence': 0.95,
        }))

    # =========================================================================
    # usda_june30_reports + usda_june30_stocks
    # =========================================================================
    for key, extra in [
        ('usda_june30_reports', {
            'content': (
                'June 30 combines Acreage Report (final planted acres survey) + Grain Stocks '
                '(Jun 1 effective date). Historically the most volatile USDA day: (1) Planted '
                'acres can swing 1-3M acres vs March Prospective Plantings; (2) Grain Stocks '
                'anchors Q2 residual demand. A 1M acre shift on corn at 175 bpa = 175 mbu, '
                'enough to move balance sheet by ~1 percentage point of stocks/use.'
            ),
            'volatility_rank': '#1 trading day of USDA calendar',
            'typical_release_time_et': '12:00',
        }),
        ('usda_june30_stocks', {
            'content': (
                'Grain Stocks component of June 30. Effective date Jun 1. Captures Q2 (Mar-May) '
                'implied disappearance for corn, soy, wheat. Q2 corn residual often surprises — '
                'in 2024 Q2 residual -150 mbu vs expectation drove July WASDE feed cut.'
            ),
        }),
    ]:
        nid = lookup(cur, key)
        if nid:
            FILLS.append(insert_context(cur, nid, 'seasonal_event_rule', f'{key}_framework',
                {**extra, 'confidence': 0.9}))

    # =========================================================================
    # rfs2 — FOUNDATIONAL POLICY
    # =========================================================================
    nid = lookup(cur, 'rfs2')
    if nid:
        FILLS.append(insert_context(cur, nid, 'policy_framework', 'rfs2_mechanics', {
            'content': (
                'The Renewable Fuel Standard (RFS2, EISA 2007) mandates that US obligated parties '
                '(refiners, importers) blend renewable fuels into US transportation fuel. EPA sets '
                'annual Renewable Volume Obligations (RVOs) by category: total, advanced, BBD, '
                'cellulosic. RINs (Renewable Identification Numbers) are generated at production '
                'and retired at blend — the RIN market clears the mandate. Categories: D6 (conventional '
                'ethanol), D5 (advanced non-BBD), D4 (biomass-based diesel), D3 (cellulosic). RIN '
                'prices reflect marginal cost to meet mandate. Small refineries can petition for '
                'exemption (SRE); EPA grants have been politically volatile.'
            ),
            'rvo_2025_estimate': {'total_bgal': 21.87, 'bbd_bgal': 3.35, 'advanced_bgal': 6.72, 'cellulosic_mgal': 1.38},
            'rin_categories': {'D3': 'cellulosic', 'D4': 'BBD', 'D5': 'advanced', 'D6': 'conventional_ethanol'},
            'authority': 'EPA under EISA 2007 + CAA 211(o)',
            'key_risk': 'RVO rule-making timing + SRE grants drive RIN price volatility',
            'confidence': 0.95,
        }))

    # =========================================================================
    # us.corn_belt region — referenced throughout
    # =========================================================================
    nid = lookup(cur, 'us.corn_belt')
    if nid:
        FILLS.append(insert_context(cur, nid, 'structural_definition', 'corn_belt_states', {
            'content': (
                'US Corn Belt: Iowa, Illinois, Indiana, Nebraska, Minnesota, Ohio, South Dakota, '
                'Wisconsin, Missouri, Kansas. These 10 states produce ~85% of US corn and ~80% '
                'of US soybeans. Iowa and Illinois alone account for ~30% of US corn production. '
                'Rotating corn-soybean-corn is the dominant cropping pattern. Production '
                'concentration means weather shocks to IA/IL/IN cascade to national S&D.'
            ),
            'states_ranked_corn_2024': ['IA', 'IL', 'NE', 'MN', 'IN', 'OH', 'SD', 'WI', 'MO', 'KS'],
            'production_share': {'corn_pct': 85, 'soybeans_pct': 80},
            'top_2_concentration': 'IA+IL ~30% of US corn',
            'dominant_rotation': 'corn-soybean',
            'confidence': 0.95,
        }))

    # =========================================================================
    # nopa.monthly (monthly crush data)
    # =========================================================================
    nid = lookup(cur, 'nopa.monthly')
    if nid:
        FILLS.append(insert_context(cur, nid, 'release_cadence', 'nopa_monthly_15th', {
            'content': (
                'NOPA (National Oilseed Processors Association) monthly crush report released '
                'around the 15th of each month, reporting prior-month crush volumes for member '
                'companies (~95% of US commercial crush capacity). Reports: crush volume (mbu), '
                'soybean oil stocks, meal exports. Public release ~10:00 AM ET. Market-moving '
                'surprises are common. The weather_intelligence workflow and pace_tracker use '
                'NOPA to anchor monthly crush realized vs USDA annual WASDE crush forecast.'
            ),
            'frequency': 'monthly',
            'release_day_range': '13-17',
            'coverage_share_of_us_crush': '~95%',
            'confidence': 0.95,
        }))
        FILLS.append(insert_context(cur, nid, 'expert_rule', 'pace_vs_wasde_corn', {
            'content': (
                'Cumulative NOPA crush as % of WASDE annual crush estimate is the key pace '
                'indicator. If cumulative run rate through month M exceeds the typical seasonal '
                '% by >1.5pp, WASDE will usually RAISE annual crush at next report. Symmetric '
                'on the downside. Typical monthly seasonal shares are higher in Q1 MY (Sep-Nov) '
                'and lower in summer (Jul-Aug) as beans tighten and hog demand softens.'
            ),
            'seasonal_monthly_share_approx_pct': {
                'sep': 8.8, 'oct': 9.5, 'nov': 9.3, 'dec': 9.0, 'jan': 8.6, 'feb': 7.8,
                'mar': 8.3, 'apr': 8.0, 'may': 8.1, 'jun': 7.6, 'jul': 7.5, 'aug': 7.5,
            },
            'pace_signal_threshold_pp': 1.5,
            'confidence': 0.85,
        }))

    # =========================================================================
    # usda.grain_stocks_report (the report object)
    # =========================================================================
    nid = lookup(cur, 'usda.grain_stocks_report')
    if nid:
        FILLS.append(insert_context(cur, nid, 'release_cadence', 'grain_stocks_quarterly', {
            'content': (
                'Quarterly Grain Stocks report from NASS. 4 releases per year: late September '
                '(Sep 1 stocks), January (Dec 1 stocks), late March (Mar 1 stocks), late June '
                '(Jun 1 stocks, co-released with Acreage). Primary inputs: on-farm + off-farm '
                'stocks survey for corn, soybeans, wheat, sorghum, oats, barley, rice.'
            ),
            'confidence': 0.95,
        }))

    # =========================================================================
    # tallow_feedstock — MERGE into bleachable_fancy_tallow
    # =========================================================================
    src = lookup(cur, 'tallow_feedstock')
    tgt = lookup(cur, 'bleachable_fancy_tallow')
    if src and tgt:
        # Add a context on tallow_feedstock that points to BFT as canonical
        FILLS.append(insert_context(cur, src, 'alias_note', 'merged_into_bft', {
            'content': (
                'DEPRECATED: tallow_feedstock was a duplicate of bleachable_fancy_tallow (id='
                f'{tgt}). The canonical node for US tallow used as BBD feedstock is '
                'bleachable_fancy_tallow, which already distinguishes EBFT (edible) vs IBFT '
                '(inedible) grades. Consumers should query bleachable_fancy_tallow.'
            ),
            'canonical_node_key': 'bleachable_fancy_tallow',
            'canonical_node_id': tgt,
            'status': 'deprecated_alias',
            'confidence': 1.0,
        }))
        # Mark the node properties so status is discoverable
        cur.execute("""
            UPDATE core.kg_node
            SET properties = COALESCE(properties, '{}'::jsonb) || '{"status":"deprecated_alias","canonical":"bleachable_fancy_tallow"}'::jsonb,
                last_reinforced = NOW()
            WHERE id = %s
        """, (src,))

    conn.commit()

    # Audit
    print(f"Phase A inserted {len([c for c in FILLS if c])} contexts.")
    cur.execute("""
        SELECT node_type, COUNT(*) AS total,
               COUNT(*) FILTER (WHERE NOT EXISTS (SELECT 1 FROM core.kg_context c WHERE c.node_id=n.id)
                                 AND NOT EXISTS (SELECT 1 FROM core.kg_edge e WHERE e.source_node_id=n.id OR e.target_node_id=n.id)) AS orphan
        FROM core.kg_node n GROUP BY node_type ORDER BY orphan DESC, total DESC
    """)
    print("\nOrphan counts after Phase A:")
    for row in cur.fetchall():
        if row[2] > 0:
            print(f"  {row[0]}: {row[2]} of {row[1]}")

    conn.close()


if __name__ == '__main__':
    main()
