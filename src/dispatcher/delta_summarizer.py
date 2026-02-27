"""
Delta Summarizer — Computes meaningful data changes after collection.

When a collector finishes, this module queries the database to compute
week-over-week (or period-over-period) changes and flags notable moves.
Results are stored in the event_log details JSONB so the LLM briefing
contains actionable intelligence rather than just row counts.

Supported collectors:
  - cftc_cot: managed money positioning changes, percentile ranking
  - usda_nass_crop_progress: G/E condition changes, YoY comparison
  - eia_ethanol: production/stocks week-over-week changes
  - nass_processing: monthly crush changes vs prior month/year
"""

import logging
from typing import Dict, List, Optional, Any

logger = logging.getLogger(__name__)

# Commodities tracked per collector
CFTC_COMMODITIES = ['corn', 'soybeans', 'soybean_oil', 'soybean_meal', 'wheat_srw', 'wheat_hrw']
CROP_COMMODITIES = ['corn', 'soybeans', 'wheat']


def compute_delta(collector_name: str, conn) -> Optional[Dict[str, Any]]:
    """
    Compute data deltas for a collector. Returns dict with:
      - 'notable_changes': list of notable change dicts
      - 'summary_parts': list of human-readable summary fragments
      - 'data': full delta data by commodity/metric

    Returns None if no delta logic exists for this collector.
    """
    handlers = {
        'cftc_cot': _delta_cftc,
        'usda_nass_crop_progress': _delta_crop_condition,
        'eia_ethanol': _delta_eia_ethanol,
        'nass_processing': _delta_nass_processing,
    }

    handler = handlers.get(collector_name)
    if handler is None:
        return None

    try:
        return handler(conn)
    except Exception as e:
        logger.debug(f"Delta computation failed for {collector_name}: {e}")
        return None


# ------------------------------------------------------------------
# CFTC COT Deltas
# ------------------------------------------------------------------

def _delta_cftc(conn) -> Optional[Dict]:
    """
    Compute managed money positioning changes for all commodities.

    For each commodity:
      - Latest mm_net and report_date
      - Prior week mm_net and change
      - 4-week change
      - 1-year percentile ranking
      - Flag if position is extreme (>90th or <10th percentile)
    """
    cur = conn.cursor()

    # Get the two most recent report dates and mm_net for each commodity
    cur.execute("""
        WITH ranked AS (
            SELECT commodity, report_date, mm_net, open_interest,
                   ROW_NUMBER() OVER (PARTITION BY commodity ORDER BY report_date DESC) as rn
            FROM bronze.cftc_cot
            WHERE mm_net IS NOT NULL AND report_type = 'legacy'
        )
        SELECT
            r1.commodity,
            r1.report_date as latest_date,
            r1.mm_net as mm_net,
            r1.open_interest,
            r2.report_date as prior_date,
            r2.mm_net as mm_net_prior,
            r1.mm_net - r2.mm_net as mm_net_change_1w
        FROM ranked r1
        LEFT JOIN ranked r2 ON r1.commodity = r2.commodity AND r2.rn = 2
        WHERE r1.rn = 1
        ORDER BY r1.commodity
    """)
    latest = {r['commodity']: dict(r) for r in cur.fetchall()}

    if not latest:
        return None

    # Get 1-year percentile benchmarks
    cur.execute("""
        SELECT commodity,
               PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY mm_net)::bigint AS p10,
               PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY mm_net)::bigint AS p25,
               PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY mm_net)::bigint AS p50,
               PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY mm_net)::bigint AS p75,
               PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY mm_net)::bigint AS p90,
               MIN(mm_net)::bigint AS min_1y,
               MAX(mm_net)::bigint AS max_1y,
               COUNT(*) AS obs
        FROM bronze.cftc_cot
        WHERE mm_net IS NOT NULL
          AND report_date >= CURRENT_DATE - INTERVAL '1 year'
          AND report_type = 'legacy'
        GROUP BY commodity
    """)
    pctls = {r['commodity']: dict(r) for r in cur.fetchall()}

    # Get 4-week-ago data
    cur.execute("""
        WITH ranked AS (
            SELECT commodity, report_date, mm_net,
                   ROW_NUMBER() OVER (PARTITION BY commodity ORDER BY report_date DESC) as rn
            FROM bronze.cftc_cot
            WHERE mm_net IS NOT NULL AND report_type = 'legacy'
        )
        SELECT commodity, mm_net as mm_net_4w_ago
        FROM ranked
        WHERE rn = 5
    """)
    four_wk = {r['commodity']: int(r['mm_net_4w_ago']) for r in cur.fetchall()}

    data = {}
    notable = []
    summary_parts = []

    for commodity in CFTC_COMMODITIES:
        if commodity not in latest:
            continue

        row = latest[commodity]
        mm_net = int(row['mm_net']) if row['mm_net'] is not None else None
        mm_net_prior = int(row['mm_net_prior']) if row['mm_net_prior'] is not None else None
        change_1w = int(row['mm_net_change_1w']) if row['mm_net_change_1w'] is not None else None

        if mm_net is None:
            continue

        entry = {
            'report_date': str(row['latest_date']),
            'mm_net': mm_net,
            'mm_net_prior_week': mm_net_prior,
            'mm_net_change_1w': change_1w,
            'open_interest': int(row['open_interest']) if row['open_interest'] else None,
        }

        # 4-week change
        if commodity in four_wk:
            entry['mm_net_4w_ago'] = four_wk[commodity]
            entry['mm_net_change_4w'] = mm_net - four_wk[commodity]

        # Percentile ranking
        p = pctls.get(commodity)
        if p and p['max_1y'] != p['min_1y']:
            pctl_rank = round(
                (mm_net - p['min_1y']) / (p['max_1y'] - p['min_1y']) * 100, 0
            )
            entry['percentile_1y'] = int(pctl_rank)
            entry['p10'] = int(p['p10'])
            entry['p50'] = int(p['p50'])
            entry['p90'] = int(p['p90'])

            # Flag extremes
            if mm_net >= p['p90']:
                entry['risk_flag'] = 'extreme_long'
                notable.append({
                    'commodity': commodity,
                    'flag': 'extreme_long',
                    'detail': f"{commodity} MM net {mm_net:+,d} at {int(pctl_rank)}th pctl (>90th)",
                })
            elif mm_net <= p['p10']:
                entry['risk_flag'] = 'extreme_short'
                notable.append({
                    'commodity': commodity,
                    'flag': 'extreme_short',
                    'detail': f"{commodity} MM net {mm_net:+,d} at {int(pctl_rank)}th pctl (<10th)",
                })

        # Flag large weekly moves (>20,000 contracts)
        if change_1w is not None and abs(change_1w) >= 20000:
            direction = 'bought' if change_1w > 0 else 'sold'
            notable.append({
                'commodity': commodity,
                'flag': 'large_weekly_move',
                'detail': f"{commodity} MM {direction} {abs(change_1w):,d} contracts",
            })

        data[commodity] = entry

    # Build summary
    if notable:
        summary_parts = [n['detail'] for n in notable[:3]]
    elif data:
        # Summarize biggest movers
        changes = [(c, d.get('mm_net_change_1w', 0) or 0) for c, d in data.items()]
        changes.sort(key=lambda x: abs(x[1]), reverse=True)
        top = changes[0]
        if top[1] != 0:
            direction = '+' if top[1] > 0 else ''
            summary_parts.append(f"Biggest move: {top[0]} MM net {direction}{top[1]:,d}")

    return {
        'notable_changes': notable,
        'summary_parts': summary_parts,
        'data': data,
    }


# ------------------------------------------------------------------
# Crop Condition Deltas
# ------------------------------------------------------------------

def _delta_crop_condition(conn) -> Optional[Dict]:
    """
    Compute Good/Excellent condition changes for national-level crops.

    For each commodity:
      - Latest G/E percentage and week_ending
      - Prior week G/E and change
      - Year-over-year comparison (same week last year)
    """
    cur = conn.cursor()

    # Get two most recent weeks of G/E data per commodity
    cur.execute("""
        WITH ranked AS (
            SELECT commodity, week_ending, good_excellent_pct,
                   ROW_NUMBER() OVER (PARTITION BY commodity ORDER BY week_ending DESC) as rn
            FROM silver.nass_crop_condition_ge
            WHERE state = 'US' AND good_excellent_pct IS NOT NULL
        )
        SELECT
            r1.commodity,
            r1.week_ending as latest_week,
            r1.good_excellent_pct as ge_pct,
            r2.week_ending as prior_week,
            r2.good_excellent_pct as ge_pct_prior,
            r1.good_excellent_pct - r2.good_excellent_pct as ge_change_1w
        FROM ranked r1
        LEFT JOIN ranked r2 ON r1.commodity = r2.commodity AND r2.rn = 2
        WHERE r1.rn = 1
        ORDER BY r1.commodity
    """)
    latest = {r['commodity']: dict(r) for r in cur.fetchall()}

    if not latest:
        return None

    # Get year-ago values
    cur.execute("""
        SELECT
            c.commodity,
            c.good_excellent_pct as ge_current,
            p.good_excellent_pct as ge_prior_year,
            c.good_excellent_pct - p.good_excellent_pct as ge_yoy_change
        FROM silver.nass_crop_condition_ge c
        LEFT JOIN silver.nass_crop_condition_ge p
            ON c.commodity = p.commodity
            AND c.state = p.state
            AND p.week_ending BETWEEN c.week_ending - INTERVAL '368 days'
                                  AND c.week_ending - INTERVAL '358 days'
        WHERE c.state = 'US'
          AND c.week_ending = (
              SELECT MAX(week_ending) FROM silver.nass_crop_condition_ge
              WHERE state = 'US' AND commodity = c.commodity
          )
    """)
    yoy = {r['commodity']: dict(r) for r in cur.fetchall()}

    data = {}
    notable = []
    summary_parts = []

    for commodity in CROP_COMMODITIES:
        if commodity not in latest:
            continue

        row = latest[commodity]
        ge = float(row['ge_pct']) if row['ge_pct'] is not None else None
        ge_prior = float(row['ge_pct_prior']) if row['ge_pct_prior'] is not None else None
        change_1w = float(row['ge_change_1w']) if row['ge_change_1w'] is not None else None

        if ge is None:
            continue

        entry = {
            'week_ending': str(row['latest_week']),
            'ge_pct': round(ge, 1),
            'ge_pct_prior_week': round(ge_prior, 1) if ge_prior is not None else None,
            'ge_change_1w': round(change_1w, 1) if change_1w is not None else None,
        }

        # YoY
        y = yoy.get(commodity)
        if y and y['ge_prior_year'] is not None:
            entry['ge_prior_year'] = round(float(y['ge_prior_year']), 1)
            entry['ge_yoy_change'] = round(float(y['ge_yoy_change']), 1)

        # Flag significant weekly drops (>3 points)
        if change_1w is not None and change_1w <= -3:
            notable.append({
                'commodity': commodity,
                'flag': 'rapid_deterioration',
                'detail': f"{commodity} G/E dropped {abs(change_1w):.1f} pts to {ge:.1f}%",
            })
        elif change_1w is not None and change_1w >= 3:
            notable.append({
                'commodity': commodity,
                'flag': 'rapid_improvement',
                'detail': f"{commodity} G/E gained {change_1w:.1f} pts to {ge:.1f}%",
            })

        data[commodity] = entry

    if notable:
        summary_parts = [n['detail'] for n in notable[:3]]
    elif data:
        parts = []
        for c in CROP_COMMODITIES:
            if c in data and data[c].get('ge_change_1w') is not None:
                chg = data[c]['ge_change_1w']
                sign = '+' if chg >= 0 else ''
                parts.append(f"{c} {sign}{chg:.0f}")
        if parts:
            summary_parts.append(f"G/E changes: {', '.join(parts)}")

    return {
        'notable_changes': notable,
        'summary_parts': summary_parts,
        'data': data,
    }


# ------------------------------------------------------------------
# EIA Ethanol Deltas
# ------------------------------------------------------------------

def _delta_eia_ethanol(conn) -> Optional[Dict]:
    """
    Compute ethanol production and stocks week-over-week changes.
    """
    cur = conn.cursor()

    # Get two most recent weeks from silver.ethanol_weekly
    cur.execute("""
        WITH ranked AS (
            SELECT week_ending, production_kbd, stocks_kb, ma_4wk_production,
                   ROW_NUMBER() OVER (ORDER BY week_ending DESC) as rn
            FROM silver.ethanol_weekly
            WHERE production_kbd IS NOT NULL
        )
        SELECT
            r1.week_ending as latest_week,
            r1.production_kbd,
            r1.stocks_kb,
            r1.ma_4wk_production,
            r2.week_ending as prior_week,
            r2.production_kbd as production_kbd_prior,
            r2.stocks_kb as stocks_kb_prior,
            r1.production_kbd - r2.production_kbd as production_change,
            r1.stocks_kb - r2.stocks_kb as stocks_change
        FROM ranked r1
        LEFT JOIN ranked r2 ON r2.rn = 2
        WHERE r1.rn = 1
    """)
    row = cur.fetchone()

    if not row or row['production_kbd'] is None:
        return None

    data = {
        'week_ending': str(row['latest_week']),
        'production_kbd': float(row['production_kbd']),
        'production_kbd_prior': float(row['production_kbd_prior']) if row['production_kbd_prior'] else None,
        'production_change_kbd': float(row['production_change']) if row['production_change'] else None,
        'stocks_kb': float(row['stocks_kb']) if row['stocks_kb'] else None,
        'stocks_kb_prior': float(row['stocks_kb_prior']) if row['stocks_kb_prior'] else None,
        'stocks_change_kb': float(row['stocks_change']) if row['stocks_change'] else None,
        'ma_4wk_production': float(row['ma_4wk_production']) if row['ma_4wk_production'] else None,
    }

    notable = []
    summary_parts = []

    prod_chg = data.get('production_change_kbd')
    if prod_chg is not None:
        sign = '+' if prod_chg >= 0 else ''
        summary_parts.append(
            f"Ethanol production {data['production_kbd']:.0f} kbd ({sign}{prod_chg:.0f})"
        )
        # Flag large production changes (>30 kbd)
        if abs(prod_chg) >= 30:
            notable.append({
                'metric': 'production',
                'flag': 'large_production_change',
                'detail': f"Ethanol production {sign}{prod_chg:.0f} kbd",
            })

    stocks_chg = data.get('stocks_change_kb')
    if stocks_chg is not None:
        sign = '+' if stocks_chg >= 0 else ''
        summary_parts.append(f"Stocks {sign}{stocks_chg:.0f} kb")
        if abs(stocks_chg) >= 500:
            notable.append({
                'metric': 'stocks',
                'flag': 'large_stocks_change',
                'detail': f"Ethanol stocks {sign}{stocks_chg:.0f} kb",
            })

    return {
        'notable_changes': notable,
        'summary_parts': summary_parts,
        'data': {'ethanol': data},
    }


# ------------------------------------------------------------------
# NASS Processing Deltas
# ------------------------------------------------------------------

def _delta_nass_processing(conn) -> Optional[Dict]:
    """
    Compute month-over-month and year-over-year changes for
    soybean crush and corn grind data.
    """
    cur = conn.cursor()

    # Get two most recent months per commodity/source
    cur.execute("""
        WITH ranked AS (
            SELECT commodity, source, attribute, calendar_year, month, realized_value, unit,
                   ROW_NUMBER() OVER (
                       PARTITION BY commodity, source, attribute
                       ORDER BY calendar_year DESC, month DESC
                   ) as rn
            FROM silver.monthly_realized
            WHERE attribute = 'crush'
              AND source IN ('NASS_SOY_CRUSH', 'NASS_GRAIN_CRUSH')
        )
        SELECT
            r1.commodity, r1.source, r1.unit,
            r1.calendar_year as yr, r1.month as mo,
            r1.realized_value as value,
            r2.calendar_year as yr_prior, r2.month as mo_prior,
            r2.realized_value as value_prior,
            r1.realized_value - r2.realized_value as change_mom
        FROM ranked r1
        LEFT JOIN ranked r2 ON r1.commodity = r2.commodity
            AND r1.source = r2.source
            AND r1.attribute = r2.attribute
            AND r2.rn = 2
        WHERE r1.rn = 1
        ORDER BY r1.commodity
    """)
    rows = cur.fetchall()

    if not rows:
        return None

    data = {}
    notable = []
    summary_parts = []

    for row in rows:
        commodity = row['commodity']
        value = float(row['value'])
        value_prior = float(row['value_prior']) if row['value_prior'] else None
        change = float(row['change_mom']) if row['change_mom'] else None
        unit = row['unit'] or ''

        entry = {
            'period': f"{row['yr']}-{row['mo']:02d}",
            'value': value,
            'unit': unit,
            'prior_period': f"{row['yr_prior']}-{row['mo_prior']:02d}" if row['yr_prior'] else None,
            'value_prior': value_prior,
            'change_mom': change,
        }

        if change is not None and value_prior and value_prior != 0:
            entry['change_pct'] = round(change / value_prior * 100, 1)

        # Flag large month-over-month changes (>10%)
        pct = entry.get('change_pct')
        if pct is not None and abs(pct) >= 10:
            direction = 'up' if pct > 0 else 'down'
            notable.append({
                'commodity': commodity,
                'flag': f'large_monthly_{direction}',
                'detail': f"{commodity} crush {direction} {abs(pct):.1f}% MoM",
            })

        key = f"{commodity}_{row['source']}"
        data[key] = entry

    if notable:
        summary_parts = [n['detail'] for n in notable[:3]]
    elif data:
        for key, d in data.items():
            if d.get('change_pct') is not None:
                commodity = key.split('_')[0]
                sign = '+' if d['change_pct'] >= 0 else ''
                summary_parts.append(f"{commodity} crush {sign}{d['change_pct']:.1f}% MoM")

    return {
        'notable_changes': notable,
        'summary_parts': summary_parts,
        'data': data,
    }
