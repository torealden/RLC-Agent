"""The Feedstock Report — weekly data pack.

Given a week_ending date (typically Friday), assemble the structured
data payloads for the auto-rendered sections: price dashboard, credit
stack, production tracker, IFV, and S&D watch.

Usage:
    from src.reports.feedstock_report.data_pack import build_data_pack
    pack = build_data_pack(week_ending=date(2026, 5, 22))
    # pack.prices, pack.credit_stack, pack.production, pack.ifv, pack.sd_watch

The pack object is meant to be passed straight to renderers AND to be
JSON-serialized for storage in reports.feedstock_section_content.data_snapshot.
"""

from __future__ import annotations

import logging
import os
import sys
from dataclasses import dataclass, asdict, field
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / '.env')
from src.services.database.db_config import get_connection

logger = logging.getLogger(__name__)


# =============================================================
# Canonical product list (Section 02 "Price Dashboard")
# =============================================================
# Each entry: (display_name, feedstock_code, region, source_hint, unit_suffix)
# Some entries map to credit_prices or fuel_prices columns instead of
# feedstock_prices — handled in the fetch logic by `source_table`.
#
# Status: placeholders flagged where Tore noted intl/specialty gaps in
# the v2 plan. is_placeholder=True surfaces these as "TBD" in the
# dashboard until data lands.

PRICE_DASHBOARD = [
    # ── Vegetable Oils ──
    {'group': 'Vegetable Oils', 'product': 'Soybean Oil',  'location': 'Iowa',       'source_table': 'feedstock', 'fs': 'SBO', 'region': 'iowa'},
    {'group': 'Vegetable Oils', 'product': 'Soybean Oil',  'location': 'Illinois',   'source_table': 'feedstock', 'fs': 'SBO', 'region': 'central_il'},
    {'group': 'Vegetable Oils', 'product': 'Soybean Oil',  'location': 'Indiana',    'source_table': 'feedstock', 'fs': 'SBO', 'region': 'indiana'},
    {'group': 'Vegetable Oils', 'product': 'Soybean Oil',  'location': 'US Gulf',    'source_table': 'feedstock', 'fs': 'SBO', 'region': 'us_gulf'},
    {'group': 'Vegetable Oils', 'product': 'Soybean Oil',  'location': 'US PNW',     'source_table': 'feedstock', 'fs': 'SBO', 'region': 'us_pnw'},
    {'group': 'Vegetable Oils', 'product': 'Soybean Oil',  'location': 'Brazil',     'source_table': 'feedstock', 'fs': 'SBO', 'region': 'brazil', 'is_placeholder': True},
    {'group': 'Vegetable Oils', 'product': 'Soybean Oil',  'location': 'Argentina',  'source_table': 'feedstock', 'fs': 'SBO', 'region': 'argentina', 'is_placeholder': True},
    {'group': 'Vegetable Oils', 'product': 'Canola Oil',   'location': 'West Coast', 'source_table': 'feedstock', 'fs': 'CO',  'region': 'west_coast'},
    {'group': 'Vegetable Oils', 'product': 'Canola Oil',   'location': 'US PNW',     'source_table': 'feedstock', 'fs': 'CO',  'region': 'us_pnw'},
    {'group': 'Vegetable Oils', 'product': 'Canola Oil',   'location': 'US Gulf',    'source_table': 'feedstock', 'fs': 'CO',  'region': 'us_gulf'},
    {'group': 'Vegetable Oils', 'product': 'Canola Oil',   'location': 'Canada',     'source_table': 'feedstock', 'fs': 'CO',  'region': 'canada_cnf'},
    {'group': 'Vegetable Oils', 'product': 'DCO',          'location': 'Iowa',       'source_table': 'feedstock', 'fs': 'DCO', 'region': 'iowa'},
    {'group': 'Vegetable Oils', 'product': 'DCO',          'location': 'Illinois',   'source_table': 'feedstock', 'fs': 'DCO', 'region': 'illinois'},
    {'group': 'Vegetable Oils', 'product': 'DCO',          'location': 'Indiana',    'source_table': 'feedstock', 'fs': 'DCO', 'region': 'indiana'},
    {'group': 'Vegetable Oils', 'product': 'DCO',          'location': 'US Gulf',    'source_table': 'feedstock', 'fs': 'DCO', 'region': 'us_gulf'},
    {'group': 'Vegetable Oils', 'product': 'DCO',          'location': 'Canada',     'source_table': 'feedstock', 'fs': 'DCO', 'region': 'canada', 'is_placeholder': True},
    {'group': 'Vegetable Oils', 'product': 'Palm Oil',     'location': 'US West Coast','source_table': 'feedstock', 'fs': 'PALM','region': 'us_west_coast'},
    {'group': 'Vegetable Oils', 'product': 'Palm Oil',     'location': 'Malaysia',   'source_table': 'feedstock', 'fs': 'PALM','region': 'malaysia'},
    {'group': 'Vegetable Oils', 'product': 'Palm Oil',     'location': 'Indonesia',  'source_table': 'feedstock', 'fs': 'PALM','region': 'indonesia'},
    {'group': 'Vegetable Oils', 'product': 'Palm Oil',     'location': 'Europe',     'source_table': 'feedstock', 'fs': 'PALM','region': 'europe', 'is_placeholder': True},

    # ── Animal Fats & Greases ──
    {'group': 'Animal Fats & Greases', 'product': 'Inedible Tallow', 'location': 'Chicago',   'source_table': 'feedstock', 'fs': 'BFT', 'region': 'chicago'},
    {'group': 'Animal Fats & Greases', 'product': 'Inedible Tallow', 'location': 'US Gulf',   'source_table': 'feedstock', 'fs': 'BFT', 'region': 'us_gulf', 'is_placeholder': True},
    {'group': 'Animal Fats & Greases', 'product': 'Inedible Tallow', 'location': 'West Coast','source_table': 'feedstock', 'fs': 'BFT', 'region': 'west_coast'},
    {'group': 'Animal Fats & Greases', 'product': 'Inedible Tallow', 'location': 'Brazil',    'source_table': 'feedstock', 'fs': 'BFT', 'region': 'brazil', 'is_placeholder': True},
    {'group': 'Animal Fats & Greases', 'product': 'Inedible Tallow', 'location': 'Australia', 'source_table': 'feedstock', 'fs': 'BFT', 'region': 'australia', 'is_placeholder': True},
    {'group': 'Animal Fats & Greases', 'product': 'Choice White Grease', 'location': 'Missouri River', 'source_table': 'feedstock', 'fs': 'CWG', 'region': 'missouri_river'},
    {'group': 'Animal Fats & Greases', 'product': 'Choice White Grease', 'location': 'West Coast',     'source_table': 'feedstock', 'fs': 'CWG', 'region': 'west_coast'},
    {'group': 'Animal Fats & Greases', 'product': 'Poultry Fat',         'location': 'Southeast',      'source_table': 'feedstock', 'fs': 'PF',  'region': 'southeast'},
    {'group': 'Animal Fats & Greases', 'product': 'Yellow Grease',       'location': 'Iowa/Illinois',  'source_table': 'feedstock', 'fs': 'YG',  'region': 'il_wi'},
    {'group': 'Animal Fats & Greases', 'product': 'Yellow Grease',       'location': 'Los Angeles',    'source_table': 'feedstock', 'fs': 'YG',  'region': 'los_angeles'},
    {'group': 'Animal Fats & Greases', 'product': 'UCO',                 'location': 'Iowa/Illinois',  'source_table': 'feedstock', 'fs': 'UCO', 'region': 'il_wi'},
    {'group': 'Animal Fats & Greases', 'product': 'UCO',                 'location': 'SoCal',          'source_table': 'feedstock', 'fs': 'UCO', 'region': 'socal'},
    {'group': 'Animal Fats & Greases', 'product': 'UCO',                 'location': 'China FOB',      'source_table': 'feedstock', 'fs': 'UCO', 'region': 'china', 'is_placeholder': True},
    {'group': 'Animal Fats & Greases', 'product': 'UCO',                 'location': 'Europe CIF',     'source_table': 'feedstock', 'fs': 'UCO', 'region': 'europe', 'is_placeholder': True},
    {'group': 'Animal Fats & Greases', 'product': 'Brown Grease',        'location': 'Domestic',       'source_table': 'feedstock', 'fs': 'BG',  'region': 'domestic', 'is_placeholder': True},

    # ── Fuels & Credits ──
    {'group': 'Fuels & Credits', 'product': 'ULSD',             'location': 'Gulf',         'source_table': 'fuel',   'col': 'ulsd_gulf'},
    {'group': 'Fuels & Credits', 'product': 'ULSD',             'location': 'NY Harbor',    'source_table': 'fuel',   'col': 'ulsd_nyharbor'},
    {'group': 'Fuels & Credits', 'product': 'Biodiesel',        'location': 'Upper Midwest','source_table': 'fuel',   'col': 'b100_upper_midwest'},
    {'group': 'Fuels & Credits', 'product': 'Biodiesel',        'location': 'Northeast',    'source_table': 'fuel',   'col': 'b100_northeast'},
    {'group': 'Fuels & Credits', 'product': 'Renewable Diesel', 'location': 'California',   'source_table': 'fuel',   'col': 'rd_california'},
    {'group': 'Fuels & Credits', 'product': 'Jet A',            'location': 'Spot',         'source_table': 'fuel',   'col': 'jet_a_spot'},
    {'group': 'Fuels & Credits', 'product': 'D4 RIN',           'location': 'EMTS',         'source_table': 'credit', 'col': 'd4_rin'},
    {'group': 'Fuels & Credits', 'product': 'D6 RIN',           'location': 'EMTS',         'source_table': 'credit', 'col': 'd6_rin'},
    {'group': 'Fuels & Credits', 'product': 'LCFS Credit',      'location': 'California',   'source_table': 'credit', 'col': 'lcfs_ca'},
    {'group': 'Fuels & Credits', 'product': 'LCFS Credit',      'location': 'Oregon',       'source_table': 'credit', 'col': 'cfp_or'},
    {'group': 'Fuels & Credits', 'product': 'LCFS Credit',      'location': 'Washington',   'source_table': 'credit', 'col': 'cfs_wa'},
]


# =============================================================
# Data classes
# =============================================================
@dataclass
class PriceRow:
    group: str
    product: str
    location: str
    week_ending: Optional[date]
    weekly_avg: Optional[float]
    wow_change_pct: Optional[float]
    mom_change_pct: Optional[float]
    yoy_change_pct: Optional[float]
    range_52w_low: Optional[float]
    range_52w_high: Optional[float]
    unit: str
    source: str
    is_placeholder: bool = False


@dataclass
class DataPack:
    week_ending: date
    issue_number: Optional[int]
    prices: List[PriceRow] = field(default_factory=list)
    # Below populated by future builders — stubbed for now
    credit_stack: List[Dict[str, Any]] = field(default_factory=list)
    production: Dict[str, Any] = field(default_factory=dict)
    ifv: List[Dict[str, Any]] = field(default_factory=list)
    sd_watch: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'week_ending': self.week_ending.isoformat(),
            'issue_number': self.issue_number,
            'prices': [asdict(p) for p in self.prices],
            'credit_stack': self.credit_stack,
            'production': self.production,
            'ifv': self.ifv,
            'sd_watch': self.sd_watch,
        }


# =============================================================
# Price fetchers
# =============================================================
def _fetch_feedstock_price(cur, fs_code: str, region: str, target_date: date) -> Optional[Dict[str, Any]]:
    """Latest feedstock price on or before target_date for (fs, region)."""
    cur.execute("""
        SELECT price_date, price_per_lb, source
        FROM bronze.feedstock_prices
        WHERE feedstock_code = %s AND region = %s
          AND price_date <= %s AND price_per_lb > 0
        ORDER BY price_date DESC LIMIT 1
    """, (fs_code, region, target_date))
    r = cur.fetchone()
    if not r:
        return None
    return {'date': r['price_date'], 'value': float(r['price_per_lb']), 'source': r['source']}


def _fetch_table_col_price(cur, table: str, col: str, target_date: date) -> Optional[Dict[str, Any]]:
    """Latest value of a specific column in fuel_prices or credit_prices on/before target_date."""
    cur.execute(f"""
        SELECT price_date, {col} AS val, source
        FROM bronze.{table}_prices
        WHERE price_date <= %s AND {col} IS NOT NULL
        ORDER BY price_date DESC LIMIT 1
    """, (target_date,))
    r = cur.fetchone()
    if not r or r['val'] is None:
        return None
    return {'date': r['price_date'], 'value': float(r['val']), 'source': r['source']}


def _fetch_52w_range(cur, source_table: str, **kwargs) -> Optional[Dict[str, float]]:
    """52-week min/max for the given series, ending at target_date."""
    target_date = kwargs['target_date']
    range_start = target_date - timedelta(weeks=52)
    if source_table == 'feedstock':
        cur.execute("""
            SELECT MIN(price_per_lb) AS lo, MAX(price_per_lb) AS hi
            FROM bronze.feedstock_prices
            WHERE feedstock_code = %s AND region = %s
              AND price_date BETWEEN %s AND %s
              AND price_per_lb > 0
        """, (kwargs['fs'], kwargs['region'], range_start, target_date))
    else:  # fuel or credit
        col = kwargs['col']
        cur.execute(f"""
            SELECT MIN({col}) AS lo, MAX({col}) AS hi
            FROM bronze.{source_table}_prices
            WHERE price_date BETWEEN %s AND %s AND {col} IS NOT NULL
        """, (range_start, target_date))
    r = cur.fetchone()
    if not r or r['lo'] is None:
        return None
    return {'low': float(r['lo']), 'high': float(r['hi'])}


def _pct_change(curr: Optional[float], prior: Optional[float]) -> Optional[float]:
    if curr is None or prior is None or prior == 0:
        return None
    return round((curr - prior) / prior * 100, 2)


def build_price_dashboard(week_ending: date) -> List[PriceRow]:
    """Build the price dashboard rows for one week."""
    rows: List[PriceRow] = []
    with get_connection() as conn:
        with conn.cursor() as cur:
            for entry in PRICE_DASHBOARD:
                src_table = entry['source_table']
                is_ph = entry.get('is_placeholder', False)

                # Skip the DB query for known placeholders to avoid noise
                if is_ph:
                    rows.append(PriceRow(
                        group=entry['group'], product=entry['product'], location=entry['location'],
                        week_ending=None, weekly_avg=None, wow_change_pct=None,
                        mom_change_pct=None, yoy_change_pct=None,
                        range_52w_low=None, range_52w_high=None,
                        unit='$/lb' if src_table == 'feedstock' else '',
                        source='placeholder', is_placeholder=True,
                    ))
                    continue

                # Fetch current week
                if src_table == 'feedstock':
                    curr = _fetch_feedstock_price(cur, entry['fs'], entry['region'], week_ending)
                    rng_args = {'fs': entry['fs'], 'region': entry['region'], 'target_date': week_ending}
                    unit = '$/lb'
                elif src_table == 'fuel':
                    curr = _fetch_table_col_price(cur, 'fuel', entry['col'], week_ending)
                    rng_args = {'col': entry['col'], 'target_date': week_ending}
                    unit = '$/gal' if 'ulsd' in entry['col'] or 'b100' in entry['col'] or 'rd_' in entry['col'] else '$/gal'
                elif src_table == 'credit':
                    curr = _fetch_table_col_price(cur, 'credit', entry['col'], week_ending)
                    rng_args = {'col': entry['col'], 'target_date': week_ending}
                    unit = '$/credit'
                else:
                    curr = None
                    rng_args = {}
                    unit = ''

                if not curr:
                    rows.append(PriceRow(
                        group=entry['group'], product=entry['product'], location=entry['location'],
                        week_ending=None, weekly_avg=None, wow_change_pct=None,
                        mom_change_pct=None, yoy_change_pct=None,
                        range_52w_low=None, range_52w_high=None,
                        unit=unit, source='no_data', is_placeholder=False,
                    ))
                    continue

                # Fetch prior periods
                def _hist(d):
                    if src_table == 'feedstock':
                        return _fetch_feedstock_price(cur, entry['fs'], entry['region'], d)
                    return _fetch_table_col_price(cur, src_table, entry['col'], d)

                wow = _hist(week_ending - timedelta(weeks=1))
                mom = _hist(week_ending - timedelta(weeks=4))
                yoy = _hist(week_ending - timedelta(weeks=52))

                # 52-week range
                rng = _fetch_52w_range(cur, src_table, **rng_args)

                rows.append(PriceRow(
                    group=entry['group'], product=entry['product'], location=entry['location'],
                    week_ending=curr['date'], weekly_avg=curr['value'],
                    wow_change_pct=_pct_change(curr['value'], wow['value'] if wow else None),
                    mom_change_pct=_pct_change(curr['value'], mom['value'] if mom else None),
                    yoy_change_pct=_pct_change(curr['value'], yoy['value'] if yoy else None),
                    range_52w_low=rng['low'] if rng else None,
                    range_52w_high=rng['high'] if rng else None,
                    unit=unit, source=curr['source'] or 'unknown', is_placeholder=False,
                ))
    return rows


# =============================================================
# Top-level builder
# =============================================================
def build_data_pack(week_ending: date, issue_number: Optional[int] = None) -> DataPack:
    """Assemble the weekly data pack for a single issue.

    For now this only fills `prices`. Credit stack, production, IFV, and
    S&D will be added as their respective renderers are built.
    """
    logger.info(f"Building data pack for week ending {week_ending}")
    pack = DataPack(week_ending=week_ending, issue_number=issue_number)
    pack.prices = build_price_dashboard(week_ending)
    logger.info(f"  prices: {len(pack.prices)} rows "
                f"({sum(1 for p in pack.prices if p.weekly_avg is not None)} with data, "
                f"{sum(1 for p in pack.prices if p.is_placeholder)} placeholders)")
    return pack


# =============================================================
# Snapshot persistence
# =============================================================
def save_price_snapshot(issue_id: int, pack: DataPack) -> int:
    """Insert price-dashboard rows into reports.feedstock_price_dashboard_snapshot.
    Returns number of rows inserted/updated."""
    n = 0
    with get_connection() as conn:
        with conn.cursor() as cur:
            for p in pack.prices:
                cur.execute("""
                    INSERT INTO reports.feedstock_price_dashboard_snapshot
                        (issue_id, product, location, week_ending, weekly_avg,
                         wow_change_pct, mom_change_pct, yoy_change_pct,
                         range_52w_low, range_52w_high, unit, source, is_placeholder)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (issue_id, product, location) DO UPDATE SET
                        week_ending = EXCLUDED.week_ending,
                        weekly_avg = EXCLUDED.weekly_avg,
                        wow_change_pct = EXCLUDED.wow_change_pct,
                        mom_change_pct = EXCLUDED.mom_change_pct,
                        yoy_change_pct = EXCLUDED.yoy_change_pct,
                        range_52w_low = EXCLUDED.range_52w_low,
                        range_52w_high = EXCLUDED.range_52w_high,
                        source = EXCLUDED.source,
                        is_placeholder = EXCLUDED.is_placeholder
                """, (
                    issue_id, p.product, p.location,
                    p.week_ending if p.week_ending else pack.week_ending,
                    p.weekly_avg, p.wow_change_pct, p.mom_change_pct, p.yoy_change_pct,
                    p.range_52w_low, p.range_52w_high, p.unit, p.source, p.is_placeholder,
                ))
                n += 1
            conn.commit()
    return n


# =============================================================
# CLI smoke test
# =============================================================
if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Build feedstock report data pack')
    parser.add_argument('--week-ending', type=str, default=None,
                        help='Friday of the week to build (YYYY-MM-DD). Default: most recent Friday.')
    args = parser.parse_args()

    if args.week_ending:
        week = date.fromisoformat(args.week_ending)
    else:
        today = date.today()
        # Last Friday
        days_back = (today.weekday() - 4) % 7
        if days_back == 0 and today.weekday() != 4:
            days_back = 7
        week = today - timedelta(days=days_back)

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)s %(message)s')
    pack = build_data_pack(week_ending=week)
    print(f"\nWeek ending: {pack.week_ending}")
    print(f"Price rows: {len(pack.prices)}")
    print()
    print(f"{'Group':22} {'Product':18} {'Location':22} {'Latest':>9} {'WoW%':>7} {'MoM%':>7} {'YoY%':>7}  source")
    print('-' * 130)
    for p in pack.prices:
        if p.weekly_avg is None:
            tag = '[PLACEHOLDER]' if p.is_placeholder else '[no data]'
            print(f"  {p.group[:20]:22} {p.product[:16]:18} {p.location[:20]:22} {tag}")
        else:
            print(f"  {p.group[:20]:22} {p.product[:16]:18} {p.location[:20]:22} "
                  f"{p.weekly_avg:>9.4f} {p.wow_change_pct or 0:>+6.1f}% "
                  f"{p.mom_change_pct or 0:>+6.1f}% {p.yoy_change_pct or 0:>+6.1f}%  {p.source}")
