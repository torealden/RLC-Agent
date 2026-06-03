"""Census trade gap auditor + targeted backfill.

Identifies (hs_code, flow) pairs that have a *suspicious internal gap* —
runs of 12+ consecutive months with no data, surrounded by data on both
sides. Real-world tariff shutdowns of 12+ months are rare; the gap
pattern almost always indicates a backfill bug.

Two modes:
  --report          Just print the audit findings, no API calls.
  --fix             Backfill the gap years for each suspicious pair.

Examples:
  python scripts/census_trade_gap_audit.py --report
  python scripts/census_trade_gap_audit.py --fix
  python scripts/census_trade_gap_audit.py --fix --min-gap 6

Run periodically (e.g., monthly after Census FT-900 release) to catch
new gaps before they affect downstream consumers.
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
load_dotenv(PROJECT_ROOT / '.env')

from src.services.database.db_config import get_connection
from src.agents.collectors.us.census_trade_collector import CensusTradeCollector

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
log = logging.getLogger('census_trade_gap_audit')


def consecutive_groups(sorted_months):
    """Group sorted [(year, month), ...] into runs of consecutive months."""
    groups = []
    cur = []
    for ym in sorted_months:
        if cur:
            py, pm = cur[-1]
            nxt = (py, pm + 1) if pm < 12 else (py + 1, 1)
            if ym == nxt:
                cur.append(ym)
            else:
                groups.append(cur)
                cur = [ym]
        else:
            cur = [ym]
    if cur:
        groups.append(cur)
    return groups


def expected_months(start=(2013, 1), end=(2026, 3)):
    out = []
    y, m = start
    while (y, m) <= end:
        out.append((y, m))
        m += 1
        if m > 12:
            m = 1
            y += 1
    return out


def audit(min_gap=12, start=(2013, 1), end=(2026, 3)):
    """Returns list of (hs_code, flow, years_to_fill, total_months_missing)."""
    expected = expected_months(start, end)
    fix_list = []
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT hs_code, flow FROM bronze.census_trade "
                "ORDER BY hs_code, flow"
            )
            for r in cur.fetchall():
                hs, flow = r['hs_code'], r['flow']
                cur.execute(
                    "SELECT DISTINCT year, month FROM bronze.census_trade "
                    "WHERE hs_code = %s AND flow = %s ORDER BY year, month",
                    (hs, flow),
                )
                actual = {(rr['year'], rr['month']) for rr in cur.fetchall()}
                if not actual:
                    continue
                min_a, max_a = min(actual), max(actual)
                in_range = [ym for ym in expected if min_a <= ym <= max_a]
                missing = sorted(ym for ym in in_range if ym not in actual)
                big = [g for g in consecutive_groups(missing) if len(g) >= min_gap]
                if big:
                    sus = [ym for g in big for ym in g]
                    years = sorted({y for (y, m) in sus})
                    fix_list.append((hs, flow, years, len(sus)))
    return fix_list


def fix(fix_list):
    """Backfill years_to_fill for each (hs, flow) in fix_list."""
    c = CensusTradeCollector()
    total_added = 0
    for hs, flow, years, _ in fix_list:
        for yr in years:
            r = c.collect(
                start_date=date(yr, 1, 1),
                end_date=date(yr, 12, 31),
                flow=flow,
                hs_codes=[hs],
            )
            n = r.records_fetched or 0
            total_added += n
            log.info(f'  {hs:14s} {flow:8s} {yr}: +{n}')
    return total_added


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--report', action='store_true',
                        help='Print audit, do not backfill.')
    parser.add_argument('--fix', action='store_true',
                        help='Backfill the identified gaps.')
    parser.add_argument('--min-gap', type=int, default=12,
                        help='Min consecutive missing months to flag (default 12).')
    args = parser.parse_args()
    if not (args.report or args.fix):
        parser.error('Must specify --report or --fix')

    log.info(f'Auditing (min_gap={args.min_gap})...')
    fix_list = audit(min_gap=args.min_gap)
    print(f'\n{len(fix_list)} HS/flow pairs flagged:')
    for hs, flow, years, total in fix_list:
        print(f'  {hs:14s} {flow:8s} {total:>3} missing months  years: {years}')

    if args.fix and fix_list:
        log.info(f'\nBackfilling {sum(len(y) for _,_,y,_ in fix_list)} (hs_code, year) requests...')
        n = fix(fix_list)
        log.info(f'Total records added: {n}')

        # Re-audit
        log.info('\nRe-auditing post-fix...')
        remaining = audit(min_gap=args.min_gap)
        if not remaining:
            log.info('All gaps resolved.')
        else:
            log.warning(f'{len(remaining)} pairs still have gaps (may be real-world gaps with no Census data):')
            for hs, flow, years, total in remaining:
                print(f'  {hs:14s} {flow:8s} {total:>3} months  years: {years}')


if __name__ == '__main__':
    main()
