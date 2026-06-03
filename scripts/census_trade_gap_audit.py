"""Census trade gap auditor + targeted backfill.

Identifies (hs_code, flow) pairs that have a *suspicious internal gap* —
runs of N+ consecutive months with no data, surrounded by data on both
sides. Real-world tariff shutdowns of 12+ months are rare; the gap
pattern almost always indicates a backfill bug.

Uses bronze.census_trade_verified_empty (mig 131) to memoize confirmed-
empty (hs_code, flow, year) tuples so subsequent runs only chase NEW
gaps, not previously-verified-empty ones.

Two modes:
  --report          Audit findings, no API calls. Shows real gaps and
                    flags which years are already verified-empty.
  --fix             Backfill the gap years for each suspicious pair.
                    Records confirmed-empty results to skip on next run.

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


def load_verified_empty():
    """Returns set of (hs_code, flow, year) tuples already confirmed empty."""
    verified = set()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                'SELECT hs_code, flow, year FROM bronze.census_trade_verified_empty'
            )
            for r in cur.fetchall():
                verified.add((r['hs_code'], r['flow'], r['year']))
    return verified


def record_verified_empty(hs_code: str, flow: str, year: int):
    """Mark (hs_code, flow, year) as verified empty by Census API.
    Idempotent via the unique constraint."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO bronze.census_trade_verified_empty
                    (hs_code, flow, year, verified_count)
                VALUES (%s, %s, %s, 1)
                ON CONFLICT (hs_code, flow, year) DO UPDATE SET
                    verified_count = bronze.census_trade_verified_empty.verified_count + 1,
                    verified_at = NOW()
                """,
                (hs_code, flow, year),
            )
            conn.commit()


def audit(min_gap=12, start=(2013, 1), end=(2026, 3)):
    """Returns list of (hs_code, flow, years_to_fill, total_months_missing,
    years_verified_empty)."""
    expected = expected_months(start, end)
    verified = load_verified_empty()
    fix_list = []
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                'SELECT DISTINCT hs_code, flow FROM bronze.census_trade '
                'ORDER BY hs_code, flow'
            )
            for r in cur.fetchall():
                hs, flow = r['hs_code'], r['flow']
                cur.execute(
                    'SELECT DISTINCT year, month FROM bronze.census_trade '
                    'WHERE hs_code = %s AND flow = %s ORDER BY year, month',
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
                    all_years = sorted({y for (y, m) in sus})
                    years_verified = sorted([y for y in all_years
                                              if (hs, flow, y) in verified])
                    years_to_fill = sorted([y for y in all_years
                                             if (hs, flow, y) not in verified])
                    fix_list.append((hs, flow, years_to_fill, len(sus), years_verified))
    return fix_list


def fix(fix_list):
    """Backfill years_to_fill for each (hs, flow) in fix_list. Records
    Census-empty results to bronze.census_trade_verified_empty."""
    c = CensusTradeCollector()
    total_added = 0
    total_verified = 0
    for hs, flow, years_to_fill, _, _ in fix_list:
        for yr in years_to_fill:
            r = c.collect(
                start_date=date(yr, 1, 1),
                end_date=date(yr, 12, 31),
                flow=flow,
                hs_codes=[hs],
            )
            n = r.records_fetched or 0
            if n == 0:
                record_verified_empty(hs, flow, yr)
                total_verified += 1
                log.info(f'  {hs:14s} {flow:8s} {yr}: +0  (recorded as verified-empty)')
            else:
                total_added += n
                log.info(f'  {hs:14s} {flow:8s} {yr}: +{n}')
    return total_added, total_verified


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--report', action='store_true',
                        help='Print audit, no API calls.')
    parser.add_argument('--fix', action='store_true',
                        help='Backfill identified gaps.')
    parser.add_argument('--min-gap', type=int, default=12,
                        help='Min consecutive missing months to flag (default 12).')
    args = parser.parse_args()
    if not (args.report or args.fix):
        parser.error('Must specify --report or --fix')

    log.info(f'Auditing (min_gap={args.min_gap})...')
    fix_list = audit(min_gap=args.min_gap)
    print(f'\n{len(fix_list)} HS/flow pairs flagged:')
    n_to_fill = 0
    n_verified_already = 0
    for hs, flow, years_to_fill, total, years_verified in fix_list:
        bits = []
        if years_to_fill:
            bits.append(f'fill: {years_to_fill}')
            n_to_fill += len(years_to_fill)
        if years_verified:
            bits.append(f'verified-empty: {years_verified}')
            n_verified_already += len(years_verified)
        print(f'  {hs:14s} {flow:8s} {total:>3} months  |  {"  ".join(bits)}')
    print(f'\nAPI calls needed: {n_to_fill}  (already verified-empty: {n_verified_already})')

    if args.fix and fix_list and n_to_fill > 0:
        log.info(f'\nBackfilling {n_to_fill} (hs_code, year) requests...')
        added, verified = fix(fix_list)
        log.info(f'Records added: {added}')
        log.info(f'New verified-empty tuples: {verified}')

        log.info('\nRe-auditing post-fix...')
        remaining = audit(min_gap=args.min_gap)
        unverified_remaining = [(hs, flow, ytf, tot, yv)
                                 for hs, flow, ytf, tot, yv in remaining
                                 if ytf]  # only show ones with unverified years left
        if not unverified_remaining:
            log.info('All gaps either filled or verified-empty.')
        else:
            log.warning(f'{len(unverified_remaining)} pairs have unverified gap years remaining:')
            for hs, flow, years_to_fill, total, _ in unverified_remaining:
                print(f'  {hs:14s} {flow:8s} fill: {years_to_fill}')
    elif args.fix:
        log.info('Nothing to backfill — all flagged gaps are verified-empty.')


if __name__ == '__main__':
    main()
