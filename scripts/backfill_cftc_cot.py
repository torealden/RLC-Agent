"""
CFTC COT Historical Backfill Script

Downloads CFTC bulk historical data and loads into bronze.cftc_cot.

Sources:
- Disaggregated Futures Only (2006-present): managed money, producer, swap dealer, other
  URL pattern: https://www.cftc.gov/files/dea/history/fut_disagg_txt_{year}.zip
- Legacy Combined (1986-2005): commercial, non-commercial, non-reportable
  URL pattern: https://www.cftc.gov/files/dea/history/deacot{year}.zip

Usage:
    python scripts/backfill_cftc_cot.py                     # Disaggregated 2006-2026
    python scripts/backfill_cftc_cot.py --legacy             # Also backfill legacy 1986-2005
    python scripts/backfill_cftc_cot.py --year 2024          # Single year disaggregated
    python scripts/backfill_cftc_cot.py --legacy --year 2000 # Single year legacy
"""

import argparse
import csv
import io
import logging
import os
import sys
import tempfile
import time
import zipfile
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from src.services.database.db_config import get_connection

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Contract code -> commodity name mapping
# These are the CFTC_Contract_Market_Code values we care about
# ---------------------------------------------------------------------------
DISAGG_CONTRACT_MAP = {
    # Grains & Oilseeds
    '002602': 'corn',
    '005602': 'soybeans',
    '001602': 'wheat_srw',
    '001612': 'wheat_hrw',
    '001626': 'wheat_hrs',
    '007601': 'soybean_oil',
    '026603': 'soybean_meal',
    '033661': 'cotton',
    '080732': 'sugar',
    '083731': 'coffee',
    '073732': 'cocoa',
    '039601': 'rough_rice',
    '004603': 'oats',
    # Livestock
    '057642': 'live_cattle',
    '054642': 'lean_hogs',
    '061641': 'feeder_cattle',
    # Energy
    '067651': 'crude_oil',
    '023651': 'natural_gas',
    '111659': 'rbob_gasoline',
    '022651': 'ny_harbor_ulsd',
    # Biofuel adjacent
    '025651': 'ethanol',
}

# Legacy report uses the same contract codes
LEGACY_CONTRACT_MAP = DISAGG_CONTRACT_MAP.copy()

# Exchange lookup from contract code
EXCHANGE_MAP = {
    '002602': 'CBOT', '005602': 'CBOT', '001602': 'CBOT', '001612': 'CBOT',
    '001626': 'MGEX', '007601': 'CBOT', '026603': 'CBOT', '033661': 'ICE',
    '080732': 'ICE', '083731': 'ICE', '073732': 'ICE', '039601': 'CBOT',
    '004603': 'CBOT', '057642': 'CME', '054642': 'CME', '061641': 'CME',
    '067651': 'NYMEX', '023651': 'NYMEX', '111659': 'NYMEX', '022651': 'NYMEX',
    '025651': 'NYMEX',
}


def safe_int(value: Any) -> Optional[int]:
    """Safely convert a value to int, returning None on failure."""
    if value is None or str(value).strip() == '':
        return None
    try:
        return int(float(str(value).strip().replace(',', '')))
    except (ValueError, TypeError):
        return None


def download_zip(url: str, timeout: int = 120) -> Optional[bytes]:
    """Download a ZIP file, return bytes or None on failure."""
    try:
        logger.info(f"  Downloading {url}")
        r = requests.get(url, timeout=timeout)
        if r.status_code == 200:
            logger.info(f"  Downloaded {len(r.content) / 1024:.0f} KB")
            return r.content
        else:
            logger.warning(f"  HTTP {r.status_code} for {url}")
            return None
    except Exception as e:
        logger.error(f"  Download failed: {e}")
        return None


def parse_disaggregated_csv(zip_bytes: bytes, year: int) -> List[Dict]:
    """Parse a disaggregated futures-only ZIP into records for bronze.cftc_cot."""
    records = []
    try:
        z = zipfile.ZipFile(io.BytesIO(zip_bytes))
        fname = z.namelist()[0]
        with z.open(fname) as f:
            reader = csv.DictReader(io.TextIOWrapper(f, encoding='utf-8'))
            for row in reader:
                # Filter: FutOnly only
                if row.get('FutOnly_or_Combined', '').strip() != 'FutOnly':
                    continue

                code = row.get('CFTC_Contract_Market_Code', '').strip()
                if code not in DISAGG_CONTRACT_MAP:
                    continue

                commodity = DISAGG_CONTRACT_MAP[code]
                exchange = EXCHANGE_MAP.get(code, '')

                # Handle both date column names (changed ~2013)
                report_date = row.get('Report_Date_as_YYYY-MM-DD', '').strip()
                if not report_date:
                    # Older files (2006-2012) use column name MM_DD_YYYY
                    # but the actual data may be YYYY-MM-DD or MM/DD/YYYY
                    raw_date = row.get('Report_Date_as_MM_DD_YYYY', '').strip()
                    if raw_date:
                        # Try ISO format first, then MM/DD/YYYY
                        if len(raw_date) == 10 and raw_date[4] == '-':
                            report_date = raw_date  # Already YYYY-MM-DD
                        else:
                            try:
                                dt = datetime.strptime(raw_date, '%m/%d/%Y')
                                report_date = dt.strftime('%Y-%m-%d')
                            except ValueError:
                                pass

                as_of_date = row.get('As_of_Date_In_Form_YYMMDD', '').strip()

                if not report_date:
                    continue

                # Positions (use _All columns)
                mm_long = safe_int(row.get('M_Money_Positions_Long_All'))
                mm_short = safe_int(row.get('M_Money_Positions_Short_All'))
                mm_spread = safe_int(row.get('M_Money_Positions_Spread_All'))
                prod_long = safe_int(row.get('Prod_Merc_Positions_Long_All'))
                prod_short = safe_int(row.get('Prod_Merc_Positions_Short_All'))
                swap_long = safe_int(row.get('Swap_Positions_Long_All'))
                swap_short = safe_int(row.get('Swap__Positions_Short_All'))
                swap_spread = safe_int(row.get('Swap__Positions_Spread_All'))
                other_long = safe_int(row.get('Other_Rept_Positions_Long_All'))
                other_short = safe_int(row.get('Other_Rept_Positions_Short_All'))
                nonrept_long = safe_int(row.get('NonRept_Positions_Long_All'))
                nonrept_short = safe_int(row.get('NonRept_Positions_Short_All'))
                open_interest = safe_int(row.get('Open_Interest_All'))

                # Changes
                mm_long_chg = safe_int(row.get('Change_in_M_Money_Long_All'))
                mm_short_chg = safe_int(row.get('Change_in_M_Money_Short_All'))

                # Compute nets
                mm_net = (mm_long or 0) - (mm_short or 0)
                mm_net_change = (mm_long_chg or 0) - (mm_short_chg or 0)
                prod_net = (prod_long or 0) - (prod_short or 0)
                swap_net = (swap_long or 0) - (swap_short or 0)

                records.append({
                    'report_date': report_date,
                    'as_of_date': as_of_date,
                    'commodity': commodity,
                    'exchange': exchange,
                    'contract_code': code,
                    'mm_long': mm_long,
                    'mm_short': mm_short,
                    'mm_spread': mm_spread,
                    'mm_net': mm_net,
                    'mm_net_change': mm_net_change,
                    'prod_long': prod_long,
                    'prod_short': prod_short,
                    'prod_net': prod_net,
                    'swap_long': swap_long,
                    'swap_short': swap_short,
                    'swap_spread': swap_spread,
                    'swap_net': swap_net,
                    'other_long': other_long,
                    'other_short': other_short,
                    'nonrept_long': nonrept_long,
                    'nonrept_short': nonrept_short,
                    'open_interest': open_interest,
                    'report_type': 'disaggregated',
                    'source': 'CFTC',
                })
    except Exception as e:
        logger.error(f"  Parse error for {year}: {e}")
    return records


def parse_legacy_csv(zip_bytes: bytes, year: int) -> List[Dict]:
    """Parse a legacy combined ZIP into records for bronze.cftc_cot."""
    records = []
    try:
        z = zipfile.ZipFile(io.BytesIO(zip_bytes))
        fname = z.namelist()[0]
        with z.open(fname) as f:
            reader = csv.DictReader(io.TextIOWrapper(f, encoding='utf-8'))
            for row in reader:
                code = row.get('CFTC Contract Market Code', '').strip()
                if code not in LEGACY_CONTRACT_MAP:
                    continue

                commodity = LEGACY_CONTRACT_MAP[code]
                exchange = EXCHANGE_MAP.get(code, '')
                report_date = row.get('As of Date in Form YYYY-MM-DD', '').strip()
                as_of_date = row.get('As of Date in Form YYMMDD', '').strip()

                if not report_date:
                    continue

                # Legacy: Non-Commercial -> mm_ fields, Commercial -> prod_ fields
                noncomm_long = safe_int(row.get('Noncommercial Positions-Long (All)'))
                noncomm_short = safe_int(row.get('Noncommercial Positions-Short (All)'))
                noncomm_spread = safe_int(row.get('Noncommercial Positions-Spreading (All)'))
                comm_long = safe_int(row.get('Commercial Positions-Long (All)'))
                comm_short = safe_int(row.get('Commercial Positions-Short (All)'))
                nonrept_long = safe_int(row.get('Nonreportable Positions-Long (All)'))
                nonrept_short = safe_int(row.get('Nonreportable Positions-Short (All)'))
                open_interest = safe_int(row.get('Open Interest (All)'))

                # Changes
                noncomm_long_chg = safe_int(row.get('Change in Noncommercial-Long (All)'))
                noncomm_short_chg = safe_int(row.get('Change in Noncommercial-Short (All)'))

                noncomm_net = (noncomm_long or 0) - (noncomm_short or 0)
                comm_net = (comm_long or 0) - (comm_short or 0)
                noncomm_net_chg = (noncomm_long_chg or 0) - (noncomm_short_chg or 0)

                records.append({
                    'report_date': report_date,
                    'as_of_date': as_of_date,
                    'commodity': commodity,
                    'exchange': exchange,
                    'contract_code': code,
                    'mm_long': noncomm_long,
                    'mm_short': noncomm_short,
                    'mm_spread': noncomm_spread,
                    'mm_net': noncomm_net,
                    'mm_net_change': noncomm_net_chg,
                    'prod_long': comm_long,
                    'prod_short': comm_short,
                    'prod_net': comm_net,
                    'swap_long': None,
                    'swap_short': None,
                    'swap_spread': None,
                    'swap_net': None,
                    'other_long': None,
                    'other_short': None,
                    'nonrept_long': nonrept_long,
                    'nonrept_short': nonrept_short,
                    'open_interest': open_interest,
                    'report_type': 'legacy',
                    'source': 'CFTC',
                })
    except Exception as e:
        logger.error(f"  Parse error for {year}: {e}")
    return records


def upsert_records(records: List[Dict]) -> int:
    """Upsert records into bronze.cftc_cot. Returns count inserted/updated."""
    if not records:
        return 0

    sql = """
        INSERT INTO bronze.cftc_cot (
            report_date, as_of_date, commodity, exchange, contract_code,
            mm_long, mm_short, mm_spread, mm_net, mm_net_change,
            prod_long, prod_short, prod_net,
            swap_long, swap_short, swap_spread, swap_net,
            other_long, other_short,
            nonrept_long, nonrept_short,
            open_interest, report_type, source, collected_at
        ) VALUES (
            %(report_date)s, %(as_of_date)s, %(commodity)s, %(exchange)s, %(contract_code)s,
            %(mm_long)s, %(mm_short)s, %(mm_spread)s, %(mm_net)s, %(mm_net_change)s,
            %(prod_long)s, %(prod_short)s, %(prod_net)s,
            %(swap_long)s, %(swap_short)s, %(swap_spread)s, %(swap_net)s,
            %(other_long)s, %(other_short)s,
            %(nonrept_long)s, %(nonrept_short)s,
            %(open_interest)s, %(report_type)s, %(source)s, NOW()
        )
        ON CONFLICT (report_date, commodity, report_type)
        DO UPDATE SET
            as_of_date = EXCLUDED.as_of_date,
            exchange = EXCLUDED.exchange,
            contract_code = EXCLUDED.contract_code,
            mm_long = EXCLUDED.mm_long,
            mm_short = EXCLUDED.mm_short,
            mm_spread = EXCLUDED.mm_spread,
            mm_net = EXCLUDED.mm_net,
            mm_net_change = EXCLUDED.mm_net_change,
            prod_long = EXCLUDED.prod_long,
            prod_short = EXCLUDED.prod_short,
            prod_net = EXCLUDED.prod_net,
            swap_long = EXCLUDED.swap_long,
            swap_short = EXCLUDED.swap_short,
            swap_spread = EXCLUDED.swap_spread,
            swap_net = EXCLUDED.swap_net,
            other_long = EXCLUDED.other_long,
            other_short = EXCLUDED.other_short,
            nonrept_long = EXCLUDED.nonrept_long,
            nonrept_short = EXCLUDED.nonrept_short,
            open_interest = EXCLUDED.open_interest,
            source = EXCLUDED.source,
            collected_at = NOW()
    """

    count = 0
    with get_connection() as conn:
        cur = conn.cursor()
        # Batch in chunks of 500
        batch_size = 500
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            for rec in batch:
                cur.execute(sql, rec)
            conn.commit()
            count += len(batch)
    return count


def backfill_disaggregated_historical() -> int:
    """Download the combined 2006-2016 historical file and load 2006-2009 data."""
    url = "https://www.cftc.gov/files/dea/history/fut_disagg_txt_hist_2006_2016.zip"
    logger.info("=== Disaggregated Historical (2006-2009) from combined file ===")

    zip_bytes = download_zip(url, timeout=180)
    if zip_bytes is None:
        logger.warning("Historical file download failed")
        return 0

    # Parse all records, but only keep 2006-2009 (2010+ already loaded from yearly files)
    all_records = parse_disaggregated_csv(zip_bytes, 2006)
    records = [r for r in all_records if r['report_date'] < '2010-01-01']
    logger.info(f"Parsed {len(all_records)} total, filtered to {len(records)} records (pre-2010)")

    if records:
        n = upsert_records(records)
        logger.info(f"Upserted {n} records from historical file")
        return n
    return 0


def backfill_disaggregated(start_year: int = 2006, end_year: int = 2026, single_year: int = None):
    """Download and load disaggregated futures data."""
    years = [single_year] if single_year else list(range(start_year, end_year + 1))
    total = 0

    logger.info(f"=== Disaggregated Futures Backfill: {years[0]}-{years[-1]} ===")

    # If we need 2006-2009, use the combined historical file first
    needs_historical = any(y < 2010 for y in years) and single_year is None
    if needs_historical:
        total += backfill_disaggregated_historical()
        # Skip years already covered by historical file
        years = [y for y in years if y >= 2010]

    for year in years:
        url = f"https://www.cftc.gov/files/dea/history/fut_disagg_txt_{year}.zip"
        logger.info(f"[{year}] Downloading disaggregated data...")

        zip_bytes = download_zip(url)
        if zip_bytes is None:
            logger.warning(f"[{year}] Skipped (download failed)")
            continue

        records = parse_disaggregated_csv(zip_bytes, year)
        logger.info(f"[{year}] Parsed {len(records)} records")

        if records:
            n = upsert_records(records)
            total += n
            logger.info(f"[{year}] Upserted {n} records (running total: {total})")

        # Brief pause to be polite to CFTC servers
        time.sleep(0.5)

    logger.info(f"=== Disaggregated backfill complete: {total} total records ===")
    return total


def backfill_legacy(start_year: int = 1986, end_year: int = 2005, single_year: int = None):
    """Download and load legacy combined data."""
    years = [single_year] if single_year else list(range(start_year, end_year + 1))
    total = 0

    logger.info(f"=== Legacy Report Backfill: {years[0]}-{years[-1]} ===")

    for year in years:
        url = f"https://www.cftc.gov/files/dea/history/deacot{year}.zip"
        logger.info(f"[{year}] Downloading legacy data...")

        zip_bytes = download_zip(url)
        if zip_bytes is None:
            logger.warning(f"[{year}] Skipped (download failed)")
            continue

        records = parse_legacy_csv(zip_bytes, year)
        logger.info(f"[{year}] Parsed {len(records)} records")

        if records:
            n = upsert_records(records)
            total += n
            logger.info(f"[{year}] Upserted {n} records (running total: {total})")

        time.sleep(0.5)

    logger.info(f"=== Legacy backfill complete: {total} total records ===")
    return total


def print_summary():
    """Print a summary of what's in bronze.cftc_cot after backfill."""
    with get_connection() as conn:
        cur = conn.cursor()

        cur.execute("""
            SELECT report_type, COUNT(*) as cnt, MIN(report_date) as min_dt,
                   MAX(report_date) as max_dt,
                   COUNT(DISTINCT commodity) as n_commodities
            FROM bronze.cftc_cot
            GROUP BY report_type
            ORDER BY report_type
        """)
        rows = cur.fetchall()
        print("\n" + "=" * 70)
        print("BRONZE.CFTC_COT SUMMARY")
        print("=" * 70)
        for row in rows:
            print(f"  {row['report_type']:15s}  {row['cnt']:>7,} rows  "
                  f"{row['min_dt']} to {row['max_dt']}  "
                  f"({row['n_commodities']} commodities)")

        cur.execute("""
            SELECT commodity, report_type, COUNT(*) as cnt,
                   MIN(report_date) as min_date, MAX(report_date) as max_date
            FROM bronze.cftc_cot
            GROUP BY commodity, report_type
            ORDER BY commodity, report_type
        """)
        rows = cur.fetchall()
        print("\nBy Commodity:")
        for row in rows:
            print(f"  {row['commodity']:20s} [{row['report_type']:14s}]  "
                  f"{row['cnt']:>5,} rows  {row['min_date']} to {row['max_date']}")

        cur.execute("SELECT COUNT(*) as total FROM bronze.cftc_cot")
        total = cur.fetchone()['total']
        print(f"\nTOTAL: {total:,} rows")
        print("=" * 70)


def main():
    parser = argparse.ArgumentParser(description='CFTC COT Historical Backfill')
    parser.add_argument('--legacy', action='store_true',
                        help='Also backfill legacy data (1986-2005)')
    parser.add_argument('--legacy-only', action='store_true',
                        help='Only backfill legacy data')
    parser.add_argument('--year', type=int, default=None,
                        help='Backfill a single year only')
    parser.add_argument('--start-year', type=int, default=None,
                        help='Override start year')
    parser.add_argument('--end-year', type=int, default=None,
                        help='Override end year')
    parser.add_argument('--summary-only', action='store_true',
                        help='Just print current DB summary')
    args = parser.parse_args()

    if args.summary_only:
        print_summary()
        return

    grand_total = 0

    if not args.legacy_only:
        # Disaggregated: 2006-2026
        start = args.start_year or 2006
        end = args.end_year or 2026
        grand_total += backfill_disaggregated(
            start_year=start, end_year=end, single_year=args.year
        )

    if args.legacy or args.legacy_only:
        # Legacy: 1986-2005 (or full range if specified)
        start = args.start_year or 1986
        end = args.end_year or 2005
        grand_total += backfill_legacy(
            start_year=start, end_year=end, single_year=args.year
        )

    logger.info(f"Grand total: {grand_total:,} records upserted")
    print_summary()


if __name__ == '__main__':
    main()
