"""Targeted ingest of NASS peanut STOCKS into bronze.nass_processing.

Bypasses the existing collector because (a) the freq_desc=MONTHLY filter
excluded these series (peanut stocks are POINT IN TIME), and (b) a single
out-of-range row would abort the whole transaction in save_to_bronze.

Targets four short_descs that feed peanut_crush.xlsm columns M/N/O/S:
  - PEANUTS, SHELLED - STOCKS, MEASURED IN LB                (M)
  - PEANUTS - STOCKS, MEASURED IN LB, IN SHELL BASIS         (N)
  - PEANUTS, IN SHELL, ROASTING - STOCKS, MEASURED IN LB     (O)
  - PEANUTS, SHELLED, CRUSHED, CAKE & MEAL - STOCKS, ...     (S)
"""
import os, sys, logging, requests
from dotenv import load_dotenv
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
load_dotenv(PROJECT_ROOT / '.env')

from src.services.database.db_config import get_connection

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
log = logging.getLogger('ingest_peanut_stocks')

TARGETS = [
    'PEANUTS, SHELLED - STOCKS, MEASURED IN LB',
    'PEANUTS - STOCKS, MEASURED IN LB, IN SHELL BASIS',
    'PEANUTS, IN SHELL, ROASTING - STOCKS, MEASURED IN LB',
    'PEANUTS, SHELLED, CRUSHED, CAKE & MEAL - STOCKS, MEASURED IN LB',
]

NASS_URL = 'https://quickstats.nass.usda.gov/api/api_GET/'

MONTH_MAP = {
    'JAN':1,'FEB':2,'MAR':3,'APR':4,'MAY':5,'JUN':6,
    'JUL':7,'AUG':8,'SEP':9,'OCT':10,'NOV':11,'DEC':12,
    'JANUARY':1,'FEBRUARY':2,'MARCH':3,'APRIL':4,
    'JUNE':6,'JULY':7,'AUGUST':8,'SEPTEMBER':9,
    'OCTOBER':10,'NOVEMBER':11,'DECEMBER':12,
}

def parse_month(period):
    p = (period or '').upper().strip()
    if p.startswith('END OF '):
        p = p[len('END OF '):]
    return MONTH_MAP.get(p)

def fetch_year(api_key, year):
    """One call per (target_short_desc, year). Returns list of records."""
    out = []
    for sd in TARGETS:
        params = {
            'key': api_key,
            'commodity_desc': 'PEANUTS',
            'statisticcat_desc': 'STOCKS',
            'short_desc': sd,
            'year__GE': str(year),
            'year__LE': str(year),
            'agg_level_desc': 'NATIONAL',
            'format': 'JSON',
        }
        try:
            r = requests.get(NASS_URL, params=params, timeout=45)
        except requests.RequestException as e:
            log.warning(f'  {year}/{sd[:40]}: HTTP error {e}')
            continue
        if r.status_code != 200:
            continue
        data = r.json().get('data', [])
        for d in data:
            rp = d.get('reference_period_desc', '') or ''
            if 'THRU' in rp.upper():
                continue
            month = parse_month(rp)
            if month is None:
                continue
            v_str = str(d.get('Value', '')).replace(',', '').strip()
            if not v_str or v_str in ('(D)', '(NA)', '(X)', '(-)'):
                continue
            try:
                value = float(v_str)
            except ValueError:
                continue
            out.append({
                'commodity_desc': d.get('commodity_desc'),
                'class_desc':     d.get('class_desc', ''),
                'statisticcat':   d.get('statisticcat_desc'),
                'short_desc':     d.get('short_desc'),
                'unit':           d.get('unit_desc'),
                'domaincat_desc': d.get('domaincat_desc', ''),
                'year':           int(d.get('year', 0)),
                'ref_period':     rp,
                'month':          month,
                'value':          value,
                'source':         'NASS_PEANUT_STOCKS',
            })
    return out

def upsert_savepoint(cur, rec):
    """Try one INSERT with a SAVEPOINT so a single failure can't poison the tx."""
    cur.execute('SAVEPOINT row_sp')
    try:
        cur.execute("""
            INSERT INTO bronze.nass_processing (
                commodity_desc, class_desc, statisticcat_desc,
                short_desc, unit_desc, domaincat_desc,
                year, reference_period_desc, month,
                value, report_type, source
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (
                commodity_desc, COALESCE(class_desc,''), statisticcat_desc,
                short_desc, year, COALESCE(month,0), COALESCE(domaincat_desc,'')
            ) DO UPDATE SET
                value = EXCLUDED.value,
                unit_desc = EXCLUDED.unit_desc,
                reference_period_desc = EXCLUDED.reference_period_desc,
                collected_at = NOW()
        """, (
            rec['commodity_desc'], rec['class_desc'], rec['statisticcat'],
            rec['short_desc'], rec['unit'], rec['domaincat_desc'],
            rec['year'], rec['ref_period'], rec['month'],
            rec['value'], 'peanut_stocks', rec['source'],
        ))
        cur.execute('RELEASE SAVEPOINT row_sp')
        return True
    except Exception as e:
        cur.execute('ROLLBACK TO SAVEPOINT row_sp')
        log.warning(f'  row fail: {e}  ({rec["short_desc"][:40]} {rec["year"]}-{rec["month"]})')
        return False

def main():
    key = os.environ.get('NASS_API_KEY') or os.environ.get('USDA_NASS_API_KEY')
    if not key:
        log.error('NASS_API_KEY not set'); sys.exit(1)

    total_in = 0
    total_ok = 0
    with get_connection() as conn:
        with conn.cursor() as cur:
            for yr in range(1979, 2027):
                recs = fetch_year(key, yr)
                if not recs:
                    continue
                yr_ok = 0
                for r in recs:
                    if upsert_savepoint(cur, r):
                        yr_ok += 1
                conn.commit()
                total_in += len(recs)
                total_ok += yr_ok
                log.info(f'  {yr}: fetched={len(recs)} saved={yr_ok}')
    log.info(f'\nTOTAL: fetched={total_in}  saved={total_ok}')

if __name__ == '__main__':
    main()
