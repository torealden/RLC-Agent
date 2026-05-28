"""Backfill new AMS byproduct slugs (2837, 2839, 3510) into bronze + silver.

Per memory:reference_history_start_dates.md the project default is energies
from Jan 1993, oilseeds/grains from Oct 1993 — but the MARS API only
exposes these byproduct slugs from mid-2022 onward, so we take what's
available. Earlier history would need a separate AMS data-archive request.
"""
import datetime as dt
import logging
from dotenv import load_dotenv

load_dotenv('.env')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(name)s %(levelname)s: %(message)s',
)
log = logging.getLogger('backfill_ams_byproducts')

from src.agents.collectors.us.ams_cash_price_collector import AMSCashPriceCollector

SLUGS = ['2837', '2839', '3510']
START = dt.date(2022, 1, 1)
END = dt.date.today()
WINDOW_DAYS = 60

def chunk_dates(start: dt.date, end: dt.date, days: int):
    cur = start
    while cur <= end:
        nxt = min(cur + dt.timedelta(days=days - 1), end)
        yield cur, nxt
        cur = nxt + dt.timedelta(days=1)

def main():
    collector = AMSCashPriceCollector(slug_ids=SLUGS)
    total_fetched = 0
    total_bronze = 0
    total_silver = 0
    for w_start, w_end in chunk_dates(START, END, WINDOW_DAYS):
        log.info(f'Window {w_start} -> {w_end}')
        res = collector.collect_and_save(start_date=w_start, end_date=w_end)
        if not res.get('success'):
            log.error(f'  FAIL: {res.get("error")}')
            continue
        total_fetched += res.get('records_fetched', 0)
        total_bronze  += res.get('bronze_saved', 0)
        silver = res.get('silver') or {}
        total_silver  += silver.get('specialty_price', 0)
        log.info(
            f"  fetched={res.get('records_fetched')} "
            f"bronze={res.get('bronze_saved')} "
            f"silver={silver.get('specialty_price', 0)}"
        )
    log.info(
        f'\n=== TOTAL ===\nfetched={total_fetched}  '
        f'bronze={total_bronze}  silver={total_silver}'
    )

if __name__ == '__main__':
    main()
