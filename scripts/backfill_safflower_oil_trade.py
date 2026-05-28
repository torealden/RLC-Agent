"""One-shot backfill: pull Census trade data for safflower oil HS codes,
2013-current. Crude (1512.11.00.40) + refined (1512.19.00.40).

Migration 117 added these HS codes to silver.trade_commodity_reference;
this script populates bronze.census_trade for them.

Run-time estimate: ~14 years x 2 codes x 2 flows x partner rows ~ a few
minutes total. Yearly chunks for progress visibility.
"""

import sys, pathlib, time, logging
from datetime import date

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))
from dotenv import load_dotenv; load_dotenv('.env')
from src.agents.collectors.us.census_trade_collector import CensusTradeCollector

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

SAFFLOWER_OIL_CODES = ['1512110040', '1512190040']

c = CensusTradeCollector()
print(f"Backfilling safflower oil: {SAFFLOWER_OIL_CODES}", flush=True)
t0 = time.time()
total = 0
for yr in range(2013, 2027):
    sd = date(yr, 1, 1)
    ed = date(yr, 12, 31) if yr < 2026 else date(2026, 4, 30)
    yt0 = time.time()
    result = c.collect(start_date=sd, end_date=ed, hs_codes=SAFFLOWER_OIL_CODES)
    total += result.records_fetched
    print(f"  {yr}: {result.records_fetched:>5} rows in {time.time()-yt0:.0f}s "
          f"(cum {total}, {len(result.warnings or [])} warns)", flush=True)
print(f"\nTotal elapsed: {time.time()-t0:.1f}s, total inserted: {total}", flush=True)
