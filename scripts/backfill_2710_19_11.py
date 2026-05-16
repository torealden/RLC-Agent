"""Backfill HS 2710.19.11 (Kerosene-type jet fuel) Census data.

Code is in reference table but never collected. Premium-priced shipments
likely represent SAF cargoes within this petroleum-jet code.
"""
import sys, pathlib, logging, time
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))
from dotenv import load_dotenv; load_dotenv('.env')
from datetime import date

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

from src.agents.collectors.us.census_trade_collector import CensusTradeCollector
c = CensusTradeCollector()
codes = ['2710191100']
print(f"Backfilling HS 2710.19.11 (jet kerosene) 2013-2026", flush=True)
t0 = time.time()
total = 0
for yr in range(2013, 2027):
    sd = date(yr, 1, 1)
    ed = date(yr, 12, 31) if yr < 2026 else date(2026, 3, 31)
    yt0 = time.time()
    try:
        result = c.collect(start_date=sd, end_date=ed, hs_codes=codes)
        n = result.records_fetched
        total += n
        print(f"  {yr}: {n:>5} records  ({time.time()-yt0:.0f}s)  cum={total}", flush=True)
    except Exception as e:
        print(f"  {yr}: FAIL {type(e).__name__}: {e}", flush=True)
print(f"\nDone in {time.time()-t0:.0f}s. Total: {total}", flush=True)
