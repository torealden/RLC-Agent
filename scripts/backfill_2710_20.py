"""Focused backfill: HS 2710.20.x codes only, 2013-current.

Faster than backfill_census_fuels.py (which iterates all 27 fuel codes).
Logs per (code × year) progress so we can see if anything hangs.
"""
import sys, pathlib, logging, time
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))
from dotenv import load_dotenv; load_dotenv('.env')
from datetime import date

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

from src.agents.collectors.us.census_trade_collector import CensusTradeCollector

c = CensusTradeCollector()

# Only the new 2710.20.x codes
new_codes = ['2710200500', '2710201000', '2710201500', '2710202500', '2710209000']

print(f"Backfilling {len(new_codes)} new HS 2710.20.x codes for 2013-2026", flush=True)
t0 = time.time()
total = 0
for yr in range(2013, 2027):
    sd = date(yr, 1, 1)
    ed = date(yr, 12, 31) if yr < 2026 else date(2026, 3, 31)
    yt0 = time.time()
    try:
        result = c.collect(start_date=sd, end_date=ed, hs_codes=new_codes)
        n = result.records_fetched
        warns = len(result.warnings or [])
        total += n
        print(f"  {yr}: {n:>5} records  ({time.time()-yt0:.0f}s, {warns} warnings)  cum={total}", flush=True)
        if warns and warns < 5:
            for w in (result.warnings or [])[:3]:
                print(f"      warn: {w}", flush=True)
    except Exception as e:
        print(f"  {yr}: FAIL {type(e).__name__}: {e}", flush=True)

print(f"\nDone in {time.time()-t0:.0f}s. Total records: {total}", flush=True)
