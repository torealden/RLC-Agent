"""Backfill NASS QuickStats wheat supply history into bronze (1990-2025).

Fills the gaps the wheat pipeline needs: national production (+ forecast-vintage march),
quarterly stocks, and by-class area/production (winter / spring-excl-durum / durum). Aggregate
area is already solid 2000+. Idempotent (collector upserts on the bronze conflict keys).
"""
import sys
from pathlib import Path
ROOT = Path(r"C:/dev/RLC-Agent"); sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv; load_dotenv(ROOT / ".env")
from src.agents.collectors.us.usda_nass_collector import NASSCollector

col = NASSCollector()
if not col.config.api_key:
    print("ERROR: no NASS_API_KEY"); sys.exit(1)

# (data_type, commodities) — commodities batched per year inside fetch_data
PLAN = [
    ('production', ['wheat', 'wheat_winter', 'wheat_spring', 'wheat_durum']),
    ('stocks',     ['wheat']),
    ('acreage',    ['wheat_winter', 'wheat_spring', 'wheat_durum']),
]
YEARS = range(1990, 2026)

totals = {}
for dtype, comms in PLAN:
    saved = 0; ok = 0; errs = 0
    for y in YEARS:
        try:
            r = col.fetch_data(data_type=dtype, commodities=comms, year=y)
        except Exception as e:
            errs += 1; print(f"  {dtype} {y}: EXC {e}"); continue
        if getattr(r, 'success', False):
            n = col.save_to_bronze(r, dtype)
            saved += n; ok += 1
        else:
            errs += 1
    totals[dtype] = saved
    print(f"{dtype:11}: {ok}/{len(YEARS)} yrs ok, {errs} err, {saved} rows saved")
print("DONE:", totals)
