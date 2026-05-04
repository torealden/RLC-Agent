"""
Backfill bronze.census_trade with corn oil HS codes from 2013 to present.

Targets the codes added today (2026-05-04) plus revalidates the existing
1515210000 / 1515290040 series.

Per US schedule asymmetry:
  - 1515210000: HTS imports only (Schedule B exports use 10010/10050)
  - 1515210010: Schedule B exports only (food-grade crude)
  - 1515210050: Schedule B exports only (NESOI / industrial = DCO)
  - 1515290020: both flows (once-refined corn oil)
  - 1515290040: both flows (fully refined corn oil)

The collector's fetch_data writes to bronze.census_trade via save_to_bronze.
ON CONFLICT (year, month, flow, hs_code, country_code) DO UPDATE — idempotent.

Usage:
    python scripts/backfill_corn_oil_hs_codes.py
    # or
    python scripts/backfill_corn_oil_hs_codes.py --start-year 2020
"""
from __future__ import annotations

import argparse
import time
from datetime import date

from dotenv import load_dotenv
load_dotenv()

from src.agents.collectors.us.census_trade_collector import CensusTradeCollector


# HS code → which flow(s) to fetch (only fetch flows that physically exist)
TARGETS = [
    ('1515210000', ['imports']),                # HTS imports only
    ('1515210010', ['exports']),                # Schedule B food-grade crude
    ('1515210050', ['exports']),                # Schedule B NESOI = DCO
    ('1515290020', ['imports', 'exports']),     # once-refined, both flows
    ('1515290040', ['imports', 'exports']),     # fully refined, both flows
]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--start-year', type=int, default=2013,
                    help='First year to backfill (Census monthly data starts 2013)')
    ap.add_argument('--end-year', type=int, default=date.today().year,
                    help='Last year to backfill (default = current year)')
    args = ap.parse_args()

    print(f"Backfilling corn oil HS codes {args.start_year}-{args.end_year}")
    print(f"Targets: {len(TARGETS)} HS codes, "
          f"{sum(len(flows) for _, flows in TARGETS)} (code, flow) pairs total")
    print()

    collector = CensusTradeCollector()
    t_total = time.time()

    for hs_code, flows in TARGETS:
        for flow in flows:
            t0 = time.time()
            print(f"  Fetching hs={hs_code} flow={flow} years={args.start_year}-{args.end_year}...",
                  flush=True)
            try:
                result = collector.fetch_data(
                    start_date=date(args.start_year, 1, 1),
                    end_date=date(args.end_year, 12, 31),
                    flow=flow,
                    hs_codes=[hs_code],
                )
                elapsed = time.time() - t0
                print(f"    -> success={result.success}  records={result.records_fetched}  "
                      f"elapsed={elapsed:.1f}s", flush=True)
                if result.warnings:
                    for w in result.warnings:
                        print(f"    WARN: {w}", flush=True)
                # Persist to bronze
                if result.success and result.data is not None:
                    n_saved = collector.save_to_bronze(result.data)
                    print(f"    bronze rows upserted: {n_saved}", flush=True)
            except Exception as e:
                print(f"    FAILED: {e}", flush=True)

    print(f"\nTotal elapsed: {(time.time() - t_total) / 60:.1f} min")


if __name__ == "__main__":
    main()
