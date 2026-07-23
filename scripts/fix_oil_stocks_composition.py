"""
Fix / backfill silver.monthly_realized.oil_stocks composition (session 5, 2026-07-23).

THE BUG: `nass_processing_collector._map_attribute` mapped every NASS oil-stocks short_desc
to a single `oil_stocks` attribute, so the crude and once-refined components collided on the
silver upsert key and once-refined (the last writer) alone survived -- understating stocks
~5-6x. For 2026-03 soybean oil the stored value was 476,013,000 lb; the true total is
crude 2,127,030,000 + once-refined 476,013,000 = 2,603,043,000.

THE FIX lives in src/agents/collectors/us/oil_stocks_composition.py (shared with the
collector so future collections don't regress). This script is the runnable backfill /
verification wrapper. Default is a dry run; pass --apply to write.

  crude_total   = COALESCE("ONSITE & OFFSITE, CRUDE", bare "CRUDE")   # bare = corn only
  refined_total = "ONSITE & OFFSITE, [ONCE] REFINED"
  oil_stocks    = crude_total + refined_total
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.services.database.db_config import get_connection
from src.agents.collectors.us.oil_stocks_composition import (
    build_component_index, recompute_oil_stocks,
)


def main(apply):
    with get_connection() as conn:
        cur = conn.cursor()
        idx, problems = build_component_index(cur)
        if problems:
            print("COMPONENT VALIDATION PROBLEMS (used components multi-valued):")
            for p in problems:
                print("  " + p)
            return 2

        stats = recompute_oil_stocks(conn, apply=apply)
        print(f"\n{'APPLIED' if apply else 'DRY RUN'} -- oil_stocks recomposition")
        for k, v in stats.items():
            print(f"  {k:12}: {v}")

        # tie-out: canonical soybean 2026-03 must be 2,603,043,000
        s = idx.get(('SOYBEAN', 2026, 3))
        if s:
            tot = (s['crude'] or 0) + (s['refined'] or 0)
            ok = abs(tot - 2_603_043_000) < 1.0
            print(f"\n  TIE-OUT soybean 2026-03: crude {s['crude']:,.0f} + refined "
                  f"{s['refined']:,.0f} = {tot:,.0f}  {'OK' if ok else 'MISMATCH'}")
            if not ok:
                return 2

        # post-apply spot check straight from silver
        if apply:
            cur.execute("""
                SELECT attribute, realized_value FROM silver.monthly_realized
                WHERE commodity='soybeans' AND source='NASS_FATS_OILS'
                  AND calendar_year=2026 AND month=3
                  AND attribute IN ('oil_stocks','oil_stocks_crude','oil_stocks_once_refined')
                ORDER BY attribute
            """)
            print("\n  silver after apply (soybeans NASS_FATS_OILS 2026-03):")
            for r in cur.fetchall():
                print(f"    {r['attribute']:26} {float(r['realized_value']):,.0f}")
    return 0


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--apply', action='store_true', help='write changes (default: dry run)')
    args = ap.parse_args()
    sys.exit(main(args.apply))
