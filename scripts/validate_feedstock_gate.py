"""Feedstock allocation validation gate (corrected spec).

Replaces the runbook's inline snippet, which compared the raked TOTAL against the full EIA BBD
control — including Tallow and Yellow Grease. But the rake deliberately leaves RLC_CANONICAL
feedstocks (tallow, UCO/YG) EXEMPT from scaling to EIA (control_basis='EXEMPT_RLC'), because RLC's
own supply build is authoritative there (EIA's Tallow bucket is believed contaminated). So the old
gate could never pass whenever RLC tallow diverged from EIA — it cried wolf by exactly the
intended divergence.

Corrected gate:
  1. RAKE-CONTROLLED feedstocks (control_basis EIA_TOTAL / EIA_BDRD / USDA_SEASONAL) must reconcile
     to their EIA control within +/-5%. This is the meaningful check — these are the ones the rake
     actually scales to EIA.
  2. PRESENCE: every expected feedstock present in the month (SBO, DCO, CO, CWG, EBFT, IBFT, UCO).
  3. RLC_CANONICAL (tallow, UCO/YG) reported vs EIA as INFORMATION, not a pass/fail — a large
     divergence is a flag for analytical review, not a pipeline failure.

Usage:  python scripts/validate_feedstock_gate.py [--run-day YYYY-MM-DD] [--year 2026]
Exit code 1 if the controlled-reconciliation or presence check fails (publish blocker).
"""
import sys
import argparse
from pathlib import Path

ROOT = Path(r"C:/dev/RLC-Agent"); sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv; load_dotenv(ROOT / ".env")
from src.services.database.db_config import get_connection

BBD = ['Soybean Oil', 'Tallow', 'Corn Oil', 'Yellow Grease', 'Canola Oil',
       'Other Vegetable Oil', 'White Grease']
CONTROLLED_BASES = ('EIA_TOTAL', 'EIA_BDRD', 'USDA_SEASONAL')
REQUIRED_CODES = {'SBO', 'DCO', 'CO', 'CWG', 'EBFT', 'IBFT', 'UCO'}
TOL = 5.0  # +/- percent


def validate(run_day: str, year: int) -> bool:
    with get_connection() as conn:
        cur = conn.cursor()
        # EIA control per (month, feedstock_name)
        cur.execute("""SELECT month, feedstock_name, quantity_mil_lbs q FROM bronze.eia_feedstock_monthly
                       WHERE plant_type='total' AND NOT is_withheld AND quantity_mil_lbs IS NOT NULL
                         AND year=%s AND feedstock_name = ANY(%s)""", (year, BBD))
        eia = {}
        for r in cur.fetchall():
            eia[(r['month'], r['feedstock_name'])] = float(r['q'])

        # raked, split by controlled vs exempt, per (month, eia_feedstock)
        cur.execute("""SELECT extract(month from period)::int m, eia_feedstock,
                         control_basis, sum(raked_mil_lbs) q, string_agg(DISTINCT feedstock_code,',') codes
                       FROM gold.bbd_feedstock_raked
                       WHERE run_day=%s AND extract(year from period)=%s
                       GROUP BY 1,2,3 ORDER BY 1,2""", (run_day, year))
        controlled = {}   # (m, eia_fs) -> raked
        exempt = {}       # (m, eia_fs) -> raked
        codes_by_m = {}
        for r in cur.fetchall():
            key = (r['m'], r['eia_feedstock']); q = float(r['q'])
            if r['control_basis'] in CONTROLLED_BASES:
                controlled[key] = controlled.get(key, 0) + q
            elif r['control_basis'] == 'EXEMPT_RLC':
                exempt[key] = exempt.get(key, 0) + q
            for c in (r['codes'] or '').split(','):
                codes_by_m.setdefault(r['m'], set()).add(c)

        months = sorted({m for (m, _) in controlled} | {m for (m, _) in exempt})
        ok = True
        print(f"=== Validation gate  run_day={run_day}  year={year} ===\n")
        print("1. RAKE-CONTROLLED reconciliation to EIA (+/-5%):")
        for m in months:
            # controlled feedstocks present this month
            fs = {f for (mm, f) in controlled if mm == m}
            rk = sum(controlled[(m, f)] for f in fs)
            ec = sum(eia.get((m, f), 0) for f in fs)
            pct = (rk / ec * 100) if ec else None
            passed = pct is not None and (100 - TOL) <= pct <= (100 + TOL)
            ok = ok and passed
            print(f"   {year}-{m:02d}  raked={rk/1e3:.3f}B  EIA={ec/1e3:.3f}B  "
                  f"{pct:.1f}%  {'PASS' if passed else 'FAIL'}")

        print("\n2. PRESENCE (SBO,DCO,CO,CWG,EBFT,IBFT,UCO):")
        for m in months:
            missing = REQUIRED_CODES - codes_by_m.get(m, set())
            passed = not missing
            ok = ok and passed
            print(f"   {year}-{m:02d}  {'PASS' if passed else 'MISSING: ' + ','.join(sorted(missing))}")

        print("\n3. RLC_CANONICAL vs EIA (informational — divergence is an analyst flag, not a fail):")
        for m in months:
            for fs in ('Tallow', 'Yellow Grease'):
                rl = exempt.get((m, fs))
                e = eia.get((m, fs))
                if rl is not None and e:
                    print(f"   {year}-{m:02d}  {fs:14} RLC={rl:.0f}  EIA={e:.0f}  RLC/EIA={rl/e*100:.0f}%")

        print(f"\n{'GATE PASS' if ok else 'GATE FAIL'} "
              f"(controlled reconciliation + presence)")
        return ok


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-day", default="2026-07-11")
    ap.add_argument("--year", type=int, default=2026)
    args = ap.parse_args()
    sys.exit(0 if validate(args.run_day, args.year) else 1)
