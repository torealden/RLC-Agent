"""Acceptance test for the deep allocator backfill (design v1.6 §8/1b + §4.5 tripwire).

Aimed at the silent-drop failure mode the histories exist to prevent:
  1. Shakeout casualties must show NONZERO allocation in their productive primes and ZERO after
     their closure date (the whole point of effective-dated histories).
  2. No facility shows allocation before its first history event (online) or after closure.
  3. Utilization tripwire: monthly allocated production must not exceed monthly capacity (>105%
     flags a capacity-history bug announcing itself, per §4.5).
Run after the backfill completes. Reports PASS/FAIL per check.
"""
import sys
from datetime import date
from pathlib import Path
ROOT = Path(r"C:/dev/RLC-Agent"); sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv; load_dotenv(ROOT / ".env")
from src.services.database.db_config import get_connection

RUN_DAY = '2026-07-04'   # the deep-backfill run
SHAKEOUT = {   # id -> (label, prime_period_operating, post_period_idle)
    19:  ('REG Ralston',        '2022-06-01', '2025-06-01'),
    36:  ('REG Madison',        '2022-06-01', '2025-06-01'),
    33:  ('Western Dubuque',    '2022-06-01', '2025-06-01'),
    38:  ('Hero BX Moundville', '2022-06-01', '2025-06-01'),
    18:  ('Hero BX Erie',       '2022-06-01', '2025-06-01'),
    49:  ('Hero BX Clinton',    '2022-06-01', '2025-06-01'),
}

def alloc(cur, fid, period):
    cur.execute("""SELECT COALESCE(sum(allocated_mil_lbs),0) v FROM gold.feedstock_allocation
                   WHERE facility_id=%s AND period=%s AND created_at::date=%s""", (fid, period, RUN_DAY))
    return float(cur.fetchone()['v'])

with get_connection() as c:
    cur = c.cursor()
    fails = 0
    print("=== Check 1: shakeout casualties nonzero in prime, zero after closure ===")
    for fid, (label, prime, post) in SHAKEOUT.items():
        p, q = alloc(cur, fid, prime), alloc(cur, fid, post)
        ok = p > 0 and q == 0
        fails += not ok
        print(f"  [{'PASS' if ok else 'FAIL'}] {label:20} prime({prime[:7]})={p/1e3:.1f}k lbs  post({post[:7]})={q/1e3:.1f}k lbs")

    print("\n=== Check 2: no allocation outside the facility's operating window ===")
    cur.execute("""
        SELECT count(*) n FROM gold.feedstock_allocation a
        WHERE a.created_at::date=%s AND NOT EXISTS (
            SELECT 1 FROM reference.facility_capacity_history h
            WHERE h.facility_id=a.facility_id AND h.status='operating'
              AND h.effective_date <= a.period
              AND (a.period < (SELECT min(h2.effective_date) FROM reference.facility_capacity_history h2
                               WHERE h2.facility_id=a.facility_id AND h2.status IN ('idle','closed') AND h2.effective_date > h.effective_date)
                   OR NOT EXISTS (SELECT 1 FROM reference.facility_capacity_history h3
                               WHERE h3.facility_id=a.facility_id AND h3.status IN ('idle','closed') AND h3.effective_date > h.effective_date)))
    """, (RUN_DAY,))
    n_out = cur.fetchone()['n']
    print(f"  [{'PASS' if n_out==0 else 'FAIL'}] {n_out} allocation rows outside an operating window")
    fails += n_out > 0

    print("\n=== Check 3: utilization tripwire (monthly allocated gal <= monthly capacity) ===")
    cur.execute("""
        WITH fac AS (
            SELECT a.facility_id, a.period, sum(a.allocated_mil_gal) gal FROM gold.feedstock_allocation a
            WHERE a.created_at::date=%s GROUP BY 1,2)
        SELECT count(*) n FROM fac f
        JOIN LATERAL (SELECT nameplate_mmgy FROM reference.facility_capacity_history h
                      WHERE h.facility_id=f.facility_id AND h.effective_date<=f.period
                      ORDER BY h.effective_date DESC LIMIT 1) cap ON true
        WHERE f.gal > 1.05 * (cap.nameplate_mmgy/12.0)
    """, (RUN_DAY,))
    n_util = cur.fetchone()['n']
    print(f"  [{'PASS' if n_util==0 else 'FAIL'}] {n_util} facility-months over 105% utilization (capacity-history bug)")
    fails += n_util > 0

    cur.execute("SELECT count(*) n, min(period) mn, max(period) mx, count(distinct facility_id) f FROM gold.feedstock_allocation WHERE created_at::date=%s", (RUN_DAY,))
    r = cur.fetchone(); print(f"\nbackfill: {r['n']} rows, {r['mn']}..{r['mx']}, {r['f']} facilities")
    print(f"\n{'ALL CHECKS PASS' if fails==0 else f'{fails} CHECK(S) FAILED'}")
