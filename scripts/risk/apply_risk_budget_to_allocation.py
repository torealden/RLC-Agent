"""Post-allocation risk-budget mix override.

Rewrites each facility's FEEDSTOCK MIX in gold.feedstock_allocation to match its quarterly
VaR budget, while keeping the facility's greedy TOTAL and fuel_type (production-driven). This
replaces the greedy corner-solution mix (100% highest-margin, monthly whipsaw) with the
diversified, anchor-consistent risk-budget mix — the fix flows through the rake to the flat
files. Written as a fresh per-period run (scenario='risk_budget') the rake picks up (latest
created_at). Idempotent + reversible (delete scenario='risk_budget' to revert).

Consistency: EIA-raked feedstocks (SBO/CO/DCO/CWG/PF) self-correct at the rake (scaled to EIA
totals). RLC-CANONICAL feedstocks (tallow grades/UCO/YG) are rake-EXEMPT, so the override
RENORMALIZES them back to the greedy run's per-period totals — preserving the RLC supply
guardrail while still shifting the mix (a facility gaining tallow takes it proportionally from
others, rather than inflating total tallow past its guardrail).
"""
import sys
import uuid
from collections import defaultdict
from pathlib import Path

ROOT = Path(r"C:/dev/RLC-Agent"); sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv; load_dotenv(ROOT / ".env")
from psycopg2.extras import execute_values
from src.services.database.db_config import get_connection

LPG = {'biodiesel': 7.60, 'renewable_diesel': 8.60, 'saf': 8.60, 'coprocessing': 7.60}
RLC_CANONICAL = {'EBFT', 'IBFT', 'BFT', 'UCO', 'YG'}


def quarter_of(p):
    return f"{p.year}Q{(p.month - 1) // 3 + 1}"


def main():
    with get_connection() as conn:
        cur = conn.cursor()

        # 1. latest BASE (non-override) run per period
        cur.execute("""
            WITH latest AS (SELECT DISTINCT ON (period) period, run_id FROM gold.feedstock_allocation
                            WHERE scenario IS DISTINCT FROM 'risk_budget'
                            ORDER BY period, created_at DESC)
            SELECT a.period, a.facility_id, a.fuel_type, a.feedstock_code, a.allocated_mil_lbs,
                   a.feedstock_cost_lb, a.margin_per_gal
            FROM gold.feedstock_allocation a JOIN latest l ON a.period=l.period AND a.run_id=l.run_id""")
        rows = cur.fetchall()
        by_period = defaultdict(list)
        for r in rows:
            by_period[r['period']].append(r)

        # 2. budgets: (facility_id, quarter, fuel_type) -> {feedstock: lbs}
        cur.execute("SELECT facility_id, quarter, fuel_type, buy_by_feedstock FROM risk.facility_quarterly_budget")
        budgets = {(r['facility_id'], r['quarter'], r['fuel_type']): r['buy_by_feedstock']
                   for r in cur.fetchall()}

        # 3. clear prior override, rebuild
        cur.execute("DELETE FROM gold.feedstock_allocation WHERE scenario='risk_budget'")

        insert_rows = []
        n_overridden = 0
        for period, prows in by_period.items():
            q = quarter_of(period)
            fac_total, fac_fuel = defaultdict(float), {}
            greedy_fs_total = defaultdict(float)
            orig_cm = {}
            fac_rows = defaultdict(list)
            for r in prows:
                lbs = float(r['allocated_mil_lbs'] or 0)
                fac_total[r['facility_id']] += lbs
                fac_fuel[r['facility_id']] = r['fuel_type']
                greedy_fs_total[r['feedstock_code']] += lbs
                orig_cm[(r['facility_id'], r['feedstock_code'])] = (r['feedstock_cost_lb'], r['margin_per_gal'])
                fac_rows[r['facility_id']].append((r['feedstock_code'], lbs))

            new_rows = []  # [facility_id, fuel_type, feedstock, lbs]
            for fid, tot in fac_total.items():
                ft = fac_fuel[fid]
                b = budgets.get((fid, q, ft))
                bt = sum(float(v) for v in b.values()) if b else 0
                if b and bt > 0 and tot > 0:
                    for fs, lbs in b.items():
                        new_rows.append([fid, ft, fs, tot * float(lbs) / bt])
                    n_overridden += 1
                else:  # no budget -> keep greedy rows
                    for fs, lbs in fac_rows[fid]:
                        new_rows.append([fid, ft, fs, lbs])

            # renormalize RLC-canonical feedstocks to greedy per-period totals (guardrail)
            new_fs_total = defaultdict(float)
            for nr in new_rows:
                new_fs_total[nr[2]] += nr[3]
            for nr in new_rows:
                fs = nr[2]
                if fs in RLC_CANONICAL and new_fs_total[fs] > 0:
                    nr[3] *= greedy_fs_total.get(fs, 0.0) / new_fs_total[fs]

            run_id = str(uuid.uuid4())
            fac_new_total = defaultdict(float)
            for fid, ft, fs, lbs in new_rows:
                fac_new_total[fid] += lbs
            for fid, ft, fs, lbs in new_rows:
                if lbs <= 0:
                    continue
                cost, margin = orig_cm.get((fid, fs), (None, None))
                pct = lbs / fac_new_total[fid] if fac_new_total[fid] > 0 else 0
                insert_rows.append((period, run_id, 'risk_budget', fid, ft, fs, lbs,
                                    lbs / LPG.get(ft, 8.0), pct, cost, margin, 0, 'risk_budget'))

        execute_values(cur, """INSERT INTO gold.feedstock_allocation
            (period, run_id, scenario, facility_id, fuel_type, feedstock_code, allocated_mil_lbs,
             allocated_mil_gal, pct_of_facility, feedstock_cost_lb, margin_per_gal, margin_rank,
             constraint_binding) VALUES %s""", insert_rows, page_size=2000)
        conn.commit()
        print(f"Override: {len(by_period)} periods, {n_overridden} facility-quarters remixed to budget, "
              f"{len(insert_rows)} rows written (scenario='risk_budget').")

        # verify: coprocessing feedstock split in the NEW latest run
        cur.execute("""
            WITH latest AS (SELECT DISTINCT ON (period) period, run_id FROM gold.feedstock_allocation
                            ORDER BY period, created_at DESC)
            SELECT a.feedstock_code, round(sum(a.allocated_mil_lbs)::numeric,0) tot
            FROM gold.feedstock_allocation a JOIN latest l ON a.period=l.period AND a.run_id=l.run_id
            WHERE a.fuel_type='coprocessing' GROUP BY 1 ORDER BY tot DESC""")
        print("\nCo-processing feedstock split (new latest allocation):")
        for r in cur.fetchall():
            print(f"   {r['feedstock_code']:6s} {r['tot']}")


if __name__ == "__main__":
    main()
