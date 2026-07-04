"""Seed reference.facility_capacity_history (effective_date change-log) from the current master
(design v1.6 §3.2). Reconstructs each facility's as-of-period capacity+status from what the master
carries (year_online, year_offline, current status, nameplate):

  - online event  @ year_online (or 2006 floor): status='operating', capacity=nameplate
  - if now idle/closed: offline event @ year_offline (or 2024 estimate): status=current, operating_mmgy=0
  - merged/planned/announced -> no events (dup / never ran)

State as-of a period = the row with the latest effective_date <= period. Idempotent (delete seed rows,
rebuild). Estimated offline dates flagged change_reason='offline_estimated'.
"""
import sys
from datetime import date
from pathlib import Path
ROOT = Path(r"C:/dev/RLC-Agent"); sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv; load_dotenv(ROOT / ".env")
from src.services.database.db_config import get_connection

FLOOR = date(2006, 1, 1)
EST_OFFLINE = date(2024, 1, 1)
SEED = 'seed_from_master'

# Shakeout-era cessation dates captured from THIS SESSION's research while the evidence is fresh
# (design v1.6 §3.2 population plan, item 1). Month-precision matters for monthly allocation.
# facility_id -> (effective_date, change_reason). Source stays 'master' — these are dated events.
SHAKEOUT = {
    19:  (date(2024, 3, 1),  'idled_mar2024_chevron_reg'),      # REG Ralston
    36:  (date(2024, 4, 1),  'closed_apr2024_chevron_reg'),     # REG Madison
    33:  (date(2024, 12, 1), 'paused_dec2024_40a_expiry'),      # Western Dubuque
    38:  (date(2024, 12, 1), 'ceased_end2024_herobx_receivership'),  # Hero BX Moundville
    18:  (date(2024, 12, 1), 'ceased_end2024_herobx_receivership'),  # Hero BX Erie
    49:  (date(2024, 12, 1), 'ceased_end2024_herobx_receivership'),  # Hero BX Clinton
    424: (date(2025, 12, 1), 'rdu_reverted_petroleum_dec2025'),      # CVR Wynnewood
}

def ins(cur, fid, eff, cap, op, status, reason, src):
    cur.execute("""INSERT INTO reference.facility_capacity_history
        (facility_id, effective_date, nameplate_mmgy, operating_mmgy, status, change_reason, source)
        VALUES (%s,%s,%s,%s,%s,%s,%s)""", (fid, eff, cap, op, status, reason, src))

with get_connection() as c:
    cur = c.cursor()
    cur.execute("DELETE FROM reference.facility_capacity_history WHERE source IN ('seed_from_master','estimated')")
    cur.execute("""SELECT facility_id, status, nameplate_mmgy, year_online, year_offline
                   FROM reference.biofuel_facilities WHERE nameplate_mmgy IS NOT NULL""")
    n = 0
    for r in cur.fetchall():
        st = (r['status'] or '').lower()
        if st in ('merged', 'planned', 'announced'):
            continue
        try: cap = float(r['nameplate_mmgy'])
        except (TypeError, ValueError): continue
        if cap <= 0: continue
        online = date(int(r['year_online']), 1, 1) if r['year_online'] else FLOOR
        online_src = 'seed_from_master' if r['year_online'] else 'estimated'   # FLOOR default = a guess
        fid = r['facility_id']
        if st in ('idle', 'closed'):
            if fid in SHAKEOUT:                                  # dated event from this session's log
                off, reason, off_src = (*SHAKEOUT[fid], 'seed_from_master')
            elif r['year_offline']:
                off, reason, off_src = date(int(r['year_offline']), 1, 1), 'offline', 'seed_from_master'
            else:
                off, reason, off_src = EST_OFFLINE, 'offline_estimated', 'estimated'
            if off > online:                         # real operating window then offline
                ins(cur, fid, online, cap, cap, 'operating', 'came_online', online_src); n += 1
                ins(cur, fid, off, cap, 0, st, reason, off_src); n += 1
            else:                                    # degenerate (offline <= online): never establish operating
                ins(cur, fid, online, cap, 0, st, reason, off_src); n += 1
        else:                                        # operating / under_construction
            ins(cur, fid, online, cap, cap, 'operating', 'came_online', online_src); n += 1
    c.commit()
    print(f"seeded {n} change-log rows into facility_capacity_history")
    # sanity: operating count as-of period (should climb over time as plants come online, dip after shakeout)
    for p in ('2012-06-01', '2018-06-01', '2023-06-01', '2025-06-01'):
        cur.execute("""SELECT count(*) n FROM (
            SELECT DISTINCT ON (facility_id) status FROM reference.facility_capacity_history
            WHERE effective_date <= %s ORDER BY facility_id, effective_date DESC) x WHERE status='operating'""", (p,))
        print(f"  operating as-of {p}: {cur.fetchone()['n']}")
