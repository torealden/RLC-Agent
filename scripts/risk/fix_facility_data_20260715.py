"""Facility-data fixes surfaced by the quarterly risk-budget generator (2026-07-15).

1. CANOLA code collision: 'CAN' and 'CO' are both canola. The system standardized on 'CO'
   (the code with price/vol data), so 'CAN' silently drops out of the allocator/risk engine.
   Replace 'CAN' -> 'CO' in eligible_feedstocks (dedup) across all facilities, and in
   primary_feedstock. Surfaced at BP Stage 2 but systemic (131 facilities).
   NOTE: 'CAM' (camelina) and 'CAR' (carinata) are LEFT ALONE — distinct low-CI SAF oilseeds,
   not typos; they simply aren't priced yet (flagged to the FFA calibration queue).

2. BP Stage 1 (facility_id 124) nameplate = 2018.0 MMgy — the YEAR miskeyed as capacity,
   yielding a 3.3B-lb budget. Set to a flagged ESTIMATE (60 MMgy) pending calibration;
   the Stage1/Stage2 split may also double-count one physical co-processing unit (id 125 =
   110 MMgy is the documented 2022 expansion). Routed to the calibration queue.
"""
import sys
from pathlib import Path
ROOT = Path(r"C:/dev/RLC-Agent"); sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv; load_dotenv(ROOT / ".env")
from src.services.database.db_config import get_connection

BP_STAGE1_ID = 124
BP_STAGE1_ESTIMATE_MMGY = 60.0   # flagged estimate; verify true renewable co-processing capacity

with get_connection() as conn:
    cur = conn.cursor()

    # 1. CAN -> CO in eligible_feedstocks (replace + dedup), preserve as a set
    cur.execute("SELECT count(*) n FROM reference.biofuel_facilities WHERE 'CAN'=ANY(eligible_feedstocks)")
    n_elig = cur.fetchone()['n']
    cur.execute("""
        UPDATE reference.biofuel_facilities
        SET eligible_feedstocks = (
            SELECT array_agg(DISTINCT e ORDER BY e)
            FROM unnest(array_replace(eligible_feedstocks, 'CAN', 'CO')) e)
        WHERE 'CAN' = ANY(eligible_feedstocks)""")

    # primary_feedstock CAN -> CO
    cur.execute("UPDATE reference.biofuel_facilities SET primary_feedstock='CO' WHERE primary_feedstock='CAN'")
    n_prim = cur.rowcount

    # 2. BP Stage 1 nameplate 2018.0 (year) -> flagged estimate
    cur.execute("""UPDATE reference.facility_capacity_history
                   SET nameplate_mmgy=%s
                   WHERE facility_id=%s AND nameplate_mmgy=2018.0""",
                (BP_STAGE1_ESTIMATE_MMGY, BP_STAGE1_ID))
    n_bp = cur.rowcount

    conn.commit()

    print(f"1. CANOLA: {n_elig} facilities had 'CAN' in eligible_feedstocks -> replaced with 'CO' (deduped)")
    print(f"   primary_feedstock 'CAN' -> 'CO': {n_prim} facilities")
    print(f"2. BP Stage 1 (id {BP_STAGE1_ID}) nameplate 2018.0 -> {BP_STAGE1_ESTIMATE_MMGY} MMgy (ESTIMATE): {n_bp} row")

    # verify
    cur.execute("SELECT count(*) n FROM reference.biofuel_facilities WHERE 'CAN'=ANY(eligible_feedstocks)")
    print(f"\nverify: facilities still using 'CAN': {cur.fetchone()['n']} (should be 0)")
    cur.execute("""SELECT facility_id, facility_name, eligible_feedstocks, primary_feedstock
                   FROM reference.biofuel_facilities WHERE facility_id IN (124,125)""")
    for r in cur.fetchall():
        print(f"  id={r['facility_id']} {r['facility_name']}: elig={r['eligible_feedstocks']} primary={r['primary_feedstock']}")
    cur.execute("""SELECT nameplate_mmgy FROM reference.facility_capacity_history
                   WHERE facility_id=124 ORDER BY effective_date DESC LIMIT 1""")
    print(f"  BP Stage 1 latest nameplate: {cur.fetchone()['nameplate_mmgy']} MMgy")
    # residual camelina/carinata note
    cur.execute("""SELECT count(*) n FROM reference.biofuel_facilities
                   WHERE 'CAM'=ANY(eligible_feedstocks) OR 'CAR'=ANY(eligible_feedstocks)""")
    print(f"\nNOTE: {cur.fetchone()['n']} facilities use CAM (camelina) / CAR (carinata) — distinct "
          "unpriced feedstocks, left as-is (flagged to calibration queue).")
