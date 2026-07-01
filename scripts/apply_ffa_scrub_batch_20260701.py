"""Apply the 2026-07-01 big-RD scrub batch (Tore-reviewed).

Cleans the operating denominator so the feedstock rollup isn't dominated by junk:
  - international plants counted as US -> padd='NON-US'
  - BP Cherry Point phantom capacity (2020 MMgy crude throughput -> 110 MMgy biofeed)
  - vaporware/bankrupt projects marked operating -> planned/closed/under_construction
  - Chalmette duplicate merged; Paramount 'Houston TX' zombie -> idle
  - Texmark -> fractionation (non-lipid SAF processor; excluded by rollup tech filter)
  - New Rise Reno (real operating HEFA) -> PADD5 + provisional low-CI mix
Pairs with the non-lipid technology filter added to national_feedstock_consumption.py.
Transactional. NOTE: PADD assignment for the remaining ~35 clean US plants is deferred
(only affects the '?' PADD row, not national totals).
"""
import sys
from pathlib import Path
ROOT = Path(r"C:/dev/RLC-Agent"); sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv; load_dotenv(ROOT / ".env")
from src.services.database.db_config import get_connection

INTL = [267, 287, 281, 265, 270, 291, 285, 279, 286, 292, 273]   # -> NON-US (exclude from US rollup)
STATUS = {84: 'planned', 102: 'planned', 91: 'planned', 80: 'planned', 82: 'planned',
          103: 'under_construction', 79: 'closed', 74: 'closed', 67: 'idle'}

with get_connection() as c:
    cur = c.cursor()
    for fid in INTL:
        cur.execute("UPDATE reference.biofuel_facilities SET padd='NON-US', updated_at=now() WHERE facility_id=%s", (fid,))
    cur.execute("""UPDATE reference.biofuel_facilities SET nameplate_mmgy=110, updated_at=now(),
                   notes='BP Cherry Point RD coprocessing ~110 MMgy (2.6M bbl/yr, 2022 expansion). Prior 2020 MMgy = refinery crude throughput, not biofeed.' WHERE facility_id=125""")
    for fid, st in STATUS.items():
        cur.execute("UPDATE reference.biofuel_facilities SET status=%s, updated_at=now() WHERE facility_id=%s", (st, fid))
    cur.execute("UPDATE reference.biofuel_facilities SET status='merged', notes='Duplicate of id=313 St Bernard Renewables (Chalmette, PBF/ENI JV). Merged 2026-07-01.' WHERE facility_id=85")
    cur.execute("""UPDATE reference.biofuel_facilities SET technology='fractionation', fuel_type='saf', updated_at=now(),
                   notes='Texmark, Galena Park TX: specialty chemical toller (DCPD) + SAF FRACTIONATION of finished/imported RD. Non-lipid: consumes no oils/fats. Excluded from feedstock rollup.' WHERE facility_id=105""")
    cur.execute("UPDATE reference.biofuel_facilities SET state='NV', padd='PADD5', updated_at=now() WHERE facility_id=93")
    cur.execute("UPDATE reference.biofuel_facilities SET notes='Ryze Las Vegas: bankrupt, never operational; site sold to Edgewood (id=103). Closed 2026-07-01.' WHERE facility_id=74")
    cur.execute("DELETE FROM reference.facility_assumed_mix WHERE facility_id=93")
    for code, pct in [('BFT', 40), ('UCO', 40), ('DCO', 20)]:
        cur.execute("INSERT INTO reference.facility_assumed_mix (facility_id,feedstock_code,pct,source,loaded_at) VALUES (93,%s,%s,'web_verified',now())", (code, pct))
    c.commit()
    print("Applied: 11 -> NON-US, BP 2020->110, 9 status changes, Chalmette dup merged, Texmark->fractionation, New Rise PADD5+mix.")
