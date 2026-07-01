"""FFA RD-side calibration + scrub (2026-07-01 session, Tore-reviewed). Reproducible record.

Companion to apply_ffa_calibration_batch (BD plants) and apply_ffa_scrub_batch (Tier1-3/5).
This applies: (a) real feedstock mixes for the big RD plants, (b) status/capacity fixes found
during per-plant verification, (c) non-lipid technology tags, (d) DGD dedup. All web-verified with
Tore. DB is authoritative; this documents the changes for audit/replay. Idempotent.
"""
import sys, json
from pathlib import Path
ROOT = Path(r"C:/dev/RLC-Agent"); sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv; load_dotenv(ROOT / ".env")
from src.services.database.db_config import get_connection

# facility_id -> feedstock mix (source=web_verified, pct)
MIX = {
    93:  [('BFT',40),('UCO',40),('DCO',20)],                                   # New Rise Reno
    417: [('BFT',35),('CAN',25),('DCO',15),('UCO',15),('CAM',10)],             # Montana Renewables
    422: [('SBO',35),('DCO',20),('CAN',20),('BFT',15),('UCO',10)],             # Dakota Prairie
    125: [('CAN',25),('UCO',25),('SBO',15),('DCO',15),('BFT',15),('CAR',5)],   # BP Cherry Point (Blaine)
    395: [('CAM',35),('SBO',30),('CAN',20),('UCO',15)],                        # Bakersfield RF (GCE)
    78:  [('UCO',45),('BFT',40),('CAM',15)],                                   # Chevron ReadiFuels
    312: [('BFT',35),('SBO',25),('DCO',20),('UCO',20)],                        # Martinez (Marathon/Neste)
    316: [('BFT',40),('UCO',30),('DCO',15),('SBO',15)],                        # REG Geismar (Chevron)
    311: [('UCO',30),('BFT',30),('SBO',20),('DCO',15),('CWG',5)],              # P66 Rodeo
    313: [('SBO',35),('DCO',25),('UCO',20),('BFT',20)],                        # St Bernard (PBF/Eni)
}
# facility_id -> column updates
UPD = {
    93:  {'state':'NV','padd':'PADD5','primary_feedstock':'BFT'},
    417: {'status':'operating','nameplate_mmgy':184,'primary_feedstock':'BFT'},
    422: {'status':'operating','nameplate_mmgy':184,'primary_feedstock':'SBO'},
    125: {'status':'operating','primary_feedstock':'CAN','nameplate_mmgy':110},
    395: {'status':'operating','nameplate_mmgy':210,'primary_feedstock':'CAM'},
    78:  {'primary_feedstock':'UCO'},
    312: {'state':'CA','padd':'PADD5','primary_feedstock':'BFT'},
    316: {'status':'operating','state':'LA','padd':'PADD3','nameplate_mmgy':340,'primary_feedstock':'BFT'},
    311: {'state':'CA','padd':'PADD5','primary_feedstock':'UCO'},
    313: {'state':'LA','padd':'PADD3','primary_feedstock':'SBO'},
    305: {'nameplate_mmgy':470},                                    # DGD Port Arthur (real record)
    424: {'status':'idle'},                                         # CVR Wynnewood (RDU -> petroleum Dec'25)
    104: {'status':'planned'},                                      # Valley Green (pre-production)
    # non-lipid tech tags (excluded by rollup filter)
    94:{'technology':'atj'}, 97:{'technology':'atj'}, 95:{'technology':'fischer_tropsch'},
    92:{'technology':'gasification'}, 87:{'technology':'fischer_tropsch'},
}
MERGED = {61:'id=305 DGD Port Arthur', 60:'id=410 DGD Norco', 436:'id=125 BP Cherry Point'}

with get_connection() as c:
    cur = c.cursor()
    cur.execute("""SELECT data_type FROM information_schema.columns WHERE table_schema='reference'
                   AND table_name='biofuel_facilities' AND column_name='eligible_feedstocks'""")
    jsonb = cur.fetchone()['data_type'] == 'jsonb'
    for fid, cols in UPD.items():
        sets, vals = [], []
        for k, v in cols.items():
            if k == 'eligible_feedstocks' and jsonb: v = json.dumps(v)
            sets.append(f"{k}=%s"); vals.append(v)
        sets.append("updated_at=now()"); vals.append(fid)
        cur.execute(f"UPDATE reference.biofuel_facilities SET {', '.join(sets)} WHERE facility_id=%s", vals)
    for fid, into in MERGED.items():
        cur.execute("DELETE FROM reference.facility_assumed_mix WHERE facility_id=%s", (fid,))
        cur.execute("UPDATE reference.biofuel_facilities SET status='merged', updated_at=now(), "
                    "notes=%s WHERE facility_id=%s", (f'Duplicate of {into}. Merged 2026-07-01.', fid))
    for fid, rows in MIX.items():
        cur.execute("DELETE FROM reference.facility_assumed_mix WHERE facility_id=%s", (fid,))
        for code, pct in rows:
            cur.execute("INSERT INTO reference.facility_assumed_mix (facility_id,feedstock_code,pct,source,loaded_at) "
                        "VALUES (%s,%s,%s,'web_verified',now())", (fid, code, pct))
    c.commit()
    print(f"Applied {len(MIX)} RD mixes, {len(UPD)} facility updates, {len(MERGED)} merges.")
