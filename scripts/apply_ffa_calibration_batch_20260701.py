"""Apply the 2026-07-01 FFA facility calibration batch (Tore-reviewed).

Idles the 2024-25 shakeout zombies, merges duplicates (status='merged' to respect the
gold.feedstock_allocation FK), and writes two live operating mixes (WIE, Agron). Transactional;
prints before/after for every touched facility. Re-run scripts/national_feedstock_consumption.py after.
"""
import sys, json
from pathlib import Path
ROOT = Path(r"C:/dev/RLC-Agent"); sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv; load_dotenv(ROOT / ".env")
from src.services.database.db_config import get_connection

# (facility_id, {column: value})
UPDATES = [
    # --- idled zombies (drop out of the operating rollup) ---
    (19, {'status':'idle','nameplate_mmgy':30,'primary_feedstock':'SBO','year_offline':2024,
          'notes':'Chevron REG; idled indefinitely Mar 2024 (low RVO + RD surge). First REG plant, built 2002 by West Central Coop; soy from adjacent Landus crush; expanded 12->30 MMgy 2016.'}),
    (36, {'status':'idle','nameplate_mmgy':20,'primary_feedstock':'SBO','year_offline':2024,
          'notes':'Chevron REG; closed indefinitely Apr 2024. Only BD plant in WI; ran WI soybean oil.'}),
    (33, {'status':'idle','nameplate_mmgy':30,'primary_feedstock':'SBO','year_offline':2024,
          'notes':'Western Dubuque Biodiesel, Farley IA; farmer-owned (founded 2005, prod 2007). Paused late Dec 2024 (40A expiration), retaining staff. NOT a Chevron REG plant.'}),
    (38, {'status':'idle','nameplate_mmgy':20,'primary_feedstock':'SBO','year_offline':2024,
          'eligible_feedstocks':['SBO','CO','UCO','YG','BFT','PLT','FSH'],
          'notes':'Hero BX Moundville (Lake Erie Biofuels). Long-term idled; parent federal receivership mid-2025 ($6.1M Siena default), assets in foreclosure. Multi-feedstock incl. AL catfish oil (FSH).'}),
    (18, {'status':'idle','primary_feedstock':'SBO','year_offline':2024,
          'notes':'Hero BX Erie flagship; ceased end-2024 (credit expiration); parent receivership/foreclosure 2025. Rated 45 MMgy, peaked 50; multi-feedstock/UCO.'}),
    (49, {'status':'idle','year_offline':2024,
          'notes':'Hero BX Clinton (10 MMgy); receivership Jun 2025, court-ordered sealed-bid auction (Maas Cos.). Idled.'}),
    # --- duplicate merges (FK-safe: status=merged, not deleted) ---
    (330, {'status':'merged','notes':'Duplicate of id=33 Western Dubuque Biodiesel. Merged 2026-07-01.'}),
    (391, {'status':'merged','notes':'Duplicate of id=38 Hero BX Moundville. Merged 2026-07-01.'}),
    (331, {'status':'merged','notes':'Duplicate of id=21 Western Iowa Energy (Wall Lake). Merged 2026-07-01.'}),
    # --- live operating plants (these move the gap) ---
    (21, {'status':'operating','nameplate_mmgy':45,'primary_feedstock':'BFT',
          'eligible_feedstocks':['SBO','BFT','DCO','UCO'],
          'notes':'Western Iowa Energy, Wall Lake IA. Operating (returned to full capacity on RFS RVO + 45Z). Fats-led multi-feedstock via pretreatment. Mix PROVISIONAL pending live verification.'}),
    (43, {'status':'operating','facility_name':'Agron Bioenergy (WIE) - Watsonville, CA','state':'CA','padd':'PADD5',
          'nameplate_mmgy':15,'primary_feedstock':'BFT','eligible_feedstocks':['BFT','UCO','YG'],
          'notes':'Agron Bioenergy, Watsonville CA; owned by Western Iowa Energy. Revived 2018 (LCFS-supported). Primarily beef tallow, B20 for Monterey Bay fleets. Zero-soy CA LCFS plant.'}),
]

# facility_id -> [(feedstock_code, pct)]  (source=web_verified). id=38 latent (idle, not counted).
MIXES = {
    21: [('SBO',15),('BFT',40),('DCO',25),('UCO',20)],
    43: [('BFT',55),('UCO',30),('YG',15)],
    38: [('SBO',45),('CO',8),('UCO',15),('YG',10),('BFT',12),('PLT',10)],
}
TOUCHED = sorted({u[0] for u in UPDATES} | set(MIXES))

def snapshot(cur):
    cur.execute("""SELECT facility_id, facility_name, state, padd, nameplate_mmgy, status,
                   primary_feedstock, eligible_feedstocks FROM reference.biofuel_facilities
                   WHERE facility_id = ANY(%s) ORDER BY facility_id""", (TOUCHED,))
    return {r['facility_id']: dict(r) for r in cur.fetchall()}

with get_connection() as c:
    cur = c.cursor()
    cur.execute("""SELECT data_type FROM information_schema.columns WHERE table_schema='reference'
                   AND table_name='biofuel_facilities' AND column_name='eligible_feedstocks'""")
    elig_is_jsonb = cur.fetchone()['data_type'] == 'jsonb'

    before = snapshot(cur)

    for fid, cols in UPDATES:
        sets, vals = [], []
        for k, v in cols.items():
            if k == 'eligible_feedstocks' and elig_is_jsonb:
                v = json.dumps(v)
            sets.append(f"{k} = %s")
            vals.append(v)
        sets.append("updated_at = now()")
        vals.append(fid)
        cur.execute(f"UPDATE reference.biofuel_facilities SET {', '.join(sets)} WHERE facility_id = %s", vals)

    for fid, rows in MIXES.items():
        cur.execute("DELETE FROM reference.facility_assumed_mix WHERE facility_id = %s", (fid,))
        for code, pct in rows:
            cur.execute("""INSERT INTO reference.facility_assumed_mix (facility_id, feedstock_code, pct, source, loaded_at)
                           VALUES (%s, %s, %s, 'web_verified', now())""", (fid, code, pct))

    after = snapshot(cur)
    c.commit()

print("=== FACILITY CHANGES ===")
for fid in TOUCHED:
    b, a = before.get(fid, {}), after.get(fid, {})
    print(f"\nid={fid}  {a.get('facility_name')}")
    for f in ('status','nameplate_mmgy','padd','primary_feedstock','eligible_feedstocks'):
        if b.get(f) != a.get(f):
            print(f"    {f}: {b.get(f)!r} -> {a.get(f)!r}")
print("\n=== MIXES WRITTEN (source=web_verified) ===")
for fid, rows in MIXES.items():
    print(f"  id={fid}: " + " / ".join(f"{c2} {p}" for c2, p in rows))
print("\nCommitted. Now re-run scripts/national_feedstock_consumption.py")
