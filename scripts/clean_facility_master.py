"""
Facility-master hygiene pass for reference.biofuel_facilities.

1. DELETE 16 fastmarkets spreadsheet-LABEL rows that were ingested as facilities
   ("EIA Reported Capacity", "RD Percent", "Total Feedstock Use", ...). Guarded:
   only deletes exact label names with data_source='fastmarkets' AND NULL nameplate,
   so no real facility is touched.
2. NORMALIZE state -> 2-letter US abbrev (handles 'ALABAMA', 'Bakersfield, California').
3. BACKFILL padd from normalized US state where padd IS NULL.
4. TAG foreign facilities (Canada/Brazil/etc.) with padd='NON-US' so US rollups exclude them.

DRY-RUN by default; --write to persist.
"""
import sys, argparse
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]; sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv; load_dotenv(ROOT / ".env")
from src.services.database.db_config import get_connection

JUNK_LABELS = [
    'EIA Reported Capacity', 'D4 Domestic RD', 'D4 - Domestic Renewable Jet Fuel',
    'Capacity Utilization', 'Pre-Treatment Capacity', 'Implied Monthly RD Yield',
    'Implied Monthly RD Yield ex. SAF', 'Implied Monthly BD Yield', 'Implied Monthly SAF Yield',
    'RD Percent', 'BD Percent', 'Biodiesel', 'Biodiesel Annual Capacity (mmgal/yr)',
    'SAF', 'Total Feedstock Use', 'Pre-Aug Revisions',
]
US_STATES = {
    'ALABAMA':'AL','ALASKA':'AK','ARIZONA':'AZ','ARKANSAS':'AR','CALIFORNIA':'CA','COLORADO':'CO',
    'CONNECTICUT':'CT','DELAWARE':'DE','DISTRICT OF COLUMBIA':'DC','FLORIDA':'FL','GEORGIA':'GA',
    'HAWAII':'HI','IDAHO':'ID','ILLINOIS':'IL','INDIANA':'IN','IOWA':'IA','KANSAS':'KS','KENTUCKY':'KY',
    'LOUISIANA':'LA','MAINE':'ME','MARYLAND':'MD','MASSACHUSETTS':'MA','MICHIGAN':'MI','MINNESOTA':'MN',
    'MISSISSIPPI':'MS','MISSOURI':'MO','MONTANA':'MT','NEBRASKA':'NE','NEVADA':'NV','NEW HAMPSHIRE':'NH',
    'NEW JERSEY':'NJ','NEW MEXICO':'NM','NEW YORK':'NY','NORTH CAROLINA':'NC','NORTH DAKOTA':'ND',
    'OHIO':'OH','OKLAHOMA':'OK','OREGON':'OR','PENNSYLVANIA':'PA','RHODE ISLAND':'RI','SOUTH CAROLINA':'SC',
    'SOUTH DAKOTA':'SD','TENNESSEE':'TN','TEXAS':'TX','UTAH':'UT','VERMONT':'VT','VIRGINIA':'VA',
    'WASHINGTON':'WA','WEST VIRGINIA':'WV','WISCONSIN':'WI','WYOMING':'WY',
}
STATE2PADD = {
    **{s:'PADD1' for s in ['CT','ME','MA','NH','RI','VT','NY','NJ','PA','DE','MD','DC','WV','VA','NC','SC','GA','FL']},
    **{s:'PADD2' for s in ['IL','IN','IA','KS','KY','MI','MN','MO','NE','ND','OH','OK','SD','TN','WI']},
    **{s:'PADD3' for s in ['AL','AR','LA','MS','NM','TX']},
    **{s:'PADD4' for s in ['CO','ID','MT','UT','WY']},
    **{s:'PADD5' for s in ['AK','AZ','CA','HI','NV','OR','WA']},
}
FOREIGN = ('canada','brazil','australia','argentina','singapore','netherland','china',
           'ontario','columbia','british columbia','newfoundland','quebec','alberta')

def norm_state(raw):
    """Return (us_abbrev | None, is_foreign)."""
    if not raw: return None, False
    s = str(raw).strip()
    if any(f in s.lower() for f in FOREIGN): return None, True
    tail = s.split(',')[-1].strip()          # "Bakersfield, California" -> "California"
    up = tail.upper()
    if up in STATE2PADD: return up, False     # already 2-letter
    if up in US_STATES:  return US_STATES[up], False
    return None, False                         # unknown -> leave alone

def g(r, k, i):
    try: return r[k]
    except Exception: return r[i]

def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--write", action="store_true"); args = ap.parse_args()
    with get_connection() as c:
        cur = c.cursor()
        cur.execute("SELECT count(*) n FROM reference.biofuel_facilities"); before = g(cur.fetchone(),'n',0)

        # 1. identify junk
        cur.execute("""SELECT facility_id, facility_name FROM reference.biofuel_facilities
                       WHERE facility_name = ANY(%s) AND data_source='fastmarkets' AND nameplate_mmgy IS NULL""",
                    (JUNK_LABELS,))
        junk = cur.fetchall()
        print(f"=== Facility-master hygiene (DRY-RUN={'no' if args.write else 'yes'}) — {before} rows ===\n")
        print(f"1. DELETE {len(junk)} junk label-rows:")
        for r in junk: print(f"     id={g(r,'facility_id',0)}  {g(r,'facility_name',1)}")

        # 2+3+4. state normalize / padd backfill / foreign tag
        cur.execute("SELECT facility_id, state, padd FROM reference.biofuel_facilities")
        st_fix = padd_fill = foreign = 0; samples = []
        for r in cur.fetchall():
            fid = g(r,'facility_id',0); raw = g(r,'state',1); padd = g(r,'padd',2)
            ab, isf = norm_state(raw)
            if isf:
                if padd != 'NON-US': foreign += 1
                continue
            if ab and ab != raw:
                st_fix += 1
                if len(samples) < 8: samples.append(f"'{raw}' -> {ab}")
            if ab and not padd and ab in STATE2PADD:
                padd_fill += 1
        print(f"\n2. NORMALIZE state -> 2-letter: {st_fix} rows  e.g. {', '.join(samples)}")
        print(f"3. BACKFILL padd from state: {padd_fill} rows")
        print(f"4. TAG foreign (padd='NON-US'): {foreign} rows")

        if args.write:
            cur.execute("""DELETE FROM reference.biofuel_facilities
                           WHERE facility_name = ANY(%s) AND data_source='fastmarkets' AND nameplate_mmgy IS NULL""",
                        (JUNK_LABELS,))
            cur.execute("SELECT facility_id, state, padd FROM reference.biofuel_facilities")
            updates = []
            for r in cur.fetchall():
                fid = g(r,'facility_id',0); raw = g(r,'state',1); padd = g(r,'padd',2)
                ab, isf = norm_state(raw)
                if isf:
                    if padd != 'NON-US': updates.append((raw, 'NON-US', fid))
                    continue
                if not ab: continue
                new_padd = padd or STATE2PADD.get(ab)
                if ab != raw or new_padd != padd:
                    updates.append((ab, new_padd, fid))
            for ab, padd, fid in updates:
                cur.execute("UPDATE reference.biofuel_facilities SET state=%s, padd=%s, updated_at=now() WHERE facility_id=%s",
                            (ab, padd, fid))
            c.commit()
            cur.execute("SELECT count(*) n FROM reference.biofuel_facilities")
            print(f"\n[WROTE] deleted {len(junk)} junk, updated {len(updates)} state/padd. Rows now: {g(cur.fetchone(),'n',0)}")
        else:
            print(f"\n[DRY-RUN] no writes. Re-run with --write.")

if __name__ == "__main__":
    main()
