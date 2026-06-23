"""
Build the unified facility master: anchor on curated industry lists (the facilities
we actually model, carrying capacity + status), enrich each with EPA ECHO's precise
lat/long + frs_registry_id + operating_status (cross-checked against curated status).

Crosswalk = normalized facility name + state, with city as confirmation. (Geo-matching
is the more robust technique but the curated lists mostly lack coordinates; ECHO supplies
them on match, enabling geo for downstream permit/dedup work.)

Outputs:
  - data/exports/facility_master.csv  (reviewable)
  - stats: match rate, status disagreements, curated orphans (no ECHO match),
    and ECHO majors with NO curated match (completeness — possible missing facilities)

Dry by default (CSV + stats). --write persists to silver.facility_master_unified.
"""
import csv
import re
import sys
import argparse
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv
load_dotenv(ROOT / ".env")
from src.services.database.db_config import get_connection

OUT = ROOT / "data" / "exports" / "facility_master.csv"

# curated list -> ECHO search_profile
PROFILE = {"ethanol": "ethanol", "crush": "soybean_oilseed",
           "renewable_diesel": "biodiesel_renewable_diesel",
           "biodiesel": "biodiesel_renewable_diesel"}

_SUFFIX = re.compile(r"\b(inc|llc|l\.?l\.?c|corp|corporation|company|co|lp|l\.?p|ltd|"
                     r"plant|division|the|holdings|partners|energy|fuels?|company)\b", re.I)


def norm(s: str) -> str:
    s = (s or "").lower()
    s = _SUFFIX.sub(" ", s)
    s = re.sub(r"[^a-z0-9]+", " ", s).strip()
    return re.sub(r"\s+", " ", s)


def tokens(s: str) -> set:
    return set(norm(s).split())


def name_score(a: str, b: str) -> float:
    """Token-set similarity (Jaccard-ish), with substring boost."""
    ta, tb = tokens(a), tokens(b)
    if not ta or not tb:
        return 0.0
    inter = len(ta & tb)
    jac = inter / len(ta | tb)
    na, nb = norm(a), norm(b)
    if na and (na in nb or nb in na):
        jac = max(jac, 0.8)
    return jac


def load_curated(cur):
    rows = []
    # ethanol
    cur.execute("SELECT name, operator, city, NULL county, state, nameplate_mmgy, status FROM reference.ethanol_facilities")
    for r in cur.fetchall():
        d = dict(r) if isinstance(r, dict) else dict(zip(['name','operator','city','county','state','nameplate_mmgy','status'], r))
        rows.append({**d, "industry": "ethanol", "capacity": d["nameplate_mmgy"], "cap_unit": "mmgy"})
    # crush
    cur.execute("SELECT name, operator, city, county, state, nameplate_mmbu_yr, nameplate_tpd, status FROM reference.oilseed_crush_facilities")
    for r in cur.fetchall():
        d = dict(r) if isinstance(r, dict) else dict(zip(['name','operator','city','county','state','nameplate_mmbu_yr','nameplate_tpd','status'], r))
        cap = d["nameplate_mmbu_yr"] or d["nameplate_tpd"]
        rows.append({"name": d["name"], "operator": d["operator"], "city": d["city"], "county": d["county"],
                     "state": d["state"], "status": d["status"], "industry": "crush",
                     "capacity": cap, "cap_unit": "mmbu_yr" if d["nameplate_mmbu_yr"] else "tpd"})
    # RD
    cur.execute("SELECT name, operator, city, NULL county, state, nameplate_mmgy, status FROM reference.renewable_diesel_facilities")
    for r in cur.fetchall():
        d = dict(r) if isinstance(r, dict) else dict(zip(['name','operator','city','county','state','nameplate_mmgy','status'], r))
        rows.append({**d, "industry": "renewable_diesel", "capacity": d["nameplate_mmgy"], "cap_unit": "mmgy"})
    # biodiesel (name + capacity only, no location)
    cur.execute("SELECT name, NULL operator, NULL city, NULL county, NULL state, nameplate_mmgy, NULL status FROM reference.biodiesel_facilities")
    for r in cur.fetchall():
        d = dict(r) if isinstance(r, dict) else dict(zip(['name','operator','city','county','state','nameplate_mmgy','status'], r))
        rows.append({**d, "industry": "biodiesel", "capacity": d["nameplate_mmgy"], "cap_unit": "mmgy"})
    return rows


def load_echo(cur):
    cur.execute("""SELECT facility_name, city, state, latitude, longitude, frs_registry_id,
                          operating_status, air_universe, search_profile
                   FROM bronze.epa_echo_facility WHERE search_profile IS NOT NULL""")
    by_profile = {}
    for r in cur.fetchall():
        d = dict(r) if isinstance(r, dict) else dict(zip(
            ['facility_name','city','state','latitude','longitude','frs_registry_id','operating_status','air_universe','search_profile'], r))
        by_profile.setdefault(d["search_profile"], []).append(d)
    return by_profile


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--write", action="store_true")
    ap.add_argument("--threshold", type=float, default=0.5)
    args = ap.parse_args()

    with get_connection() as c:
        cur = c.cursor()
        curated = load_curated(cur)
        echo = load_echo(cur)

    master = []
    matched_echo_keys = set()
    for f in curated:
        prof = PROFILE[f["industry"]]
        cands = echo.get(prof, [])
        st = (f.get("state") or "").upper()
        if st:
            cands = [e for e in cands if (e.get("state") or "").upper() == st]
        best, bs = None, 0.0
        for e in cands:
            s = name_score(f["name"], e["facility_name"])
            # operator can match the ECHO name too (e.g. parent company)
            if f.get("operator"):
                s = max(s, name_score(f["operator"], e["facility_name"]))
            # city agreement boost
            if f.get("city") and e.get("city") and norm(f["city"]) == norm(e["city"]):
                s += 0.15
            if s > bs:
                bs, best = s, e
        m = {"industry": f["industry"], "name": f["name"], "operator": f.get("operator"),
             "city": f.get("city"), "state": f.get("state"),
             "capacity": f.get("capacity"), "cap_unit": f.get("cap_unit"),
             "curated_status": f.get("status")}
        if best and bs >= args.threshold:
            m.update({"echo_name": best["facility_name"], "echo_frs_id": best["frs_registry_id"],
                      "lat": best["latitude"], "lon": best["longitude"],
                      "echo_status": best["operating_status"], "echo_universe": best["air_universe"],
                      "match_score": round(bs, 2)})
            matched_echo_keys.add((best["search_profile"], best["facility_name"], best.get("state")))
        else:
            m.update({"echo_name": None, "match_score": round(bs, 2)})
        master.append(m)

    # stats
    n = len(master)
    matched = sum(1 for m in master if m["echo_name"])
    has_cap = sum(1 for m in master if m["capacity"])
    status_disagree = [m for m in master if m["echo_name"] and m.get("curated_status") and m.get("echo_status")
                       and m["curated_status"].lower().startswith("oper") and "closed" in (m["echo_status"] or "").lower()]
    # ECHO majors in our profiles with NO curated match (completeness)
    echo_major_unmatched = []
    for prof, lst in echo.items():
        for e in lst:
            if (e.get("air_universe") or "").lower().startswith("major") and \
               (prof, e["facility_name"], e.get("state")) not in matched_echo_keys:
                echo_major_unmatched.append((prof, e["facility_name"], e.get("state")))

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["industry","name","operator","city","state","capacity","cap_unit",
                    "curated_status","echo_name","echo_status","echo_universe","echo_frs_id",
                    "lat","lon","match_score"])
        for m in master:
            w.writerow([m["industry"],m["name"],m.get("operator"),m.get("city"),m.get("state"),
                        m.get("capacity"),m.get("cap_unit"),m.get("curated_status"),m.get("echo_name"),
                        m.get("echo_status"),m.get("echo_universe"),m.get("echo_frs_id"),
                        m.get("lat"),m.get("lon"),m.get("match_score")])

    by_ind = {}
    for m in master:
        b = by_ind.setdefault(m["industry"], [0,0,0])
        b[0]+=1; b[1]+= 1 if m["echo_name"] else 0; b[2]+= 1 if m["capacity"] else 0
    print(f"=== facility master: {n} curated facilities -> {OUT} ===")
    print(f"  ECHO-matched: {matched}/{n} ({100*matched//n}%) | with capacity: {has_cap}/{n}")
    print(f"  status disagreements (curated Operating vs ECHO Closed): {len(status_disagree)}")
    print(f"  ECHO majors in our profiles with NO curated match (possible missing facilities): {len(echo_major_unmatched)}")
    print("  by industry (total / echo-matched / w-capacity):")
    for ind,b in sorted(by_ind.items()): print(f"    {ind:18s} {b[0]:>3} / {b[1]:>3} / {b[2]:>3}")
    if status_disagree[:8]:
        print("  sample status disagreements:")
        for m in status_disagree[:8]: print(f"    {m['name'][:34]:34s} curated={m['curated_status']} echo={m['echo_status']}")

    # review queues
    dq = OUT.parent / "facility_status_review_queue.csv"
    with open(dq, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh); w.writerow(["industry","name","state","capacity","cap_unit","curated_status","echo_status","echo_frs_id"])
        for m in status_disagree:
            w.writerow([m["industry"],m["name"],m.get("state"),m.get("capacity"),m.get("cap_unit"),m["curated_status"],m["echo_status"],m.get("echo_frs_id")])
    print(f"  -> status review queue: {dq} ({len(status_disagree)} rows)")
    uq = OUT.parent / "echo_unmatched_majors.csv"
    with open(uq, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh); w.writerow(["profile","echo_name","state"])
        for prof,nm,st in sorted(echo_major_unmatched): w.writerow([prof,nm,st])
    print(f"  -> ECHO unmatched majors (completeness, has false-positives): {uq} ({len(echo_major_unmatched)} rows)")

    if args.write:
        with get_connection() as c:
            cur = c.cursor()
            cur.execute("""CREATE TABLE IF NOT EXISTS silver.facility_master_unified (
                industry text, name text, operator text, city text, state text,
                capacity numeric, cap_unit text, curated_status text,
                echo_name text, echo_status text, echo_universe text, echo_frs_id text,
                lat double precision, lon double precision, match_score numeric,
                built_at timestamptz DEFAULT now())""")
            cur.execute("TRUNCATE silver.facility_master_unified")
            for m in master:
                cur.execute("""INSERT INTO silver.facility_master_unified
                    (industry,name,operator,city,state,capacity,cap_unit,curated_status,
                     echo_name,echo_status,echo_universe,echo_frs_id,lat,lon,match_score)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (m["industry"],m["name"],m.get("operator"),m.get("city"),m.get("state"),
                     m.get("capacity"),m.get("cap_unit"),m.get("curated_status"),m.get("echo_name"),
                     m.get("echo_status"),m.get("echo_universe"),m.get("echo_frs_id"),
                     m.get("lat"),m.get("lon"),m.get("match_score")))
            c.commit()
        print(f"  -> persisted {len(master)} rows to silver.facility_master_unified")


if __name__ == "__main__":
    main()
