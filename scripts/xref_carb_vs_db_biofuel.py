"""
Cross-reference CARB LCFS biofuel pathways against our DB facility inventory.

Inputs:
    domain_knowledge/external_lists/carb_lcfs/biofuel_pathways.json

Reads from DB:
    reference.biodiesel_facilities (192 rows)
    reference.biofuel_facilities (241 rows)
    reference.renewable_diesel_facilities (66 rows)
    reference.facility_master (filtered to biofuel-ish industry codes)

Output:
    domain_knowledge/external_lists/carb_lcfs/xref_report.md

Report buckets:
    A. CARB facilities we ALREADY HAVE in DB  (good)
    B. CARB facilities we are MISSING         (gap — to add)
    C. DB facilities NOT in CARB              (idled? closed? non-CA-shipping?)

Matching strategy:
    Aggregate CARB pathways by (fuel_producer, facility_name) — these are the
    real-world facilities. Normalize names (strip "LLC", "Inc", drop
    punctuation, lowercase) and look for substring/token matches against
    company+facility_name in each DB table.

The matching is intentionally loose — we'd rather flag a possible match for
manual review than miss one.
"""
from __future__ import annotations

import json
import os
import re
import sys
from collections import defaultdict
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

from dotenv import load_dotenv
load_dotenv()

import psycopg2
from psycopg2.extras import RealDictCursor


CARB_JSON = Path("domain_knowledge/external_lists/carb_lcfs/biofuel_pathways.json")
OUT_REPORT = Path("domain_knowledge/external_lists/carb_lcfs/xref_report.md")


def normalize(s: str) -> str:
    if not s:
        return ""
    s = s.lower()
    # Strip common corp suffixes
    s = re.sub(r"\b(llc|inc|corp|corporation|company|co|holdings|ltd|lp|llp|plc|gmbh|pte|sa|sas)\b", " ", s)
    # Drop punctuation
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    # Collapse whitespace
    s = re.sub(r"\s+", " ", s).strip()
    return s


def tokens(s: str) -> set[str]:
    return set(normalize(s).split())


def load_carb_facilities() -> list[dict]:
    """Aggregate CARB pathways into facility-level records."""
    pathways = json.loads(CARB_JSON.read_text(encoding="utf-8"))
    groups: dict[tuple, dict] = {}
    for p in pathways:
        fp = (p.get("fuel_producer") or "").strip()
        fn = (p.get("facility_name") or "").strip()
        loc = (p.get("facility_location") or "").strip()
        key = (normalize(fp), normalize(fn))
        if key == ("", ""):
            continue
        if key not in groups:
            groups[key] = {
                "fuel_producer": fp,
                "facility_name": fn,
                "location": loc,
                "fuel_types": set(),
                "feedstocks": set(),
                "pathway_count": 0,
                "pathway_ids": [],
            }
        g = groups[key]
        g["fuel_types"].add(p.get("fuel_type", ""))
        g["feedstocks"].add(p.get("feedstock", ""))
        g["pathway_count"] += 1
        g["pathway_ids"].append(p.get("pathway_id", ""))
    out = []
    for g in groups.values():
        g["fuel_types"] = sorted(g["fuel_types"])
        g["feedstocks"] = sorted(g["feedstocks"])
        out.append(g)
    return out


def load_db_facilities() -> list[dict]:
    """Pull union of biofuel-ish facility rows from all 4 tables."""
    conn = psycopg2.connect(
        host=os.getenv("RLC_PG_HOST"), port=os.getenv("RLC_PG_PORT", "5432"),
        dbname=os.getenv("RLC_PG_DB", "rlc_commodities"),
        user=os.getenv("RLC_PG_USER"), password=os.getenv("RLC_PG_PASSWORD"),
        sslmode="require",
    )
    cur = conn.cursor(cursor_factory=RealDictCursor)

    rows = []

    cur.execute("""
        SELECT facility_id, name AS facility_name, operator, parent_company,
               city, state, status, nameplate_mmgy, feedstock_primary,
               'reference.biodiesel_facilities' AS src
        FROM reference.biodiesel_facilities
    """)
    for r in cur.fetchall():
        rows.append({
            "src": r["src"], "facility_id": r["facility_id"],
            "facility_name": r["facility_name"], "operator": r["operator"],
            "parent_company": r["parent_company"], "city": r["city"],
            "state": r["state"], "status": r["status"],
            "capacity_mmgy": r["nameplate_mmgy"],
            "feedstock": r["feedstock_primary"],
            "fuel_type": "biodiesel",
        })

    cur.execute("""
        SELECT facility_id, facility_name, company AS operator,
               city, state, status, nameplate_mmgy, primary_feedstock, fuel_type,
               'reference.biofuel_facilities' AS src
        FROM reference.biofuel_facilities
    """)
    for r in cur.fetchall():
        rows.append({
            "src": r["src"], "facility_id": r["facility_id"],
            "facility_name": r["facility_name"], "operator": r["operator"],
            "parent_company": None, "city": r["city"],
            "state": r["state"], "status": r["status"],
            "capacity_mmgy": r["nameplate_mmgy"],
            "feedstock": r["primary_feedstock"],
            "fuel_type": r["fuel_type"],
        })

    cur.execute("""
        SELECT facility_id, name AS facility_name, operator, parent_company,
               city, state, status, nameplate_mmgy, feedstock_primary,
               'reference.renewable_diesel_facilities' AS src
        FROM reference.renewable_diesel_facilities
    """)
    for r in cur.fetchall():
        rows.append({
            "src": r["src"], "facility_id": r["facility_id"],
            "facility_name": r["facility_name"], "operator": r["operator"],
            "parent_company": r["parent_company"], "city": r["city"],
            "state": r["state"], "status": r["status"],
            "capacity_mmgy": r["nameplate_mmgy"],
            "feedstock": r["feedstock_primary"],
            "fuel_type": "renewable_diesel",
        })

    cur.execute("""
        SELECT facility_id, name AS facility_name, operator, parent_company,
               city, state, status, industry_code,
               'reference.facility_master' AS src
        FROM reference.facility_master
        WHERE industry_code IN ('biodiesel', 'biofuel', 'renewable_diesel', 'saf',
                                 'renewable_fuel', 'biomass_based_diesel')
    """)
    for r in cur.fetchall():
        rows.append({
            "src": r["src"], "facility_id": r["facility_id"],
            "facility_name": r["facility_name"], "operator": r["operator"],
            "parent_company": r["parent_company"], "city": r["city"],
            "state": r["state"], "status": r["status"],
            "capacity_mmgy": None, "feedstock": None,
            "fuel_type": r["industry_code"],
        })

    conn.close()
    return rows


def match(carb: dict, db_rows: list[dict]) -> list[dict]:
    """Return DB rows that plausibly match a CARB facility."""
    carb_fp_tok = tokens(carb["fuel_producer"])
    carb_fn_tok = tokens(carb["facility_name"])
    carb_all = carb_fp_tok | carb_fn_tok
    # Strip ultra-common tokens that match anything
    noise = {"the", "of", "and", "biodiesel", "renewable", "diesel", "energy",
             "fuels", "fuel", "biofuels", "biofuel", "refining", "refinery",
             "rng", "saf"}
    carb_meaningful = carb_all - noise
    if not carb_meaningful:
        return []

    matches = []
    for row in db_rows:
        db_tok = (
            tokens(row.get("facility_name") or "")
            | tokens(row.get("operator") or "")
            | tokens(row.get("parent_company") or "")
            | tokens(row.get("city") or "")
        )
        db_meaningful = db_tok - noise
        overlap = carb_meaningful & db_meaningful
        # Require at least one strong-ish token overlap (operator name or city)
        if not overlap:
            continue
        # Score: how many tokens overlap, plus bonus if state hint matches loc text
        score = len(overlap)
        if row.get("state") and row["state"].lower() in (carb.get("location") or "").lower():
            score += 2
        matches.append({**row, "_overlap": sorted(overlap), "_score": score})
    matches.sort(key=lambda m: -m["_score"])
    return matches


def main():
    carb_facilities = load_carb_facilities()
    db_rows = load_db_facilities()

    print(f"CARB facilities (after dedupe): {len(carb_facilities)}")
    print(f"DB biofuel-ish rows:            {len(db_rows)}")
    print()

    # Bucket A: CARB → matched
    # Bucket B: CARB → no matches
    matched_carb = []
    missing_carb = []
    db_matched_ids: set[tuple] = set()

    for carb in carb_facilities:
        m = match(carb, db_rows)
        # Require score >= 2 to consider it a real match
        strong = [x for x in m if x["_score"] >= 2]
        if strong:
            matched_carb.append((carb, strong[:3]))
            for s in strong[:3]:
                db_matched_ids.add((s["src"], s["facility_id"]))
        else:
            # Weak / no match — flag as missing
            missing_carb.append((carb, m[:2] if m else []))

    # Bucket C: DB → no CARB match
    db_no_carb = [r for r in db_rows
                  if (r["src"], r["facility_id"]) not in db_matched_ids
                  and (r.get("status") or "").lower() not in ("closed", "idled", "decommissioned")]

    # Emit report
    lines = []
    lines.append("# CARB LCFS ↔ DB Cross-Reference Report")
    lines.append("")
    lines.append(f"- **CARB liquid-biofuel pathway facilities:** {len(carb_facilities)}")
    lines.append(f"- **DB biofuel-ish facility rows:** {len(db_rows)}")
    lines.append(f"  - reference.biodiesel_facilities: 192")
    lines.append(f"  - reference.biofuel_facilities:   241")
    lines.append(f"  - reference.renewable_diesel_facilities: 66")
    lines.append(f"  - reference.facility_master (biofuel codes): {sum(1 for r in db_rows if r['src']=='reference.facility_master')}")
    lines.append("")
    lines.append(f"- **A. Matched (CARB ⇔ DB):** {len(matched_carb)} CARB facilities")
    lines.append(f"- **B. Missing (in CARB, not in DB):** {len(missing_carb)} CARB facilities")
    lines.append(f"- **C. DB-only (in DB, not in CARB):** {len(db_no_carb)} DB facility rows")
    lines.append("")

    # ---- Bucket B: MISSING ----
    lines.append("## B. Missing — CARB facilities we don't have in DB")
    lines.append("")
    lines.append("These are CARB-certified biofuel/SAF producers that have NO match in our DB.")
    lines.append("Each is potentially a facility we should add. Sorted by pathway_count (more")
    lines.append("pathways = more commercial activity).")
    lines.append("")
    missing_carb.sort(key=lambda x: -x[0]["pathway_count"])
    lines.append("| Fuel Producer | Facility Name | Location | Fuel Type | Feedstocks | Pathways | Weak match? |")
    lines.append("|---|---|---|---|---|---|---|")
    for carb, weak in missing_carb:
        ft = "; ".join(carb["fuel_types"])[:40]
        fs = "; ".join(carb["feedstocks"])[:50]
        loc = (carb["location"] or "")[:30]
        weak_note = ""
        if weak:
            w = weak[0]
            weak_note = f"`{w['src'].split('.')[-1]}` {w.get('facility_id','')} (score {w['_score']}, overlap: {','.join(w['_overlap'][:3])})"
        lines.append(f"| {carb['fuel_producer'][:40]} | {carb['facility_name'][:35]} | {loc} | {ft} | {fs} | {carb['pathway_count']} | {weak_note} |")
    lines.append("")

    # ---- Bucket C: DB-only ----
    lines.append("## C. DB-only — Our facilities NOT in CARB pathways")
    lines.append("")
    lines.append("Reasons something here is NOT in CARB:")
    lines.append("- Plant doesn't ship into CA market → no LCFS pathway needed")
    lines.append("- Plant is **closed/idled** but our DB still marks active")
    lines.append("- Plant exists under a different operator name CARB recognizes")
    lines.append("- Plant is new and pathway is pending")
    lines.append("")
    lines.append("Sorted by state, then operator. **Suspicious cases highlighted with ⚠️**.")
    lines.append("")
    # Group by state for readability
    by_state = defaultdict(list)
    for r in db_no_carb:
        by_state[r.get("state") or "?"].append(r)
    for state in sorted(by_state.keys()):
        lines.append(f"### {state} ({len(by_state[state])})")
        lines.append("")
        lines.append("| facility_id | name | operator | city | status | cap (mmgy) | source |")
        lines.append("|---|---|---|---|---|---|---|")
        for r in sorted(by_state[state], key=lambda x: (x.get("operator") or "", x.get("facility_name") or "")):
            flag = ""
            if (r.get("status") or "").lower() == "active" and (r.get("capacity_mmgy") or 0) and r["capacity_mmgy"] >= 20:
                flag = "⚠️ "  # active + non-trivial size + not in CARB = worth investigating
            cap = r.get("capacity_mmgy")
            cap_str = f"{cap:.1f}" if cap else ""
            lines.append(f"| {r.get('facility_id','')} | {flag}{(r.get('facility_name') or '')[:35]} | {(r.get('operator') or '')[:30]} | {r.get('city') or ''} | {r.get('status') or ''} | {cap_str} | {r['src'].split('.')[-1]} |")
        lines.append("")

    # ---- Bucket A: matched summary ----
    lines.append("## A. Matched — CARB facilities present in DB")
    lines.append("")
    lines.append(f"({len(matched_carb)} facilities matched, full list in JSON sibling)")
    lines.append("")
    lines.append("| CARB Fuel Producer | CARB Facility | Location | DB matches |")
    lines.append("|---|---|---|---|")
    matched_carb.sort(key=lambda x: x[0]["fuel_producer"])
    for carb, dbms in matched_carb[:60]:  # cap to first 60 for readability
        db_str = "; ".join(f"`{m['facility_id']}`" for m in dbms[:2])
        lines.append(f"| {carb['fuel_producer'][:35]} | {carb['facility_name'][:30]} | {carb['location'][:25]} | {db_str} |")
    if len(matched_carb) > 60:
        lines.append(f"| _...{len(matched_carb)-60} more matches..._ | | | |")
    lines.append("")

    OUT_REPORT.parent.mkdir(parents=True, exist_ok=True)
    OUT_REPORT.write_text("\n".join(lines), encoding="utf-8")
    print(f"Wrote {OUT_REPORT}")
    print()
    print(f"Summary:")
    print(f"  A. Matched (CARB ⇔ DB):      {len(matched_carb)}")
    print(f"  B. Missing (CARB not in DB): {len(missing_carb)}")
    print(f"  C. DB-only (not in CARB):    {len(db_no_carb)}")


if __name__ == "__main__":
    main()
