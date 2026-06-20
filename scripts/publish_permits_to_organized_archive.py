"""
Publish parsed permits into a human-friendly organized archive at /permits.

Reads from bronze.state_air_permits + bronze.state_air_permit_units, plus
the source PDFs in collectors/epa_echo/raw/, and produces:

  permits/<industry>/<state>/<facility>/
      ├── source.pdf                     (copy of original Title V PDF)
      ├── equipment_list.csv             (one row per emission unit, Excel-friendly)
      ├── extraction_summary.md          (narrative summary for spot-checking)
      └── process_flow_coverage.md       (cross-check vs canonical oilseed_crush
                                          ontology — flags missing categories)

Idempotent. Re-run any time after permit re-extraction completes; updates
in place. Designed for human spot-checking.
"""
from __future__ import annotations

import csv
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

import psycopg2.extras
from src.services.database.db_config import get_connection


PERMITS_ROOT = Path("permits")
RAW_PDF_DIR = Path("collectors/epa_echo/raw")


# Canonical oilseed-crush process flow steps (from
# domain_knowledge/process_flows/oilseed_crush.md).
# Each step lists the categories the LLM might tag for that step's equipment.
# A facility "covers" a step if any of these categories show up in its EUs.
# LLM-invented category strings → canonical category. Built empirically from
# observed LLM outputs across IA + IN extractions. This bridges the gap until
# we tighten the extraction prompt's enum.
CATEGORY_SYNONYMS = {
    # Loadout / loading / unloading / handling
    "loadout":              "loading/unloading",
    "loading":              "loading/unloading",
    "unloading":            "loading/unloading",
    "rail loadout":         "loading/unloading",
    "truck loadout":        "loading/unloading",
    "bulk loadout":         "loading/unloading",
    "barge loadout":        "loading/unloading",
    "shipping":             "loading/unloading",
    # Conveying
    "conveyor":             "conveying",
    "drag conveyor":        "conveying",
    "screw conveyor":       "conveying",
    "belt conveyor":        "conveying",
    "pneumatic conveying":  "conveying",
    "transfer":             "conveying",
    # Cracking / dehulling
    "cracker":              "cracking and dehulling",
    "cracking":             "cracking and dehulling",
    "dehulling":            "cracking and dehulling",
    "hull removal":         "cracking and dehulling",
    "hull aspiration":      "cracking and dehulling",
    # Conditioning
    "rotary conditioner":   "conditioning",
    "conditioner":          "conditioning",
    "tempering":            "conditioning",
    # Flaking
    "flaker":               "flaking",
    # Expanding
    "expander":             "expanding",
    "extruder":             "expanding",
    "extrusion":            "expanding",
    # Extraction
    "solvent extraction":   "extraction",
    "extractor":            "extraction",
    "hexane extraction":    "extraction",
    # DT
    "dt":                   "desolventizing",
    "desolventizer":        "desolventizing",
    "toaster":              "desolventizing",
    "desolventizer-toaster":"desolventizing",
    # Drying — overlaps with grain dryer + meal dryer
    "grain dryer":          "drying",
    "meal dryer":           "drying",
    "soybean dryer":        "drying",
    "dryer":                "drying",
    # Cooling
    "meal cooler":          "cooling",
    "cooler":               "cooling",
    "cooling tower":        "cooling",
    # Grinding
    "meal grinder":         "grinding",
    "hammer mill":          "grinding",
    "grinder":              "grinding",
    "pellet mill":          "grinding",
    "pelleting":            "grinding",
    # Storage
    "bin":                  "storage",
    "silo":                 "storage",
    "tank":                 "storage",
    "meal storage":         "storage",
    "oil storage":          "storage",
    "hexane storage":       "storage",
    "bean storage":         "storage",
    "hull storage":         "storage",
    # Receiving
    "rail receiving":       "receiving",
    "truck receiving":      "receiving",
    "intake":               "receiving",
    # Refining steps
    "degumming":            "degumming",
    "neutralization":       "neutralization",
    "caustic refining":     "neutralization",
    "alkali refining":      "neutralization",
    "bleaching":            "bleaching",
    "deodorizing":          "deodorizing",
    "deodorizer":           "deodorizing",
    "rbd":                  "deodorizing",
    # Boilers / utilities
    "boiler":               "boiler",
    "boilers":              "boiler",
    "package boiler":       "boiler",
    "coal boiler":          "boiler",
    "natural gas boiler":   "boiler",
    "fuel oil boiler":      "boiler",
    "stack economizer":     "boiler",
    "heat exchanger":       "boiler",
    # Dust control
    "baghouse":             "baghouse",
    "scrubber":             "scrubber",
    "cyclone":              "cyclone",
    "rto":                  "scrubber",
    "biofilter":            "scrubber",
    "esp":                  "scrubber",
    "mineral oil scrubber": "scrubber",
    "aspiration":           "aspiration",
    # Process / handling that should map to existing flow steps
    "handling":             "loading/unloading",
    "process":              "other",
}


def normalize_category(raw: str) -> str:
    """Map an LLM-tagged category to canonical form via the synonym dict."""
    if not raw:
        return ""
    s = raw.strip().lower()
    if s in CATEGORY_SYNONYMS:
        return CATEGORY_SYNONYMS[s]
    # Try partial match — "rail meal loadout" contains "loadout"
    for key, canon in CATEGORY_SYNONYMS.items():
        if key in s:
            return canon
    return s   # already canonical or genuinely "other"


CANONICAL_OILSEED_CRUSH_STEPS = [
    ("01_receiving",         "Receiving (truck + rail)",
        ["receiving", "handling", "loading/unloading"]),
    ("02_cleaning",          "Cleaning",
        ["cleaning"]),
    ("03_drying",            "Drying",
        ["drying"]),
    ("04_dehulling",         "Cracking & dehulling",
        ["cracking and dehulling", "cracking", "dehulling"]),
    ("05_conditioning",      "Conditioning",
        ["conditioning"]),
    ("06_flaking",           "Flaking",
        ["flaking"]),
    ("07_expanding",         "Expanding (extruder, optional)",
        ["expanding", "expander", "extrusion"]),
    ("08_extraction",        "Solvent extraction (hexane)",
        ["extraction"]),
    ("09_desolventizing",    "Desolventizer-toaster (DT)",
        ["desolventizing", "desolventizer", "toaster"]),
    ("10_meal_drying",       "Meal drying & cooling",
        ["drying", "cooling"]),  # may overlap; coverage = either present
    ("11_meal_grinding",     "Meal grinding",
        ["grinding"]),
    ("12_meal_storage",      "Meal storage",
        ["storage"]),
    ("13_meal_loadout",      "Meal loadout (rail/truck/barge)",
        ["loading/unloading", "loadout", "conveying"]),
    ("14_solvent_recovery",  "Solvent (hexane) recovery",
        ["solvent_recovery", "hexane", "recovery"]),
    ("15_degumming",         "Degumming",
        ["degumming"]),
    ("16_neutralization",    "Neutralization (caustic refining)",
        ["neutralization", "refining"]),
    ("17_bleaching",         "Bleaching",
        ["bleaching"]),
    ("18_deodorizing",       "Deodorizing (RBD final)",
        ["deodorizing"]),
    ("19_oil_storage",       "Refined oil storage / loadout",
        ["storage", "loading/unloading"]),
    # Plant-wide
    ("90_boilers",           "Boilers / steam utilities",
        ["boiler", "boilers"]),
    ("91_dust_control",      "Dust control (baghouse / cyclone / scrubber)",
        ["baghouse", "scrubber", "cyclone", "aspiration"]),
    ("92_cooling_towers",    "Cooling towers",
        ["cooling"]),
]


def slugify_facility_id(facility_id: str) -> str:
    """`ia.agp_eagle_grove` → `agp_eagle_grove` (state lives in folder)."""
    if "." in facility_id:
        return facility_id.split(".", 1)[1]
    return facility_id


def industry_for(industry: str | None, fallback_op: str | None) -> str:
    """Normalize the LLM's free-text industry tag to a canonical folder bucket.

    The extraction prompt asks for an enum, but the model frequently ignores it
    and emits free text with SIC/NAICS codes, varied spellings, and the
    occasional mis-tag. This collapses the variants so the archive doesn't sprawl
    into singleton folders (gypsum x3, electric x3, etc.). Routing happens here at
    the publish layer ONLY; bronze keeps the raw tag (medallion: bronze = raw).

    Canonical buckets are RLC's focus industries + 'other' for everything outside
    scope (the acquirer grabbed ALL IA Title V permits, not just ag/energy)."""
    if not industry:
        return "other"
    s = industry.lower()

    # Order matters: check more specific buckets before generic ones.
    # Each entry: canonical bucket -> list of substrings that route to it.
    RULES = [
        ("renewable_diesel",    ["renewable diesel", "renewable_diesel", "rd/saf", "hefa"]),
        ("biodiesel",           ["biodiesel", "fame", "fatty acid methyl"]),
        ("oilseed_crush",       ["oilseed", "crush", "soybean oil", "soy oil", "soy_processing",
                                 "soy processing", "soybean processing", "canola", "rapeseed"]),
        ("corn_wet_milling",    ["wet corn", "corn wet", "wet mill", "corn milling", "corn_milling",
                                 "corn processing", "corn_processing", "corn refining", "wet_corn"]),
        ("ethanol",             ["ethanol", "distillery", "distilling", "biorefinery", "dry mill", "dry_mill"]),
        ("wheat_milling",       ["wheat mill", "wheat_mill", "flour mill", "flour_mill", "grain milling"]),
        ("livestock_processing",["meat", "pork", "beef", "poultry", "slaughter", "rendering",
                                 "animal", "egg", "protein"]),
        ("oil_refining",        ["petroleum refin", "oil refining", "oil_refining", "refinery"]),
    ]
    for bucket, needles in RULES:
        if any(n in s for n in needles):
            return bucket
    # Everything else (gypsum, steel, tire, glass, paper, electric, landfill,
    # wastewater, wood products, food mfg, machine shops, data centers, ...) is
    # outside RLC's target industries -> 'other'. Raw tag preserved in bronze.
    return "other"


def find_source_pdf(facility_id: str, raw_pdf_path: str | None) -> Path | None:
    """Locate the original PDF in collectors/epa_echo/raw/."""
    if raw_pdf_path:
        p = Path(raw_pdf_path)
        if p.exists():
            return p
    # Fallback: try slug-based filename
    slug = slugify_facility_id(facility_id)
    candidates = [
        RAW_PDF_DIR / f"{slug}_titlev.pdf",
        RAW_PDF_DIR / f"{slug.replace('agp_', 'agp_')}_titlev.pdf",
        RAW_PDF_DIR / f"in_{slug}_titlev.pdf",
    ]
    for c in candidates:
        if c.exists():
            return c
    return None


def write_equipment_csv(folder: Path, units: list[dict]) -> None:
    """Write equipment list. SAFETY: never overwrites equipment_list.csv if
    it has been manually modified — writes to equipment_list_auto.csv
    instead. The hand-curated file is the source of truth once it exists.
    """
    out_auto = folder / "equipment_list_auto.csv"
    out_human = folder / "equipment_list.csv"

    # Determine target: if a human-edited list exists and was modified more
    # recently than the bronze record's extracted_at, preserve it.
    if out_human.exists():
        # Use _auto suffix so the human file is preserved
        out = out_auto
    else:
        # First time — use the canonical name
        out = out_human

    with open(out, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, quoting=csv.QUOTE_ALL)
        w.writerow([
            "unit_id", "category", "description",
            "rated_capacity", "rated_capacity_unit",
            "throughput_limit", "throughput_limit_unit",
            "control_devices",
        ])
        for u in units:
            w.writerow([
                u.get("unit_id") or "",
                u.get("category") or "",
                u.get("description") or "",
                u.get("rated_capacity") if u.get("rated_capacity") is not None else "",
                u.get("rated_capacity_unit") or "",
                u.get("throughput_limit") if u.get("throughput_limit") is not None else "",
                u.get("throughput_limit_unit") or "",
                ", ".join(str(x) for x in (u.get("control_devices") or [])) if isinstance(u.get("control_devices"), list) else (u.get("control_devices") or ""),
            ])


def write_summary_md(folder: Path, permit: dict, units: list[dict]) -> None:
    out = folder / "extraction_summary.md"
    cap_count = sum(1 for u in units if u.get("rated_capacity"))
    cats = sorted({(u.get("category") or "other") for u in units})
    lines = [
        f"# {permit['facility_name']}",
        "",
        f"- **Operator**: {permit.get('operator') or '?'}",
        f"- **Location**: {permit.get('city') or '?'}, {permit.get('state') or '?'}",
        f"- **Permit number**: `{permit.get('permit_number') or '?'}`",
        f"- **Permit expires**: {permit.get('expiration_date') or '?'}",
        f"- **Industry**: {permit.get('industry') or '?'}",
        f"- **Source PDF**: `{permit.get('raw_pdf_path') or '?'}`",
        f"- **Extraction model**: `{permit.get('extraction_method') or '?'}`",
        f"- **Extracted at**: {permit.get('extracted_at') or '?'}",
        "",
        f"## Counts",
        f"- Emission units extracted: **{len(units)}**",
        f"- Units with rated capacity: **{cap_count}**",
        f"- Distinct categories: **{len(cats)}** — {', '.join(cats)}",
        "",
        f"## Description",
        permit.get("facility_description") or "_(none)_",
    ]
    out.write_text("\n".join(lines), encoding="utf-8")


def coverage_report(folder: Path, units: list[dict]) -> dict:
    """Compare extracted equipment categories against the canonical
    oilseed-crush 19-step + 3-utility flow. Output Markdown report and
    return a coverage dict for cross-facility comparison."""
    extracted_cats = set()
    raw_to_canon = {}   # for showing the user how their tags mapped
    for u in units:
        raw = (u.get("category") or "").strip().lower()
        if raw:
            canon = normalize_category(raw)
            extracted_cats.add(canon)
            raw_to_canon.setdefault(canon, set()).add(raw)

    coverage = {}
    out = folder / "process_flow_coverage.md"
    lines = [
        "# Process Flow Coverage",
        "",
        "Cross-checking extracted emission units against the canonical oilseed-crush "
        "process flow (`domain_knowledge/process_flows/oilseed_crush.md`).",
        "",
        "✅ = at least one extracted unit maps to this step.  ",
        "⚠️ = the step is missing from the extraction. Could be (a) the LLM missed it, "
        "(b) the permit doesn't itemize it (bundled into another), or (c) the facility "
        "doesn't have this capability. Spot-check the source PDF to determine which.",
        "",
        "| Status | Step | Categories that satisfy it | Extracted EUs in this step |",
        "|---|---|---|---|",
    ]
    for step_id, step_label, cats in CANONICAL_OILSEED_CRUSH_STEPS:
        cats_lower = [c.lower() for c in cats]
        matching = [c for c in cats_lower if c in extracted_cats]
        ok = bool(matching)
        coverage[step_id] = ok
        # Find the actual EUs that match (after normalization)
        matching_units = []
        for u in units:
            raw = (u.get("category") or "").strip().lower()
            if normalize_category(raw) in cats_lower:
                matching_units.append(u.get("unit_id") or "?")
        eu_str = ", ".join(matching_units[:6])
        if len(matching_units) > 6:
            eu_str += f" ... (+{len(matching_units) - 6} more)"
        eu_str = eu_str or "—"
        lines.append(f"| {'✅' if ok else '⚠️'} | {step_label} | {', '.join(cats)} | {eu_str} |")

    n_covered = sum(1 for v in coverage.values() if v)
    lines += [
        "",
        f"**Coverage: {n_covered}/{len(coverage)} canonical steps**",
        "",
        "_Note: not every facility runs every step (e.g., a crude-only crusher won't "
        "have refining steps 15-18). Use this as a checklist for spot-checking, not as "
        "a hard quality score._",
    ]
    out.write_text("\n".join(lines), encoding="utf-8")
    return coverage


def main():
    PERMITS_ROOT.mkdir(exist_ok=True)
    written = []

    with get_connection() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT p.id, p.facility_id, p.facility_name, p.operator, p.city, p.state,
                   p.permit_number, p.expiration_date, p.industry, p.facility_description,
                   p.raw_pdf_path, p.extraction_method, p.extracted_at
            FROM bronze.state_air_permits p
            ORDER BY p.state, p.facility_id, p.facility_name
        """)
        permits = cur.fetchall()

        for permit in permits:
            cur.execute("""
                SELECT unit_id, description, category,
                       rated_capacity, rated_capacity_unit,
                       throughput_limit, throughput_limit_unit,
                       control_devices
                FROM bronze.state_air_permit_units
                WHERE permit_id = %s
                ORDER BY unit_id
            """, (permit["id"],))
            units = [dict(r) for r in cur.fetchall()]

            # Resolve where this facility belongs
            industry = industry_for(permit.get("industry"), permit.get("operator"))
            state = (permit.get("state") or "??").lower()
            slug = slugify_facility_id(permit.get("facility_id") or
                                        (permit.get("facility_name") or "unknown")
                                            .lower().replace(" ", "_").replace(",", ""))
            folder = PERMITS_ROOT / industry / state / slug
            folder.mkdir(parents=True, exist_ok=True)

            # Copy the source PDF (or symlink — Windows can be picky, use copy)
            src_pdf = find_source_pdf(permit["facility_id"] or "", permit.get("raw_pdf_path"))
            if src_pdf and src_pdf.exists():
                dest_pdf = folder / "source.pdf"
                if not dest_pdf.exists() or dest_pdf.stat().st_size != src_pdf.stat().st_size:
                    shutil.copy2(src_pdf, dest_pdf)

            # Equipment list as CSV
            write_equipment_csv(folder, units)

            # Narrative summary
            write_summary_md(folder, dict(permit), units)

            # Process flow coverage — only meaningful for oilseed_crush, the one
            # industry with a canonical flow defined. Generating a soybean-crush
            # checklist for a machine shop / data center / gypsum plant would be
            # misleading, so skip it for everything else. (2026-06-20)
            if industry == "oilseed_crush":
                coverage = coverage_report(folder, units)
                cov_pct = round(sum(1 for v in coverage.values() if v) / len(coverage) * 100, 0)
            else:
                cov_pct = None

            written.append({
                "facility_id": permit["facility_id"],
                "industry": industry,
                "state": state,
                "folder": str(folder),
                "n_units": len(units),
                "coverage_pct": cov_pct,
            })

    print(f"Wrote organized archive for {len(written)} facilities under {PERMITS_ROOT}/")
    print()
    print(f"  {'industry':<16s} {'state':<3s} {'facility':<35s} {'EUs':>4s}  {'cov':>3s}%")
    print("  " + "-" * 74)
    for r in written:
        slug = (r["facility_id"] or "?")
        if "." in slug:
            slug = slug.split(".", 1)[1]
        cov = f"{r['coverage_pct']:>3.0f}%" if r['coverage_pct'] is not None else "  —"
        print(f"  {r['industry']:<16s} {r['state']:<3s} {slug[:34]:<35s} "
              f"{r['n_units']:>4d}  {cov}")


if __name__ == "__main__":
    main()
