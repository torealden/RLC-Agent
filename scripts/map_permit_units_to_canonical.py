"""
Map permit emission units to canonical equipment functions.

Phase 1 deterministic pass: keyword matching against unit descriptions
+ existing `category` field. Writes to silver.permit_unit_canonical_map.

Coverage metric per plant:
    expected_required_functions / actually_mapped_required_functions

A plant with low coverage is a candidate for LLM re-extraction (Phase 4).

Usage:
    # Dry run (no DB writes)
    python scripts/map_permit_units_to_canonical.py --industry oilseed_crush --dry-run

    # Apply for one facility
    python scripts/map_permit_units_to_canonical.py --industry oilseed_crush \
        --facility-id agp_eagle_grove

    # Apply for all
    python scripts/map_permit_units_to_canonical.py --industry oilseed_crush

    # Coverage report only
    python scripts/map_permit_units_to_canonical.py --industry oilseed_crush --coverage
"""
from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from typing import Optional

from dotenv import load_dotenv
load_dotenv()

from src.services.database.db_config import get_connection


# ---------------------------------------------------------------------------
# Keyword-based classifier
# ---------------------------------------------------------------------------

# Order matters: more specific patterns first. Each entry is (regex,
# function_id, is_primary). A unit can get multiple mappings — first
# match becomes primary, subsequent matches are secondary.

# Keep patterns conservative — false positives are worse than false
# negatives because the LLM pass will catch the gaps but can't easily
# UN-claim a wrongly-mapped unit.

OILSEED_CRUSH_PATTERNS = [
    # --- specific process steps (run BEFORE the generic ones) ---

    # Solvent extraction & DT (very specific; high confidence)
    # "DTDC" = Desolventizer-Toaster-Dryer-Cooler — common combined unit
    (r"\bdesolventiz|\bdtdc\b", "oilseed_crush.desolventizer_toaster", True),
    (r"\bdt\b|toaster", "oilseed_crush.desolventizer_toaster", True),
    (r"solvent\s+(extract|recovery)", "oilseed_crush.solvent_extraction", True),
    (r"\bextractor\b|\bextraction\s+process|oil\s+extraction|"
     r"(soy|soybean)\s+oil\s+extract",
     "oilseed_crush.solvent_extraction", True),
    (r"hexane\s+(scrubber|stripper|recovery|condenser)", "oilseed_crush.solvent_recovery", True),
    (r"\bmiscella\b", "oilseed_crush.solvent_extraction", True),

    # Refining train (each step distinct)
    (r"\bdeodoriz", "oilseed_crush.deodorizing", True),
    (r"\bbleach|filter\s+aid|precoat|niagara|\bde\s+(storage|tank)\b|"
     r"diatomaceous", "oilseed_crush.bleaching", True),
    (r"neutraliz|caustic\s+(refin|wash)", "oilseed_crush.neutralization", True),
    (r"\bdegum", "oilseed_crush.degumming", True),
    # Generic "refinery" without specific subprocess → tag as bleaching
    # (most permits with "refinery" mean the post-extraction refining
    # train; refining-clay handling is the typical signature)
    (r"\boil\s+refinery\b|refinery\s+clay|refinery\s+(unit|line|process)",
     "oilseed_crush.bleaching", True),

    # Meal handling (specific BEFORE generic "meal")
    (r"meal\s+(dryer|cooler|drying|cooling)", "oilseed_crush.meal_drying_cooling", True),
    (r"cooling\s+deck", "oilseed_crush.meal_drying_cooling", True),
    (r"meal\s+(grind|hammer|screen|scalp|cooker)|amino\s+meal",
     "oilseed_crush.meal_grinding", True),
    (r"meal\s+(silo|storage|bin|warehouse|surge\s+tank)",
     "oilseed_crush.meal_storage", True),
    (r"meal\s+(load|conveyance|handling)|rail.*meal|truck.*meal",
     "oilseed_crush.meal_loadout", True),

    # Biodiesel co-location (AGP Algona/Sergeant Bluff/Mason City)
    (r"biodiesel\s+reactor|transesterif|methyl\s+ester",
     "biofuel.biodiesel_reactor", True),
    (r"methanol\s+(strip|rectif|column|tank|storage)|methanol\s+recover",
     "biofuel.methanol_handling", True),
    (r"glycerin", "biofuel.glycerin_handling", True),

    # Pre-extraction process
    (r"flaker|flaking", "oilseed_crush.flaking", True),
    (r"\bexpander|expand", "oilseed_crush.expanding", True),
    (r"condition(er|ing)", "oilseed_crush.conditioning", True),
    (r"crack.*dehull|dehull.*crack|cracking and dehulling|"
     r"cracking\s+&\s+dehulling", "oilseed_crush.dehulling_cracking", True),
    (r"\bdehull|hull\s+remov", "oilseed_crush.dehulling_cracking", True),
    (r"crack(ing|ed)?\s+(mill|roll)", "oilseed_crush.dehulling_cracking", True),

    # Hull-specific (after dehulling so hull-handling doesn't shadow)
    (r"hull\s+(grinder|conveyor|pellet|bin|silo|storage|loadout)",
     "oilseed_crush.hull_handling", True),
    (r"\bhull\b", "oilseed_crush.hull_handling", True),

    # Whole-bean / seed storage (BEFORE bean handling so storage wins)
    # "Tempering bin" = pre-conditioning surge bin for whole/cracked beans
    # "Harvestore" / "Steel Bin" = brand-name silos used for bean storage
    (r"bean\s+(tank|storage|silo|bin)|"
     r"(soy|soybean|grain|seed)\s+(silo|storage|tank)|"
     r"\d+(,\d+)?\s+bushel.*(tank|storage|silo|bin)|"
     r"tempering\s+(bin|storage)|"
     r"steel\s+bin\s+oil(ing)?\s+soybeans|"
     r"\boiling\s+(soybeans|beans)|"
     r"harvestore\s+bin|"
     r"\b(north|south|east|west|middle)\s+(harvestore|steel)\s+bin",
     "oilseed_crush.storage_seed", True),

    # Bean-side handling / receiving
    (r"\b(grain|bean|seed|soy|soybean)\s+receiv|truck\s+(dump|pit|receiv)|"
     r"rail.*receiv|barge\s+receiv", "oilseed_crush.receiving", True),
    (r"^receiving|^truck\s+pit|^east\s+\d+\s+pit|^west\s+\d+\s+pit",
     "oilseed_crush.receiving", True),
    (r"clean(er|ing)|scalper|destoner", "oilseed_crush.cleaning", True),
    (r"\bdrying|grain\s+dryer|seed\s+dryer", "oilseed_crush.drying", True),
    (r"\b(grain|bean|soy|soybean|crack|flake)\s+(handling|conveyor|drag|elevator|leg)",
     "oilseed_crush.bean_loadout_handling", True),
    # "Pellet receiving" → hull-line ancillary
    (r"pellet\s+receiv|finished\s+(crack|flake)s?\s+(conveyor|leg)",
     "oilseed_crush.bean_loadout_handling", True),

    # Refined oil handling
    (r"refined.*(oil|tank|storage|loadout)|rbd\s+(oil|tank)",
     "oilseed_crush.refined_oil_storage_loadout", True),

    # --- utilities (always on a plant; low ambiguity) ---
    (r"\bboiler|steam\s+gen|package\s+boiler", "oilseed_crush.boiler_steam", True),
    (r"cooling\s+tower", "oilseed_crush.cooling_tower", True),
    (r"compress(or|ed\s+air)", "oilseed_crush.compressed_air", True),
    (r"wastewater|api\s+separator|wwtp", "oilseed_crush.wastewater", True),

    # Boiler ancillaries
    (r"coal\s+(receiv|storage|conveying|feeder|pile)|limestone\s+(receiv|daybin)",
     "oilseed_crush.boiler_fuel_handling", True),
    (r"\bash\s+(conveying|loadout|silo|storage|handling)|fly\s+ash|bottom\s+ash",
     "oilseed_crush.ash_handling", True),

    # Emergency / backup
    (r"emergency\s+(generator|engine|pump|fire)|"
     r"diesel\s+(generator|engine|gen-set)|"
     r"fire\s+(pump|engine)",
     "oilseed_crush.emergency_equipment", True),

    # Fugitive / road / leaks
    (r"fugitive|road\s+(emiss|dust)|equipment\s+leaks|haul\s+road",
     "oilseed_crush.fugitive_emissions", True),

    # Generic loadout (after meal/hull/oil-specific have had their chance)
    (r"(truck|rail(road)?(\s+car)?)\s+(bulk\s+)?(load|loading|loadout)",
     "oilseed_crush.generic_loadout", True),
    (r"^bulk\s+loadout$|loadout\s+bin",
     "oilseed_crush.generic_loadout", True),

    # --- storage tanks ---
    (r"hexane\s+(tank|storage)", "oilseed_crush.storage_hexane", True),
    (r"crude\s+oil\s+(tank|storage)|crude\s+tank", "oilseed_crush.storage_crude_oil", True),
    (r"fuel\s+(oil\s+)?(tank|storage)|diesel\s+tank|gasoline\s+tank",
     "oilseed_crush.storage_fuel", True),
    (r"caustic\s+tank|clay\s+(silo|storage)|chemical\s+(tank|storage)",
     "oilseed_crush.storage_chemical", True),

    # --- control devices (these are SECONDARY; primary mapping should be
    # the process step they sit on, but we tag the control function too) ---
    (r"\baspiration\b", "oilseed_crush.control_aspiration", False),
    (r"\bbaghouse\b|\bbag\s+filter\b|\bfabric\s+filter\b|"
     r"vent\s+filter|dust\s+(filter|equipment|control)|internal\s+filter",
     "oilseed_crush.control_baghouse", False),
    (r"\bcyclone\b", "oilseed_crush.control_cyclone", False),
    (r"\bscrubber\b|mineral\s+oil\s+scrubber", "oilseed_crush.control_scrubber", False),
]


def classify_unit(description: str, category: Optional[str]) -> list[tuple[str, bool]]:
    """Return list of (function_id, is_primary). First match is primary."""
    text = " ".join(filter(None, [description or "", category or ""])).lower()
    matches = []
    seen = set()
    for pattern, func_id, is_primary_default in OILSEED_CRUSH_PATTERNS:
        if re.search(pattern, text, flags=re.IGNORECASE):
            if func_id in seen:
                continue
            seen.add(func_id)
            # First match becomes the primary (overrides the pattern's
            # is_primary_default if it would be the first hit)
            is_primary = is_primary_default and len(matches) == 0
            # If we already have a primary, this is secondary even if
            # the pattern would normally be primary
            if not is_primary_default:
                is_primary = False
            elif len(matches) > 0 and matches[0][1]:
                is_primary = False
            matches.append((func_id, is_primary))
    # Ensure exactly one primary if we have any matches
    if matches:
        any_primary = any(m[1] for m in matches)
        if not any_primary:
            # Promote the first to primary
            matches[0] = (matches[0][0], True)
    return matches


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

@dataclass
class UnitRow:
    unit_db_id: int
    facility_id: str
    unit_id: str
    description: str
    category: Optional[str]


def fetch_units(cur, industry_code: str, facility_id: Optional[str]) -> list[UnitRow]:
    where = ["TRUE"]
    params: list = []
    # The bronze table tracks oilseed_crush via facility prefix `agp_*`
    # / `bunge_*` / `adm_*` etc. Until industry_code is on the row, we
    # match by joining to reference.oilseed_crush_facilities.
    if facility_id:
        where.append("p.facility_id = %s")
        params.append(facility_id)

    cur.execute(
        f"""
        SELECT u.id AS unit_db_id, p.facility_id, u.unit_id,
               COALESCE(u.description, '') AS description, u.category
        FROM bronze.state_air_permits p
        JOIN bronze.state_air_permit_units u ON u.permit_id = p.id
        WHERE {' AND '.join(where)}
        ORDER BY p.facility_id, u.unit_id
        """,
        params,
    )
    return [UnitRow(**dict(r)) for r in cur.fetchall()]


def write_mappings(cur, unit: UnitRow, mappings: list[tuple[str, bool]]):
    for func_id, is_primary in mappings:
        cur.execute(
            """
            INSERT INTO silver.permit_unit_canonical_map
                (permit_unit_id, function_id, confidence, is_primary,
                 mapped_by, mapped_at)
            VALUES (%s, %s, 0.85, %s, 'keyword_v1', NOW())
            ON CONFLICT (permit_unit_id, function_id) DO UPDATE SET
                confidence = EXCLUDED.confidence,
                is_primary = EXCLUDED.is_primary,
                mapped_by = EXCLUDED.mapped_by,
                mapped_at = NOW()
            """,
            (unit.unit_db_id, func_id, is_primary),
        )


def coverage_report(cur, industry_code: str):
    """Per-facility coverage of REQUIRED canonical functions."""
    cur.execute(
        """
        SELECT function_id FROM reference.equipment_function_canonical
        WHERE industry_code = %s AND is_optional = FALSE
          AND function_category IN ('process', 'utility', 'storage', 'loadout')
        """,
        (industry_code,),
    )
    required = {dict(r)["function_id"] for r in cur.fetchall()}

    cur.execute(
        """
        SELECT p.facility_id,
               ARRAY_AGG(DISTINCT m.function_id) AS mapped_functions,
               COUNT(DISTINCT u.id) AS total_units,
               COUNT(DISTINCT u.id) FILTER (WHERE m.permit_unit_id IS NOT NULL) AS mapped_units
        FROM bronze.state_air_permits p
        JOIN bronze.state_air_permit_units u ON u.permit_id = p.id
        LEFT JOIN silver.permit_unit_canonical_map m ON m.permit_unit_id = u.id
        WHERE p.facility_id LIKE 'agp_%'
        GROUP BY p.facility_id
        ORDER BY p.facility_id
        """
    )
    print(f"\nCoverage report — required functions present per plant\n")
    print(f"  required = {len(required)} canonical functions: ")
    for r in sorted(required):
        print(f"    {r}")
    print()
    print(f"{'facility_id':25} {'units':>8}  {'mapped':>8}  {'req_cov':>9}  missing")
    for row in cur.fetchall():
        d = dict(row)
        mapped = set(d["mapped_functions"]) if d["mapped_functions"] else set()
        mapped.discard(None)
        present = required & mapped
        missing = required - mapped
        cov = len(present) / len(required) if required else 0
        missing_short = sorted(f.replace("oilseed_crush.", "") for f in missing)
        print(f"{d['facility_id']:25} {d['total_units']:>8}  "
              f"{d['mapped_units']:>8}  {cov:>8.0%}  "
              f"{', '.join(missing_short)}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--industry", default="oilseed_crush")
    parser.add_argument("--facility-id")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--coverage", action="store_true",
                        help="Just report coverage, don't re-map")
    args = parser.parse_args()

    with get_connection() as conn:
        conn.autocommit = False
        cur = conn.cursor()

        if args.coverage:
            coverage_report(cur, args.industry)
            return

        units = fetch_units(cur, args.industry, args.facility_id)
        print(f"Classifying {len(units)} units...")

        n_mapped = 0
        n_unmapped = 0
        unmapped_samples: list[str] = []
        per_facility = {}

        for unit in units:
            mappings = classify_unit(unit.description, unit.category)
            per_facility.setdefault(unit.facility_id, {"mapped": 0, "unmapped": 0})
            if not mappings:
                n_unmapped += 1
                per_facility[unit.facility_id]["unmapped"] += 1
                if len(unmapped_samples) < 30:
                    unmapped_samples.append(
                        f"{unit.facility_id}/{unit.unit_id}: {unit.description}"
                    )
                continue
            n_mapped += 1
            per_facility[unit.facility_id]["mapped"] += 1
            if not args.dry_run:
                write_mappings(cur, unit, mappings)

        if args.dry_run:
            conn.rollback()
            print("(dry run — no DB writes)")
        else:
            conn.commit()

        print(f"\nMapped:   {n_mapped:>5}")
        print(f"Unmapped: {n_unmapped:>5}")
        print()
        print(f"{'facility_id':25}  mapped  unmapped")
        for fid, c in sorted(per_facility.items()):
            print(f"{fid:25}  {c['mapped']:>6}  {c['unmapped']:>8}")

        if unmapped_samples:
            print(f"\nUnmapped samples (first {len(unmapped_samples)}):")
            for s in unmapped_samples:
                print(f"  - {s}")


if __name__ == "__main__":
    main()
