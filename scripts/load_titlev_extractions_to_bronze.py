"""
Load LLM-extracted Title V permit JSON files into bronze.state_air_permits
and bronze.state_air_permit_units (migration 045).

Walks collectors/epa_echo/output/llm_titlev/<state>/*.json (the output of
scripts/ollama/extract_titlev_permits.py) and upserts each into the bronze
tables. Idempotent: re-running updates existing rows by (state, facility_id,
permit_number).

Usage:
  python scripts/load_titlev_extractions_to_bronze.py             # all
  python scripts/load_titlev_extractions_to_bronze.py --state IA  # single state
  python scripts/load_titlev_extractions_to_bronze.py --dry-run   # preview
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

import psycopg2.extras

from src.services.database.db_config import get_connection

LLM_OUT_DIR = Path("collectors/epa_echo/output/llm_titlev")


def sha256_file(path: Path) -> str | None:
    if not path.exists():
        return None
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def parse_iso_date(s) -> str | None:
    if not s or s == "null":
        return None
    s = str(s).strip()
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except (ValueError, TypeError):
            pass
    return None


def upsert_extraction(cur, record: dict, json_path: Path) -> tuple[int, int]:
    """Upsert one extraction record. Returns (permit_id, n_units_inserted)."""
    facility = record.get("facility", {}) or {}
    units = record.get("emission_units", []) or []
    totals = record.get("facility_totals", {}) or {}
    notes = record.get("extraction_notes", "")

    pdf_path_str = record.get("_source_pdf")
    pdf_sha = sha256_file(Path(pdf_path_str)) if pdf_path_str else None

    state = (facility.get("state") or "").upper()[:2] or "??"
    # facility_id: prefer explicit field; otherwise derive from filename stem
    facility_id = facility.get("facility_id") or json_path.stem
    permit_number = facility.get("permit_number") or ""

    cur.execute(
        """
        INSERT INTO bronze.state_air_permits (
            state, facility_id, facility_name, operator, city,
            permit_number, expiration_date, industry, facility_description,
            facility_totals, raw_pdf_path, raw_pdf_sha256,
            source, extraction_method, extraction_notes,
            extracted_at, collected_at
        )
        VALUES (%s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s, NOW())
        ON CONFLICT (state, facility_id, permit_number) DO UPDATE SET
            facility_name        = EXCLUDED.facility_name,
            operator             = EXCLUDED.operator,
            city                 = EXCLUDED.city,
            expiration_date      = EXCLUDED.expiration_date,
            industry             = EXCLUDED.industry,
            facility_description = EXCLUDED.facility_description,
            facility_totals      = EXCLUDED.facility_totals,
            raw_pdf_path         = EXCLUDED.raw_pdf_path,
            raw_pdf_sha256       = EXCLUDED.raw_pdf_sha256,
            extraction_method    = EXCLUDED.extraction_method,
            extraction_notes     = EXCLUDED.extraction_notes,
            extracted_at         = EXCLUDED.extracted_at,
            collected_at         = NOW()
        RETURNING id
        """,
        (
            state,
            facility_id,
            facility.get("name") or facility_id,
            facility.get("operator"),
            facility.get("city"),
            permit_number,
            parse_iso_date(facility.get("expiration_date")),
            facility.get("industry"),
            facility.get("facility_description"),
            json.dumps(totals) if totals else None,
            pdf_path_str,
            pdf_sha,
            f"llm_extract:{state.lower()}_titlev",
            record.get("_model", "llm:unknown"),
            notes,
            record.get("_extracted_at"),
        ),
    )
    row = cur.fetchone()
    permit_id = row["id"] if isinstance(row, dict) else row[0]

    # Wipe-and-replace the unit list — simplest path; keys depend on permit_id
    cur.execute(
        "DELETE FROM bronze.state_air_permit_units WHERE permit_id = %s",
        (permit_id,),
    )

    n_inserted = 0
    for u in units:
        unit_id = (u.get("unit_id") or "").strip()
        if not unit_id:
            continue
        try:
            cur.execute(
                """
                INSERT INTO bronze.state_air_permit_units (
                    permit_id, unit_id, description, category,
                    rated_capacity, rated_capacity_unit,
                    throughput_limit, throughput_limit_unit,
                    control_devices, extra
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (permit_id, unit_id) DO NOTHING
                """,
                (
                    permit_id,
                    unit_id,
                    u.get("description"),
                    u.get("category"),
                    _num(u.get("rated_capacity")),
                    u.get("rated_capacity_unit"),
                    _num(u.get("throughput_limit")),
                    u.get("throughput_limit_unit"),
                    json.dumps(u.get("control_devices") or []),
                    json.dumps({k: v for k, v in u.items()
                               if k not in {"unit_id", "description", "category",
                                            "rated_capacity", "rated_capacity_unit",
                                            "throughput_limit", "throughput_limit_unit",
                                            "control_devices"}}) or None,
                ),
            )
            n_inserted += 1
        except Exception as e:
            print(f"  WARN: failed unit {unit_id} in {json_path.name}: {e}",
                  file=sys.stderr)

    return permit_id, n_inserted


def _num(v):
    """Coerce v to numeric or None — handles '250', 250, '250 tons/hr'."""
    if v is None or v == "":
        return None
    if isinstance(v, (int, float)):
        return v
    s = str(v).strip().replace(",", "")
    # strip trailing units if any
    import re
    m = re.match(r"^(-?\d+(?:\.\d+)?)", s)
    if m:
        try:
            return float(m.group(1))
        except ValueError:
            return None
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--state", help="Only process one state (IA, IN, ...)")
    ap.add_argument("--dry-run", action="store_true",
                    help="Parse and validate JSON files; do not write to DB")
    ap.add_argument("--in-dir", type=Path, default=LLM_OUT_DIR)
    args = ap.parse_args()

    if not args.in_dir.exists():
        sys.exit(f"Input dir not found: {args.in_dir}")

    # Find files
    if args.state:
        files = sorted((args.in_dir / args.state.upper()).glob("*.json"))
    else:
        files = sorted(args.in_dir.glob("*/*.json"))
    files = [f for f in files if not f.name.endswith(".raw.txt")]

    if not files:
        sys.exit(f"No JSON files in {args.in_dir}")

    print(f"Found {len(files)} extraction JSON files")

    # Validate JSON-load each first (cheap, surfaces parse errors before DB)
    parsed = []
    for f in files:
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
            parsed.append((f, data))
        except Exception as e:
            print(f"  SKIP {f.name}: cannot load JSON ({e})", file=sys.stderr)

    print(f"  {len(parsed)} valid JSON files")

    if args.dry_run:
        for f, data in parsed:
            fac = data.get("facility", {}) or {}
            n_units = len(data.get("emission_units", []) or [])
            print(f"  [{fac.get('state','??')}] {fac.get('name','?'):50s} "
                  f"permit={fac.get('permit_number','?'):20s}  units={n_units}")
        return

    n_permits = n_units_total = 0
    with get_connection() as conn:
        cur = conn.cursor()
        for f, data in parsed:
            try:
                pid, n_units = upsert_extraction(cur, data, f)
                n_permits += 1
                n_units_total += n_units
                print(f"  loaded {f.relative_to(args.in_dir)} -> permit_id={pid}, {n_units} units")
            except Exception as e:
                conn.rollback()
                import traceback
                print(f"  FAILED {f.name}: {e!r}", file=sys.stderr)
                traceback.print_exc()
                continue
            else:
                conn.commit()

    print(f"\nDone. {n_permits} permits, {n_units_total} emission units inserted/updated.")


if __name__ == "__main__":
    main()
