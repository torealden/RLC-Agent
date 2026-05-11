"""
Load CARB LCFS pathways from extracted JSON into bronze.carb_lcfs_pathways.

Usage:
  python scripts/load_carb_pathways_to_bronze.py
  python scripts/load_carb_pathways_to_bronze.py --snapshot-date 2026-05-10

Each load creates a new snapshot row. Existing snapshots are preserved for
historical "was certified Q1, gone by Q2" comparisons.

Source JSON: domain_knowledge/external_lists/carb_lcfs/biofuel_pathways.json
  (produced by scripts/extract_carb_lcfs_pathways.py)
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, datetime
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

from dotenv import load_dotenv
load_dotenv()

import psycopg2
from psycopg2.extras import execute_batch


JSON_PATH = Path("domain_knowledge/external_lists/carb_lcfs/biofuel_pathways.json")
SOURCE_XLSX = "domain_knowledge/external_lists/carb_lcfs/current_pathways_all.xlsx"


def to_date(s):
    if not s:
        return None
    try:
        # ISO format from JSON
        return datetime.fromisoformat(s.split("T")[0]).date()
    except (ValueError, AttributeError):
        return None


def to_numeric(v):
    if v is None or v == "" or v == "None":
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot-date", default=date.today().isoformat(),
                    help="Snapshot date (default: today)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    snapshot = datetime.fromisoformat(args.snapshot_date).date()
    pathways = json.loads(JSON_PATH.read_text(encoding="utf-8"))
    print(f"Loaded {len(pathways)} pathways from {JSON_PATH}")
    print(f"Snapshot date: {snapshot}")

    if args.dry_run:
        print("(dry run — no DB writes)")
        for p in pathways[:3]:
            print(p)
        return

    conn = psycopg2.connect(
        host=os.getenv("RLC_PG_HOST"), port=os.getenv("RLC_PG_PORT", "5432"),
        dbname=os.getenv("RLC_PG_DB", "rlc_commodities"),
        user=os.getenv("RLC_PG_USER"), password=os.getenv("RLC_PG_PASSWORD"),
        sslmode="require",
    )
    cur = conn.cursor()

    # Idempotency: delete any existing rows for this snapshot, then insert.
    # pathway_id is NOT unique in source — same ID can have multiple rows
    # for different feedstock/CI breakdowns. We use a surrogate BIGSERIAL PK.
    cur.execute("DELETE FROM bronze.carb_lcfs_pathways WHERE snapshot_date = %s", (snapshot,))
    print(f"Cleared {cur.rowcount} existing rows for snapshot {snapshot}")

    sql = """
    INSERT INTO bronze.carb_lcfs_pathways
        (snapshot_date, pathway_id, class, calc_version,
         fuel_producer, facility_name, facility_location,
         feedstock, fuel_type, ci_current, fpc,
         certification_date, applicant_description, source_file, loaded_at)
    VALUES
        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
    """

    rows = []
    for p in pathways:
        pid = p.get("pathway_id", "").strip()
        if not pid:
            continue
        rows.append((
            snapshot,
            pid,
            p.get("class") or None,
            p.get("calc_version") or None,
            p.get("fuel_producer") or None,
            p.get("facility_name") or None,
            p.get("facility_location") or None,
            p.get("feedstock") or None,
            p.get("fuel_type") or None,
            to_numeric(p.get("ci")),
            p.get("fpc") if isinstance(p.get("fpc"), str) else None,
            to_date(p.get("certification_date")),
            p.get("applicant_description") or None,
            SOURCE_XLSX,
        ))

    print(f"Prepared {len(rows)} rows for insert")
    execute_batch(cur, sql, rows, page_size=500)
    conn.commit()
    print(f"Inserted/updated {len(rows)} rows in bronze.carb_lcfs_pathways")

    # Quick sanity check
    cur.execute("SELECT COUNT(*), COUNT(DISTINCT fuel_producer), COUNT(DISTINCT facility_name) "
                "FROM bronze.carb_lcfs_pathways WHERE snapshot_date = %s", (snapshot,))
    n, n_producer, n_facility = cur.fetchone()
    print(f"  Total rows: {n}")
    print(f"  Distinct fuel_producer: {n_producer}")
    print(f"  Distinct facility_name: {n_facility}")

    # Verify silver view works
    cur.execute("SELECT COUNT(*) FROM silver.facility_carb_status")
    print(f"  silver.facility_carb_status rows: {cur.fetchone()[0]}")

    conn.close()


if __name__ == "__main__":
    main()
