"""
Compare us_*_exports/imports CSV files in data/raw/oilseeds_fats_greases/
against bronze.census_trade (AWS RDS).

Each file has multiple sections; we read only the top MT section. Rows have
shape: ,Partner,Idx,HS Code,Product,Year-Year,UOM,Jan,Feb,...,Dec,Total,...
We only consume Partner=='World Total' (Partner Code R00).

We match each file (hs_code, year, month) to bronze.census_trade where
country_code='R00' and quantity is reported in MT (verified empirically).

Outputs a discrepancy report. Pass --apply to UPSERT corrected rows.
"""
from __future__ import annotations

import argparse
import csv
import re
import sys
from collections import defaultdict
from datetime import datetime
from decimal import Decimal
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

import psycopg2.extras

from src.services.database.db_config import get_connection

RAW_DIR = Path("data/raw/oilseeds_fats_greases")

# (file_basename) -> flow (exports / imports)
def detect_flow(name: str) -> str | None:
    n = name.lower()
    if "_exports" in n: return "exports"
    if "_imports" in n: return "imports"
    return None

MONTH_HEADERS = ["January","February","March","April","May","June",
                 "July","August","September","October","November","December"]

NUM_CLEAN = re.compile(r"[,\s]")

def parse_qty(s: str) -> float | None:
    s = (s or "").strip()
    if not s or s in ("-","N/A","N.A."): return None
    s = NUM_CLEAN.sub("", s)
    try: return float(s)
    except ValueError: return None


def iter_world_total_rows(csv_path: Path):
    """Yield dicts: {hs_code, year, uom, monthly: {1..12: float|None}} for World Total rows in the MT section."""
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        for row in reader:
            # Real data row: idx,World Total,1,HS,Product,YYYY-YYYY,UOM,jan,feb,...,dec,total,pct,US,R00,HS
            if len(row) < 21: continue
            if (row[1] or "").strip() != "World Total": continue
            hs = (row[3] or "").strip()
            if not hs.isdigit(): continue
            yr_field = (row[5] or "").strip()
            m = re.match(r"^(\d{4})-(\d{4})$", yr_field)
            if not m: continue
            year = int(m.group(1))
            uom = (row[6] or "").strip()
            monthly = {}
            for i in range(12):
                monthly[i+1] = parse_qty(row[7+i])
            yield {"hs_code": hs, "year": year, "uom": uom, "monthly": monthly}


def collect_file_rows(csv_path: Path):
    """Returns list of (hs, year, month, qty, uom, flow) tuples from one file."""
    flow = detect_flow(csv_path.name)
    if not flow:
        return []
    out = []
    for r in iter_world_total_rows(csv_path):
        if r["uom"].upper() != "MT":
            continue
        for m, q in r["monthly"].items():
            if q is None: continue
            out.append((r["hs_code"], r["year"], m, q, "MT", flow))
    return out


def fetch_db_world_totals(cur, hs_codes: set[str]) -> dict:
    """Returns {(hs, year, month, flow): quantity_in_MT} for the World-Total row.

    Census collector marks world-total as country_code='-' / country_name='TOTAL FOR ALL COUNTRIES'.
    Quantity is stored in kg; we convert to MT (kg/1000) so the comparison is in MT
    matching the source CSV's UOM=MT section.
    """
    if not hs_codes: return {}
    cur.execute("""
        SELECT hs_code, year, month, flow, quantity
        FROM bronze.census_trade
        WHERE country_code='-' AND country_name='TOTAL FOR ALL COUNTRIES'
          AND hs_code = ANY(%s)
    """, (list(hs_codes),))
    out = {}
    for row in cur.fetchall():
        q = row["quantity"]
        out[(row["hs_code"], row["year"], row["month"], row["flow"])] = (
            float(q) / 1000.0 if q is not None else None
        )
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit-files", type=int, default=0,
                    help="Process only first N files (debug)")
    ap.add_argument("--tolerance", type=float, default=0.5,
                    help="Absolute MT tolerance for treating two values as equal")
    ap.add_argument("--rel-tolerance", type=float, default=0.005,
                    help="Relative tolerance (0.005 = 0.5%%) for non-zero values")
    ap.add_argument("--show-detail", type=int, default=10,
                    help="Show detail for first N discrepancies per file")
    ap.add_argument("--apply", action="store_true",
                    help="UPSERT corrected values into bronze.census_trade")
    ap.add_argument("--gaps-only", action="store_true",
                    help="Only fill rows missing from DB with non-zero file values "
                         "(skip mismatches and zero-valued misses; preserves existing DB semantics)")
    args = ap.parse_args()

    files = sorted([p for p in RAW_DIR.glob("us_*_*.csv") if detect_flow(p.name)])
    if args.limit_files:
        files = files[:args.limit_files]
    print(f"Found {len(files)} trade files in {RAW_DIR}\n")

    with get_connection() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # Collect all file rows + HS codes
        per_file = {}
        all_hs = set()
        for p in files:
            rows = collect_file_rows(p)
            per_file[p.name] = rows
            for r in rows:
                all_hs.add(r[0])

        print(f"Distinct HS codes across files: {len(all_hs)}")
        print(f"  {sorted(all_hs)}\n")

        db = fetch_db_world_totals(cur, all_hs)
        print(f"DB rows fetched (country_code=R00 for these HS): {len(db):,}\n")

        # Compare
        summary = []
        global_missing = 0
        global_mismatch = 0
        global_match = 0
        global_zero_in_db = 0
        rows_to_upsert = []  # (hs, year, month, flow, file_qty)

        for fname, rows in per_file.items():
            n_match = n_mis = n_miss = n_zero = 0
            details = []
            for hs, yr, mo, fqty, uom, flow in rows:
                k = (hs, yr, mo, flow)
                if k not in db:
                    n_miss += 1
                    if len(details) < args.show_detail:
                        details.append(("MISSING", hs, yr, mo, flow, fqty, None))
                    rows_to_upsert.append((hs, yr, mo, flow, fqty))
                    continue
                dbq = db[k]
                if dbq is None:
                    n_zero += 1
                    if len(details) < args.show_detail:
                        details.append(("DB_NULL", hs, yr, mo, flow, fqty, None))
                    rows_to_upsert.append((hs, yr, mo, flow, fqty))
                    continue
                dbqf = float(dbq)
                diff = abs(dbqf - fqty)
                rel = diff / max(abs(fqty), 1e-9)
                if diff <= args.tolerance or rel <= args.rel_tolerance:
                    n_match += 1
                else:
                    n_mis += 1
                    if len(details) < args.show_detail:
                        details.append(("MISMATCH", hs, yr, mo, flow, fqty, dbqf))
                    rows_to_upsert.append((hs, yr, mo, flow, fqty))

            global_match += n_match
            global_mismatch += n_mis
            global_missing += n_miss
            global_zero_in_db += n_zero
            summary.append((fname, len(rows), n_match, n_mis, n_miss, n_zero, details))

        # Report
        print(f"{'File':<50s} {'rows':>5s} {'ok':>5s} {'mism':>5s} {'miss':>5s} {'null':>5s}")
        print("-" * 80)
        for fname, n, ok, mis, miss, zero, _ in summary:
            print(f"{fname:<50s} {n:>5d} {ok:>5d} {mis:>5d} {miss:>5d} {zero:>5d}")
        print("-" * 80)
        print(f"{'TOTAL':<50s} {sum(s[1] for s in summary):>5d} {global_match:>5d} {global_mismatch:>5d} {global_missing:>5d} {global_zero_in_db:>5d}")
        print()
        print(f"  ok       = file MT matches DB MT (within tol {args.tolerance} or {args.rel_tolerance*100}%)")
        print(f"  mism     = file MT and DB MT differ beyond tolerance")
        print(f"  miss     = no DB row for (hs,year,month,flow,country_code='R00')")
        print(f"  null     = DB row exists but quantity is NULL")
        print()

        # Show details for any files with discrepancies
        any_disc = global_mismatch + global_missing + global_zero_in_db
        if any_disc > 0:
            print(f"=== Sample discrepancies (first {args.show_detail} per file) ===\n")
            for fname, n, ok, mis, miss, zero, details in summary:
                if not details: continue
                print(f"--- {fname} ---")
                for kind, hs, yr, mo, flow, fqty, dbqf in details:
                    if kind == "MISMATCH":
                        delta = fqty - dbqf
                        pct = (delta/dbqf*100) if dbqf else float('inf')
                        print(f"  {kind:8s} hs={hs} {yr}-{mo:02d} {flow:7s}  file={fqty:>15,.2f}  db={dbqf:>15,.2f}  d={delta:>+12,.2f} ({pct:+.1f}%)")
                    else:
                        print(f"  {kind:8s} hs={hs} {yr}-{mo:02d} {flow:7s}  file={fqty:>15,.2f}  db=(none)")
                print()

        # In gaps-only mode, restrict to rows where DB had no row at all AND file value > 0.
        if args.gaps_only:
            gap_rows = []
            # Re-scan to know which were MISSING (had no DB key) — re-derive from db dict.
            for hs, yr, mo, flow, q_mt in rows_to_upsert:
                if (hs, yr, mo, flow) not in db and q_mt > 0:
                    gap_rows.append((hs, yr, mo, flow, q_mt))
            print(f"\n=== gaps-only: {len(gap_rows):,} non-zero MISSING rows (down from {len(rows_to_upsert):,}) ===")
            rows_to_upsert = gap_rows

        if args.apply and rows_to_upsert:
            # DB stores quantity in kg; file is in MT, so convert MT -> kg.
            print(f"\n=== APPLYING {len(rows_to_upsert):,} upserts to bronze.census_trade ===")
            psycopg2.extras.execute_batch(cur, """
                INSERT INTO bronze.census_trade
                  (year, month, flow, hs_code, country_code, country_name, value_usd, quantity, source, collected_at)
                VALUES (%s, %s, %s, %s, '-', 'TOTAL FOR ALL COUNTRIES', NULL, %s, 'usatradeonline_csv_oilseeds_fats_greases', NOW())
                ON CONFLICT (year, month, flow, hs_code, country_code) DO UPDATE SET
                  quantity = EXCLUDED.quantity,
                  source = EXCLUDED.source,
                  collected_at = EXCLUDED.collected_at
            """, [(yr, mo, flow, hs, q_mt * 1000.0) for (hs, yr, mo, flow, q_mt) in rows_to_upsert])
            conn.commit()
            print("Committed.")
        elif rows_to_upsert:
            print(f"\nDry-run only — {len(rows_to_upsert):,} rows would be upserted. Re-run with --apply to commit.")


if __name__ == "__main__":
    main()
