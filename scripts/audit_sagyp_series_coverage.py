"""Audit SAGyP silver series coverage for code-drift holes.

Exact-posicion mapping means a historical code change kills a series silently at the
change date (Desktop follow-up on mig 173). After any backfill pass:

    python scripts/audit_sagyp_series_coverage.py

Prints (1) publication-day counts per series per year — a multi-year hole is a drifted
code needing a historical map entry; (2) unreviewed posiciones in curated HS6 families
(no disposition row at all); (3) re-runs the mig-175 auto-seed so newly backfilled
known-family variants get bronze-only disposition rows instead of firing the daily
collector's warning forever.
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv; load_dotenv(ROOT / ".env")
from src.services.database.db_config import get_connection


def main():
    with get_connection() as conn:
        cur = conn.cursor()

        # (3) sweep newly observed known-family variants into disposition rows first,
        # so (2) reports only what is genuinely new after this audit.
        cur.execute("""
            INSERT INTO reference.sagyp_position_map (posicion, series_key, description, is_active)
            SELECT DISTINCT b.posicion, NULL,
                   'auto-seeded disposition row (reviewed family, variant not promoted)', false
            FROM bronze.sagyp_fob_raw b
            WHERE left(b.posicion, 6) IN (SELECT left(posicion, 6)
                                          FROM reference.sagyp_position_map WHERE is_active)
              AND NOT EXISTS (SELECT 1 FROM reference.sagyp_position_map m
                              WHERE m.posicion = b.posicion)
            ON CONFLICT (posicion) DO NOTHING
            RETURNING posicion""")
        seeded = [r["posicion"] for r in cur.fetchall()]
        conn.commit()
        if seeded:
            print(f"auto-seeded {len(seeded)} new disposition rows: {seeded}\n")

        # (1) publication days per series per year; gaps mark drifted codes.
        cur.execute("""
            SELECT m.series_key, EXTRACT(year FROM b.fecha)::int AS yr,
                   count(DISTINCT b.fecha) AS pub_days
            FROM bronze.sagyp_fob_raw b
            JOIN reference.sagyp_position_map m ON m.posicion = b.posicion AND m.is_active
            GROUP BY 1, 2""")
        grid = {}
        years = set()
        for r in cur.fetchall():
            grid.setdefault(r["series_key"], {})[r["yr"]] = r["pub_days"]
            years.add(r["yr"])
        years = sorted(years)
        cur.execute("SELECT EXTRACT(year FROM fecha)::int yr, count(DISTINCT fecha) n "
                    "FROM bronze.sagyp_fob_raw GROUP BY 1")
        total_days = {r["yr"]: r["n"] for r in cur.fetchall()}

        print("publication days per series per year (bronze days in header; '.' = ZERO):")
        hdr = "series".ljust(22) + "".join(f"{y % 100:>5}" for y in years)
        print(hdr)
        print("bronze days".ljust(22) + "".join(f"{total_days.get(y, 0):>5}" for y in years))
        holes = []
        for sk in sorted(grid):
            row = sk.ljust(22)
            missing = []
            for y in years:
                n = grid[sk].get(y, 0)
                row += f"{n:>5}" if n else "    ."
                if not n and total_days.get(y, 0) > 0:
                    missing.append(y)
            print(row)
            if missing:
                holes.append((sk, missing))

        print()
        if holes:
            print("HOLES (drifted code? add a historical map entry):")
            for sk, yrs in holes:
                print(f"  {sk}: no data in {yrs}")
        else:
            print("no per-year holes in any curated series")

        # (2) anything still unreviewed after the sweep (shouldn't happen)
        cur.execute("""
            SELECT DISTINCT b.posicion FROM bronze.sagyp_fob_raw b
            WHERE left(b.posicion, 6) IN (SELECT left(posicion, 6)
                                          FROM reference.sagyp_position_map WHERE is_active)
              AND NOT EXISTS (SELECT 1 FROM reference.sagyp_position_map m
                              WHERE m.posicion = b.posicion)""")
        leftover = [r["posicion"] for r in cur.fetchall()]
        if leftover:
            print(f"\nSTILL UNREVIEWED (unexpected): {leftover}")


if __name__ == "__main__":
    main()
