"""
Load CPC Gridded Layers data into PostgreSQL.

Thin CLI wrapper that reads a manifest JSON (produced by
nass_cpc_gridded_collector.py --output-json) and upserts into:
  - bronze.cpc_file_manifest
  - bronze.cpc_region_stats
  - bronze.cpc_ingest_run

Can also invoke the collector directly for a one-step backfill.

Usage:
  # Load from pre-built manifest JSON
  python scripts/load_cpc_gridded_data.py --manifest-json output/manifest.json

  # Direct ingest (extract + manifest + load in one step)
  python scripts/load_cpc_gridded_data.py --direct --year 2025

  # Full backfill (all years)
  python scripts/load_cpc_gridded_data.py --direct

  # Verify loaded data
  python scripts/load_cpc_gridded_data.py --verify
"""

import argparse
import json
import logging
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

# Add project root to path for imports
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


def get_db_connection():
    """Get PostgreSQL connection."""
    import psycopg2
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / ".env")

    password = (
        os.environ.get("RLC_PG_PASSWORD")
        or os.environ.get("DATABASE_PASSWORD")
        or os.environ.get("DB_PASSWORD")
    )

    return psycopg2.connect(
        host=os.environ.get("DATABASE_HOST", "localhost"),
        port=os.environ.get("DATABASE_PORT", "5432"),
        database=os.environ.get("DATABASE_NAME", "rlc_commodities"),
        user=os.environ.get("DATABASE_USER", "postgres"),
        password=password,
    )


def load_from_manifest_json(json_path: Path, year: int = None):
    """Load manifest JSON file and upsert into database."""
    logger.info(f"Loading manifest from: {json_path}")

    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    manifest_rows = data.get("manifest", [])
    stats_rows = data.get("region_stats", [])

    logger.info(f"  Manifest entries: {len(manifest_rows)}")
    logger.info(f"  Region stats:    {len(stats_rows)}")

    # Filter by year if specified
    if year is not None:
        manifest_rows = [m for m in manifest_rows if m.get("year") == year]
        stats_rows = [s for s in stats_rows if s.get("year") == year]
        logger.info(f"  After year={year} filter: {len(manifest_rows)} files, {len(stats_rows)} stats")

    if not manifest_rows:
        print("No manifest entries to load.")
        return

    conn = get_db_connection()
    cur = conn.cursor()
    run_id = str(uuid.uuid4())

    files_added = 0
    files_updated = 0
    stats_added = 0
    qa_failures = sum(1 for m in manifest_rows if not m.get("qa_passed", True))

    try:
        # Record ingest run
        cur.execute("""
            INSERT INTO bronze.cpc_ingest_run
                (ingest_run_id, collector_version, source, years_processed)
            VALUES (%s, %s, 'local', %s)
        """, (run_id, data.get("collector_version", "unknown"),
              str(year) if year else "all"))

        # Upsert manifest
        for m in manifest_rows:
            cur.execute("""
                INSERT INTO bronze.cpc_file_manifest
                    (file_sha256, series_id, ingest_run_id,
                     year, nass_week, week_ending_date,
                     file_path, file_name, file_bytes, modified_utc,
                     crs_wkt, pixel_size_m, dtype, nodata_value,
                     width, height,
                     bbox_xmin, bbox_ymin, bbox_xmax, bbox_ymax,
                     value_min, value_max, value_mean, pct_nodata,
                     qa_passed, qa_notes)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s)
                ON CONFLICT (series_id, year, nass_week)
                DO UPDATE SET
                    file_sha256 = EXCLUDED.file_sha256,
                    ingest_run_id = EXCLUDED.ingest_run_id,
                    file_path = EXCLUDED.file_path,
                    file_bytes = EXCLUDED.file_bytes,
                    modified_utc = EXCLUDED.modified_utc,
                    value_min = EXCLUDED.value_min,
                    value_max = EXCLUDED.value_max,
                    value_mean = EXCLUDED.value_mean,
                    pct_nodata = EXCLUDED.pct_nodata,
                    qa_passed = EXCLUDED.qa_passed,
                    qa_notes = EXCLUDED.qa_notes,
                    collected_at = NOW()
            """, (
                m.get("file_sha256"), m.get("series_id"), run_id,
                m.get("year"), m.get("nass_week"), m.get("week_ending_date"),
                m.get("file_path"), m.get("file_name"), m.get("file_bytes"),
                m.get("modified_utc"),
                m.get("crs_wkt"), m.get("pixel_size_m"), m.get("dtype"),
                m.get("nodata_value"), m.get("width"), m.get("height"),
                m.get("bbox_xmin"), m.get("bbox_ymin"),
                m.get("bbox_xmax"), m.get("bbox_ymax"),
                m.get("value_min"), m.get("value_max"), m.get("value_mean"),
                m.get("pct_nodata"),
                m.get("qa_passed", True), m.get("qa_notes", ""),
            ))
            files_added += 1

        # Upsert region stats
        for s in stats_rows:
            cur.execute("""
                INSERT INTO bronze.cpc_region_stats
                    (series_id, year, nass_week, week_ending_date,
                     region_id, region_type, stat_name, value, pixel_count,
                     ingest_run_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (series_id, year, nass_week, region_id, stat_name)
                DO UPDATE SET
                    value = EXCLUDED.value,
                    pixel_count = EXCLUDED.pixel_count,
                    ingest_run_id = EXCLUDED.ingest_run_id,
                    collected_at = NOW()
            """, (
                s.get("series_id"), s.get("year"), s.get("nass_week"),
                s.get("week_ending_date"),
                s.get("region_id"), s.get("region_type"), s.get("stat_name"),
                s.get("value"), s.get("pixel_count"), run_id,
            ))
            stats_added += 1

        # Update ingest run counts
        cur.execute("""
            UPDATE bronze.cpc_ingest_run
            SET files_added = %s, files_updated = %s, qa_failures = %s
            WHERE ingest_run_id = %s
        """, (files_added, files_updated, qa_failures, run_id))

        conn.commit()

        print(f"\nLoad Summary:")
        print(f"  Run ID:        {run_id}")
        print(f"  Files loaded:  {files_added}")
        print(f"  Stats loaded:  {stats_added}")
        print(f"  QA failures:   {qa_failures}")

    except Exception as e:
        conn.rollback()
        logger.error(f"Database error: {e}")
        raise
    finally:
        cur.close()
        conn.close()


def run_direct_ingest(year: int = None, crop: str = None, skip_stats: bool = False):
    """Run the collector directly and save to DB."""
    from src.agents.collectors.us.nass_cpc_gridded_collector import (
        ARCHIVE_ROOT, extract_all_zips, build_manifest, save_to_database
    )

    start = datetime.now()

    print("Step 1/3: Extracting ZIPs...")
    extract_all_zips(ARCHIVE_ROOT, year=year, crop=crop)

    print("Step 2/3: Building manifest...")
    manifest, region_stats = build_manifest(ARCHIVE_ROOT, year=year, crop=crop, skip_stats=skip_stats)

    duration = (datetime.now() - start).total_seconds()

    print("Step 3/3: Saving to database...")
    summary = save_to_database(
        manifest, region_stats,
        years_processed=str(year) if year else "all",
        crops_processed=crop if crop else "all",
        duration_sec=duration,
    )

    print(f"\nDirect Ingest Summary:")
    print(f"  Run ID:        {summary['ingest_run_id']}")
    print(f"  Files added:   {summary['files_added']}")
    print(f"  Files updated: {summary['files_updated']}")
    print(f"  Stats rows:    {summary['stats_rows']}")
    print(f"  QA failures:   {summary['qa_failures']}")
    print(f"  Duration:      {duration:.1f}s")


def verify_loaded_data():
    """Print summary of CPC data in the database."""
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        # File manifest counts
        cur.execute("""
            SELECT sc.crop, sc.product, COUNT(*) as file_count,
                   MIN(fm.year) as min_year, MAX(fm.year) as max_year,
                   SUM(CASE WHEN fm.qa_passed THEN 1 ELSE 0 END) as qa_pass,
                   SUM(CASE WHEN NOT fm.qa_passed THEN 1 ELSE 0 END) as qa_fail
            FROM bronze.cpc_file_manifest fm
            JOIN bronze.cpc_series_catalog sc ON fm.series_id = sc.series_id
            GROUP BY sc.crop, sc.product
            ORDER BY sc.crop, sc.product
        """)
        rows = cur.fetchall()

        print(f"\n{'='*70}")
        print(f"  CPC Gridded Layers — Database Verification")
        print(f"{'='*70}")

        if not rows:
            print("  No data found in bronze.cpc_file_manifest")
        else:
            print(f"\n  {'Crop':<16} {'Product':<12} {'Files':>6} {'Years':>12} {'QA Pass':>8} {'QA Fail':>8}")
            print(f"  {'-'*16} {'-'*12} {'-'*6} {'-'*12} {'-'*8} {'-'*8}")
            total_files = 0
            for crop, product, count, min_yr, max_yr, qa_pass, qa_fail in rows:
                print(f"  {crop:<16} {product:<12} {count:>6} {min_yr}-{max_yr:>6} {qa_pass:>8} {qa_fail:>8}")
                total_files += count
            print(f"  {'TOTAL':<16} {'':<12} {total_files:>6}")

        # Region stats counts
        cur.execute("""
            SELECT COUNT(*) FROM bronze.cpc_region_stats
        """)
        stat_count = cur.fetchone()[0]
        print(f"\n  Region stats rows: {stat_count}")

        # Ingest runs
        cur.execute("""
            SELECT ingest_run_id, run_ts_utc, files_added, files_updated, qa_failures
            FROM bronze.cpc_ingest_run
            ORDER BY run_ts_utc DESC
            LIMIT 5
        """)
        runs = cur.fetchall()
        if runs:
            print(f"\n  Recent ingest runs:")
            for run_id, ts, added, updated, qa_fail in runs:
                print(f"    {str(run_id)[:8]}... @ {ts}: +{added} files, ~{updated} updated, {qa_fail} QA fail")

        print(f"{'='*70}\n")

    finally:
        cur.close()
        conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="Load CPC Gridded Layers data into PostgreSQL"
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--manifest-json", type=str,
                       help="Path to manifest JSON file from collector")
    group.add_argument("--direct", action="store_true",
                       help="Run collector directly (extract + manifest + load)")
    group.add_argument("--verify", action="store_true",
                       help="Verify loaded data in database")

    parser.add_argument("--year", type=int, help="Process only a specific year")
    parser.add_argument("--crop", type=str, help="Process only a specific crop")
    parser.add_argument("--skip-stats", action="store_true",
                        help="Skip region statistics")
    parser.add_argument("--verbose", "-v", action="store_true")

    args = parser.parse_args()

    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    if args.manifest_json:
        load_from_manifest_json(Path(args.manifest_json), year=args.year)
    elif args.direct:
        run_direct_ingest(year=args.year, crop=args.crop, skip_stats=args.skip_stats)
    elif args.verify:
        verify_loaded_data()


if __name__ == "__main__":
    main()
