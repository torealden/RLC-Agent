#!/usr/bin/env python3
"""
Brazil Soybean Weekly Report - Master Runner

ONE COMMAND to run the entire weekly report pipeline:
1. Ingest raw data files
2. Validate data quality
3. Generate weekly data pack (JSON)
4. Create charts (PNG)
5. Output report materials for LLM

Usage:
    # Run everything for current week
    python run_brazil_soy_report.py

    # Run for specific date
    python run_brazil_soy_report.py --date 2026-01-22

    # Dry run (validate only, no outputs)
    python run_brazil_soy_report.py --dry-run

    # Skip charts (faster, text-only output)
    python run_brazil_soy_report.py --no-charts
"""

import argparse
import sys
import subprocess
from pathlib import Path
from datetime import date, datetime
from typing import Dict, Any

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.brazil_soy_report.brazil_soy_config import (
    OUTPUT_DIR,
    RAW_DIR,
    get_report_week_dates,
)


def print_header(text: str, char: str = "="):
    """Print a formatted header."""
    width = 60
    print()
    print(char * width)
    print(f" {text}")
    print(char * width)


def print_step(step_num: int, text: str):
    """Print a step indicator."""
    print(f"\n[Step {step_num}] {text}")
    print("-" * 40)


def check_raw_files() -> Dict[str, bool]:
    """Check which raw files are available."""
    from scripts.brazil_soy_report.brazil_soy_config import DATA_SOURCES

    found = {}
    for source, config in DATA_SOURCES.items():
        # Check if any matching files exist
        pattern = config.file_pattern
        files = list(RAW_DIR.glob(pattern))
        if not files:
            # Try broader pattern
            files = list(RAW_DIR.glob(f"*{config.name}*"))
        found[source] = len(files) > 0

    return found


def run_ingestion(report_date: date, dry_run: bool = False) -> bool:
    """Run data ingestion."""
    print_step(1, "Ingesting raw data files")

    from scripts.brazil_soy_report.ingest_brazil_soy_data import ingest_all

    try:
        results = ingest_all(report_date, validate_only=dry_run)

        # Check for critical failures
        loaded = sum(1 for r in results['sources'].values()
                     if r['status'] in ['valid', 'warnings'])
        return loaded > 0

    except Exception as e:
        print(f"  ERROR: {e}")
        return False


def run_data_pack(report_date: date) -> bool:
    """Generate the weekly data pack."""
    print_step(2, "Generating weekly data pack (JSON)")

    from scripts.brazil_soy_report.weekly_data_pack import generate_data_pack

    try:
        data_pack = generate_data_pack(report_date)

        if data_pack:
            print(f"  Sources loaded: {len(data_pack['metadata']['sources_loaded'])}")
            print(f"  Sources missing: {len(data_pack['metadata']['sources_missing'])}")
            return True
        return False

    except Exception as e:
        print(f"  ERROR: {e}")
        return False


def run_charts(report_date: date) -> bool:
    """Generate visualization charts."""
    print_step(3, "Generating charts (PNG)")

    try:
        from scripts.brazil_soy_report.brazil_soy_charts import generate_all_charts
        charts = generate_all_charts(report_date)

        created = sum(1 for v in charts.values() if v is not None)
        print(f"  Charts created: {created}/{len(charts)}")
        return created > 0

    except ImportError:
        print("  SKIPPED: matplotlib not available")
        return True
    except Exception as e:
        print(f"  ERROR: {e}")
        return False


def print_output_summary(week: Dict):
    """Print summary of generated outputs."""
    print_header("OUTPUT FILES", "-")

    output_files = list(OUTPUT_DIR.glob(f"*{week['end_wed'].strftime('%Y%m%d')}*"))

    if output_files:
        for f in sorted(output_files):
            size_kb = f.stat().st_size / 1024
            print(f"  {f.name} ({size_kb:.1f} KB)")
    else:
        print("  No output files generated")

    print()
    print(f"Output directory: {OUTPUT_DIR}")


def print_next_steps(week: Dict):
    """Print instructions for next steps."""
    print_header("NEXT STEPS", "-")

    print("""
1. REVIEW THE DATA PACK:
   Open the JSON file to verify all data is correct:
   data/brazil_soy/output/weekly_data_pack_YYYYMMDD.json

2. FEED TO LLM:
   Use the prompt file with your LLM to generate the narrative:
   data/brazil_soy/output/llm_prompt_YYYYMMDD.txt

3. ASSEMBLE FINAL REPORT:
   - Copy the generated text into your report template
   - Insert the PNG charts
   - Export to PDF

4. QA CHECKLIST:
   [ ] All price changes match source data
   [ ] Units are correct (BRL/60kg, USc/bu, MMT)
   [ ] Week dates are correct (Wed to Wed)
   [ ] Charts render properly
   [ ] No placeholder text remains
""")


def main():
    parser = argparse.ArgumentParser(
        description='Run the complete Brazil Soybean Weekly Report pipeline'
    )
    parser.add_argument(
        '--date',
        type=str,
        help='Report date (YYYY-MM-DD). Default: latest Wednesday'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Validate only, do not generate outputs'
    )
    parser.add_argument(
        '--no-charts',
        action='store_true',
        help='Skip chart generation'
    )
    parser.add_argument(
        '--check-files',
        action='store_true',
        help='Only check which raw files are available'
    )

    args = parser.parse_args()

    # Parse date
    report_date = None
    if args.date:
        report_date = datetime.strptime(args.date, "%Y-%m-%d").date()

    week = get_report_week_dates(report_date)

    # Header
    print_header(f"BRAZIL SOYBEAN WEEKLY REPORT")
    print(f"Report Week: {week['week_label']}")
    print(f"Start: {week['start_wed']} (Wednesday)")
    print(f"End:   {week['end_wed']} (Wednesday)")

    # Check raw files
    if args.check_files:
        print_header("RAW FILE CHECK", "-")
        found = check_raw_files()
        for source, exists in found.items():
            status = "[OK]" if exists else "[MISSING]"
            print(f"  {status} {source}")
        print()
        print(f"Place raw files in: {RAW_DIR}")
        print("""
Expected naming convention:
  YYYY-MM-DD__SOURCE__CONTENT__DETAIL.ext

Examples:
  2026-01-22__CEPEA__SOY_PARANAGUA_RS_SC_DAILY.xls
  2026-01-22__CBOT__SOY_FUTURES_SETTLEMENTS_BARCHART.csv
  2026-01-22__IMEA__MT_SOY_PRICES_RS_SC.xls
  2026-01-22__ANEC__SOY_SHIPMENTS_WEEK04_2026.csv
  2026-01-22__NOAA__BRAZIL_WEATHER_SIGNAL.csv
""")
        return

    # Run pipeline
    success = True

    # Step 1: Ingestion
    if not run_ingestion(report_date, args.dry_run):
        print("\n  WARNING: No data was loaded. Check that raw files exist.")
        # Continue anyway to show what's missing

    if args.dry_run:
        print("\n[Dry run complete - no outputs generated]")
        return

    # Step 2: Data Pack
    if not run_data_pack(report_date):
        print("\n  WARNING: Data pack generation had issues.")

    # Step 3: Charts
    if not args.no_charts:
        run_charts(report_date)
    else:
        print_step(3, "Skipping charts (--no-charts)")

    # Summary
    print_output_summary(week)
    print_next_steps(week)

    print_header("COMPLETE")
    print(f"Report materials for {week['week_label']} are ready.")
    print()


if __name__ == '__main__':
    main()
