"""
RLC Notion Daily Sync Runner
==============================
Orchestrates the full daily sync pipeline:
  1. Generate facts/patch from repo state
  2. Upsert to Notion via the sync worker
  3. Write sync run log

Usage:
    python scripts/run_notion_sync.py                    # Full sync
    python scripts/run_notion_sync.py --dry-run          # Preview only
    python scripts/run_notion_sync.py --generate-only    # Only generate patch, don't upsert

Can be scheduled via Windows Task Scheduler:
    schtasks /create /tn "RLC Notion Sync" /tr "python C:\\dev\\RLC-Agent_local_copy\\scripts\\run_notion_sync.py" /sc daily /st 18:00
"""

import os
import sys
import json
import logging
import argparse
from datetime import datetime, date
from pathlib import Path

# Add project root to path
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.agents.integration.notion_facts_generator import NotionFactsGenerator
from src.agents.integration.notion_sync_worker import (
    NotionSyncWorker,
    API_VERSION_LEGACY,
    API_VERSION_MODERN,
)

logger = logging.getLogger("notion_sync_runner")


def run_sync(
    dry_run: bool = False,
    generate_only: bool = False,
    verbose: bool = False,
    api_version: str = API_VERSION_MODERN,
):
    """Run the full Notion sync pipeline."""

    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    export_dir = Path(
        os.environ.get("RLC_EXPORT_DIR", Path.home() / "rlc_scheduler" / "exports")
    )
    export_dir.mkdir(parents=True, exist_ok=True)

    # Step 1: Generate patch
    print("=" * 60)
    print("Step 1: Generating facts from repository state...")
    print("=" * 60)

    generator = NotionFactsGenerator(project_root=PROJECT_ROOT)
    patch = generator.generate_patch(include_git_diff=True)

    patch_path = export_dir / f"notion_patch_{date.today().isoformat()}.json"
    with open(patch_path, "w", encoding="utf-8") as f:
        json.dump(patch, f, indent=2, default=str)

    print(f"  Patch generated: {patch_path}")
    print(f"  Total records: {patch['summary']['total_records']}")
    for table, count in patch["summary"]["by_table"].items():
        print(f"    {table}: {count}")

    if generate_only:
        print("\n--generate-only flag set. Skipping Notion upsert.")
        return patch_path

    # Step 2: Upsert to Notion
    print("\n" + "=" * 60)
    print("Step 2: Upserting to Notion...")
    print("=" * 60)

    api_key = os.environ.get("NOTION_API_KEY", "")
    if not api_key:
        print("ERROR: NOTION_API_KEY not set. Cannot upsert to Notion.")
        print("Set it in your .env file or as an environment variable.")
        print(f"Patch file saved at: {patch_path}")
        return patch_path

    try:
        worker = NotionSyncWorker(
            api_key=api_key, dry_run=dry_run, api_version=api_version
        )
        summary = worker.process_patch(patch)
    except Exception as e:
        print(f"ERROR during Notion sync: {e}")
        print(f"Patch file saved at: {patch_path}")
        return patch_path

    # Step 3: Write sync log
    print("\n" + "=" * 60)
    print("Step 3: Writing sync run log...")
    print("=" * 60)

    log_path = worker.write_sync_log(summary, output_dir=export_dir)

    # Final summary
    print("\n" + "=" * 60)
    print("SYNC COMPLETE")
    print("=" * 60)
    print(f"  Mode: {'DRY RUN' if dry_run else 'LIVE'}")
    print(f"  Records processed: {summary['total_records']}")
    print(f"  Created: {summary['created']}")
    print(f"  Updated: {summary['updated']}")
    print(f"  Errors: {summary['errors']}")
    print(f"  Patch file: {patch_path}")
    print(f"  Sync log: {log_path}")

    if summary["errors"] > 0:
        print(f"\n  ERRORS ({summary['errors']}):")
        for r in summary["results"]:
            if r.get("status") == "error":
                print(f"    - {r['table']}/{r['upsert_value']}: {r.get('error')}")

    return log_path


def main():
    parser = argparse.ArgumentParser(
        description="RLC Notion Daily Sync - generate patch and upsert to Notion"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview changes without writing to Notion",
    )
    parser.add_argument(
        "--generate-only",
        action="store_true",
        help="Only generate patch file, don't upsert",
    )
    parser.add_argument(
        "--api-version",
        default=API_VERSION_MODERN,
        choices=[API_VERSION_LEGACY, API_VERSION_MODERN],
        help=f"Notion API version (default: {API_VERSION_MODERN}). "
        f"Use {API_VERSION_LEGACY} if your notion-client SDK doesn't support data sources yet.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )

    args = parser.parse_args()
    run_sync(
        dry_run=args.dry_run,
        generate_only=args.generate_only,
        verbose=args.verbose,
        api_version=args.api_version,
    )


if __name__ == "__main__":
    main()
