"""
Notion Sync Worker - Idempotent Upsert Engine for RLC OS
=========================================================
Consumes a structured patch JSON and performs idempotent upserts
to Notion databases with rate limiting and audit logging.

Supports both legacy (pre-2025-09-03) and modern Notion API versions.
The modern API separates databases (containers) from data sources (schema/content).
See: https://developers.notion.com/guides/get-started/upgrade-guide-2025-09-03

Usage:
    python -m src.agents.integration.notion_sync_worker --patch path/to/notion_patch.json
    python -m src.agents.integration.notion_sync_worker --patch path/to/notion_patch.json --dry-run
    python -m src.agents.integration.notion_sync_worker --patch path/to/patch.json --api-version 2025-09-03

Patch JSON format:
    {
        "records": [
            {
                "table": "agent_registry",
                "upsert_key": "Agent Name",
                "upsert_value": "USDA NASS Collector",
                "properties": { ... }
            }
        ]
    }
"""

import os
import sys
import json
import time
import logging
import argparse
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

logger = logging.getLogger("notion_sync_worker")

# Notion RLC OS IDs.
# Under API 2025-09-03, these are DATA SOURCE IDs (not database container IDs).
# The data_source_id is what you use for queries and page creation.
# The parent database_id (container) is different and listed for reference only.
NOTION_DATA_SOURCE_IDS = {
    "agent_registry": "2dbead02-3dee-804a-b611-000b7fe5b299",
    "data_sources_registry": "2dbead02-3dee-8062-ae13-000ba10e3beb",
    "architecture_decisions": "2dbead02-3dee-802f-a0a7-000b20d183ca",
    "runbooks": "2dbead02-3dee-804d-b167-000b11e5f92f",
    "lessons_learned": "2e6ead02-3dee-80d1-a7d7-000bf28e86d6",
    "reconciliation_log": "2dbead02-3dee-8050-ae40-000bd8ff835c",
    "master_timeline": "2dcead02-3dee-80ae-8990-000b75ea7d59",
}

# Parent database container IDs (for reference / database-level operations)
NOTION_DATABASE_IDS = {
    "agent_registry": "2dbead02-3dee-802b-a5da-d3cc4c9b904d",
    "data_sources_registry": "2dbead02-3dee-8088-9f44-c4727b9b8f81",
    "architecture_decisions": "2dbead02-3dee-809c-b603-f74314330e75",
    "runbooks": "2dbead02-3dee-8037-9944-cdec7a696cb2",
    "lessons_learned": "2e6ead02-3dee-8045-aa72-d76c998b5b10",
    "reconciliation_log": "2dbead02-3dee-8092-8368-da028b8a5e80",
    "master_timeline": "2dcead02-3dee-801d-bede-c4a67752801b",
}

# Backward-compatible alias
NOTION_DBS = NOTION_DATA_SOURCE_IDS

# Rate limit: Notion allows ~3 req/sec (180/min)
RATE_LIMIT_DELAY = 0.35  # seconds between requests
MAX_RETRIES = 3
RETRY_BASE_DELAY = 1.0  # seconds, will be multiplied by retry number

# Notion API version constants
API_VERSION_LEGACY = "2022-06-28"
API_VERSION_MODERN = "2025-09-03"


class NotionSyncWorker:
    """Performs idempotent upserts to Notion with rate limiting and logging.

    Supports two API modes:
    - Legacy (pre-2025-09-03): Uses database_id for queries and page creation.
    - Modern (2025-09-03+): Discovers data_source_id from each database_id,
      then uses data_source_id for queries and page creation.
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        dry_run: bool = False,
        api_version: str = API_VERSION_MODERN,
    ):
        self.api_key = api_key or os.environ.get("NOTION_API_KEY", "")
        self.dry_run = dry_run
        self.api_version = api_version
        self.use_data_sources = api_version >= API_VERSION_MODERN
        self.client = None
        self.sync_log: List[Dict[str, Any]] = []
        self._last_request_time = 0.0
        # Cache: database_id -> data_source_id (discovered at runtime)
        self._data_source_cache: Dict[str, str] = {}

        if not self.api_key:
            raise ValueError(
                "NOTION_API_KEY not set. Set it in your .env file or pass it as an argument."
            )

    def _ensure_client(self):
        """Initialize the Notion client on first use."""
        if self.client is None:
            try:
                from notion_client import Client

                self.client = Client(auth=self.api_key)
                logger.info(
                    f"Notion client initialized (api_version={self.api_version}, "
                    f"use_data_sources={self.use_data_sources})"
                )
            except ImportError:
                raise ImportError(
                    "notion-client package not installed. Run: pip install notion-client"
                )

    def _resolve_data_source_id(self, table_name: str) -> str:
        """Resolve the data_source_id for a table.

        NOTION_DBS already contains data_source_ids (confirmed via the
        2025-09-03 search API). This method returns them directly.
        If a parent database_id is needed, use NOTION_DATABASE_IDS.
        """
        return NOTION_DATA_SOURCE_IDS.get(table_name, "")

    def _rate_limit(self):
        """Enforce rate limiting between API calls."""
        elapsed = time.time() - self._last_request_time
        if elapsed < RATE_LIMIT_DELAY:
            time.sleep(RATE_LIMIT_DELAY - elapsed)
        self._last_request_time = time.time()

    def _api_call_with_retry(self, func, *args, **kwargs) -> Any:
        """Execute an API call with retry logic for rate limiting."""
        for attempt in range(MAX_RETRIES):
            try:
                self._rate_limit()
                return func(*args, **kwargs)
            except Exception as e:
                error_str = str(e)
                if "rate_limited" in error_str or "429" in error_str:
                    delay = RETRY_BASE_DELAY * (attempt + 1)
                    logger.warning(
                        f"Rate limited, retrying in {delay}s (attempt {attempt + 1}/{MAX_RETRIES})"
                    )
                    time.sleep(delay)
                elif attempt == MAX_RETRIES - 1:
                    raise
                else:
                    logger.warning(f"API error (attempt {attempt + 1}): {e}")
                    time.sleep(RETRY_BASE_DELAY)
        return None

    def find_page_by_title(
        self, data_source_id: str, title_property: str, title_value: str
    ) -> Optional[Dict[str, Any]]:
        """Query a Notion data source for a page matching a title value.

        Under API 2025-09-03, queries use /data_sources/{id}/query.
        Falls back to /databases/{id}/query for older SDK versions.
        """
        self._ensure_client()

        filter_obj = {
            "property": title_property,
            "title": {"equals": title_value},
        }

        # Use data_sources.query (2025-09-03 API) if available,
        # otherwise fall back to databases.query.
        if self.use_data_sources and hasattr(self.client, "data_sources"):
            response = self._api_call_with_retry(
                self.client.data_sources.query,
                data_source_id=data_source_id,
                filter=filter_obj,
                page_size=1,
            )
        else:
            # Fallback: databases.query still works if the ID happens to be
            # a single-source database or if the SDK is older.
            response = self._api_call_with_retry(
                self.client.databases.query,
                database_id=data_source_id,
                filter=filter_obj,
                page_size=1,
            )

        results = response.get("results", []) if response else []
        return results[0] if results else None

    def build_notion_properties(
        self, table: str, properties: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Convert simplified property dict to Notion API property format.

        Supports these shorthand formats:
            {"Field": "value"}              -> rich_text
            {"Field": {"type": "select", "value": "Option"}}
            {"Field": {"type": "multi_select", "value": ["A", "B"]}}
            {"Field": {"type": "date", "value": "2026-01-01"}}
            {"Field": {"type": "number", "value": 42}}
            {"Field": {"type": "url", "value": "https://..."}}
            {"Field": {"type": "checkbox", "value": true}}
            {"Field": {"type": "title", "value": "Page Title"}}
        """
        notion_props = {}

        for field, value in properties.items():
            if isinstance(value, dict) and "type" in value:
                prop_type = value["type"]
                prop_value = value.get("value", "")

                if prop_type == "title":
                    notion_props[field] = {
                        "title": [{"text": {"content": str(prop_value)}}]
                    }
                elif prop_type == "select":
                    notion_props[field] = {"select": {"name": str(prop_value)}}
                elif prop_type == "multi_select":
                    items = (
                        prop_value if isinstance(prop_value, list) else [prop_value]
                    )
                    notion_props[field] = {
                        "multi_select": [{"name": str(item)} for item in items]
                    }
                elif prop_type == "date":
                    notion_props[field] = {"date": {"start": str(prop_value)}}
                elif prop_type == "number":
                    notion_props[field] = {"number": float(prop_value)}
                elif prop_type == "url":
                    notion_props[field] = {"url": str(prop_value)}
                elif prop_type == "checkbox":
                    notion_props[field] = {"checkbox": bool(prop_value)}
                elif prop_type == "rich_text":
                    notion_props[field] = {
                        "rich_text": [{"text": {"content": str(prop_value)}}]
                    }
                else:
                    # Default to rich_text
                    notion_props[field] = {
                        "rich_text": [{"text": {"content": str(prop_value)}}]
                    }
            elif isinstance(value, str):
                notion_props[field] = {
                    "rich_text": [{"text": {"content": value}}]
                }
            elif isinstance(value, bool):
                notion_props[field] = {"checkbox": value}
            elif isinstance(value, (int, float)):
                notion_props[field] = {"number": value}
            elif isinstance(value, list):
                notion_props[field] = {
                    "multi_select": [{"name": str(item)} for item in value]
                }

        return notion_props

    def upsert_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Perform an idempotent upsert for a single record.

        Args:
            record: Dict with keys: table, upsert_key, upsert_value, properties

        Returns:
            Result dict with status, page_id, action taken
        """
        table = record.get("table", "")
        upsert_key = record.get("upsert_key", "")
        upsert_value = record.get("upsert_value", "")
        properties = record.get("properties", {})

        ds_id = NOTION_DATA_SOURCE_IDS.get(table)
        if not ds_id:
            return {
                "status": "error",
                "table": table,
                "upsert_value": upsert_value,
                "error": f"Unknown table: {table}",
            }

        result = {
            "table": table,
            "upsert_key": upsert_key,
            "upsert_value": upsert_value,
            "timestamp": datetime.now().isoformat(),
        }

        if self.dry_run:
            result["status"] = "dry_run"
            result["action"] = "would_upsert"
            result["properties"] = properties
            logger.info(f"[DRY RUN] Would upsert {table}: {upsert_value}")
            return result

        try:
            self._ensure_client()
            notion_props = self.build_notion_properties(table, properties)

            # Check if page exists (query the data source)
            existing = self.find_page_by_title(ds_id, upsert_key, upsert_value)

            if existing:
                # Update existing page (pages.update is unchanged across API versions)
                page_id = existing["id"]
                self._api_call_with_retry(
                    self.client.pages.update, page_id=page_id, properties=notion_props
                )
                result["status"] = "updated"
                result["action"] = "update"
                result["page_id"] = page_id
                logger.info(f"Updated {table}: {upsert_value}")
            else:
                # Create new page
                # Ensure the title property is set
                if upsert_key not in notion_props:
                    notion_props[upsert_key] = {
                        "title": [{"text": {"content": upsert_value}}]
                    }

                # Under 2025-09-03 API, page parent uses data_source_id
                if self.use_data_sources:
                    parent = {"type": "data_source_id", "data_source_id": ds_id}
                else:
                    parent = {"database_id": ds_id}

                response = self._api_call_with_retry(
                    self.client.pages.create,
                    parent=parent,
                    properties=notion_props,
                )
                result["status"] = "created"
                result["action"] = "create"
                result["page_id"] = response["id"] if response else None
                logger.info(f"Created {table}: {upsert_value}")

        except Exception as e:
            result["status"] = "error"
            result["error"] = str(e)
            logger.error(f"Failed to upsert {table}/{upsert_value}: {e}")

        return result

    def process_patch(self, patch_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process a full patch file, upserting all records.

        Args:
            patch_data: Dict with "records" list

        Returns:
            Sync run summary
        """
        records = patch_data.get("records", [])
        logger.info(f"Processing patch with {len(records)} records")

        run_summary = {
            "run_date": datetime.now().isoformat(),
            "total_records": len(records),
            "created": 0,
            "updated": 0,
            "errors": 0,
            "dry_run": self.dry_run,
            "results": [],
        }

        for i, record in enumerate(records):
            logger.info(
                f"Processing record {i + 1}/{len(records)}: "
                f"{record.get('table')}/{record.get('upsert_value')}"
            )
            result = self.upsert_record(record)
            run_summary["results"].append(result)

            if result["status"] == "created":
                run_summary["created"] += 1
            elif result["status"] == "updated":
                run_summary["updated"] += 1
            elif result["status"] == "error":
                run_summary["errors"] += 1

        logger.info(
            f"Sync complete: {run_summary['created']} created, "
            f"{run_summary['updated']} updated, {run_summary['errors']} errors"
        )

        return run_summary

    def write_sync_log(
        self, run_summary: Dict[str, Any], output_dir: Optional[Path] = None
    ) -> Path:
        """Write the sync run log to a JSON file."""
        if output_dir is None:
            output_dir = Path(
                os.environ.get(
                    "RLC_EXPORT_DIR",
                    Path.home() / "rlc_scheduler" / "exports",
                )
            )

        output_dir.mkdir(parents=True, exist_ok=True)
        log_path = output_dir / f"sync_run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

        with open(log_path, "w", encoding="utf-8") as f:
            json.dump(run_summary, f, indent=2, default=str)

        logger.info(f"Sync log written to: {log_path}")
        return log_path


def main():
    parser = argparse.ArgumentParser(
        description="Notion Sync Worker - Upsert records to RLC OS Notion databases"
    )
    parser.add_argument(
        "--patch",
        required=True,
        help="Path to the patch JSON file",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview changes without writing to Notion",
    )
    parser.add_argument(
        "--api-key",
        help="Notion API key (defaults to NOTION_API_KEY env var)",
    )
    parser.add_argument(
        "--api-version",
        default=API_VERSION_MODERN,
        choices=[API_VERSION_LEGACY, API_VERSION_MODERN],
        help=f"Notion API version to target (default: {API_VERSION_MODERN}). "
        f"Use {API_VERSION_LEGACY} for older single-source databases.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    # Load patch file
    patch_path = Path(args.patch)
    if not patch_path.exists():
        logger.error(f"Patch file not found: {patch_path}")
        sys.exit(1)

    with open(patch_path, "r", encoding="utf-8") as f:
        patch_data = json.load(f)

    # Initialize worker
    worker = NotionSyncWorker(
        api_key=args.api_key, dry_run=args.dry_run, api_version=args.api_version
    )

    # Process patch
    summary = worker.process_patch(patch_data)

    # Write sync log
    log_path = worker.write_sync_log(summary)

    # Print summary
    print(f"\nSync Summary:")
    print(f"  Total records: {summary['total_records']}")
    print(f"  Created: {summary['created']}")
    print(f"  Updated: {summary['updated']}")
    print(f"  Errors: {summary['errors']}")
    print(f"  Log: {log_path}")

    if summary["errors"] > 0:
        print(f"\nErrors:")
        for r in summary["results"]:
            if r.get("status") == "error":
                print(f"  - {r['table']}/{r['upsert_value']}: {r.get('error')}")
        sys.exit(1)


if __name__ == "__main__":
    main()
