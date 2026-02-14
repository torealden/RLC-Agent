"""
Notion Facts Generator - Build sync patch from repo state
==========================================================
Scans the repository for current agent/collector/source state and
generates a structured patch JSON for the Notion Sync Worker.

Usage:
    python -m src.agents.integration.notion_facts_generator
    python -m src.agents.integration.notion_facts_generator --output path/to/patch.json
    python -m src.agents.integration.notion_facts_generator --include-git-diff

Output: notion_patch_input.json containing structured facts for Notion upsert.
"""

import os
import sys
import json
import csv
import logging
import argparse
import subprocess
from datetime import datetime, date
from pathlib import Path
from typing import Dict, Any, List, Optional

logger = logging.getLogger("notion_facts_generator")

# Project root detection
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent.parent  # src/agents/integration -> root


class NotionFactsGenerator:
    """Generates structured facts from repository state for Notion sync."""

    def __init__(self, project_root: Optional[Path] = None):
        self.project_root = project_root or PROJECT_ROOT
        self.records: List[Dict[str, Any]] = []

    def scan_inventory_csvs(self):
        """Load agent and component inventories from CSV exports."""
        exports_dir = self.project_root / "data" / "exports"

        if not exports_dir.exists():
            logger.warning(f"Exports directory not found: {exports_dir}")
            return

        # Map CSV files to Notion tables
        csv_mappings = {
            "inventory_agents.csv": "agent_registry",
            "inventory_collectors.csv": "agent_registry",
            "inventory_orchestrators.csv": "agent_registry",
            "inventory_schedulers.csv": "agent_registry",
            "inventory_report_writers.csv": "agent_registry",
            "inventory_special_systems.csv": "agent_registry",
        }

        for csv_file, table in csv_mappings.items():
            csv_path = exports_dir / csv_file
            if not csv_path.exists():
                logger.debug(f"CSV not found: {csv_path}")
                continue

            try:
                with open(csv_path, "r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        name = row.get("Name", row.get("name", row.get("Agent", "")))
                        if not name:
                            continue

                        record = {
                            "table": table,
                            "upsert_key": "Agent Name",
                            "upsert_value": name.strip(),
                            "properties": {
                                "Agent Name": {
                                    "type": "title",
                                    "value": name.strip(),
                                },
                                "Last Verified": {
                                    "type": "date",
                                    "value": date.today().isoformat(),
                                },
                            },
                        }

                        # Map CSV columns to Notion properties
                        if row.get("Status", row.get("status")):
                            record["properties"]["Status"] = {
                                "type": "select",
                                "value": row.get("Status", row.get("status", "")),
                            }
                        if row.get("Category", row.get("category")):
                            record["properties"]["Category"] = {
                                "type": "multi_select",
                                "value": [
                                    c.strip()
                                    for c in row.get(
                                        "Category", row.get("category", "")
                                    ).split(",")
                                ],
                            }
                        if row.get("Purpose", row.get("purpose", row.get("Description"))):
                            record["properties"]["Purpose"] = {
                                "type": "rich_text",
                                "value": row.get(
                                    "Purpose",
                                    row.get("purpose", row.get("Description", "")),
                                ),
                            }
                        if row.get("Location", row.get("location", row.get("Path"))):
                            location = row.get(
                                "Location",
                                row.get("location", row.get("Path", "")),
                            )
                            record["properties"]["File"] = {
                                "type": "rich_text",
                                "value": location,
                            }

                        self.records.append(record)

                logger.info(f"Loaded inventory from {csv_file}")

            except Exception as e:
                logger.error(f"Error reading {csv_file}: {e}")

    def scan_notion_export(self):
        """Load existing Notion export data to merge/update."""
        export_path = self.project_root / "docs" / "notion_data_export.json"

        if not export_path.exists():
            logger.warning(f"Notion export not found: {export_path}")
            return

        try:
            with open(export_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            logger.error(f"Error reading Notion export: {e}")
            return

        # Track names we've already added from CSV
        existing_names = {r["upsert_value"] for r in self.records}

        # Process agent_registry entries from export
        for agent in data.get("agent_registry", []):
            name = agent.get("agent_name", "")
            if not name or name in existing_names:
                continue

            record = {
                "table": "agent_registry",
                "upsert_key": "Agent Name",
                "upsert_value": name,
                "properties": {
                    "Agent Name": {"type": "title", "value": name},
                    "Last Verified": {
                        "type": "date",
                        "value": date.today().isoformat(),
                    },
                },
            }

            if agent.get("status"):
                record["properties"]["Status"] = {
                    "type": "select",
                    "value": agent["status"],
                }
            if agent.get("category"):
                record["properties"]["Category"] = {
                    "type": "multi_select",
                    "value": agent["category"],
                }
            if agent.get("purpose"):
                record["properties"]["Purpose"] = {
                    "type": "rich_text",
                    "value": agent["purpose"],
                }
            if agent.get("location"):
                record["properties"]["File"] = {
                    "type": "rich_text",
                    "value": agent["location"],
                }
            if agent.get("autonomy_level"):
                # Map short codes to full Notion select values
                autonomy_map = {
                    "L1": "L1 - Approval Required",
                    "L2": "L2 - Routine Auto",
                    "L3": "L3 - High Auto",
                }
                raw = agent["autonomy_level"]
                mapped = autonomy_map.get(raw, raw)
                record["properties"]["Autonomy Level"] = {
                    "type": "select",
                    "value": mapped,
                }
            if agent.get("dependencies"):
                record["properties"]["Dependencies"] = {
                    "type": "multi_select",
                    "value": agent["dependencies"],
                }
            if agent.get("system_of_record"):
                record["properties"]["System of Record"] = {
                    "type": "multi_select",
                    "value": agent["system_of_record"],
                }
            if agent.get("inputs"):
                record["properties"]["Inputs"] = {
                    "type": "rich_text",
                    "value": agent["inputs"],
                }
            if agent.get("outputs"):
                record["properties"]["Outputs"] = {
                    "type": "rich_text",
                    "value": agent["outputs"],
                }

            self.records.append(record)
            existing_names.add(name)

        # Process data_sources_registry
        for source in data.get("data_sources_registry", []):
            name = source.get("source_name", "")
            if not name:
                continue

            record = {
                "table": "data_sources_registry",
                "upsert_key": "Source Name",
                "upsert_value": name,
                "properties": {
                    "Source Name": {"type": "title", "value": name},
                },
            }

            if source.get("domain"):
                record["properties"]["Domain"] = {
                    "type": "select",
                    "value": source["domain"],
                }
            if source.get("status"):
                record["properties"]["Status"] = {
                    "type": "select",
                    "value": source["status"],
                }
            if source.get("priority"):
                record["properties"]["Priority"] = {
                    "type": "select",
                    "value": source["priority"],
                }
            if source.get("access_method"):
                record["properties"]["Access Method"] = {
                    "type": "select",
                    "value": source["access_method"],
                }
            if source.get("credential_type"):
                record["properties"]["Credential Type"] = {
                    "type": "select",
                    "value": source["credential_type"],
                }
            if source.get("refresh_rate"):
                record["properties"]["Refresh Rate"] = {
                    "type": "rich_text",
                    "value": source["refresh_rate"],
                }
            if source.get("known_failure_modes"):
                record["properties"]["Known Failure Modes"] = {
                    "type": "rich_text",
                    "value": source["known_failure_modes"],
                }

            self.records.append(record)

        # Process architecture_decisions
        for adr in data.get("architecture_decisions", []):
            title = adr.get("title", adr.get("adr_title", ""))
            if not title:
                continue

            record = {
                "table": "architecture_decisions",
                "upsert_key": "ADR Title",
                "upsert_value": title,
                "properties": {
                    "ADR Title": {"type": "title", "value": title},
                },
            }

            if adr.get("status"):
                record["properties"]["Status"] = {
                    "type": "select",
                    "value": adr["status"],
                }
            if adr.get("context"):
                record["properties"]["Context"] = {
                    "type": "rich_text",
                    "value": adr["context"],
                }
            if adr.get("decision"):
                record["properties"]["Decision"] = {
                    "type": "rich_text",
                    "value": adr["decision"],
                }
            if adr.get("alternatives") or adr.get("alternatives_considered"):
                record["properties"]["Alternatives Considered"] = {
                    "type": "rich_text",
                    "value": adr.get(
                        "alternatives", adr.get("alternatives_considered", "")
                    ),
                }
            if adr.get("consequences"):
                record["properties"]["Consequences"] = {
                    "type": "rich_text",
                    "value": adr["consequences"],
                }

            self.records.append(record)

        # Process lessons_learned
        for lesson in data.get("lessons_learned", []):
            issue = lesson.get("issue", lesson.get("title", ""))
            if not issue:
                continue

            record = {
                "table": "lessons_learned",
                "upsert_key": "Issue",
                "upsert_value": issue,
                "properties": {
                    "Issue": {"type": "title", "value": issue},
                },
            }

            if lesson.get("category"):
                record["properties"]["Category"] = {
                    "type": "select",
                    "value": lesson["category"],
                }
            if lesson.get("symptom"):
                record["properties"]["Symptom"] = {
                    "type": "rich_text",
                    "value": lesson["symptom"],
                }
            if lesson.get("root_cause"):
                record["properties"]["Root Cause"] = {
                    "type": "rich_text",
                    "value": lesson["root_cause"],
                }
            if lesson.get("prevention_rule"):
                record["properties"]["Prevention Rule"] = {
                    "type": "rich_text",
                    "value": lesson["prevention_rule"],
                }
            if lesson.get("status"):
                record["properties"]["Status"] = {
                    "type": "select",
                    "value": lesson["status"],
                }

            self.records.append(record)

        # Process runbooks
        for runbook in data.get("runbooks", []):
            title = runbook.get("title", runbook.get("runbook_title", ""))
            if not title:
                continue

            record = {
                "table": "runbooks",
                "upsert_key": "Runbook Title",
                "upsert_value": title,
                "properties": {
                    "Runbook Title": {"type": "title", "value": title},
                },
            }

            if runbook.get("category"):
                record["properties"]["Select"] = {
                    "type": "select",
                    "value": runbook["category"],
                }
            if runbook.get("steps"):
                steps_text = runbook["steps"]
                if isinstance(steps_text, list):
                    steps_text = "\n".join(
                        f"{i+1}. {s}" for i, s in enumerate(steps_text)
                    )
                record["properties"]["Steps"] = {
                    "type": "rich_text",
                    "value": steps_text[:2000],  # Notion limit
                }

            self.records.append(record)

        # Process master_timeline / projects
        for project in data.get("projects", data.get("master_timeline", [])):
            name = project.get("name", project.get("initiative", ""))
            if not name:
                continue

            record = {
                "table": "master_timeline",
                "upsert_key": "Initiative",
                "upsert_value": name,
                "properties": {
                    "Initiative": {"type": "title", "value": name},
                },
            }

            if project.get("status"):
                record["properties"]["Status"] = {
                    "type": "select",
                    "value": project["status"],
                }
            if project.get("priority"):
                record["properties"]["Priority"] = {
                    "type": "select",
                    "value": project["priority"],
                }
            if project.get("project"):
                record["properties"]["Project"] = {
                    "type": "select",
                    "value": project["project"],
                }

            self.records.append(record)

        logger.info(
            f"Loaded Notion export: {len(self.records)} total records after merge"
        )

    def scan_collectors_directory(self):
        """Scan the collectors directory for Python files and verify paths."""
        collectors_dir = self.project_root / "src" / "agents" / "collectors"

        if not collectors_dir.exists():
            logger.warning(f"Collectors directory not found: {collectors_dir}")
            return

        existing_names = {r["upsert_value"] for r in self.records}

        for py_file in collectors_dir.rglob("*.py"):
            if py_file.name.startswith("__"):
                continue

            rel_path = py_file.relative_to(self.project_root).as_posix()

            # Check if any existing record references this path
            path_found = False
            for record in self.records:
                current_path = (
                    record.get("properties", {})
                    .get("Current Path", {})
                    .get("value", "")
                )
                if current_path and rel_path in current_path:
                    path_found = True
                    break

            if not path_found:
                # Derive a name from filename
                name = py_file.stem.replace("_", " ").title()
                if name not in existing_names:
                    self.records.append(
                        {
                            "table": "agent_registry",
                            "upsert_key": "Agent Name",
                            "upsert_value": name,
                            "properties": {
                                "Agent Name": {"type": "title", "value": name},
                                "File": {
                                    "type": "rich_text",
                                    "value": rel_path,
                                },
                                "Category": {
                                    "type": "multi_select",
                                    "value": ["Data Collector"],
                                },
                                "Status": {"type": "select", "value": "Live"},
                                "Last Verified": {
                                    "type": "date",
                                    "value": date.today().isoformat(),
                                },
                            },
                        }
                    )
                    existing_names.add(name)

    def add_security_lesson(self):
        """Add a lessons_learned entry for the hardcoded credentials issue."""
        existing_issues = {
            r["upsert_value"]
            for r in self.records
            if r.get("table") == "lessons_learned"
        }

        issue_name = "Hardcoded credentials in Python scripts"
        if issue_name not in existing_issues:
            self.records.append(
                {
                    "table": "lessons_learned",
                    "upsert_key": "Issue",
                    "upsert_value": issue_name,
                    "properties": {
                        "Issue": {"type": "title", "value": issue_name},
                        "Category": {"type": "select", "value": "Security"},
                        "Symptom": {
                            "type": "rich_text",
                            "value": "Database password 'SoupBoss1' was hardcoded in 8 Python files "
                            "instead of using environment variables.",
                        },
                        "Root Cause": {
                            "type": "rich_text",
                            "value": "Scripts were written with hardcoded credentials for quick development "
                            "without adopting the os.environ.get() pattern used in db_config.py.",
                        },
                        "Prevention Rule": {
                            "type": "rich_text",
                            "value": "All database credentials MUST use os.environ.get() with no hardcoded "
                            "fallback defaults. The DB_PASSWORD environment variable should be set via "
                            ".env file which is git-ignored. Never commit actual secrets to any script.",
                        },
                        "Status": {"type": "select", "value": "Fixed"},
                    },
                }
            )

    def include_daily_activity_log(self):
        """Merge any pending daily activity log entries."""
        log_dir = Path(
            os.environ.get(
                "RLC_LOG_DIR", Path.home() / "rlc_scheduler" / "logs"
            )
        )
        today_log = log_dir / f"activity_{date.today().isoformat()}.json"

        if not today_log.exists():
            logger.debug("No daily activity log for today")
            return

        try:
            with open(today_log, "r", encoding="utf-8") as f:
                log_data = json.load(f)

            for entry in log_data.get("entries", []):
                db = entry.get("database", "master_timeline")
                description = entry.get("description", "")
                if not description:
                    continue

                # Map to the correct upsert key based on table
                upsert_keys = {
                    "agent_registry": "Agent Name",
                    "data_sources_registry": "Source Name",
                    "architecture_decisions": "ADR Title",
                    "runbooks": "Runbook Title",
                    "lessons_learned": "Issue",
                    "reconciliation_log": "Issue/Topic",
                    "master_timeline": "Initiative",
                }

                upsert_key = upsert_keys.get(db, "Initiative")

                record = {
                    "table": db,
                    "upsert_key": upsert_key,
                    "upsert_value": description[:100],
                    "properties": {
                        upsert_key: {
                            "type": "title",
                            "value": description[:100],
                        },
                    },
                }

                if entry.get("status"):
                    record["properties"]["Status"] = {
                        "type": "select",
                        "value": entry["status"],
                    }
                if entry.get("priority"):
                    record["properties"]["Priority"] = {
                        "type": "select",
                        "value": entry["priority"],
                    }

                self.records.append(record)

            logger.info(
                f"Merged {len(log_data.get('entries', []))} daily activity entries"
            )

        except Exception as e:
            logger.error(f"Error reading daily activity log: {e}")

    def get_git_diff_summary(self) -> Optional[str]:
        """Get a summary of recent git changes."""
        try:
            result = subprocess.run(
                ["git", "diff", "--stat", "HEAD~1"],
                capture_output=True,
                text=True,
                cwd=str(self.project_root),
                timeout=10,
            )
            if result.returncode == 0 and result.stdout.strip():
                return result.stdout.strip()
        except Exception as e:
            logger.debug(f"Could not get git diff: {e}")
        return None

    def generate_patch(
        self, include_git_diff: bool = False
    ) -> Dict[str, Any]:
        """Generate the complete patch document."""
        # Run all scanners
        self.scan_inventory_csvs()
        self.scan_notion_export()
        self.scan_collectors_directory()
        self.add_security_lesson()
        self.include_daily_activity_log()

        # Deduplicate by (table, upsert_value) - keep last occurrence
        seen = {}
        for record in self.records:
            key = (record["table"], record["upsert_value"])
            if key in seen:
                # Merge properties
                seen[key]["properties"].update(record["properties"])
            else:
                seen[key] = record

        deduped = list(seen.values())

        patch = {
            "generated_at": datetime.now().isoformat(),
            "project_root": str(self.project_root),
            "summary": {
                "total_records": len(deduped),
                "by_table": {},
            },
            "records": deduped,
        }

        # Count by table
        for record in deduped:
            table = record["table"]
            patch["summary"]["by_table"][table] = (
                patch["summary"]["by_table"].get(table, 0) + 1
            )

        if include_git_diff:
            diff = self.get_git_diff_summary()
            if diff:
                patch["git_diff_summary"] = diff

        return patch


def main():
    parser = argparse.ArgumentParser(
        description="Generate Notion sync patch from repository state"
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output path for patch JSON (default: exports dir)",
    )
    parser.add_argument(
        "--project-root",
        default=None,
        help="Project root directory (default: auto-detect)",
    )
    parser.add_argument(
        "--include-git-diff",
        action="store_true",
        help="Include git diff summary in patch",
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

    project_root = Path(args.project_root) if args.project_root else None
    generator = NotionFactsGenerator(project_root=project_root)
    patch = generator.generate_patch(include_git_diff=args.include_git_diff)

    # Determine output path
    if args.output:
        output_path = Path(args.output)
    else:
        export_dir = Path(
            os.environ.get(
                "RLC_EXPORT_DIR",
                Path.home() / "rlc_scheduler" / "exports",
            )
        )
        export_dir.mkdir(parents=True, exist_ok=True)
        output_path = export_dir / f"notion_patch_{date.today().isoformat()}.json"

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(patch, f, indent=2, default=str)

    print(f"\nPatch generated: {output_path}")
    print(f"  Total records: {patch['summary']['total_records']}")
    for table, count in patch["summary"]["by_table"].items():
        print(f"    {table}: {count}")


if __name__ == "__main__":
    main()
