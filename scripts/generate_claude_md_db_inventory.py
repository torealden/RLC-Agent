"""Rewrite the generated DB-inventory region of CLAUDE.md from information_schema.

CLAUDE.md's database counts drifted for months because they were prose (89/93/180 hand-typed
2026-05-19, wrong by August). A prose promise can't prevent drift; this pair of scripts is the
hard lock:

  * generate_claude_md_db_inventory.py (this script) — computes the live inventory and rewrites
    the marked region between BEGIN/END markers. Run it whenever the DB changes shape (or when
    check_claude_md_db_drift.py tells you to).
  * check_claude_md_db_drift.py — fails nonzero when the region is stale OR when ANY
    schema-qualified object named anywhere in CLAUDE.md does not exist in the database.
    Wired as a daily dispatcher job (CNS event on drift) and a pre-commit hook.

The generated region is COUNTS ONLY. The hand-curated table/view descriptions elsewhere in
CLAUDE.md stay hand-curated (a generator can't write analyst-grade descriptions) — but every
object name they mention is existence-checked by the drift checker, so curation can't rot
silently anymore.

Usage:  python scripts/generate_claude_md_db_inventory.py [--dry-run]
"""

from __future__ import annotations

import re
import sys
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

CLAUDE_MD = PROJECT_ROOT / "CLAUDE.md"

BEGIN_MARK = "<!-- BEGIN GENERATED: DB INVENTORY (scripts/generate_claude_md_db_inventory.py — do not hand-edit) -->"
END_MARK = "<!-- END GENERATED: DB INVENTORY -->"

# Schemas the inventory covers; also the schema whitelist the drift checker greps CLAUDE.md for.
SCHEMAS = ["bronze", "silver", "gold", "core", "reference", "risk", "sys"]

# Lines the drift checker ignores when comparing the region to a fresh regeneration
# (pure metadata; changes daily without the inventory itself changing).
DATE_LINE_RE = re.compile(r"^\*Generated .*\*$")


def compute_inventory_lines(conn) -> list[str]:
    """The region body, one markdown line per fact, deterministic given DB state."""
    cur = conn.cursor()
    cur.execute(
        """SELECT table_schema AS s,
                  SUM(CASE WHEN table_type = 'BASE TABLE' THEN 1 ELSE 0 END) AS tables,
                  SUM(CASE WHEN table_type = 'VIEW' THEN 1 ELSE 0 END) AS views
           FROM information_schema.tables
           WHERE table_schema = ANY(%s)
           GROUP BY table_schema""",
        (SCHEMAS,),
    )
    rows = cur.fetchall()
    counts = {(r["s"] if isinstance(r, dict) else r[0]):
              ((r["tables"], r["views"]) if isinstance(r, dict) else (r[1], r[2]))
              for r in rows}
    cur.execute("SELECT COUNT(DISTINCT collector_name) AS n FROM core.collection_status")
    r = cur.fetchone()
    n_collectors = r["n"] if isinstance(r, dict) else r[0]

    lines = []
    for schema in SCHEMAS:
        t, v = counts.get(schema, (0, 0))
        parts = []
        if t:
            parts.append(f"{t} tables")
        if v:
            parts.append(f"{v} views")
        lines.append(f"- **{schema}**: {' + '.join(parts) if parts else 'empty'}")
    lines.append(f"- **{n_collectors} distinct collectors** seen in `core.collection_status`")
    lines.append(
        "- The hand-curated bronze/silver/gold objects documented in detail elsewhere in this "
        "file are the **commodities-focused subset**; the live database spans facilities, "
        "permits, trade, fuel, refining, and adjacent industries. Every object name in this "
        "file is existence-checked daily by `scripts/check_claude_md_db_drift.py`."
    )
    return lines


def build_region(conn) -> str:
    body = "\n".join(compute_inventory_lines(conn))
    return (
        f"{BEGIN_MARK}\n"
        f"*Generated {date.today().isoformat()} — do not edit by hand; run "
        f"`python scripts/generate_claude_md_db_inventory.py` to refresh.*\n\n"
        f"{body}\n"
        f"{END_MARK}"
    )


def replace_region(text: str, region: str) -> str:
    if BEGIN_MARK in text and END_MARK in text:
        pattern = re.compile(re.escape(BEGIN_MARK) + r".*?" + re.escape(END_MARK), re.DOTALL)
        return pattern.sub(lambda _: region, text)
    raise SystemExit(
        f"CLAUDE.md has no generated region markers.\n"
        f"Insert these two lines where the inventory belongs, then rerun:\n{BEGIN_MARK}\n{END_MARK}"
    )


def main() -> int:
    dry = "--dry-run" in sys.argv
    from src.services.database.db_config import get_connection
    with get_connection() as conn:
        region = build_region(conn)
    text = CLAUDE_MD.read_text(encoding="utf-8")
    new_text = replace_region(text, region)
    if dry:
        print(region)
        print("\n--dry-run: CLAUDE.md not written")
        return 0
    if new_text == text:
        print("CLAUDE.md inventory already current")
        return 0
    CLAUDE_MD.write_text(new_text, encoding="utf-8")
    print("CLAUDE.md inventory region rewritten")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
