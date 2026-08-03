"""Fail nonzero when CLAUDE.md disagrees with the live database.

Two checks (see generate_claude_md_db_inventory.py for the design rationale):

  1. PHANTOM OBJECTS — every schema-qualified name in CLAUDE.md (bronze.x, gold.y, ...) must
     exist in the database as a table, view, matview, or function. This is the check that would
     have caught gold.cftc_corn_positioning & friends being documented but never created.
  2. STALE REGION — the generated inventory region must match a fresh regeneration
     (date line ignored). Fix by running the generator.

Exit codes: 0 clean · 1 drift found · 2 could not check (DB unreachable — reported, NOT drift).

Wiring:
  * Daily dispatcher job (`claude_md_drift_check`, class ClaudeMdDriftCheck below): on drift it
    writes a `system_alert` CNS event via core.log_event(), so it surfaces in get_briefing()
    at session start per the Session Protocol.
  * Pre-commit hook (scripts/hooks/pre-commit, installed to .git/hooks/): blocks a commit that
    touches CLAUDE.md if the staged content names phantom objects or carries a stale region.
    On DB-unreachable the hook warns and lets the commit through (exit 2 → soft-pass): a network
    hiccup must not block unrelated work; the daily job still catches real drift.

Usage:
  python scripts/check_claude_md_db_drift.py            # check working-tree CLAUDE.md
  python scripts/check_claude_md_db_drift.py --stdin    # check content piped on stdin (hook path)
"""

from __future__ import annotations

import json
import re
import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.generate_claude_md_db_inventory import (  # noqa: E402
    BEGIN_MARK, END_MARK, CLAUDE_MD, DATE_LINE_RE, SCHEMAS, build_region,
)

OBJECT_RE = re.compile(r"\b(" + "|".join(SCHEMAS) + r")\.([a-zA-Z_][a-zA-Z0-9_]*)\b")


def named_objects(text: str) -> set[str]:
    return {f"{m.group(1)}.{m.group(2)}".lower() for m in OBJECT_RE.finditer(text)}


def existing_objects(conn, names: set[str]) -> set[str]:
    """Subset of names that exist as a relation (table/view/matview/foreign/partitioned) or function."""
    if not names:
        return set()
    cur = conn.cursor()
    cur.execute(
        """SELECT n.nspname || '.' || c.relname AS obj
           FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
           WHERE c.relkind IN ('r','v','m','f','p') AND n.nspname || '.' || c.relname = ANY(%s)
           UNION
           SELECT n.nspname || '.' || p.proname
           FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
           WHERE n.nspname || '.' || p.proname = ANY(%s)""",
        (list(names), list(names)),
    )
    rows = cur.fetchall()
    return {(r["obj"] if isinstance(r, dict) else r[0]) for r in rows}


def _strip_date_lines(region: str) -> str:
    return "\n".join(l for l in region.splitlines() if not DATE_LINE_RE.match(l))


def check(text: str, conn) -> list[str]:
    """Returns a list of human-readable drift findings (empty = clean)."""
    findings = []

    names = named_objects(text)
    missing = sorted(names - existing_objects(conn, names))
    for obj in missing:
        findings.append(f"phantom object: `{obj}` is named in CLAUDE.md but does not exist in the database")

    if BEGIN_MARK not in text or END_MARK not in text:
        findings.append("generated inventory region markers missing from CLAUDE.md")
    else:
        current = re.search(re.escape(BEGIN_MARK) + r".*?" + re.escape(END_MARK), text, re.DOTALL)
        fresh = build_region(conn)
        if _strip_date_lines(current.group(0)) != _strip_date_lines(fresh):
            findings.append(
                "generated inventory region is STALE — run "
                "`python scripts/generate_claude_md_db_inventory.py`"
            )
    return findings


def run_check(text: str) -> tuple[int, list[str]]:
    from src.services.database.db_config import get_connection
    try:
        with get_connection() as conn:
            findings = check(text, conn)
    except Exception as e:
        print(f"WARNING: could not check CLAUDE.md drift (DB unreachable?): {e}", file=sys.stderr)
        return 2, []
    for f in findings:
        print(f"DRIFT: {f}")
    if not findings:
        print("CLAUDE.md is consistent with the database")
    return (1 if findings else 0), findings


class ClaudeMdDriftCheck:
    """Dispatcher job wrapper: daily drift check that raises a CNS briefing event on drift."""

    COLLECTOR_NAME = "claude_md_drift_check"

    def collect(self, triggered_by: str = "manual") -> dict:
        started = datetime.now()
        text = CLAUDE_MD.read_text(encoding="utf-8")
        code, findings = run_check(text)
        try:
            from src.services.database.db_config import get_connection
            with get_connection() as conn:
                cur = conn.cursor()
                if findings:
                    cur.execute(
                        "SELECT core.log_event(%s, %s, %s, %s, %s)",
                        ("system_alert", self.COLLECTOR_NAME,
                         f"CLAUDE.md drift: {len(findings)} finding(s) — "
                         "docs name objects the DB doesn't have, or the inventory region is stale",
                         json.dumps({"findings": findings}), 2),
                    )
                cur.execute(
                    """INSERT INTO core.collection_status
                       (collector_name, run_started_at, run_finished_at, status, rows_collected,
                        rows_inserted, error_message, is_new_data, triggered_by)
                       VALUES (%s,%s,now(),%s,%s,%s,%s,%s,%s)""",
                    (self.COLLECTOR_NAME, started,
                     "SUCCESS" if code in (0, 1) else "FAILED",
                     len(findings), 0,
                     None if code in (0, 1) else "DB unreachable during drift check",
                     bool(findings), triggered_by),
                )
                conn.commit()
        except Exception as e:  # pragma: no cover
            print(f"WARNING: could not log drift-check result: {e}", file=sys.stderr)
        return {"success": code != 2, "drift_findings": len(findings), "findings": findings}


def main() -> int:
    if "--stdin" in sys.argv:
        # Read bytes and decode UTF-8 explicitly: on Windows sys.stdin defaults to cp1252,
        # which mangles the em-dash in the region markers and falsely reports them missing.
        text = sys.stdin.buffer.read().decode("utf-8", errors="replace")
    else:
        text = CLAUDE_MD.read_text(encoding="utf-8")
    code, _ = run_check(text)
    return code


if __name__ == "__main__":
    raise SystemExit(main())
