"""Step 3 -- repo inventory: tracked files, SQL scripts, git-side VBA, and the scheduling join.

Answers half of Q3 ("is this code alive?") by supplying the denominator: every tracked Python
and SQL file becomes a node, and the ones a scheduler actually names get an inbound
SCHEDULED_AS edge. Everything else is a candidate for the R1 cleanup list -- a candidate, not
a conclusion.

The registry join is exact for what it covers and silent about the rest: COLLECTOR_MAP knows
about collectors, and says nothing whatsoever about the 248 files in scripts/.
"""

from __future__ import annotations

import json
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

TRACKED_EXT = {".py", ".sql", ".bas"}


def _git_files() -> list[str]:
    out = subprocess.run(
        ["git", "ls-files"], cwd=ROOT, capture_output=True, text=True, timeout=120
    ).stdout
    return [ln for ln in out.splitlines() if ln.strip()]


def extract(conn, store) -> dict:
    files = _git_files()
    counts = {"py": 0, "sql": 0, "bas": 0, "missing_from_worktree": 0}

    for rel in files:
        ext = Path(rel).suffix.lower()
        if ext not in TRACKED_EXT:
            continue
        abspath = ROOT / rel
        present = abspath.exists()
        if not present:
            counts["missing_from_worktree"] += 1

        props = {
            "ext": ext,
            "top_dir": rel.split("/", 1)[0],
            "in_worktree": present,
            "size_bytes": abspath.stat().st_size if present else None,
        }

        if ext == ".sql":
            counts["sql"] += 1
            store.add_node("sql_script", f"repo:{rel}", label=rel, properties=props,
                           extraction_method="regex", confidence=1.00)
        else:
            counts["py" if ext == ".py" else "bas"] += 1
            if ext == ".bas":
                props["vba_source"] = True
            store.add_node("repo_file", f"repo:{rel}", label=rel, properties=props,
                           extraction_method="regex", confidence=1.00)

    counts.update(_scheduled_jobs(store, set(files)))
    return counts


def _scheduled_jobs(store, tracked: set[str]) -> dict:
    """SCHEDULED_AS edges from two independent sources, kept separate so a disagreement shows.

    `collector_registry.COLLECTOR_MAP` gives a dotted module path -- exact, resolves cleanly.
    `config/collection_schedule.json` gives a bare filename -- often ambiguous, and marked so
    rather than guessed at.
    """
    stats = {"jobs_registry": 0, "jobs_schedule": 0, "jobs_unresolved": 0, "jobs_ambiguous": 0}

    by_basename: dict[str, list[str]] = {}
    for rel in tracked:
        by_basename.setdefault(Path(rel).name, []).append(rel)

    # --- source 1: the dispatcher registry -------------------------------
    reg_path = ROOT / "src" / "dispatcher" / "collector_registry.py"
    if reg_path.exists():
        src = reg_path.read_text(encoding="utf-8", errors="replace")
        # Parse the literal rather than importing it -- the module does lazy imports and we do
        # not want extraction to execute collector code.
        for m in re.finditer(
            r"'([a-z0-9_]+)'\s*:\s*\{\s*'module'\s*:\s*'([\w.]+)'\s*,\s*'class'\s*:\s*'(\w+)'",
            src,
        ):
            key, module, cls = m.group(1), m.group(2), m.group(3)
            job_key = f"job:registry/{key}"
            target_rel = module.replace(".", "/") + ".py"
            resolved = target_rel in tracked
            store.add_node(
                "scheduled_job", job_key, label=key,
                properties={"source": "collector_registry", "module": module, "class": cls},
                extraction_method="regex", confidence=1.00,
                resolution_status="resolved" if resolved else "unresolved",
            )
            file_key = f"repo:{target_rel}"
            if not resolved:
                stats["jobs_unresolved"] += 1
                store.add_node("repo_file", file_key, label=target_rel,
                               properties={"ext": ".py", "phantom": True},
                               extraction_method="regex", confidence=0.40,
                               resolution_status="unresolved")
            store.add_edge(
                job_key, "SCHEDULED_AS", file_key,
                extraction_method="regex", confidence=1.00,
                resolution_status="resolved" if resolved else "unresolved",
                evidence={"file": "src/dispatcher/collector_registry.py"},
            )
            stats["jobs_registry"] += 1

    # --- source 2: the schedule JSON -------------------------------------
    sched_path = ROOT / "config" / "collection_schedule.json"
    if sched_path.exists():
        cfg = json.loads(sched_path.read_text(encoding="utf-8", errors="replace"))
        for key, spec in (cfg.get("schedules") or {}).items():
            collector = spec.get("collector")
            job_key = f"job:schedule/{key}"
            store.add_node(
                "scheduled_job", job_key, label=spec.get("name", key),
                properties={
                    "source": "collection_schedule.json",
                    "frequency": spec.get("frequency"),
                    "enabled": spec.get("enabled"),
                    "collector": collector,
                },
                extraction_method="regex", confidence=1.00,
            )
            stats["jobs_schedule"] += 1
            if not collector:
                continue
            matches = by_basename.get(Path(collector).name, [])
            if len(matches) == 1:
                store.add_edge(job_key, "SCHEDULED_AS", f"repo:{matches[0]}",
                               extraction_method="regex", confidence=0.90,
                               evidence={"file": "config/collection_schedule.json",
                                         "declared_as": collector})
            elif len(matches) > 1:
                stats["jobs_ambiguous"] += 1
                for rel in matches:
                    store.add_edge(job_key, "SCHEDULED_AS", f"repo:{rel}",
                                   extraction_method="regex", confidence=0.40,
                                   resolution_status="ambiguous",
                                   evidence={"file": "config/collection_schedule.json",
                                             "declared_as": collector,
                                             "candidates": len(matches)})
            else:
                stats["jobs_unresolved"] += 1
                ghost = f"repo:UNRESOLVED/{collector}"
                store.add_node("repo_file", ghost, label=collector,
                               properties={"phantom": True, "declared_in": "collection_schedule.json"},
                               extraction_method="regex", confidence=0.40,
                               resolution_status="unresolved")
                store.add_edge(job_key, "SCHEDULED_AS", ghost,
                               extraction_method="regex", confidence=0.40,
                               resolution_status="unresolved",
                               evidence={"file": "config/collection_schedule.json",
                                         "declared_as": collector})

    return stats
