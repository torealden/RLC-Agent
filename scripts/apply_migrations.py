"""
RLC-Agent migration runner.

Reads `database/migrations/*.sql`, applies any not yet recorded in
`core.schema_migrations`. Each migration runs in its own transaction; on
failure, the runner rolls back and exits non-zero (no partial state).

Subcommands:
  status              Show applied vs behind.
  audit               For each behind migration, regex-extract the
                      artifacts it would create (tables/views/schemas)
                      and probe the DB to guess whether it was already
                      applied manually pre-tracker.
  apply [version]     Apply one specific migration, or all behind in
                      order if no version given.
  mark-applied <v>    Record a migration as applied without running it.
                      For backfilling pre-tracker history.

The tracker table is created idempotently on every run, so the script
is safe to run on a fresh DB or an existing one.
"""

from __future__ import annotations

import argparse
import hashlib
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

from src.services.database.db_config import get_connection

MIGRATIONS_DIR = ROOT / "database" / "migrations"

TRACKER_DDL = """
CREATE SCHEMA IF NOT EXISTS core;

CREATE TABLE IF NOT EXISTS core.schema_migrations (
    version       TEXT PRIMARY KEY,
    name          TEXT NOT NULL,
    sha256        TEXT NOT NULL,
    applied_at    TIMESTAMP NOT NULL DEFAULT NOW(),
    applied_by    TEXT NOT NULL DEFAULT CURRENT_USER,
    backfilled    BOOLEAN NOT NULL DEFAULT FALSE,
    note          TEXT
);

CREATE INDEX IF NOT EXISTS schema_migrations_applied_at_idx
    ON core.schema_migrations (applied_at);
"""


# --- file discovery ------------------------------------------------------------

VERSION_RE = re.compile(r"^(\d{3,4})_(.+)\.sql$")


def discover_migrations() -> list[tuple[str, str, Path]]:
    """Return [(version, name, path)] sorted by version."""
    out = []
    for p in sorted(MIGRATIONS_DIR.glob("*.sql")):
        m = VERSION_RE.match(p.name)
        if not m:
            # non-versioned files (like populate_calculated_columns.sql) are skipped
            continue
        out.append((m.group(1), m.group(2), p))
    return out


def sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    h.update(p.read_bytes())
    return h.hexdigest()


# --- artifact extraction (regex; not perfect, just for audit hints) -----------

CREATE_RE = re.compile(
    r"CREATE\s+(?:OR\s+REPLACE\s+)?(TABLE|VIEW|MATERIALIZED\s+VIEW|SCHEMA|INDEX|TYPE|FUNCTION)"
    r"\s+(?:IF\s+NOT\s+EXISTS\s+)?([a-zA-Z_][\w.]*)",
    re.IGNORECASE,
)


def extract_artifacts(sql: str) -> list[tuple[str, str]]:
    """Return [(kind, fully_qualified_name)] from CREATE statements."""
    return [(m.group(1).upper().replace("MATERIALIZED VIEW", "MAT_VIEW"), m.group(2))
            for m in CREATE_RE.finditer(sql)]


def artifact_exists(conn, kind: str, name: str) -> bool:
    """Probe the DB for an artifact. Returns True if found."""
    if "." in name:
        schema, base = name.split(".", 1)
    else:
        schema, base = "public", name

    sql = None
    if kind in ("TABLE", "VIEW", "MAT_VIEW"):
        sql = (
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema=%s AND table_name=%s"
        )
        args = (schema, base)
    elif kind == "SCHEMA":
        sql = "SELECT 1 FROM information_schema.schemata WHERE schema_name=%s"
        args = (name,)
    elif kind == "INDEX":
        sql = "SELECT 1 FROM pg_indexes WHERE schemaname=%s AND indexname=%s"
        args = (schema, base)
    elif kind == "TYPE":
        sql = (
            "SELECT 1 FROM pg_type t "
            "JOIN pg_namespace n ON t.typnamespace=n.oid "
            "WHERE n.nspname=%s AND t.typname=%s"
        )
        args = (schema, base)
    elif kind == "FUNCTION":
        sql = (
            "SELECT 1 FROM pg_proc p "
            "JOIN pg_namespace n ON p.pronamespace=n.oid "
            "WHERE n.nspname=%s AND p.proname=%s"
        )
        args = (schema, base)
    else:
        return False

    cur = conn.cursor()
    cur.execute(sql, args)
    return cur.fetchone() is not None


# --- tracker operations -------------------------------------------------------

def ensure_tracker(conn):
    cur = conn.cursor()
    cur.execute(TRACKER_DDL)


def applied_versions(conn) -> dict[str, dict]:
    cur = conn.cursor()
    cur.execute(
        "SELECT version, name, sha256, applied_at, backfilled, note "
        "FROM core.schema_migrations"
    )
    return {r["version"]: dict(r) for r in cur.fetchall()}


def record_applied(conn, version: str, name: str, sha: str,
                   backfilled: bool = False, note: str | None = None):
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO core.schema_migrations "
        "(version, name, sha256, backfilled, note) VALUES (%s, %s, %s, %s, %s) "
        "ON CONFLICT (version) DO UPDATE SET "
        "  sha256=EXCLUDED.sha256, "
        "  backfilled=EXCLUDED.backfilled, "
        "  note=EXCLUDED.note, "
        "  applied_at=NOW()",
        (version, name, sha, backfilled, note),
    )


# --- commands -----------------------------------------------------------------

def cmd_status(args):
    migrations = discover_migrations()
    with get_connection() as conn:
        ensure_tracker(conn)
        applied = applied_versions(conn)

    behind, ok, drift = [], [], []
    for v, n, p in migrations:
        if v not in applied:
            behind.append((v, n, p))
        elif applied[v]["sha256"] != sha256_file(p):
            drift.append((v, n, p, applied[v]["sha256"]))
        else:
            ok.append((v, n, p))

    print(f"On disk:    {len(migrations)}")
    print(f"Applied:    {len(ok)}")
    print(f"Drifted:    {len(drift)}  (file changed since apply)")
    print(f"Behind:     {len(behind)}")
    if behind:
        print()
        print("Behind:")
        for v, n, _ in behind:
            print(f"  {v}  {n}")
    if drift:
        print()
        print("DRIFTED (file modified after apply):")
        for v, n, _, applied_sha in drift:
            print(f"  {v}  {n}  applied_sha={applied_sha[:12]}")


def cmd_audit(args):
    migrations = discover_migrations()
    with get_connection() as conn:
        ensure_tracker(conn)
        applied = applied_versions(conn)

        print(f"Auditing {len(migrations)} migrations on disk...")
        print()
        for v, n, p in migrations:
            if v in applied and applied[v]["sha256"] == sha256_file(p):
                continue
            sql = p.read_text(encoding="utf-8", errors="ignore")
            artifacts = extract_artifacts(sql)
            existing = [a for a in artifacts if artifact_exists(conn, *a)]
            missing = [a for a in artifacts if a not in existing]

            status = "BEHIND"
            if v in applied:
                status = "DRIFTED"

            print(f"--- {v}  {n}  [{status}]")
            print(f"  artifacts:        {len(artifacts)}")
            print(f"  already in DB:    {len(existing)}")
            print(f"  not in DB:        {len(missing)}")
            if missing[:3]:
                for k, na in missing[:3]:
                    print(f"    - {k:<10} {na}")
                if len(missing) > 3:
                    print(f"    - ...and {len(missing)-3} more")
            if existing and not missing:
                print(f"  HINT: artifacts all present — likely applied pre-tracker.")
                print(f"        run: python scripts/apply_migrations.py mark-applied {v} "
                      f"--reason \"detected: artifacts present\"")
            elif existing and missing:
                print(f"  HINT: partial state. Inspect manually.")
            else:
                print(f"  HINT: not applied. run: python scripts/apply_migrations.py apply {v}")
            print()


def cmd_apply(args):
    migrations = discover_migrations()
    target = args.version

    with get_connection() as conn:
        ensure_tracker(conn)
        applied = applied_versions(conn)

    to_run = []
    for v, n, p in migrations:
        if target and v != target:
            continue
        if v in applied:
            continue
        to_run.append((v, n, p))

    if not to_run:
        print("Nothing to apply.")
        return

    print(f"Will apply {len(to_run)} migration(s):")
    for v, n, _ in to_run:
        print(f"  {v}  {n}")
    print()
    if not args.yes:
        ans = input("Proceed? [y/N] ").strip().lower()
        if ans != "y":
            print("Aborted.")
            return

    for v, n, p in to_run:
        sql = p.read_text(encoding="utf-8", errors="ignore")
        sha = sha256_file(p)
        print(f"Applying {v} {n} ...")
        try:
            with get_connection() as conn:
                cur = conn.cursor()
                cur.execute(sql)
                ensure_tracker(conn)
                record_applied(conn, v, n, sha, backfilled=False)
            print(f"  OK")
        except Exception as e:
            print(f"  FAILED: {e}")
            print(f"  (rolled back; remaining migrations skipped)")
            sys.exit(1)


def cmd_mark_applied(args):
    target = args.version
    migrations = {v: (n, p) for v, n, p in discover_migrations()}
    if target not in migrations:
        print(f"Migration {target} not found in {MIGRATIONS_DIR}")
        sys.exit(1)
    n, p = migrations[target]
    sha = sha256_file(p)
    with get_connection() as conn:
        ensure_tracker(conn)
        record_applied(conn, target, n, sha, backfilled=True, note=args.reason)
    print(f"Marked {target} {n} as applied (backfilled).")


def cmd_mark_applied_range(args):
    """Bulk-mark a contiguous range applied (for first-time tracker bootstrap)."""
    lo, hi = args.start, args.end
    migrations = discover_migrations()
    targets = [(v, n, p) for v, n, p in migrations if lo <= v <= hi]
    if not targets:
        print(f"No migrations in range {lo}..{hi}")
        return
    print(f"Will mark {len(targets)} migration(s) as applied (backfilled, reason={args.reason!r}):")
    for v, n, _ in targets:
        print(f"  {v}  {n}")
    if not args.yes:
        ans = input("Proceed? [y/N] ").strip().lower()
        if ans != "y":
            print("Aborted.")
            return
    with get_connection() as conn:
        ensure_tracker(conn)
        for v, n, p in targets:
            record_applied(conn, v, n, sha256_file(p), backfilled=True, note=args.reason)
    print(f"Done.")


# --- entrypoint ---------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("status", help="Show applied vs behind").set_defaults(func=cmd_status)
    sub.add_parser("audit", help="Probe each behind migration's artifacts").set_defaults(func=cmd_audit)

    sp = sub.add_parser("apply", help="Apply one or all behind migrations")
    sp.add_argument("version", nargs="?", help="version to apply (or omit for all)")
    sp.add_argument("--yes", "-y", action="store_true", help="skip confirmation")
    sp.set_defaults(func=cmd_apply)

    sp = sub.add_parser("mark-applied", help="Record a migration as applied without running")
    sp.add_argument("version")
    sp.add_argument("--reason", default="manual mark-applied", help="note for tracker")
    sp.set_defaults(func=cmd_mark_applied)

    sp = sub.add_parser("mark-applied-range", help="Bulk-mark a contiguous range as applied")
    sp.add_argument("start", help="start version (inclusive)")
    sp.add_argument("end", help="end version (inclusive)")
    sp.add_argument("--reason", default="bootstrap backfill", help="note for tracker")
    sp.add_argument("--yes", "-y", action="store_true", help="skip confirmation")
    sp.set_defaults(func=cmd_mark_applied_range)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
