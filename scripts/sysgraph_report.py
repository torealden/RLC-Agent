"""Read findings out of the system graph. No extraction here -- run sysgraph_scan.py first.

    python scripts/sysgraph_report.py                 # all sections
    python scripts/sysgraph_report.py vba links       # named sections only
    python scripts/sysgraph_report.py trace --key "attribute=oil_stocks"

Sections
    vba      git .bas vs the modules actually embedded in workbooks
    links    external workbook links whose stored target does not exist on disk
    orphans  R1 candidate list -- nodes with no inbound edge. Candidates, never conclusions.
    unres    code references that do not resolve against the live catalog
    trace    blast radius for a series (default: the Q1 oil_stocks defect)
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(ROOT / ".env")

from src.services.database.db_config import get_connection  # noqa: E402
from src.sysgraph.trace import trace_series  # noqa: E402


def hdr(t):
    print(f"\n{'=' * 78}\n{t}\n{'=' * 78}")


def sec_vba(cur):
    hdr("VBA -- git .bas source vs the module that actually runs")
    cur.execute("""
        SELECT source_key, target_key,
               (properties->>'identical')::boolean AS identical,
               properties->>'git_lines' AS git_lines,
               properties->>'workbook_lines' AS wb_lines
          FROM sys.v_edge_named WHERE edge_type = 'DEPLOYED_AS'
         ORDER BY identical, source_key, target_key
    """)
    rows = cur.fetchall()
    same = [r for r in rows if r["identical"]]
    drift = [r for r in rows if not r["identical"]]
    print(f"{len(rows)} deployments of a tracked .bas: {len(same)} identical, {len(drift)} drifted\n")
    for r in drift:
        mod = r["source_key"].split("/")[-1]
        print(f"  DRIFT {mod:32s} git={r['git_lines']:>5} lines  wb={r['wb_lines']:>5}  "
              f"{r['target_key'].replace('wb:', '')}")

    cur.execute("""
        SELECT n.node_key FROM sys.v_node n
         WHERE n.node_type = 'repo_file' AND n.properties->>'vba_source' = 'true'
           AND NOT EXISTS (SELECT 1 FROM sys.v_edge e
                            WHERE e.source_node_id = n.node_id AND e.edge_type = 'DEPLOYED_AS')
         ORDER BY 1
    """)
    orphans = [r["node_key"].split("/")[-1] for r in cur.fetchall()]
    print(f"\n  {len(orphans)} tracked .bas modules embedded in NO workbook in the repo:")
    for m in orphans:
        print(f"     {m}")
    print("  (not the same as dead -- a module can be pasted into a workbook that lives "
          "outside\n   the repo, or run from the VBE. This is a candidate list.)")

    cur.execute("""
        SELECT node_key, properties->>'module' AS m FROM sys.v_node
         WHERE node_type = 'vba_module' AND properties->>'kind' = 'bas'
           AND NOT EXISTS (SELECT 1 FROM sys.v_edge e
                            WHERE e.target_node_id = sys.v_node.node_id AND e.edge_type = 'DEPLOYED_AS')
         ORDER BY 2, 1
    """)
    rows = cur.fetchall()
    print(f"\n  {len(rows)} embedded standard modules with NO tracked source in git:")
    for r in rows:
        print(f"     {r['m']:28s} {r['node_key'].split('#')[0].replace('wb:', '')}")


def sec_links(cur):
    hdr("Workbook external links whose stored target is not on disk")
    cur.execute("""
        SELECT source_key, count(*) AS n, count(DISTINCT target_key) AS linkers
          FROM sys.v_edge_named
         WHERE edge_type = 'LINKS_TO' AND resolution_status = 'unresolved'
           AND source_key NOT LIKE 'wb:EXTERNAL/%'
         GROUP BY 1 ORDER BY n DESC LIMIT 30
    """)
    rows = cur.fetchall()
    for r in rows:
        print(f"  {r['n']:3d} links from {r['linkers']:2d} workbooks -> {r['source_key'].replace('wb:', '')}")
    cur.execute("""
        SELECT count(*) AS n FROM sys.v_edge_named
         WHERE edge_type = 'LINKS_TO' AND source_key LIKE 'wb:EXTERNAL/%'
    """)
    print(f"\n  plus {cur.fetchone()['n']} links to paths outside the repo entirely "
          f"(Dropbox, CONAB network shares, Eurostat).")
    print("  NOT a conclusion: Excel may repair a moved link at open time. This lists what "
          "is\n  STORED in the file, which is not the same as what Excel resolves.")


def sec_orphans(cur):
    hdr("R1 candidate list -- no inbound edge (candidates, never conclusions)")
    cur.execute("""
        SELECT node_type, claim, count(*) AS n FROM sys.v_no_inbound
         GROUP BY 1, 2 ORDER BY n DESC
    """)
    for r in cur.fetchall():
        print(f"  {r['n']:5d}  {r['node_type']:12s}  {r['claim']}")
    cur.execute("""
        SELECT node_key FROM sys.v_no_inbound
         WHERE node_type IN ('repo_file', 'sql_script') AND node_key LIKE 'repo:scripts/%'
         ORDER BY 1 LIMIT 25
    """)
    rows = cur.fetchall()
    print(f"\n  first {len(rows)} in scripts/ (R4: for code, no inbound edge IS evidence):")
    for r in rows:
        print(f"     {r['node_key'][len('repo:'):]}")


def sec_unres(cur):
    hdr("Code references that do not resolve against the live catalog")
    cur.execute("""
        SELECT n.node_key, count(e.edge_id) AS refs
          FROM sys.v_node n JOIN sys.v_edge e
            ON e.source_node_id = n.node_id OR e.target_node_id = n.node_id
         WHERE n.node_type = 'db_relation' AND n.resolution_status = 'unresolved'
         GROUP BY 1 ORDER BY refs DESC LIMIT 25
    """)
    for r in cur.fetchall():
        print(f"  {r['refs']:4d} refs  {r['node_key']}")


def sec_trace(cur, conn, key):
    hdr(f"Blast radius: {key}")
    hops = trace_series(conn, key, "down")
    by_depth = {}
    for h in hops:
        by_depth.setdefault(h["depth"], []).append(h)
    for d in sorted(by_depth):
        print(f"\n  depth {d}  ({len(by_depth[d])} nodes)")
        for h in sorted(by_depth[d], key=lambda x: (x["node_type"], x["node_key"]))[:30]:
            life = "" if h["lifecycle"] == "unknown" else f" [{h['lifecycle']}]"
            print(f"    {h['node_type']:14s} {h['via_edge'] or '':12s} {h['node_key'][:78]}{life}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("sections", nargs="*", default=None)
    ap.add_argument("--key", default="attribute=oil_stocks")
    args = ap.parse_args()
    want = set(args.sections or ["vba", "links", "orphans", "unres", "trace"])

    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT scan_id, finished_at, git_sha FROM sys.scan "
                    "WHERE scan_id = (SELECT scan_id FROM sys.v_current_scan)")
        s = cur.fetchone()
        if not s:
            print("no completed scan -- run scripts/sysgraph_scan.py first")
            return
        print(f"scan {s['scan_id']}  finished {s['finished_at']}  git {(s['git_sha'] or '')[:8]}")

        if "vba" in want:
            sec_vba(cur)
        if "links" in want:
            sec_links(cur)
        if "orphans" in want:
            sec_orphans(cur)
        if "unres" in want:
            sec_unres(cur)
        if "trace" in want:
            sec_trace(cur, conn, args.key)


if __name__ == "__main__":
    main()
