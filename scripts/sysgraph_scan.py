"""Run a system-graph scan.

    python scripts/sysgraph_scan.py                 # all steps
    python scripts/sysgraph_scan.py --steps 2,3     # catalog + repo inventory only
    python scripts/sysgraph_scan.py --no-strict     # report binding-check failures, do not raise

Design: docs/specs/system_knowledge_graph_design_v1.md
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(ROOT / ".env")

from src.sysgraph.scan import run  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--steps", default="all", help="'all' or a comma list, e.g. 2,3,6")
    ap.add_argument("--no-strict", action="store_true",
                    help="record binding-check failures instead of raising")
    ap.add_argument("--json", action="store_true", help="dump full stats as JSON")
    args = ap.parse_args()

    stats = run(steps=args.steps, strict=not args.no_strict)
    if args.json:
        print(json.dumps(stats, indent=2, default=str))


if __name__ == "__main__":
    main()
