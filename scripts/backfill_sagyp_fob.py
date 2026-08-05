"""Resumable SAGyP FOB backfill: weekday loop at <=1 req/sec.

History works back to 1993-01-04 (~8,300 weekday requests, ~2.5h at 1 req/sec).
Run 2020->present first (recent history for tonight's report), then the full
1993 pass in the background:

    python scripts/backfill_sagyp_fob.py --start 2020-01-01
    python scripts/backfill_sagyp_fob.py --start 1993-01-04 --end 2019-12-31

Resumable: progress is checkpointed per completed date in a state file, so a
killed run restarts where it left off (holidays/empty days are checkpointed too,
so they are not re-hit on resume). Bronze/silver writes are idempotent upserts;
silver promotion runs per flushed chunk through the collector's shared persist().
"""
import argparse
import json
import sys
import time
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv; load_dotenv(ROOT / ".env")

import requests

from src.agents.collectors.south_america.sagyp_fob_collector import (
    fetch_day, parse_posts, persist)

STATE_DIR = ROOT / "data" / "backfill_state"
FLUSH_EVERY = 20  # dates per DB flush


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True, help="YYYY-MM-DD")
    ap.add_argument("--end", default=None, help="YYYY-MM-DD, default today")
    ap.add_argument("--rate", type=float, default=1.0, help="seconds between requests (>=1)")
    args = ap.parse_args()

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end) if args.end else date.today()
    rate = max(args.rate, 1.0)  # be polite: never faster than 1 req/sec

    STATE_DIR.mkdir(parents=True, exist_ok=True)
    state_path = STATE_DIR / f"sagyp_fob_{start.isoformat()}_{end.isoformat()}.json"
    state = json.loads(state_path.read_text()) if state_path.exists() else {}
    done_through = state.get("completed_through")
    cur = max(start, date.fromisoformat(done_through) + timedelta(days=1)) if done_through else start
    if cur > end:
        print(f"already complete through {done_through}; nothing to do")
        return

    session = requests.Session()
    pending, total_rows, empty_days, req_count = {}, 0, 0, 0
    t0 = time.time()

    def flush(through: date):
        nonlocal pending, total_rows
        if pending:
            total_rows += persist(pending)
            pending = {}
        state["completed_through"] = through.isoformat()
        state["total_rows"] = total_rows
        state["empty_days"] = empty_days
        state_path.write_text(json.dumps(state))

    d = cur
    while d <= end:
        if d.weekday() < 5:
            t_req = time.time()
            try:
                posts = fetch_day(d, session)
            except RuntimeError as e:
                # flush what we have, then die loudly — resume picks up here
                flush(d - timedelta(days=1))
                print(f"FATAL at {d}: {e}", flush=True)
                sys.exit(1)
            req_count += 1
            if posts:
                pending[d] = parse_posts(d, posts)
            else:
                empty_days += 1
            if len(pending) >= FLUSH_EVERY:
                flush(d)
                el = time.time() - t0
                print(f"{d}  reqs={req_count}  rows={total_rows}  empty={empty_days}  "
                      f"({req_count/el:.2f} req/s)", flush=True)
            sleep_left = rate - (time.time() - t_req)
            if sleep_left > 0:
                time.sleep(sleep_left)
        d += timedelta(days=1)
    flush(end)
    print(f"DONE {start} -> {end}: {req_count} requests, {total_rows} bronze rows, "
          f"{empty_days} empty weekdays (holidays), {(time.time()-t0)/60:.1f} min", flush=True)


if __name__ == "__main__":
    main()
