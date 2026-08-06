"""The Feedstock Report — CLI.

Verbs (handoff spec 2026-08-06):
    python -m src.reports.feedstock_report.cli issue create --n 0 --date 2026-08-08 --free
    python -m src.reports.feedstock_report.cli snapshot prices  --issue 0
    python -m src.reports.feedstock_report.cli snapshot credits --issue 0
    python -m src.reports.feedstock_report.cli snapshot ifv     --issue 0
    python -m src.reports.feedstock_report.cli snapshot manual  --issue 0 --file m.csv [--kind prices|credits]
    python -m src.reports.feedstock_report.cli news add    --issue 0 --headline "..." [--url ...] [--source ...] [--note ...] [--rank N]
    python -m src.reports.feedstock_report.cli section set --issue 0 --code signal --file signal.md
    python -m src.reports.feedstock_report.cli render      --issue 0
    python -m src.reports.feedstock_report.cli lock        --issue 0
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from src.reports.feedstock_report import snapshot as snap


def main(argv=None):
    logging.basicConfig(level=logging.INFO, format='%(levelname)s %(message)s')
    p = argparse.ArgumentParser(prog='report', description='The Feedstock Report pipeline')
    sub = p.add_subparsers(dest='cmd', required=True)

    p_issue = sub.add_parser('issue')
    issue_sub = p_issue.add_subparsers(dest='sub', required=True)
    pc = issue_sub.add_parser('create')
    pc.add_argument('--n', type=int, required=True)
    pc.add_argument('--date', required=True, help='issue date YYYY-MM-DD')
    pc.add_argument('--free', action='store_true', help='free_mode=TRUE (IFVS-008 gates)')
    pc.add_argument('--title')

    p_snap = sub.add_parser('snapshot')
    snap_sub = p_snap.add_subparsers(dest='sub', required=True)
    for name in ('prices', 'credits', 'ifv'):
        sp = snap_sub.add_parser(name)
        sp.add_argument('--issue', type=int, required=True)
    sm = snap_sub.add_parser('manual')
    sm.add_argument('--issue', type=int, required=True)
    sm.add_argument('--file', required=True)
    sm.add_argument('--kind', choices=['prices', 'credits'], default='prices')

    p_news = sub.add_parser('news')
    news_sub = p_news.add_subparsers(dest='sub', required=True)
    na = news_sub.add_parser('add')
    na.add_argument('--issue', type=int, required=True)
    na.add_argument('--headline', required=True)
    na.add_argument('--url', default='')
    na.add_argument('--source', default='')
    na.add_argument('--note', default='')
    na.add_argument('--rank', type=int)

    p_sec = sub.add_parser('section')
    sec_sub = p_sec.add_subparsers(dest='sub', required=True)
    ss = sec_sub.add_parser('set')
    ss.add_argument('--issue', type=int, required=True)
    ss.add_argument('--code', required=True)
    ss.add_argument('--file', required=True, help='markdown file with the section body')
    ss.add_argument('--author', default='tore')

    pr = sub.add_parser('render')
    pr.add_argument('--issue', type=int, required=True)
    pr.add_argument('--test-ifv-numeric-injection', action='store_true',
                    help='TEST ONLY: inject a numeric IFV value to prove the '
                         'free-mode gate hard-fails')

    pl = sub.add_parser('lock')
    pl.add_argument('--issue', type=int, required=True)

    args = p.parse_args(argv)

    if args.cmd == 'issue' and args.sub == 'create':
        snap.create_issue(args.n, date.fromisoformat(args.date),
                          free_mode=args.free, title=args.title)
    elif args.cmd == 'snapshot' and args.sub == 'prices':
        n = snap.snapshot_prices(args.issue)
        print(f"snapshotted {n} price rows")
    elif args.cmd == 'snapshot' and args.sub == 'credits':
        n = snap.snapshot_credits(args.issue)
        print(f"snapshotted {n} credit instrument rows")
    elif args.cmd == 'snapshot' and args.sub == 'ifv':
        entries = snap.build_ifv_leaderboard(args.issue)
        print(f"IFV leaderboard: {len(entries)} entries ranked")
    elif args.cmd == 'snapshot' and args.sub == 'manual':
        n = snap.load_manual_csv(args.issue, args.file, kind=args.kind)
        print(f"loaded {n} manual {args.kind} rows (database only — render "
              f"pulls them through the same gates)")
    elif args.cmd == 'news' and args.sub == 'add':
        snap.add_news(args.issue, args.headline, args.url, args.source,
                      args.note, args.rank)
        print("news item added")
    elif args.cmd == 'section' and args.sub == 'set':
        body = Path(args.file).read_text(encoding='utf-8')
        snap.set_section(args.issue, args.code, body, author=args.author)
        print(f"section {args.code} set ({len(body.split())} words)")
    elif args.cmd == 'render':
        from src.reports.feedstock_report.render import render_issue
        from src.reports.feedstock_report.gates import GateError
        try:
            paths = render_issue(args.issue,
                                 inject_ifv_numeric_test=args.test_ifv_numeric_injection)
        except GateError as e:
            print(str(e), file=sys.stderr)
            print("RENDER ABORTED — no output written.", file=sys.stderr)
            sys.exit(2)
        snap.set_issue_status(args.issue, 'draft',
                              html_path=paths['html'], linkedin_kit_path=paths['kit'])
        print(f"rendered: {paths['html']}\nkit:      {paths['kit']}")
    elif args.cmd == 'lock':
        snap.set_issue_status(args.issue, 'locked')
        print(f"issue {args.issue} locked")


if __name__ == '__main__':
    main()
