"""
Dispatcher CLI

Command-line interface for the RLC-Agent Dispatcher.

Usage:
    python -m src.dispatcher start          # Start the daemon
    python -m src.dispatcher run <name>     # Run a single collector
    python -m src.dispatcher today          # Run all scheduled for today
    python -m src.dispatcher status         # Show data freshness
    python -m src.dispatcher list           # List registered collectors
    python -m src.dispatcher schedule       # Show weekly schedule
"""

import argparse
import logging
import sys
import signal
import time
from pathlib import Path

# Ensure project root is on path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


def setup_logging(level: str = "INFO"):
    logging.basicConfig(
        level=getattr(logging, level),
        format='%(asctime)s [%(name)s] %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )


def cmd_start(args):
    """Start the dispatcher daemon."""
    from src.dispatcher.dispatcher import Dispatcher

    dispatcher = Dispatcher()
    dispatcher.start()

    print(f"\nDispatcher running. Press Ctrl+C to stop.\n")
    print(dispatcher.get_schedule_summary())

    # Handle graceful shutdown
    def shutdown(sig, frame):
        print("\nShutting down...")
        dispatcher.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    # Keep alive
    try:
        while True:
            time.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        dispatcher.stop()


def cmd_run(args):
    """Run a single collector."""
    from src.dispatcher.collector_runner import CollectorRunner
    from src.dispatcher.collector_registry import CollectorRegistry

    registry = CollectorRegistry()
    runner = CollectorRunner(registry)

    if not registry.is_registered(args.collector):
        print(f"Error: '{args.collector}' is not a registered collector")
        print(f"\nAvailable collectors:")
        for c in registry.list_collectors():
            print(f"  {c['name']}")
        sys.exit(1)

    print(f"Running {args.collector}...")
    result = runner.run_collector(
        args.collector,
        triggered_by='manual',
    )

    if result.success:
        print(f"  Status:  {result.status}")
        print(f"  Rows:    {result.rows_collected}")
        print(f"  Period:  {result.data_period}")
        elapsed = (result.finished_at - result.started_at).total_seconds()
        print(f"  Time:    {elapsed:.1f}s")
    else:
        print(f"  Status:  FAILED")
        print(f"  Error:   {result.error_message}")

    if result.warnings:
        print(f"  Warnings:")
        for w in result.warnings:
            print(f"    - {w}")


def cmd_today(args):
    """Show/run today's scheduled collectors."""
    from src.schedulers.master_scheduler import ReportScheduler
    from datetime import date

    scheduler = ReportScheduler()
    collections = scheduler.get_todays_collections()

    print(f"\n=== Collections Scheduled for Today ({date.today().strftime('%A, %B %d')}) ===\n")

    if not collections:
        print("No scheduled collections today.")
        return

    for c in collections:
        time_str = (c.release_schedule.release_time.strftime('%H:%M')
                    if c.release_schedule.release_time else 'N/A')
        print(f"  [{c.priority}] {c.collector_name}")
        print(f"      Time: {time_str} ET")
        print(f"      {c.release_schedule.description}")
        print(f"      Commodities: {', '.join(c.commodities)}")
        print()

    if args.execute:
        print("Executing today's collectors...\n")
        from src.dispatcher.dispatcher import Dispatcher
        dispatcher = Dispatcher()
        results = dispatcher.run_todays_collectors()
        for r in results:
            status_icon = '[OK]' if r.success else '[FAIL]'
            print(f"  {status_icon} {r.collector_name}: {r.status} ({r.rows_collected} rows)")


def cmd_status(args):
    """Show data freshness from the database."""
    from src.services.database.db_config import get_connection

    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT collector_name, last_collected, last_status,
                       last_row_count, data_period, is_new_data,
                       hours_since_collection, expected_frequency, is_overdue
                FROM core.data_freshness
                ORDER BY hours_since_collection DESC NULLS LAST
            """)
            rows = cursor.fetchall()

            if not rows:
                print("No collection data yet. Run some collectors first.")
                return

            print(f"\n{'Collector':<30} {'Last Run':<20} {'Status':<10} {'Rows':<8} {'Age (hrs)':<10} {'Overdue'}")
            print("-" * 100)

            for row in rows:
                name = row['collector_name'] or ''
                last = str(row['last_collected'])[:16] if row['last_collected'] else 'never'
                status = row['last_status'] or ''
                row_count = row['last_row_count'] or 0
                hours = f"{row['hours_since_collection']:.1f}" if row['hours_since_collection'] else '-'
                overdue = '! YES' if row['is_overdue'] else ''

                print(f"  {name:<28} {last:<20} {status:<10} {row_count:<8} {hours:<10} {overdue}")

    except Exception as e:
        print(f"Error querying data_freshness: {e}")
        sys.exit(1)


def cmd_list(args):
    """List all registered collectors."""
    from src.dispatcher.collector_registry import CollectorRegistry
    from src.schedulers.master_scheduler import RELEASE_SCHEDULES

    registry = CollectorRegistry()
    collectors = registry.list_collectors()

    print(f"\n{'Name':<35} {'Class':<30} {'Scheduled':<10}")
    print("-" * 75)

    for c in collectors:
        scheduled = 'Y' if c['name'] in RELEASE_SCHEDULES else ''
        enabled = RELEASE_SCHEDULES[c['name']].enabled if c['name'] in RELEASE_SCHEDULES else False
        status = f"{'Y' if enabled else 'N'}" if scheduled else '-'
        print(f"  {c['name']:<33} {c['class']:<30} {status}")


def cmd_schedule(args):
    """Show the full weekly schedule."""
    from src.dispatcher.dispatcher import Dispatcher
    dispatcher = Dispatcher()
    print(dispatcher.get_schedule_summary())


def main():
    parser = argparse.ArgumentParser(
        description='RLC-Agent Dispatcher — Automated Data Collection',
        prog='python -m src.dispatcher'
    )
    parser.add_argument(
        '-v', '--verbose', action='store_true',
        help='Enable debug logging'
    )

    subparsers = parser.add_subparsers(dest='command', help='Command')

    # start
    subparsers.add_parser('start', help='Start the dispatcher daemon')

    # run
    run_parser = subparsers.add_parser('run', help='Run a single collector')
    run_parser.add_argument('collector', help='Collector name (e.g., cftc_cot)')

    # today
    today_parser = subparsers.add_parser('today', help="Show today's schedule")
    today_parser.add_argument('--execute', '-x', action='store_true',
                              help='Execute all scheduled collectors')

    # status
    subparsers.add_parser('status', help='Show data freshness')

    # list
    subparsers.add_parser('list', help='List registered collectors')

    # schedule
    subparsers.add_parser('schedule', help='Show weekly schedule')

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    setup_logging('DEBUG' if args.verbose else 'INFO')

    commands = {
        'start': cmd_start,
        'run': cmd_run,
        'today': cmd_today,
        'status': cmd_status,
        'list': cmd_list,
        'schedule': cmd_schedule,
    }

    commands[args.command](args)


if __name__ == '__main__':
    main()
