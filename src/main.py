#!/usr/bin/env python3
"""
RLC Agent System - Main Entry Point

Unified command-line interface for the multi-agent commodity data system.

Usage:
    python -m src.main status           # Show system status
    python -m src.main collect          # Collect data from all sources
    python -m src.main collect usda     # Collect USDA data
    python -m src.main daily            # Run daily workflow
    python -m src.main schedule         # Show schedule
    python -m src.main scheduler start  # Start the scheduler
    python -m src.main db status        # Database status
    python -m src.main db query         # Query database

Round Lakes Commodities
"""

import asyncio
import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

# Add src to path if running directly
if __name__ == "__main__":
    src_path = Path(__file__).parent.parent
    if str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))

from src.orchestrators.analyst_agent import AnalystAgent, create_analyst_agent
from src.core.config import get_settings
from src.core.events import get_event_bus, EventType


def setup_logging(log_level: str = 'INFO', log_file: str = None):
    """Configure logging for the application"""
    handlers = [logging.StreamHandler(sys.stdout)]

    if log_file:
        Path(log_file).parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_file))

    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=handlers
    )

    return logging.getLogger('rlc_agent')


def print_header():
    """Print the application header"""
    print("\n" + "=" * 60)
    print("RLC AGENT SYSTEM")
    print("Round Lakes Commodities - Commodity Market Data Automation")
    print("=" * 60 + "\n")


def print_json(data, indent=2):
    """Pretty print JSON data"""
    print(json.dumps(data, indent=indent, default=str))


async def cmd_status(analyst: AnalystAgent, args):
    """Show system status"""
    print("Fetching system status...\n")

    status = await analyst.get_system_status()

    print("SYSTEM STATUS")
    print("-" * 40)
    print(f"Agent: {status.get('agent')}")
    print(f"Status: {status.get('status')}")
    print(f"Timestamp: {status.get('timestamp')}")

    # Database
    db = status.get('database', {})
    print(f"\nDATABASE")
    print(f"  Total Records: {db.get('total_records', 'N/A'):,}" if isinstance(db.get('total_records'), int) else f"  Error: {db.get('error', 'Unknown')}")
    if 'unique_commodities' in db:
        print(f"  Commodities: {db.get('unique_commodities')}")
        print(f"  Sources: {db.get('unique_sources')}")

    # Scheduler
    scheduler = status.get('scheduler', {})
    print(f"\nSCHEDULER")
    print(f"  Running: {scheduler.get('running', False)}")
    print(f"  Tasks: {scheduler.get('task_count', 0)}")

    # Next tasks
    next_tasks = status.get('next_tasks', [])
    if next_tasks:
        print(f"\nNEXT SCHEDULED TASKS")
        for task in next_tasks[:5]:
            print(f"  - {task.get('name')} at {task.get('next_run')}")

    # Tools
    tools = status.get('tools', {})
    print(f"\nTOOLS")
    print(f"  Registered: {tools.get('tool_count', 0)}")
    print(f"  Executions: {tools.get('total_executions', 0)}")


async def cmd_collect(analyst: AnalystAgent, args):
    """Collect data from sources"""
    source = args.source or 'usda_ams'

    print(f"Collecting data from: {source}")
    if args.date:
        print(f"Date: {args.date}")

    params = {}
    if args.date:
        params['report_date'] = args.date

    result = await analyst.collect_data(source, params)

    print("\nCOLLECTION RESULT")
    print("-" * 40)
    print(f"Success: {result.success}")
    if result.data:
        print(f"Records: {result.data.get('count', 0)}")
        print(f"Inserted: {result.data.get('inserted', 0)}")
        print(f"Skipped: {result.data.get('skipped', 0)}")
    if result.error:
        print(f"Error: {result.error}")


async def cmd_daily(analyst: AnalystAgent, args):
    """Run daily workflow"""
    print("Running daily workflow...\n")

    result = await analyst.run_daily_workflow()

    print("DAILY WORKFLOW RESULT")
    print("-" * 40)
    print(f"Success: {result.success}")
    print(f"Duration: {result.duration_seconds:.2f} seconds")
    print(f"\nStages Completed: {', '.join(result.stages_completed)}")

    if result.stages_failed:
        print(f"Stages Failed: {', '.join(result.stages_failed)}")

    if result.data:
        print(f"\nRecords Collected: {result.data.get('total_records_collected', 0)}")

        collection = result.data.get('collection_results', {})
        if collection:
            print("\nBy Source:")
            for source, info in collection.items():
                status = "OK" if info.get('success') else "FAILED"
                print(f"  {source}: {status} ({info.get('record_count', 0)} records)")

    if result.errors:
        print("\nErrors:")
        for error in result.errors[:5]:
            print(f"  - {error}")


async def cmd_schedule(analyst: AnalystAgent, args):
    """Show schedule"""
    schedule = analyst.get_schedule()

    print("TODAY'S SCHEDULE")
    print("-" * 40)

    today = schedule.get('today', [])
    if today:
        for task in today:
            status = "Enabled" if task.get('enabled') else "Disabled"
            print(f"  {task.get('name')}")
            print(f"    Next: {task.get('next_run')}")
            print(f"    Status: {status}")
    else:
        print("  No tasks scheduled for today")

    print("\nUPCOMING TASKS")
    print("-" * 40)
    upcoming = schedule.get('upcoming', [])
    for task in upcoming[:10]:
        print(f"  {task.get('name')} - {task.get('next_run')}")


async def cmd_scheduler(analyst: AnalystAgent, args):
    """Scheduler operations"""
    if args.scheduler_cmd == 'start':
        print("Starting scheduler...")
        print("Press Ctrl+C to stop\n")

        try:
            await analyst.start_scheduler()
        except KeyboardInterrupt:
            print("\nStopping scheduler...")
            analyst.stop_scheduler()

    elif args.scheduler_cmd == 'status':
        status = analyst.scheduler.get_status()
        print("SCHEDULER STATUS")
        print("-" * 40)
        print_json(status)


async def cmd_db(analyst: AnalystAgent, args):
    """Database operations"""
    if args.db_cmd == 'status':
        status = await analyst.database_team.get_database_status()
        print("DATABASE STATUS")
        print("-" * 40)
        print_json(status)

    elif args.db_cmd == 'query':
        params = {}
        if args.commodity:
            params['commodity'] = args.commodity
        if args.source:
            params['source_report'] = args.source
        if args.limit:
            params['limit'] = args.limit

        result = await analyst.database_team.query(params)

        if result.success:
            records = result.data.get('records', [])
            print(f"Found {len(records)} records\n")

            for record in records[:10]:
                print(f"  {record.get('report_date')} | {record.get('commodity')} | "
                      f"${record.get('price_avg') or record.get('price') or 'N/A'}")

            if len(records) > 10:
                print(f"\n  ... and {len(records) - 10} more")
        else:
            print(f"Query failed: {result.error}")

    elif args.db_cmd == 'health':
        health = await analyst.database_team.check_database_health()
        print("DATABASE HEALTH")
        print("-" * 40)
        print_json(health)


async def cmd_tools(analyst: AnalystAgent, args):
    """List available tools"""
    tools = analyst._tools.list_tools()

    print("AVAILABLE TOOLS")
    print("-" * 40)

    for tool in tools:
        print(f"\n{tool['name']}")
        print(f"  {tool['description']}")
        print(f"  Executions: {tool['execution_count']}")
        if tool['parameters']:
            print("  Parameters:")
            for name, info in tool['parameters'].items():
                req = "required" if info.get('required') else "optional"
                print(f"    - {name}: {info.get('description', '')} ({req})")


async def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='RLC Agent System - Commodity Market Data Automation',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Commands:
  status              Show system status
  collect [source]    Collect data (default: usda_ams)
  daily               Run daily workflow
  schedule            Show task schedule
  scheduler start     Start the background scheduler
  db status           Show database status
  db query            Query the database
  db health           Check database health
  tools               List available tools

Examples:
  python -m src.main status
  python -m src.main collect usda_ams --date 12/04/2025
  python -m src.main daily
  python -m src.main db query --commodity corn --limit 20
        """
    )

    parser.add_argument('command', choices=[
        'status', 'collect', 'daily', 'schedule', 'scheduler', 'db', 'tools'
    ], help='Command to execute')

    parser.add_argument('subcommand', nargs='?', help='Subcommand or source')

    parser.add_argument('--date', '-d', help='Report date (MM/DD/YYYY)')
    parser.add_argument('--source', '-s', help='Data source or filter')
    parser.add_argument('--commodity', '-c', help='Commodity filter')
    parser.add_argument('--limit', '-l', type=int, default=100, help='Result limit')
    parser.add_argument('--log-level', default='INFO',
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                        help='Logging level')
    parser.add_argument('--json', action='store_true', help='Output as JSON')

    args = parser.parse_args()

    # Map subcommand for certain commands
    if args.command == 'collect':
        args.source = args.subcommand or args.source or 'usda_ams'
    elif args.command == 'scheduler':
        args.scheduler_cmd = args.subcommand or 'status'
    elif args.command == 'db':
        args.db_cmd = args.subcommand or 'status'

    # Setup
    logger = setup_logging(args.log_level)
    print_header()

    # Create analyst agent
    try:
        analyst = create_analyst_agent()
    except Exception as e:
        print(f"Failed to initialize system: {e}")
        sys.exit(1)

    # Emit startup event
    await get_event_bus().emit(
        EventType.SYSTEM_STARTUP,
        source='cli',
        data={'command': args.command}
    )

    # Execute command
    try:
        if args.command == 'status':
            await cmd_status(analyst, args)
        elif args.command == 'collect':
            await cmd_collect(analyst, args)
        elif args.command == 'daily':
            await cmd_daily(analyst, args)
        elif args.command == 'schedule':
            await cmd_schedule(analyst, args)
        elif args.command == 'scheduler':
            await cmd_scheduler(analyst, args)
        elif args.command == 'db':
            await cmd_db(analyst, args)
        elif args.command == 'tools':
            await cmd_tools(analyst, args)

    except KeyboardInterrupt:
        print("\nInterrupted by user")
    except Exception as e:
        logger.error(f"Command failed: {e}")
        print(f"\nError: {e}")
        sys.exit(1)

    print("\n" + "=" * 60)
    print("Done")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    asyncio.run(main())
