#!/usr/bin/env python3
"""
South America Trade Data Pipeline - Main Entry Point

Command-line interface for running the trade data pipeline.

Usage:
    python -m south_america_trade_data.main [command] [options]

Commands:
    fetch       - Fetch data for a specific country and period
    monthly     - Run full monthly pipeline for all countries
    backfill    - Run historical backfill
    schedule    - Start the scheduler for automated pulls
    status      - Show pipeline and agent status
    validate    - Validate configuration and connectivity
"""

import argparse
import json
import logging
import sys
from datetime import datetime
from dataclasses import asdict

from .config.settings import SouthAmericaTradeConfig, default_config
from .services.orchestrator import TradeDataOrchestrator
from .services.scheduler import TradeDataScheduler, get_recommended_cron_schedules
from .database.models import init_database

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
    ]
)
logger = logging.getLogger(__name__)


def cmd_fetch(args):
    """Fetch data for a specific country"""
    from .agents.argentina_agent import ArgentinaINDECAgent
    from .agents.brazil_agent import BrazilComexStatAgent
    from .agents.colombia_agent import ColombiaDANEAgent
    from .agents.uruguay_agent import UruguayDNAAgent
    from .agents.paraguay_agent import ParaguayAgent

    config = SouthAmericaTradeConfig()

    agents = {
        'ARG': (ArgentinaINDECAgent, config.argentina),
        'BRA': (BrazilComexStatAgent, config.brazil),
        'COL': (ColombiaDANEAgent, config.colombia),
        'URY': (UruguayDNAAgent, config.uruguay),
        'PRY': (ParaguayAgent, config.paraguay),
    }

    country = args.country.upper()
    if country not in agents:
        print(f"Unknown country: {country}")
        print(f"Available: {list(agents.keys())}")
        return 1

    agent_class, agent_config = agents[country]
    agent = agent_class(agent_config)

    flows = args.flows or ['export', 'import']

    for flow in flows:
        print(f"\nFetching {country} {flow}s for {args.year}-{args.month:02d}...")

        result = agent.fetch_data(args.year, args.month, flow)

        print(f"  Success: {result.success}")
        print(f"  Records: {result.records_fetched}")

        if result.error_message:
            print(f"  Error: {result.error_message}")

        if result.success and result.data is not None and args.verbose:
            print(f"  Columns: {list(result.data.columns)}")
            print(f"  Sample:\n{result.data.head()}")

            if args.transform:
                records = agent.transform_to_records(result.data, flow)
                print(f"  Transformed records: {len(records)}")

    return 0


def cmd_monthly(args):
    """Run monthly pipeline"""
    orchestrator = TradeDataOrchestrator()

    countries = args.countries if args.countries else None
    flows = args.flows if args.flows else None

    result = orchestrator.run_monthly_pipeline(
        year=args.year,
        month=args.month,
        countries=countries,
        flows=flows,
        parallel=not args.sequential
    )

    # Print summary
    print("\n" + "=" * 60)
    print("MONTHLY PIPELINE RESULTS")
    print("=" * 60)
    print(f"Period: {args.year}-{args.month:02d}")
    print(f"Success: {result.success}")
    print(f"Duration: {(result.end_time - result.start_time).total_seconds():.1f}s")
    print(f"Total records fetched: {result.total_records_fetched}")
    print(f"Total records loaded: {result.total_records_loaded}")
    print(f"Total errors: {result.total_errors}")

    print("\nCountry Results:")
    for country, country_result in result.country_results.items():
        print(f"\n  {country}:")
        for flow, flow_result in country_result.items():
            status = "OK" if flow_result.get('success') else "FAILED"
            records = flow_result.get('records_loaded', 0)
            print(f"    {flow}: {status} ({records} records)")

    if result.harmonization_results:
        print("\nHarmonization:")
        print(f"  Input records: {result.harmonization_results.get('input_records')}")
        print(f"  Harmonized records: {result.harmonization_results.get('harmonized_records')}")

    if result.quality_alerts:
        print(f"\nQuality Alerts: {len(result.quality_alerts)}")
        for alert in result.quality_alerts[:5]:
            print(f"  - {alert.get('message', 'Unknown')}")

    if args.output:
        with open(args.output, 'w') as f:
            json.dump(asdict(result), f, indent=2, default=str)
        print(f"\nFull results saved to: {args.output}")

    return 0 if result.success else 1


def cmd_backfill(args):
    """Run historical backfill"""
    orchestrator = TradeDataOrchestrator()

    print(f"Starting backfill from {args.start_year}-{args.start_month:02d}")

    results = orchestrator.run_historical_backfill(
        start_year=args.start_year,
        start_month=args.start_month,
        end_year=args.end_year,
        end_month=args.end_month,
        countries=args.countries
    )

    # Summary
    successful = sum(1 for r in results if r.success)
    failed = len(results) - successful
    total_records = sum(r.total_records_loaded for r in results)

    print("\n" + "=" * 60)
    print("BACKFILL RESULTS")
    print("=" * 60)
    print(f"Periods processed: {len(results)}")
    print(f"Successful: {successful}")
    print(f"Failed: {failed}")
    print(f"Total records loaded: {total_records}")

    return 0 if failed == 0 else 1


def cmd_schedule(args):
    """Start the scheduler"""
    import time

    orchestrator = TradeDataOrchestrator()
    scheduler = TradeDataScheduler(orchestrator=orchestrator)

    if args.list:
        status = scheduler.get_schedule_status()
        print(json.dumps(status, indent=2, default=str))
        return 0

    if args.cron:
        crons = get_recommended_cron_schedules()
        print("Recommended cron schedules:")
        for country, cron in crons.items():
            print(f"  {country}: {cron}")
        return 0

    if args.run_task:
        result = scheduler.run_task(args.run_task)
        print(json.dumps(result, indent=2, default=str))
        return 0 if result.get('success') else 1

    # Start continuous scheduler
    print("Starting scheduler (Ctrl+C to stop)...")
    scheduler.start(check_interval=args.interval)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        scheduler.stop()
        print("\nScheduler stopped")

    return 0


def cmd_status(args):
    """Show pipeline status"""
    orchestrator = TradeDataOrchestrator()
    status = orchestrator.get_pipeline_status()

    print("\n" + "=" * 60)
    print("PIPELINE STATUS")
    print("=" * 60)
    print(f"Pipeline: {status['pipeline_name']} v{status['pipeline_version']}")
    print(f"Enabled countries: {', '.join(status['enabled_countries'])}")

    print("\nAgent Status:")
    for country, agent_status in status['agent_status'].items():
        health = "Healthy" if agent_status.get('is_healthy') else "Unhealthy"
        last_success = agent_status.get('last_success', 'Never')
        print(f"  {country}: {health}, Last success: {last_success}")

    return 0


def cmd_validate(args):
    """Validate configuration and connectivity"""
    print("Validating configuration...")

    # Check configuration
    config = SouthAmericaTradeConfig()
    print(f"  Pipeline: {config.pipeline_name} v{config.pipeline_version}")

    # Check enabled countries
    enabled = config.get_enabled_countries()
    print(f"  Enabled countries: {', '.join(enabled)}")

    # Check for placeholder credentials
    warnings = []

    if 'XXX' in config.colombia.socrata_app_token:
        warnings.append("Colombia: Socrata app token not configured (optional)")

    if 'XXX' in config.colombia.export_dataset_id:
        warnings.append("Colombia: Export dataset ID not configured")

    if 'XXX' in config.uruguay.export_resource_id:
        warnings.append("Uruguay: Export resource ID not configured")

    if 'XXX' in config.paraguay.comtrade_api_key:
        warnings.append("Paraguay: UN Comtrade API key not configured (WITS will be used)")

    if warnings:
        print("\nWarnings (credentials needing configuration):")
        for w in warnings:
            print(f"  - {w}")

    # Test database connection
    print("\nTesting database connection...")
    try:
        session_factory, engine = init_database(config.database.get_connection_string())
        print(f"  Database: OK ({config.database.db_type.value})")
    except Exception as e:
        print(f"  Database: FAILED ({e})")

    print("\nValidation complete.")
    return 0


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='South America Trade Data Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Fetch Brazil exports for November 2024
  python -m south_america_trade_data.main fetch --country BRA --year 2024 --month 11 --flows export

  # Run monthly pipeline for all countries
  python -m south_america_trade_data.main monthly --year 2024 --month 10

  # Historical backfill from January 2024
  python -m south_america_trade_data.main backfill --start-year 2024 --start-month 1

  # Start scheduler
  python -m south_america_trade_data.main schedule

  # Check status
  python -m south_america_trade_data.main status
        """
    )

    parser.add_argument('--version', action='version', version='%(prog)s 1.0.0')

    subparsers = parser.add_subparsers(dest='command', help='Command to run')

    # Fetch command
    fetch_parser = subparsers.add_parser('fetch', help='Fetch data for a specific country')
    fetch_parser.add_argument('--country', '-c', required=True, help='Country code (ARG, BRA, COL, URY, PRY)')
    fetch_parser.add_argument('--year', '-y', type=int, default=datetime.now().year)
    fetch_parser.add_argument('--month', '-m', type=int, default=max(1, datetime.now().month - 1))
    fetch_parser.add_argument('--flows', '-f', nargs='+', choices=['export', 'import'])
    fetch_parser.add_argument('--verbose', '-v', action='store_true')
    fetch_parser.add_argument('--transform', '-t', action='store_true', help='Transform records')

    # Monthly command
    monthly_parser = subparsers.add_parser('monthly', help='Run monthly pipeline')
    monthly_parser.add_argument('--year', '-y', type=int, default=datetime.now().year)
    monthly_parser.add_argument('--month', '-m', type=int, default=max(1, datetime.now().month - 1))
    monthly_parser.add_argument('--countries', '-c', nargs='+', help='Country codes')
    monthly_parser.add_argument('--flows', '-f', nargs='+', choices=['export', 'import'])
    monthly_parser.add_argument('--sequential', action='store_true', help='Run sequentially')
    monthly_parser.add_argument('--output', '-o', help='Output file for results (JSON)')

    # Backfill command
    backfill_parser = subparsers.add_parser('backfill', help='Run historical backfill')
    backfill_parser.add_argument('--start-year', type=int, required=True)
    backfill_parser.add_argument('--start-month', type=int, default=1)
    backfill_parser.add_argument('--end-year', type=int)
    backfill_parser.add_argument('--end-month', type=int)
    backfill_parser.add_argument('--countries', '-c', nargs='+')

    # Schedule command
    schedule_parser = subparsers.add_parser('schedule', help='Scheduler operations')
    schedule_parser.add_argument('--list', '-l', action='store_true', help='List scheduled tasks')
    schedule_parser.add_argument('--cron', action='store_true', help='Show recommended cron expressions')
    schedule_parser.add_argument('--run-task', help='Run specific task')
    schedule_parser.add_argument('--interval', type=int, default=60, help='Check interval in seconds')

    # Status command
    status_parser = subparsers.add_parser('status', help='Show pipeline status')

    # Validate command
    validate_parser = subparsers.add_parser('validate', help='Validate configuration')

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 0

    # Route to command handler
    handlers = {
        'fetch': cmd_fetch,
        'monthly': cmd_monthly,
        'backfill': cmd_backfill,
        'schedule': cmd_schedule,
        'status': cmd_status,
        'validate': cmd_validate,
    }

    handler = handlers.get(args.command)
    if handler:
        return handler(args)
    else:
        parser.print_help()
        return 1


if __name__ == '__main__':
    sys.exit(main())
