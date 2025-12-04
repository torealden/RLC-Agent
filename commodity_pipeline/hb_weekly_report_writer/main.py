#!/usr/bin/env python3
"""
HB Weekly Report Writer - Main Entry Point

Command-line interface for the HigbyBarrett Weekly Report Writer Agent.
Supports manual report generation, scheduling, and status checks.

Usage:
    python main.py generate [--date YYYY-MM-DD]
    python main.py schedule [--start]
    python main.py status
    python main.py validate
"""

import argparse
import json
import logging
import sys
from datetime import date, datetime
from dataclasses import asdict
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from commodity_pipeline.hb_weekly_report_writer.config.settings import (
    HBWeeklyReportConfig,
    default_config,
)
from commodity_pipeline.hb_weekly_report_writer.services.orchestrator import (
    HBReportOrchestrator,
)
from commodity_pipeline.hb_weekly_report_writer.services.scheduler import (
    ReportScheduler,
    setup_apscheduler,
)
from commodity_pipeline.hb_weekly_report_writer.utils.validation import (
    validate_configuration,
)


def setup_logging(log_level: str = "INFO"):
    """Configure logging"""
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
        ]
    )


def cmd_generate(args):
    """Generate a weekly report"""
    logger = logging.getLogger("main")

    # Parse date
    if args.date:
        try:
            report_date = datetime.strptime(args.date, "%Y-%m-%d").date()
        except ValueError:
            logger.error(f"Invalid date format: {args.date}. Use YYYY-MM-DD")
            return 1
    else:
        report_date = date.today()

    logger.info(f"Generating report for {report_date}")

    # Load configuration
    if args.config:
        # Load from file (future enhancement)
        config = HBWeeklyReportConfig.from_environment()
    else:
        config = HBWeeklyReportConfig.from_environment()

    # Create orchestrator and run
    orchestrator = HBReportOrchestrator(config)
    result = orchestrator.run_weekly_report(report_date)

    # Output result
    if args.json:
        print(json.dumps(asdict(result), indent=2, default=str))
    else:
        print("\n" + "=" * 60)
        print("HB WEEKLY REPORT GENERATION RESULT")
        print("=" * 60)
        print(f"Status: {'SUCCESS' if result.success else 'FAILED'}")
        print(f"Report Date: {result.report_date}")
        print(f"Duration: {result.total_duration_seconds:.1f} seconds")
        print()

        if result.document_path:
            print(f"Document: {result.document_path}")
        if result.dropbox_path:
            print(f"Dropbox: {result.dropbox_path}")

        print(f"\nCompleteness: {result.completeness_score:.1f}%")
        print(f"Placeholders: {result.placeholders_count}")
        print(f"LLM Estimates: {result.llm_estimates_count}")

        if result.questions_raised:
            print(f"\nQuestions: {result.questions_raised} raised, {result.questions_answered} answered")

        if result.errors:
            print(f"\nErrors:")
            for error in result.errors:
                print(f"  - {error}")

        if result.warnings:
            print(f"\nWarnings:")
            for warning in result.warnings:
                print(f"  - {warning}")

        print("=" * 60)

    return 0 if result.success else 1


def cmd_schedule(args):
    """Manage scheduling"""
    logger = logging.getLogger("main")

    config = HBWeeklyReportConfig.from_environment()
    orchestrator = HBReportOrchestrator(config)

    if args.start:
        logger.info("Starting scheduler...")

        if args.apscheduler:
            # Use APScheduler for production
            scheduler = setup_apscheduler(config, orchestrator.run_weekly_report)
            if scheduler:
                scheduler.start()
                print("APScheduler started. Press Ctrl+C to stop.")
                try:
                    # Keep main thread alive
                    import time
                    while True:
                        time.sleep(60)
                except KeyboardInterrupt:
                    scheduler.shutdown()
                    print("\nScheduler stopped.")
            else:
                print("Failed to start APScheduler. Install with: pip install apscheduler")
                return 1
        else:
            # Use built-in scheduler
            scheduler = ReportScheduler(config, orchestrator.run_weekly_report)
            scheduler.start()
            print("Scheduler started. Press Ctrl+C to stop.")
            try:
                import time
                while True:
                    time.sleep(60)
                    status = scheduler.get_status()
                    print(f"Next run: {status['task']['next_run']}")
            except KeyboardInterrupt:
                scheduler.stop()
                print("\nScheduler stopped.")

    elif args.trigger:
        logger.info("Manually triggering report generation...")
        scheduler = ReportScheduler(config, orchestrator.run_weekly_report)
        success = scheduler.trigger_now()
        return 0 if success else 1

    elif args.cron:
        # Show cron expression
        from commodity_pipeline.hb_weekly_report_writer.services.scheduler import CronScheduler
        cron = CronScheduler(config)
        expr = cron.get_cron_expression()
        print(f"Cron expression: {expr}")
        print(f"\nTo install in system crontab, run:")
        script_path = Path(__file__).resolve()
        print(f"  {expr} cd {script_path.parent} && python main.py generate")

    else:
        # Show schedule status
        scheduler = ReportScheduler(config)
        status = scheduler.get_status()
        print(json.dumps(status, indent=2))

    return 0


def cmd_status(args):
    """Show agent status"""
    config = HBWeeklyReportConfig.from_environment()
    orchestrator = HBReportOrchestrator(config)
    status = orchestrator.get_status()

    if args.json:
        print(json.dumps(status, indent=2))
    else:
        print("\n" + "=" * 40)
        print("HB WEEKLY REPORT WRITER STATUS")
        print("=" * 40)
        for key, value in status.items():
            print(f"{key}: {value}")
        print("=" * 40)

    return 0


def cmd_validate(args):
    """Validate configuration"""
    logger = logging.getLogger("main")

    config = HBWeeklyReportConfig.from_environment()
    result = validate_configuration(config)

    print("\n" + "=" * 40)
    print("CONFIGURATION VALIDATION")
    print("=" * 40)
    print(f"Valid: {result.is_valid}")

    if result.errors:
        print("\nErrors:")
        for error in result.errors:
            print(f"  [ERROR] {error}")

    if result.warnings:
        print("\nWarnings:")
        for warning in result.warnings:
            print(f"  [WARN] {warning}")

    if not result.errors and not result.warnings:
        print("\nConfiguration looks good!")

    print("=" * 40)

    return 0 if result.is_valid else 1


def cmd_test(args):
    """Run component tests"""
    logger = logging.getLogger("main")

    print("\n" + "=" * 40)
    print("COMPONENT TESTS")
    print("=" * 40)

    config = HBWeeklyReportConfig.from_environment()

    # Test 1: Configuration
    print("\n1. Configuration Loading...")
    try:
        print(f"   Agent: {config.agent_name} v{config.agent_version}")
        print(f"   Data Source: {config.internal_data_source.value}")
        print("   [PASS]")
    except Exception as e:
        print(f"   [FAIL] {e}")

    # Test 2: Database Connection
    print("\n2. Database Connection...")
    try:
        from commodity_pipeline.hb_weekly_report_writer.database.models import init_database
        conn_string = config.database.get_connection_string()
        engine, session_factory = init_database(conn_string)
        print(f"   Connection: {conn_string.split('@')[-1] if '@' in conn_string else conn_string}")
        print("   [PASS]")
    except Exception as e:
        print(f"   [SKIP] {e}")

    # Test 3: Document Builder
    print("\n3. Document Builder...")
    try:
        from commodity_pipeline.hb_weekly_report_writer.services.document_builder import DocumentBuilder
        builder = DocumentBuilder(config)
        print(f"   DOCX Available: {builder._docx_available}")
        print("   [PASS]" if builder._docx_available else "   [WARN] Using HTML fallback")
    except Exception as e:
        print(f"   [FAIL] {e}")

    # Test 4: LLM Connection
    print("\n4. LLM Configuration...")
    try:
        if config.llm.enabled:
            print(f"   Provider: {config.llm.provider}")
            print(f"   Model: {config.llm.model}")
            if config.llm.api_key:
                print("   API Key: [SET]")
            else:
                print("   API Key: [NOT SET]")
            print("   [PASS]" if config.llm.api_key else "   [WARN]")
        else:
            print("   LLM Disabled")
            print("   [SKIP]")
    except Exception as e:
        print(f"   [FAIL] {e}")

    # Test 5: Dropbox Configuration
    print("\n5. Dropbox Configuration...")
    try:
        if config.dropbox.enabled:
            if config.dropbox.access_token or config.dropbox.refresh_token:
                print("   Credentials: [SET]")
                print("   [PASS]")
            else:
                print("   Credentials: [NOT SET]")
                print("   [WARN]")
        else:
            print("   Dropbox Disabled")
            print("   [SKIP]")
    except Exception as e:
        print(f"   [FAIL] {e}")

    print("\n" + "=" * 40)
    print("Tests complete.")

    return 0


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="HB Weekly Report Writer Agent",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py generate                    # Generate report for today
  python main.py generate --date 2024-12-03  # Generate for specific date
  python main.py schedule --start            # Start scheduler
  python main.py schedule --trigger          # Manually trigger report
  python main.py status                      # Show agent status
  python main.py validate                    # Validate configuration
  python main.py test                        # Run component tests
        """
    )

    parser.add_argument(
        '--log-level',
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        help='Logging level'
    )

    parser.add_argument(
        '--config',
        help='Path to configuration file (optional)'
    )

    subparsers = parser.add_subparsers(dest='command', help='Command to execute')

    # Generate command
    gen_parser = subparsers.add_parser('generate', help='Generate weekly report')
    gen_parser.add_argument('--date', help='Report date (YYYY-MM-DD)')
    gen_parser.add_argument('--json', action='store_true', help='Output as JSON')

    # Schedule command
    sched_parser = subparsers.add_parser('schedule', help='Manage scheduling')
    sched_parser.add_argument('--start', action='store_true', help='Start scheduler')
    sched_parser.add_argument('--trigger', action='store_true', help='Manually trigger')
    sched_parser.add_argument('--cron', action='store_true', help='Show cron expression')
    sched_parser.add_argument('--apscheduler', action='store_true', help='Use APScheduler')

    # Status command
    status_parser = subparsers.add_parser('status', help='Show agent status')
    status_parser.add_argument('--json', action='store_true', help='Output as JSON')

    # Validate command
    validate_parser = subparsers.add_parser('validate', help='Validate configuration')

    # Test command
    test_parser = subparsers.add_parser('test', help='Run component tests')

    args = parser.parse_args()

    # Setup logging
    setup_logging(args.log_level)

    # Execute command
    if args.command == 'generate':
        return cmd_generate(args)
    elif args.command == 'schedule':
        return cmd_schedule(args)
    elif args.command == 'status':
        return cmd_status(args)
    elif args.command == 'validate':
        return cmd_validate(args)
    elif args.command == 'test':
        return cmd_test(args)
    else:
        parser.print_help()
        return 0


if __name__ == '__main__':
    sys.exit(main())
