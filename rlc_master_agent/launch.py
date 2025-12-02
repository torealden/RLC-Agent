#!/usr/bin/env python3
"""
RLC Master Agent - Launch Script
Convenience script to start the assistant
Round Lakes Commodities
"""

import sys
import os
import argparse
import logging
from pathlib import Path

# Ensure the package is in the path
sys.path.insert(0, str(Path(__file__).parent))

from config.settings import get_settings, Settings
from master_agent import RLCMasterAgent, AgentMode


def setup_argument_parser() -> argparse.ArgumentParser:
    """Set up command line argument parser"""
    parser = argparse.ArgumentParser(
        description='RLC Master Agent - AI Business Partner for Round Lakes Commodities',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  python launch.py                    # Start interactive mode
  python launch.py --mode automated   # Run automated daily workflow
  python launch.py --test             # Run in test mode
  python launch.py --status           # Check system status
        '''
    )

    parser.add_argument(
        '--mode', '-m',
        choices=['interactive', 'automated', 'scheduled'],
        default='interactive',
        help='Operating mode (default: interactive)'
    )

    parser.add_argument(
        '--test',
        action='store_true',
        help='Run in test mode (safe/sandbox environment)'
    )

    parser.add_argument(
        '--status',
        action='store_true',
        help='Show system status and exit'
    )

    parser.add_argument(
        '--daily',
        action='store_true',
        help='Run daily workflow and exit'
    )

    parser.add_argument(
        '--health',
        action='store_true',
        help='Run health check and exit'
    )

    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug logging'
    )

    parser.add_argument(
        '--config',
        type=str,
        help='Path to custom .env file'
    )

    return parser


def run_status_check():
    """Run status check and print results"""
    print("\n" + "=" * 60)
    print("RLC Master Agent - System Status")
    print("=" * 60 + "\n")

    settings = get_settings()

    print("Configuration:")
    print(f"  LLM Provider: {settings.llm.provider}")
    print(f"  LLM Model: {settings.llm.ollama_model if settings.llm.provider == 'ollama' else settings.llm.openai_model}")
    print(f"  Autonomy Level: {settings.autonomy_level}")
    print()

    print("API Keys:")
    print(f"  USDA API: {'Configured' if settings.api.usda_api_key else 'Not configured'}")
    print(f"  Census API: {'Configured' if settings.api.census_api_key else 'Not configured'}")
    print(f"  Weather API: {'Configured' if settings.api.weather_api_key else 'Not configured'}")
    print()

    print("Notion Integration:")
    print(f"  API Key: {'Configured' if settings.notion.api_key else 'Not configured'}")
    print(f"  Tasks DB: {'Configured' if settings.notion.tasks_db_id else 'Not configured'}")
    print(f"  Memory DB: {'Configured' if settings.notion.memory_db_id else 'Not configured'}")
    print()

    print("Google Integration:")
    gmail_creds = settings.google.get_credentials_path('gmail_work')
    calendar_creds = settings.google.get_credentials_path('calendar')
    print(f"  Gmail Credentials: {'Found' if gmail_creds.exists() else 'Not found'}")
    print(f"  Calendar Credentials: {'Found' if calendar_creds.exists() else 'Not found'}")
    print()

    # Validate configuration
    issues = settings.validate()
    if issues['errors']:
        print("Errors:")
        for error in issues['errors']:
            print(f"  - {error}")
    if issues['warnings']:
        print("Warnings:")
        for warning in issues['warnings']:
            print(f"  - {warning}")

    print()


def run_health_check():
    """Run health check on all components"""
    print("\n" + "=" * 60)
    print("RLC Master Agent - Health Check")
    print("=" * 60 + "\n")

    agent = RLCMasterAgent(mode=AgentMode.INTERACTIVE)
    health = agent.health_check()

    import json
    print(json.dumps(health, indent=2, default=str))
    print()


def run_daily_workflow():
    """Run the daily automated workflow"""
    print("\n" + "=" * 60)
    print("RLC Master Agent - Daily Workflow")
    print("=" * 60 + "\n")

    agent = RLCMasterAgent(mode=AgentMode.AUTOMATED)
    results = agent.daily_workflow()

    import json
    print("Workflow Results:")
    print(json.dumps(results, indent=2, default=str))
    print()


def run_interactive(test_mode: bool = False):
    """Run the agent in interactive mode"""
    if test_mode:
        print("\n*** RUNNING IN TEST MODE ***")
        print("Actions that would modify external services are simulated.\n")

    mode = AgentMode.INTERACTIVE
    agent = RLCMasterAgent(mode=mode)
    agent.run_interactive()


def main():
    """Main entry point"""
    parser = setup_argument_parser()
    args = parser.parse_args()

    # Set up logging level
    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    # Load custom config if specified
    if args.config:
        os.environ['DOTENV_PATH'] = args.config

    # Handle different run modes
    if args.status:
        run_status_check()
        return 0

    if args.health:
        run_health_check()
        return 0

    if args.daily:
        run_daily_workflow()
        return 0

    # Run the agent
    if args.mode == 'interactive':
        run_interactive(test_mode=args.test)
    elif args.mode == 'automated':
        run_daily_workflow()
    elif args.mode == 'scheduled':
        print("Scheduled mode not yet implemented.")
        print("Use cron or Windows Task Scheduler to run: python launch.py --daily")
        return 1

    return 0


if __name__ == '__main__':
    sys.exit(main())
