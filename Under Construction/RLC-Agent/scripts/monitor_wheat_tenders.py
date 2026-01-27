#!/usr/bin/env python3
"""
Wheat Tender Monitoring Script

Runs the wheat tender collector on a schedule and sends alerts.
Can be run as a cron job, systemd timer, or standalone daemon.

Usage:
    # Run once
    python scripts/monitor_wheat_tenders.py

    # Run as daemon (continuous monitoring)
    python scripts/monitor_wheat_tenders.py --daemon

    # Run with custom interval
    python scripts/monitor_wheat_tenders.py --daemon --interval 30

    # Save results to file
    python scripts/monitor_wheat_tenders.py --output results.json

Cron example (every hour):
    0 * * * * /path/to/python /path/to/monitor_wheat_tenders.py >> /var/log/wheat_tenders.log 2>&1
"""

import os
import sys
import json
import time
import signal
import logging
import argparse
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.agents.collectors.tenders.wheat_tender_collector import (
    WheatTenderCollector,
    WheatTenderConfig,
)
from src.agents.collectors.tenders.alert_system import (
    TenderAlertManager,
    AlertConfig,
)

# Configure logging
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
LOG_DIR = PROJECT_ROOT / 'logs'
LOG_DIR.mkdir(exist_ok=True)

logger = logging.getLogger('wheat_tender_monitor')


class TenderMonitor:
    """
    Monitors wheat tenders and sends alerts.

    Can run as a single execution or continuous daemon.
    """

    def __init__(
        self,
        interval_minutes: int = 60,
        output_file: Optional[str] = None,
        db_connection = None
    ):
        self.interval_minutes = interval_minutes
        self.output_file = output_file
        self.db_connection = db_connection

        # Initialize collector
        self.config = WheatTenderConfig(
            scrape_interval_minutes=interval_minutes
        )
        self.collector = WheatTenderCollector(self.config)

        # Initialize alert manager
        self.alert_manager = TenderAlertManager(db_connection=db_connection)

        # State tracking
        self.running = True
        self.last_run = None
        self.run_count = 0
        self.total_tenders_found = 0
        self.total_alerts_sent = 0

        # Seen articles (to avoid duplicate alerts)
        self.seen_articles: set = set()

        logger.info(
            f"TenderMonitor initialized with {interval_minutes} minute interval"
        )

    def run_once(self) -> Dict:
        """
        Run a single collection cycle.

        Returns:
            Dictionary with results summary
        """
        self.run_count += 1
        start_time = datetime.now()
        logger.info(f"Starting collection run #{self.run_count}")

        result = {
            'run_number': self.run_count,
            'start_time': start_time.isoformat(),
            'tenders_found': 0,
            'new_tenders': 0,
            'alerts_triggered': [],
            'errors': [],
        }

        try:
            # Collect tenders
            collection_result = self.collector.collect(use_cache=False)

            if not collection_result.success:
                result['errors'].append(collection_result.error_message)
                logger.error(f"Collection failed: {collection_result.error_message}")
                return result

            # Get records
            if hasattr(collection_result.data, 'to_dict'):
                records = collection_result.data.to_dict('records')
            else:
                records = collection_result.data or []

            result['tenders_found'] = len(records)
            self.total_tenders_found += len(records)

            # Process each tender
            new_tenders = []
            for record in records:
                article_id = record.get('source_article_id', '')

                # Skip if already seen
                if article_id in self.seen_articles:
                    continue

                self.seen_articles.add(article_id)
                new_tenders.append(record)

                # Send alerts for new tenders
                triggered = self.alert_manager.process_tender(record)
                if triggered:
                    result['alerts_triggered'].extend(triggered)
                    self.total_alerts_sent += len(triggered)

            result['new_tenders'] = len(new_tenders)

            # Save to output file if specified
            if self.output_file and new_tenders:
                self._save_results(new_tenders)

            # Log warnings
            if collection_result.warnings:
                result['warnings'] = collection_result.warnings
                for warning in collection_result.warnings:
                    logger.warning(warning)

        except Exception as e:
            result['errors'].append(str(e))
            logger.error(f"Error in collection run: {e}", exc_info=True)

        result['end_time'] = datetime.now().isoformat()
        result['duration_seconds'] = (
            datetime.now() - start_time
        ).total_seconds()

        self.last_run = datetime.now()

        logger.info(
            f"Run #{self.run_count} complete: "
            f"found {result['tenders_found']} tenders, "
            f"{result['new_tenders']} new, "
            f"{len(result['alerts_triggered'])} alerts"
        )

        return result

    def run_daemon(self):
        """
        Run continuous monitoring loop.

        Runs until interrupted (Ctrl+C or SIGTERM).
        """
        logger.info(
            f"Starting daemon mode with {self.interval_minutes} minute interval"
        )

        # Set up signal handlers
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

        while self.running:
            try:
                result = self.run_once()

                # Log status
                if result['errors']:
                    logger.warning(f"Run had {len(result['errors'])} errors")

                # Wait for next interval
                if self.running:
                    sleep_seconds = self.interval_minutes * 60
                    logger.info(
                        f"Sleeping for {self.interval_minutes} minutes..."
                    )
                    time.sleep(sleep_seconds)

            except Exception as e:
                logger.error(f"Unexpected error in daemon loop: {e}")
                time.sleep(60)  # Wait 1 minute before retrying

        logger.info("Daemon shutdown complete")

    def _handle_shutdown(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info(f"Received signal {signum}, shutting down...")
        self.running = False

    def _save_results(self, records: List[Dict]):
        """Save results to output file"""
        try:
            output_path = Path(self.output_file)

            # Load existing data if file exists
            existing = []
            if output_path.exists():
                with open(output_path, 'r') as f:
                    existing = json.load(f)

            # Append new records
            existing.extend(records)

            # Save
            with open(output_path, 'w') as f:
                json.dump(existing, f, indent=2, default=str)

            logger.info(f"Saved {len(records)} records to {output_path}")

        except Exception as e:
            logger.error(f"Failed to save results: {e}")

    def get_status(self) -> Dict:
        """Get current monitor status"""
        return {
            'running': self.running,
            'run_count': self.run_count,
            'last_run': self.last_run.isoformat() if self.last_run else None,
            'interval_minutes': self.interval_minutes,
            'total_tenders_found': self.total_tenders_found,
            'total_alerts_sent': self.total_alerts_sent,
            'seen_articles_count': len(self.seen_articles),
        }


def setup_logging(verbose: bool = False, log_file: str = None):
    """Configure logging"""
    level = logging.DEBUG if verbose else logging.INFO

    handlers = [logging.StreamHandler()]

    if log_file:
        handlers.append(logging.FileHandler(log_file))
    else:
        # Default log file
        default_log = LOG_DIR / 'wheat_tender_monitor.log'
        handlers.append(logging.FileHandler(default_log))

    logging.basicConfig(
        level=level,
        format=LOG_FORMAT,
        handlers=handlers
    )


def main():
    parser = argparse.ArgumentParser(
        description='Wheat Tender Monitoring Script',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run once and exit
  python scripts/monitor_wheat_tenders.py

  # Run as continuous daemon
  python scripts/monitor_wheat_tenders.py --daemon

  # Run every 30 minutes
  python scripts/monitor_wheat_tenders.py --daemon --interval 30

  # Save results to JSON file
  python scripts/monitor_wheat_tenders.py --output tenders.json
        """
    )

    parser.add_argument(
        '--daemon', '-d',
        action='store_true',
        help='Run as continuous daemon'
    )

    parser.add_argument(
        '--interval', '-i',
        type=int,
        default=60,
        help='Collection interval in minutes (default: 60)'
    )

    parser.add_argument(
        '--output', '-o',
        help='Output file for results (JSON)'
    )

    parser.add_argument(
        '--log-file', '-l',
        help='Log file path'
    )

    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Verbose output'
    )

    parser.add_argument(
        '--status',
        action='store_true',
        help='Show collector status and exit'
    )

    args = parser.parse_args()

    # Setup logging
    setup_logging(verbose=args.verbose, log_file=args.log_file)

    # Status check
    if args.status:
        collector = WheatTenderCollector()
        status = collector.get_status()
        print(json.dumps(status, indent=2))
        return

    # Create monitor
    monitor = TenderMonitor(
        interval_minutes=args.interval,
        output_file=args.output
    )

    # Run
    if args.daemon:
        monitor.run_daemon()
    else:
        result = monitor.run_once()
        print(json.dumps(result, indent=2, default=str))


if __name__ == '__main__':
    main()
