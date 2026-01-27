#!/usr/bin/env python3
"""
Agricultural Data Scheduler

Orchestrates automated collection of agricultural commodity data from multiple sources.
Can run as a one-shot check or as a continuous background service.

Release Schedule (approximate):
- WASDE: ~12th of each month (8:00 AM ET)
- ERS Feed Grains: ~13th-15th of each month
- NASS Crop Production: ~10th of each month
- Census Trade: ~5th of month (2-month lag)
- Export Sales: Every Thursday (8:30 AM ET)

Usage:
    # Check all sources once
    python scripts/data_scheduler.py --check-all

    # Run specific collector
    python scripts/data_scheduler.py --source ers_feed_grains

    # Run as background scheduler
    python scripts/data_scheduler.py --daemon

    # Show next scheduled runs
    python scripts/data_scheduler.py --schedule
"""

import argparse
import json
import logging
import os
import signal
import sqlite3
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from collectors.ers_feed_grains_collector import ERSFeedGrainsCollector, get_release_schedule
from collectors.ers_monthly_outlook_collector import ERSMonthlyOutlookCollector, OUTLOOK_SOURCES

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(PROJECT_ROOT / "logs" / "scheduler.log")
    ]
)
logger = logging.getLogger('data_scheduler')

# Paths
CONFIG_PATH = PROJECT_ROOT / "config" / "scheduler_config.json"
STATE_PATH = PROJECT_ROOT / "config" / "scheduler_state.json"
DB_PATH = PROJECT_ROOT / "data" / "rlc_commodities.db"


class DataScheduler:
    """
    Manages scheduled data collection from multiple agricultural data sources.
    """

    def __init__(self):
        self.running = True
        self.config = self._load_config()
        self.state = self._load_state()

        # Ensure log directory exists
        (PROJECT_ROOT / "logs").mkdir(parents=True, exist_ok=True)

        # Available collectors
        self.collectors = {
            'ers_feed_grains': ERSFeedGrainsCollector,
            'oil_crops_outlook': lambda force=False: ERSMonthlyOutlookCollector('oil_crops', force),
            'feed_grains_outlook': lambda force=False: ERSMonthlyOutlookCollector('feed_grains', force),
            # Add more collectors as they're implemented:
            # 'wasde': WASDECollector,
            # 'nass_crop': NASSCropCollector,
            # 'census_trade': CensusTradeCollector,
            # 'export_sales': ExportSalesCollector,
        }

    def _load_config(self) -> dict:
        """Load scheduler configuration."""
        default_config = {
            "check_interval_hours": 6,  # How often to check for new data
            "sources": {
                "ers_feed_grains": {
                    "enabled": True,
                    "check_days": [10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20],  # Days of month to check
                    "priority": 1
                },
                "oil_crops_outlook": {
                    "enabled": True,
                    "check_days": [10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20],  # Monthly outlook
                    "priority": 1
                },
                "feed_grains_outlook": {
                    "enabled": True,
                    "check_days": [10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20],  # Monthly outlook
                    "priority": 1
                },
                "wasde": {
                    "enabled": False,  # Not implemented yet
                    "check_days": [8, 9, 10, 11, 12, 13, 14, 15],
                    "priority": 1
                },
                "census_trade": {
                    "enabled": False,  # Not implemented yet
                    "check_days": list(range(1, 32)),  # Check any day (API-based)
                    "priority": 2
                }
            },
            "notifications": {
                "on_success": True,
                "on_failure": True,
                "log_to_db": True
            }
        }

        if CONFIG_PATH.exists():
            with open(CONFIG_PATH) as f:
                loaded = json.load(f)
                # Merge with defaults
                for key, value in default_config.items():
                    if key not in loaded:
                        loaded[key] = value
                return loaded

        # Save default config
        CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(CONFIG_PATH, 'w') as f:
            json.dump(default_config, f, indent=2)

        return default_config

    def _load_state(self) -> dict:
        """Load scheduler state (last runs, etc.)."""
        if STATE_PATH.exists():
            with open(STATE_PATH) as f:
                return json.load(f)
        return {"last_runs": {}, "run_history": []}

    def _save_state(self):
        """Save scheduler state."""
        STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(STATE_PATH, 'w') as f:
            json.dump(self.state, f, indent=2, default=str)

    def _log_run(self, source: str, result: dict):
        """Log a collection run to state and optionally database."""
        # Update state
        self.state["last_runs"][source] = {
            "timestamp": datetime.now().isoformat(),
            "success": result.get("success", False),
            "message": result.get("message", "")
        }

        # Keep run history (last 100 runs)
        self.state["run_history"].append({
            "source": source,
            "timestamp": datetime.now().isoformat(),
            "result": result
        })
        self.state["run_history"] = self.state["run_history"][-100:]

        self._save_state()

        # Log to database if configured
        if self.config.get("notifications", {}).get("log_to_db"):
            self._log_to_database(source, result)

    def _log_to_database(self, source: str, result: dict):
        """Log collection run to the database."""
        try:
            conn = sqlite3.connect(str(DB_PATH))
            conn.execute("""
                CREATE TABLE IF NOT EXISTS collection_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source TEXT NOT NULL,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    success INTEGER,
                    message TEXT,
                    downloaded_file TEXT,
                    records_ingested INTEGER
                )
            """)
            conn.execute("""
                INSERT INTO collection_log (source, success, message, downloaded_file)
                VALUES (?, ?, ?, ?)
            """, (
                source,
                1 if result.get("success") else 0,
                result.get("message"),
                result.get("downloaded_file")
            ))
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"Failed to log to database: {e}")

    def should_check_source(self, source: str) -> bool:
        """Determine if we should check a source based on schedule."""
        source_config = self.config.get("sources", {}).get(source, {})

        if not source_config.get("enabled", False):
            return False

        # Check if today is a check day
        today = datetime.now().day
        check_days = source_config.get("check_days", [])
        if check_days and today not in check_days:
            logger.debug(f"Skipping {source}: day {today} not in check days")
            return False

        # Check if we've already checked recently
        last_run = self.state.get("last_runs", {}).get(source, {})
        if last_run:
            last_time = datetime.fromisoformat(last_run["timestamp"])
            hours_since = (datetime.now() - last_time).total_seconds() / 3600
            min_interval = self.config.get("check_interval_hours", 6)

            if hours_since < min_interval:
                logger.debug(f"Skipping {source}: checked {hours_since:.1f}h ago")
                return False

        return True

    def run_collector(self, source: str, force: bool = False) -> dict:
        """Run a specific collector."""
        if source not in self.collectors:
            return {"success": False, "message": f"Unknown source: {source}"}

        logger.info(f"Running collector: {source}")

        try:
            collector_class = self.collectors[source]
            collector = collector_class(force=force)
            result = collector.collect()

            self._log_run(source, result)

            if result.get("success"):
                logger.info(f"Collector {source} completed: {result.get('message')}")
            else:
                logger.warning(f"Collector {source} failed: {result.get('message')}")

            return result

        except Exception as e:
            logger.error(f"Collector {source} error: {e}")
            result = {"success": False, "message": str(e)}
            self._log_run(source, result)
            return result

    def check_all_sources(self) -> Dict[str, dict]:
        """Check all enabled sources for updates."""
        results = {}

        # Sort by priority
        sources = list(self.config.get("sources", {}).keys())
        sources.sort(key=lambda s: self.config["sources"].get(s, {}).get("priority", 99))

        for source in sources:
            if self.should_check_source(source):
                results[source] = self.run_collector(source)
            else:
                results[source] = {"success": True, "message": "Skipped (not scheduled)"}

        return results

    def get_next_scheduled_runs(self) -> List[dict]:
        """Get upcoming scheduled runs."""
        schedule = get_release_schedule()
        upcoming = []
        today = datetime.now()

        for source, info in schedule.items():
            # Calculate next expected release
            if info.get("frequency") == "monthly":
                release_day = info.get("typical_release_day", 15)

                if today.day < release_day:
                    next_release = today.replace(day=release_day)
                else:
                    # Next month
                    if today.month == 12:
                        next_release = today.replace(year=today.year + 1, month=1, day=release_day)
                    else:
                        next_release = today.replace(month=today.month + 1, day=release_day)

                upcoming.append({
                    "source": source,
                    "name": info["name"],
                    "next_release": next_release.strftime("%Y-%m-%d"),
                    "days_until": (next_release - today).days
                })

            elif info.get("frequency") == "weekly":
                # Find next occurrence of release day
                day_name = info.get("release_day", "Thursday")
                day_map = {"Monday": 0, "Tuesday": 1, "Wednesday": 2, "Thursday": 3,
                           "Friday": 4, "Saturday": 5, "Sunday": 6}
                target_day = day_map.get(day_name, 3)
                days_ahead = target_day - today.weekday()
                if days_ahead <= 0:
                    days_ahead += 7
                next_release = today + timedelta(days=days_ahead)

                upcoming.append({
                    "source": source,
                    "name": info["name"],
                    "next_release": next_release.strftime("%Y-%m-%d"),
                    "days_until": days_ahead
                })

        # Sort by days until release
        upcoming.sort(key=lambda x: x["days_until"])
        return upcoming

    def run_daemon(self):
        """Run as a background scheduler daemon."""
        logger.info("Starting scheduler daemon...")
        logger.info(f"Check interval: {self.config.get('check_interval_hours', 6)} hours")

        # Setup signal handlers
        def handle_signal(signum, frame):
            logger.info("Received shutdown signal")
            self.running = False

        signal.signal(signal.SIGINT, handle_signal)
        signal.signal(signal.SIGTERM, handle_signal)

        check_interval = self.config.get("check_interval_hours", 6) * 3600  # Convert to seconds

        while self.running:
            logger.info("Running scheduled check...")
            results = self.check_all_sources()

            # Log summary
            for source, result in results.items():
                status = "✓" if result.get("success") else "✗"
                logger.info(f"  {status} {source}: {result.get('message')}")

            # Wait for next check
            logger.info(f"Next check in {check_interval/3600:.1f} hours")

            # Sleep in small increments to allow for graceful shutdown
            sleep_end = time.time() + check_interval
            while self.running and time.time() < sleep_end:
                time.sleep(60)  # Check every minute

        logger.info("Scheduler daemon stopped")


def main():
    parser = argparse.ArgumentParser(description='Agricultural Data Scheduler')
    parser.add_argument('--check-all', action='store_true', help='Check all enabled sources once')
    parser.add_argument('--source', type=str, help='Run specific source collector')
    parser.add_argument('--force', action='store_true', help='Force collection even if data seems current')
    parser.add_argument('--daemon', action='store_true', help='Run as background scheduler')
    parser.add_argument('--schedule', action='store_true', help='Show upcoming scheduled releases')
    parser.add_argument('--status', action='store_true', help='Show collector status')
    args = parser.parse_args()

    scheduler = DataScheduler()

    if args.schedule:
        print("\n=== Upcoming Data Releases ===\n")
        for item in scheduler.get_next_scheduled_runs():
            status = "🟢" if item["days_until"] <= 3 else "🟡" if item["days_until"] <= 7 else "⚪"
            print(f"{status} {item['name']}")
            print(f"   Expected: {item['next_release']} ({item['days_until']} days)")
            print()
        return

    if args.status:
        print("\n=== Collector Status ===\n")
        for source, info in scheduler.config.get("sources", {}).items():
            enabled = "✓ Enabled" if info.get("enabled") else "✗ Disabled"
            last_run = scheduler.state.get("last_runs", {}).get(source, {})
            if last_run:
                last_time = last_run.get("timestamp", "Never")
                last_status = "✓" if last_run.get("success") else "✗"
                print(f"{source}: {enabled}")
                print(f"  Last run: {last_time} {last_status}")
                print(f"  Message: {last_run.get('message', 'N/A')}")
            else:
                print(f"{source}: {enabled}")
                print(f"  Last run: Never")
            print()
        return

    if args.daemon:
        scheduler.run_daemon()
        return

    if args.source:
        result = scheduler.run_collector(args.source, force=args.force)
        print(f"\n=== {args.source} ===")
        print(f"Success: {result.get('success')}")
        print(f"Message: {result.get('message')}")
        return

    if args.check_all:
        print("\n=== Checking All Sources ===\n")
        results = scheduler.check_all_sources()
        for source, result in results.items():
            status = "✓" if result.get("success") else "✗"
            print(f"{status} {source}: {result.get('message')}")
        return

    # Default: show help
    parser.print_help()


if __name__ == '__main__':
    main()
