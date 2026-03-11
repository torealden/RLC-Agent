"""
Backfill Runner

Populates historical data for all collectors using the existing
CollectorRunner infrastructure (gets CNS logging, event_log, KG
enrichment for free).

Usage:
    python -m src.dispatcher backfill                       # All tiers
    python -m src.dispatcher backfill --tier 1              # Prices only
    python -m src.dispatcher backfill --tier 1,2            # Multiple tiers
    python -m src.dispatcher backfill --collectors cftc_cot eia_ethanol
    python -m src.dispatcher backfill --since 7             # Last 7 days
    python -m src.dispatcher backfill --resume              # Resume interrupted
    python -m src.dispatcher backfill --dry-run             # Show plan only
"""

import json
import logging
import signal
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any

from src.dispatcher.collector_registry import CollectorRegistry
from src.dispatcher.collector_runner import CollectorRunner

logger = logging.getLogger(__name__)

PROGRESS_FILE = Path(__file__).parent.parent.parent / "data" / "backfill_progress.json"


# ---------------------------------------------------------------------------
# Backfill Plan
# ---------------------------------------------------------------------------

@dataclass
class BackfillTask:
    """A single unit of backfill work."""
    collector: str
    strategy: str          # 'single' or 'annual'
    start_year: int
    end_year: int = None   # defaults to current year
    chunk_delay: int = 5   # seconds between annual chunks
    kwargs: Dict = field(default_factory=dict)
    notes: str = ""

    def __post_init__(self):
        if self.end_year is None:
            self.end_year = date.today().year


# Tier 1 — Prices (run first)
TIER_1: List[BackfillTask] = [
    BackfillTask(
        collector='yfinance_futures',
        strategy='single',
        start_year=2000,
        notes='10 commodity futures, free via yfinance',
    ),
    BackfillTask(
        collector='usda_ams_cash_prices',
        strategy='annual',
        start_year=2000,
        chunk_delay=10,
        notes='MARS API, 10s between yearly chunks',
    ),
    BackfillTask(
        collector='eia_petroleum',
        strategy='single',
        start_year=1990,
        notes='15 series, one call each',
    ),
]

# Tier 2 — Primary S&D (fastest, most critical)
TIER_2: List[BackfillTask] = [
    BackfillTask(
        collector='cftc_cot',
        strategy='single',
        start_year=2006,
        kwargs={'limit_per_contract': 5000},
        notes='6 contracts, limit_per_contract=5000',
    ),
    BackfillTask(
        collector='usda_fas_export_sales',
        strategy='annual',
        start_year=2020,
        chunk_delay=5,
        notes='Only current+prior MY reliable',
    ),
    BackfillTask(
        collector='epa_rfs',
        strategy='single',
        start_year=2010,
        notes='Bulk Excel download',
    ),
]

# Tier 3 — Secondary S&D
TIER_3: List[BackfillTask] = [
    BackfillTask(
        collector='usda_nass_crop_progress',
        strategy='annual',
        start_year=2000,
        chunk_delay=5,
        kwargs={},
        notes='5 commodities x 26 years at 30/min',
    ),
    BackfillTask(
        collector='eia_ethanol',
        strategy='single',
        start_year=2010,
        notes='9 series',
    ),
    BackfillTask(
        collector='census_trade',
        strategy='annual',
        start_year=2013,
        chunk_delay=30,
        notes='1,560 calls, 30s between years',
    ),
    BackfillTask(
        collector='mpob',
        strategy='single',
        start_year=2010,
        notes='Monthly palm oil',
    ),
    BackfillTask(
        collector='drought_monitor',
        strategy='annual',
        start_year=2000,
        chunk_delay=5,
        notes='Weekly drought data',
    ),
    BackfillTask(
        collector='canada_cgc',
        strategy='annual',
        start_year=2010,
        chunk_delay=10,
        notes='Weekly Canadian grain',
    ),
    BackfillTask(
        collector='canada_statscan',
        strategy='annual',
        start_year=2010,
        chunk_delay=5,
        notes='Monthly Canadian ag',
    ),
]

TIERS = {1: TIER_1, 2: TIER_2, 3: TIER_3}

# Collectors excluded with reasons (for --dry-run display)
EXCLUDED = {
    'cme_settlements': 'CME blocks scraping, returns nulls',
    'futures_overnight': 'Requires IBKR gateway running',
    'futures_us_session': 'Requires IBKR gateway running',
    'futures_settlement': 'Requires IBKR gateway running',
    'usda_wasde': 'Use scripts/backfill_fas_psd.py instead',
    'usda_ams_feedstocks': 'Text reports, current week only',
    'usda_ams_ddgs': 'Text reports, current week only',
}


# ---------------------------------------------------------------------------
# Progress Tracker
# ---------------------------------------------------------------------------

class ProgressTracker:
    """Tracks completed backfill task keys in a JSON file."""

    def __init__(self, path: Path = PROGRESS_FILE):
        self.path = path
        self.data: Dict[str, Any] = self._load()

    def _load(self) -> Dict:
        if self.path.exists():
            try:
                return json.loads(self.path.read_text())
            except (json.JSONDecodeError, OSError):
                return {"completed": {}, "started_at": None}
        return {"completed": {}, "started_at": None}

    def save(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(self.data, indent=2, default=str))

    def is_done(self, task_key: str) -> bool:
        return task_key in self.data.get("completed", {})

    def mark_done(self, task_key: str, rows: int = 0):
        if "completed" not in self.data:
            self.data["completed"] = {}
        self.data["completed"][task_key] = {
            "finished_at": datetime.now().isoformat(),
            "rows": rows,
        }
        self.save()

    def mark_started(self):
        self.data["started_at"] = datetime.now().isoformat()
        self.save()

    def get_summary(self) -> Dict:
        completed = self.data.get("completed", {})
        total_rows = sum(v.get("rows", 0) for v in completed.values())
        return {
            "tasks_completed": len(completed),
            "total_rows": total_rows,
            "started_at": self.data.get("started_at"),
        }


# ---------------------------------------------------------------------------
# BackfillRunner
# ---------------------------------------------------------------------------

class BackfillRunner:
    """
    Orchestrates historical data backfill through the standard
    CollectorRunner pipeline.
    """

    def __init__(self, delay_override: int = None):
        self.registry = CollectorRegistry()
        self.runner = CollectorRunner(self.registry)
        self.progress = ProgressTracker()
        self.delay_override = delay_override
        self._interrupted = False

        # Graceful Ctrl+C handling
        self._orig_sigint = signal.getsignal(signal.SIGINT)
        signal.signal(signal.SIGINT, self._handle_interrupt)

    def _handle_interrupt(self, sig, frame):
        """Save progress on Ctrl+C, exit cleanly on second press."""
        if self._interrupted:
            print("\nForce quit.")
            sys.exit(1)
        self._interrupted = True
        print("\n\nInterrupted — saving progress. Run with --resume to continue.")
        self.progress.save()

    def cleanup(self):
        """Restore original signal handler."""
        signal.signal(signal.SIGINT, self._orig_sigint)

    def get_tasks(
        self,
        tiers: List[int] = None,
        collectors: List[str] = None,
        since_days: int = None,
    ) -> List[Dict]:
        """
        Build the flat task list from the backfill plan.

        Returns list of dicts with keys:
            task_key, collector, year (or None), kwargs, chunk_delay, tier, notes
        """
        # Select tiers
        if tiers:
            selected = []
            for t in tiers:
                selected.extend(TIERS.get(t, []))
        else:
            selected = TIER_1 + TIER_2 + TIER_3

        # Filter by specific collectors
        if collectors:
            selected = [t for t in selected if t.collector in collectors]

        tasks = []
        current_year = date.today().year

        for bt in selected:
            tier_num = next(
                (k for k, v in TIERS.items() if bt in v), 0
            )

            if since_days is not None:
                # Disaster recovery mode: single recent-window task
                task_key = bt.collector
                kwargs = dict(bt.kwargs)
                kwargs['start_date'] = date.today() - timedelta(days=since_days)
                tasks.append({
                    'task_key': task_key,
                    'collector': bt.collector,
                    'year': None,
                    'kwargs': kwargs,
                    'chunk_delay': 2,
                    'tier': tier_num,
                    'notes': f"disaster recovery: last {since_days} days",
                })
            elif bt.strategy == 'single':
                task_key = bt.collector
                kwargs = dict(bt.kwargs)
                if bt.start_year:
                    kwargs['start_date'] = date(bt.start_year, 1, 1)
                tasks.append({
                    'task_key': task_key,
                    'collector': bt.collector,
                    'year': None,
                    'kwargs': kwargs,
                    'chunk_delay': bt.chunk_delay,
                    'tier': tier_num,
                    'notes': bt.notes,
                })
            elif bt.strategy == 'annual':
                for year in range(bt.start_year, current_year + 1):
                    task_key = f"{bt.collector}::{year}"
                    kwargs = dict(bt.kwargs)
                    # NASS needs year= kwarg, others use start_date/end_date
                    if bt.collector == 'usda_nass_crop_progress':
                        kwargs['year'] = year
                    else:
                        kwargs['start_date'] = date(year, 1, 1)
                        kwargs['end_date'] = date(year, 12, 31)
                    delay = self.delay_override if self.delay_override is not None else bt.chunk_delay
                    tasks.append({
                        'task_key': task_key,
                        'collector': bt.collector,
                        'year': year,
                        'kwargs': kwargs,
                        'chunk_delay': delay,
                        'tier': tier_num,
                        'notes': bt.notes,
                    })

        return tasks

    def dry_run(self, tasks: List[Dict], resume: bool = False):
        """Print the backfill plan without executing."""
        print("\n=== Backfill Plan (DRY RUN) ===\n")

        current_tier = None
        total = 0
        skip_count = 0

        for t in tasks:
            if t['tier'] != current_tier:
                current_tier = t['tier']
                print(f"\n--- Tier {current_tier} ---")

            is_done = self.progress.is_done(t['task_key']) if resume else False
            registered = self.registry.is_registered(t['collector'])
            status_parts = []
            if is_done:
                status_parts.append("DONE")
                skip_count += 1
            if not registered:
                status_parts.append("NOT REGISTERED")
            status = f" [{', '.join(status_parts)}]" if status_parts else ""

            year_str = f" ({t['year']})" if t['year'] else ""
            kwargs_str = ""
            if t['kwargs']:
                filtered = {k: v for k, v in t['kwargs'].items()
                            if k not in ('start_date', 'end_date', 'year')}
                if filtered:
                    kwargs_str = f"  kwargs={filtered}"

            print(f"  {t['task_key']:<40}{year_str}{status}")
            if kwargs_str:
                print(f"    {kwargs_str}")

            total += 1

        print(f"\n--- Summary ---")
        print(f"  Total tasks:     {total}")
        if resume:
            print(f"  Already done:    {skip_count}")
            print(f"  Remaining:       {total - skip_count}")

        print(f"\n--- Excluded Collectors ---")
        for name, reason in EXCLUDED.items():
            print(f"  {name:<30} {reason}")

        print()

    def run(
        self,
        tiers: List[int] = None,
        collectors: List[str] = None,
        since_days: int = None,
        resume: bool = False,
        dry_run: bool = False,
    ) -> Dict:
        """
        Execute the backfill plan.

        Returns summary dict with counts of success/fail/skipped.
        """
        tasks = self.get_tasks(tiers=tiers, collectors=collectors, since_days=since_days)

        if dry_run:
            self.dry_run(tasks, resume=resume)
            return {"dry_run": True, "total_tasks": len(tasks)}

        self.progress.mark_started()

        # Log backfill start event
        try:
            from src.services.database.db_config import get_connection
            with get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT core.log_event(%s, %s, %s, %s, %s)",
                    ('backfill_started', 'backfill',
                     f"Backfill started: {len(tasks)} tasks",
                     json.dumps({
                         'total_tasks': len(tasks),
                         'tiers': tiers,
                         'collectors': collectors,
                         'since_days': since_days,
                         'resume': resume,
                     }),
                     3)
                )
                conn.commit()
        except Exception as e:
            logger.warning(f"Could not log backfill start: {e}")

        results = {"success": 0, "failed": 0, "skipped": 0, "total_rows": 0}
        total = len(tasks)

        print(f"\n=== Starting Backfill: {total} tasks ===\n")

        for i, task in enumerate(tasks, 1):
            if self._interrupted:
                print(f"\nStopped at task {i}/{total}. Use --resume to continue.")
                break

            task_key = task['task_key']

            # Skip completed tasks in resume mode
            if resume and self.progress.is_done(task_key):
                results["skipped"] += 1
                continue

            # Skip unregistered collectors
            if not self.registry.is_registered(task['collector']):
                logger.warning(f"Skipping unregistered: {task['collector']}")
                results["skipped"] += 1
                continue

            year_str = f" ({task['year']})" if task['year'] else ""
            print(f"[{i}/{total}] {task['collector']}{year_str}...", end=" ", flush=True)

            try:
                result = self.runner.run_collector(
                    task['collector'],
                    triggered_by='backfill',
                    **task['kwargs'],
                )

                if result.success:
                    rows = result.rows_collected
                    elapsed = (result.finished_at - result.started_at).total_seconds()
                    print(f"OK ({rows} rows, {elapsed:.1f}s)")
                    results["success"] += 1
                    results["total_rows"] += rows
                    self.progress.mark_done(task_key, rows)
                else:
                    print(f"FAILED: {result.error_message}")
                    results["failed"] += 1
                    # Don't mark as done — will retry on --resume

            except Exception as e:
                print(f"ERROR: {e}")
                results["failed"] += 1
                logger.error(f"Backfill task {task_key} exception: {e}", exc_info=True)

            # Inter-chunk delay
            if i < total and not self._interrupted:
                delay = task['chunk_delay']
                if delay > 0:
                    time.sleep(delay)

        # Final summary
        self.cleanup()
        print(f"\n=== Backfill Complete ===")
        print(f"  Success:  {results['success']}")
        print(f"  Failed:   {results['failed']}")
        print(f"  Skipped:  {results['skipped']}")
        print(f"  Rows:     {results['total_rows']:,}")

        # Log completion event
        try:
            from src.services.database.db_config import get_connection
            with get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT core.log_event(%s, %s, %s, %s, %s)",
                    ('backfill_complete', 'backfill',
                     f"Backfill complete: {results['success']} ok, "
                     f"{results['failed']} failed, {results['total_rows']:,} rows",
                     json.dumps(results),
                     3)
                )
                conn.commit()
        except Exception as e:
            logger.warning(f"Could not log backfill completion: {e}")

        return results
