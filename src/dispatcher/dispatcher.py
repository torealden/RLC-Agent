"""
Dispatcher Daemon

APScheduler-based daemon that reads RELEASE_SCHEDULES from
MasterScheduler and fires collectors at their scheduled times.

Usage:
    dispatcher = Dispatcher()
    dispatcher.start()       # Start background scheduler
    dispatcher.stop()        # Graceful shutdown
    dispatcher.run_collector('cftc_cot')   # Manual run
    dispatcher.run_todays_collectors()      # Run all scheduled for today
"""

import json
import logging
import os
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Optional, Any

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from src.schedulers.master_scheduler import (
    RELEASE_SCHEDULES,
    ReleaseFrequency,
    DayOfWeek,
    ReportScheduler,
)
from src.dispatcher.collector_registry import CollectorRegistry
from src.dispatcher.collector_runner import CollectorRunner, CollectorRunResult

logger = logging.getLogger(__name__)

HEARTBEAT_FILE = Path(__file__).resolve().parent.parent.parent / 'scripts' / 'deployment' / 'dispatcher_heartbeat.json'

# APScheduler day-of-week mapping
DOW_MAP = {
    DayOfWeek.MONDAY: 'mon',
    DayOfWeek.TUESDAY: 'tue',
    DayOfWeek.WEDNESDAY: 'wed',
    DayOfWeek.THURSDAY: 'thu',
    DayOfWeek.FRIDAY: 'fri',
    DayOfWeek.SATURDAY: 'sat',
    DayOfWeek.SUNDAY: 'sun',
}


class Dispatcher:
    """
    The CNS Dispatcher Daemon.

    Reads RELEASE_SCHEDULES from master_scheduler.py and registers
    APScheduler jobs to fire collector_runner at the right times.
    """

    def __init__(self):
        self.scheduler = BackgroundScheduler(
            timezone='America/New_York',
            job_defaults={
                'coalesce': True,       # If multiple fires missed, run once
                'max_instances': 1,      # Don't overlap same collector
                'misfire_grace_time': 3600,  # 1 hour grace for missed jobs
            }
        )
        self.registry = CollectorRegistry()
        self.runner = CollectorRunner(self.registry)
        self.report_scheduler = ReportScheduler()
        self._running = False

    def start(self):
        """Start the dispatcher daemon."""
        if self._running:
            logger.warning("Dispatcher already running")
            return

        logger.info("Starting RLC-Agent Dispatcher Daemon...")
        self._register_all_jobs()
        self.scheduler.start()
        self._running = True
        self._write_heartbeat()  # Immediate heartbeat on startup

        jobs = self.scheduler.get_jobs()
        logger.info(f"Dispatcher started with {len(jobs)} scheduled jobs")

        # Log startup event
        try:
            from src.services.database.db_config import get_connection
            import json
            with get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT core.log_event(%s, %s, %s, %s, %s)",
                    ('system_startup', 'dispatcher',
                     f"Dispatcher started with {len(jobs)} scheduled collectors",
                     json.dumps({'job_count': len(jobs),
                                 'collectors': [j.id for j in jobs]}),
                     3)
                )
                conn.commit()
        except Exception as e:
            logger.warning(f"Could not log startup event: {e}")

    def stop(self):
        """Gracefully stop the dispatcher."""
        if not self._running:
            return

        logger.info("Stopping dispatcher...")
        self.scheduler.shutdown(wait=True)
        self._running = False
        HEARTBEAT_FILE.unlink(missing_ok=True)
        logger.info("Dispatcher stopped")

    def _register_all_jobs(self):
        """Register APScheduler jobs for all enabled collectors."""
        for schedule_key, schedule in RELEASE_SCHEDULES.items():
            if not schedule.enabled:
                logger.debug(f"Skipping disabled collector: {schedule_key}")
                continue

            if not self.registry.is_registered(schedule_key):
                logger.debug(f"Skipping unregistered collector: {schedule_key}")
                continue

            release = schedule.release_schedule

            trigger = self._build_trigger(release)
            if trigger is None:
                logger.debug(f"No trigger for {schedule_key} (on-demand)")
                continue

            self.scheduler.add_job(
                func=self._run_scheduled_collector,
                trigger=trigger,
                id=schedule_key,
                name=f"Collect: {schedule.collector_name}",
                kwargs={
                    'schedule_key': schedule_key,
                    'max_retries': schedule.retry_attempts,
                    'retry_delay_minutes': schedule.retry_delay_minutes,
                    'commodities': schedule.commodities,
                },
                replace_existing=True,
            )

            try:
                job = self.scheduler.get_job(schedule_key)
                next_run = getattr(job, 'next_run_time', None)
                logger.debug(f"Registered: {schedule_key} -> next run: {next_run}")
            except Exception:
                logger.debug(f"Registered: {schedule_key}")

        # Register daily overdue check at 8:00 AM ET (Mon-Fri)
        self.scheduler.add_job(
            func=self._check_overdue_data,
            trigger=CronTrigger(
                day_of_week='mon-fri',
                hour=8,
                minute=0,
            ),
            id='_overdue_check',
            name='Daily Overdue Data Check',
            replace_existing=True,
        )
        logger.debug("Registered daily overdue check at 08:00 ET (Mon-Fri)")

        # Register failure alert check every 2 hours (Mon-Fri, 7 AM - 9 PM)
        self.scheduler.add_job(
            func=self._check_failure_alerts,
            trigger=CronTrigger(
                day_of_week='mon-fri',
                hour='7,9,11,13,15,17,19,21',
                minute=15,
            ),
            id='_failure_alert_check',
            name='Collector Failure Alert Check',
            replace_existing=True,
        )
        logger.debug("Registered failure alert check every 2h (Mon-Fri)")

        # Register heartbeat every 60 minutes so the watchdog can detect a zombie
        self.scheduler.add_job(
            func=self._write_heartbeat,
            trigger=IntervalTrigger(minutes=60),
            id='_heartbeat',
            name='Dispatcher Heartbeat',
            replace_existing=True,
        )
        logger.debug("Registered heartbeat (every 60 min)")

    def _write_heartbeat(self):
        """Write a heartbeat timestamp so the watchdog can detect a zombie dispatcher."""
        try:
            jobs = self.scheduler.get_jobs()
            payload = {
                'timestamp': datetime.now().isoformat(),
                'pid': os.getpid(),
                'job_count': len(jobs),
                'running': self._running,
            }
            HEARTBEAT_FILE.write_text(json.dumps(payload))
        except Exception as e:
            logger.warning(f"Failed to write heartbeat: {e}")

    def _check_failure_alerts(self):
        """
        Check for collectors with 3+ consecutive failures and send email alert.

        Queries event_log for recent collection_error events, groups by source,
        and alerts if any collector has failed 3+ times in a row without a
        success in between. Deduplicates: only alerts once per collector per day.
        """
        logger.info("Running failure alert check...")

        try:
            from src.services.database.db_config import get_connection
            import json as _json

            with get_connection() as conn:
                cursor = conn.cursor()

                # Find collectors with 3+ consecutive failures (no success in between)
                cursor.execute("""
                    WITH recent_events AS (
                        SELECT source, event_type, event_time,
                               ROW_NUMBER() OVER (PARTITION BY source ORDER BY event_time DESC) AS rn
                        FROM core.event_log
                        WHERE event_type IN ('collection_success', 'collection_error')
                          AND event_time > NOW() - INTERVAL '7 days'
                    ),
                    streaks AS (
                        SELECT source,
                               COUNT(*) FILTER (WHERE event_type = 'collection_error') AS consecutive_failures
                        FROM recent_events
                        WHERE rn <= (
                            SELECT COALESCE(MIN(rn2.rn) - 1, 10)
                            FROM recent_events rn2
                            WHERE rn2.source = recent_events.source
                              AND rn2.event_type = 'collection_success'
                        )
                        GROUP BY source
                    )
                    SELECT source, consecutive_failures
                    FROM streaks
                    WHERE consecutive_failures >= 3
                    ORDER BY consecutive_failures DESC
                """)
                failing = cursor.fetchall()

                if not failing:
                    logger.info("No collectors with 3+ consecutive failures")
                    return

                # Check which ones we've already alerted today
                alert_needed = []
                for row in failing:
                    collector = row['source']
                    cursor.execute("""
                        SELECT COUNT(*) AS cnt FROM core.event_log
                        WHERE event_type = 'failure_alert'
                          AND source = %s
                          AND event_time::date = CURRENT_DATE
                    """, (collector,))
                    if cursor.fetchone()['cnt'] == 0:
                        alert_needed.append({
                            'collector': collector,
                            'failures': row['consecutive_failures'],
                        })

                if not alert_needed:
                    logger.info("All failure alerts already sent today")
                    return

                # Build alert message
                lines = ["RLC-Agent Collector Failure Alert\n"]
                lines.append(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M ET')}\n")
                for item in alert_needed:
                    lines.append(
                        f"  - {item['collector']}: {item['failures']} consecutive failures"
                    )
                alert_text = '\n'.join(lines)

                # Log the alert events
                for item in alert_needed:
                    cursor.execute(
                        "SELECT core.log_event(%s, %s, %s, %s, %s)",
                        ('failure_alert', item['collector'],
                         f"{item['collector']} has failed {item['failures']} times in a row",
                         _json.dumps(item, default=str), 1)
                    )
                conn.commit()

                logger.warning(f"Failure alert: {len(alert_needed)} collectors failing")

                # Send email (best effort)
                try:
                    from dotenv import load_dotenv
                    load_dotenv()
                    from src.agents.publishing.channels.email_channel import send_email

                    html_body = "<h2>RLC-Agent Collector Failure Alert</h2><ul>"
                    for item in alert_needed:
                        html_body += (
                            f"<li><b>{item['collector']}</b>: "
                            f"{item['failures']} consecutive failures</li>"
                        )
                    html_body += "</ul><p>Check the ops dashboard or event log for details.</p>"

                    send_email(
                        subject=f"[RLC Alert] {len(alert_needed)} collector(s) failing",
                        html_body=html_body,
                        text_body=alert_text,
                    )
                    logger.info("Failure alert email sent")
                except Exception as e:
                    logger.warning(f"Could not send failure alert email: {e}")

        except Exception as e:
            logger.error(f"Failure alert check failed: {e}", exc_info=True)

    def _check_overdue_data(self):
        """
        Check for overdue data collectors and log alerts to event_log.

        Runs daily at 8:00 AM ET. Queries core.data_freshness for
        is_overdue=TRUE and logs a schedule_overdue event for each,
        so the LLM briefing surfaces stale data alerts.
        Deduplicates: only logs once per collector per day.
        """
        logger.info("Running daily overdue data check...")

        try:
            from src.services.database.db_config import get_connection
            import json as _json

            with get_connection() as conn:
                cursor = conn.cursor()

                cursor.execute("""
                    SELECT collector_name, display_name, category,
                           last_collected, hours_since_collection,
                           expected_frequency, expected_release_day
                    FROM core.data_freshness
                    WHERE is_overdue = TRUE
                """)
                overdue_rows = cursor.fetchall()

                if not overdue_rows:
                    logger.info("No overdue collectors found")
                    return

                logger.warning(f"Found {len(overdue_rows)} overdue collectors")

                for row in overdue_rows:
                    collector = row['collector_name']

                    # Check if already logged today (dedup)
                    cursor.execute("""
                        SELECT COUNT(*) AS cnt FROM core.event_log
                        WHERE event_type = 'schedule_overdue'
                          AND source = %s
                          AND event_time::date = CURRENT_DATE
                    """, (collector,))
                    if cursor.fetchone()['cnt'] > 0:
                        logger.debug(f"Already logged overdue for {collector} today")
                        continue

                    hours = round(row['hours_since_collection'], 1) if row['hours_since_collection'] else None
                    display = row.get('display_name') or collector
                    summary = (
                        f"{display} is OVERDUE -- "
                        f"last collected {hours}h ago "
                        f"(expected: {row.get('expected_frequency', '?')})"
                    )
                    details = {
                        'collector_name': collector,
                        'display_name': row.get('display_name'),
                        'category': row.get('category'),
                        'hours_since_collection': hours,
                        'expected_frequency': row.get('expected_frequency'),
                        'expected_release_day': row.get('expected_release_day'),
                        'last_collected': row['last_collected'].isoformat() if row.get('last_collected') else None,
                    }

                    cursor.execute(
                        "SELECT core.log_event(%s, %s, %s, %s, %s)",
                        ('schedule_overdue', collector, summary,
                         _json.dumps(details, default=str), 2)
                    )
                    logger.info(f"Logged overdue alert: {collector}")

                conn.commit()

        except Exception as e:
            logger.error(f"Overdue check failed: {e}", exc_info=True)

    def _build_trigger(self, release) -> Optional[CronTrigger]:
        """Build an APScheduler CronTrigger from a ReleaseSchedule."""
        if release.frequency == ReleaseFrequency.ON_DEMAND:
            return None

        hour = release.release_time.hour if release.release_time else 12
        minute = release.release_time.minute if release.release_time else 0

        if release.frequency == ReleaseFrequency.DAILY:
            return CronTrigger(
                day_of_week='mon-fri',
                hour=hour,
                minute=minute,
            )

        elif release.frequency == ReleaseFrequency.WEEKLY:
            if release.day_of_week is None:
                return None
            dow = DOW_MAP.get(release.day_of_week, 'mon')
            return CronTrigger(
                day_of_week=dow,
                hour=hour,
                minute=minute,
            )

        elif release.frequency == ReleaseFrequency.MONTHLY:
            # Use exact release dates if available for this year.
            # Fire on all possible days (the job function guards against
            # non-release days via _is_release_day check).
            if release.release_dates:
                from datetime import date as _date
                year = _date.today().year
                year_dates = release.release_dates.get(year, [])
                if year_dates:
                    possible_days = sorted(set(d.day for d in year_dates))
                    day_str = ','.join(str(d) for d in possible_days)
                    return CronTrigger(
                        day=day_str,
                        hour=hour,
                        minute=minute,
                    )

            day = release.day_of_month or 15
            if day < 0:
                # APScheduler doesn't support negative days directly;
                # use "last" for -1, otherwise approximate
                day = 28 + day + 1  # -5 → day 24
            return CronTrigger(
                day=day,
                hour=hour,
                minute=minute,
            )

        elif release.frequency == ReleaseFrequency.QUARTERLY:
            day = release.day_of_month or 1
            return CronTrigger(
                month='1,3,6,9',
                day=day,
                hour=hour,
                minute=minute,
            )

        return None

    def _run_scheduled_collector(
        self,
        schedule_key: str,
        max_retries: int = 3,
        retry_delay_minutes: int = 15,
        commodities: List[str] = None,
    ):
        """Called by APScheduler — runs a collector with retry.

        For collectors with exact release_dates, verify today is actually
        a release day (the CronTrigger fires on all possible days in the
        range, so we need to guard here).
        """
        from datetime import date as _date

        schedule = RELEASE_SCHEDULES.get(schedule_key)
        if schedule and schedule.release_schedule.release_dates:
            today = _date.today()
            year_dates = schedule.release_schedule.release_dates.get(today.year, [])
            if year_dates and today not in year_dates:
                logger.debug(
                    f"Skipping {schedule_key}: {today} is not a release day "
                    f"(next: {min((d for d in year_dates if d >= today), default='?')})"
                )
                return

        logger.info(f"Scheduler firing: {schedule_key}")
        self.runner.run_with_retry(
            collector_name=schedule_key,
            max_retries=max_retries,
            retry_delay_minutes=retry_delay_minutes,
            triggered_by='scheduler',
            commodities=commodities,
        )

    def run_collector(self, name: str, **kwargs) -> CollectorRunResult:
        """
        Run a single collector on demand.

        Args:
            name: Collector schedule key
            **kwargs: Passed to collector.collect()

        Returns:
            CollectorRunResult
        """
        return self.runner.run_collector(name, triggered_by='manual', **kwargs)

    def run_todays_collectors(self) -> List[CollectorRunResult]:
        """
        Run all collectors scheduled for today, immediately.

        Returns:
            List of CollectorRunResult for each collector run
        """
        scheduled = self.report_scheduler.get_todays_collections()
        results = []

        if not scheduled:
            logger.info("No collectors scheduled for today")
            return results

        logger.info(f"Running {len(scheduled)} collectors scheduled for today")

        for schedule in scheduled:
            schedule_key = None
            # Find the schedule key by matching collector_name
            for key, sched in RELEASE_SCHEDULES.items():
                if sched.collector_name == schedule.collector_name:
                    schedule_key = key
                    break

            if schedule_key is None:
                logger.warning(f"No schedule key found for {schedule.collector_name}")
                continue

            if not self.registry.is_registered(schedule_key):
                logger.debug(f"Skipping unregistered: {schedule_key}")
                continue

            result = self.runner.run_with_retry(
                collector_name=schedule_key,
                max_retries=schedule.retry_attempts,
                retry_delay_minutes=schedule.retry_delay_minutes,
                triggered_by='manual',
                commodities=schedule.commodities,
            )
            results.append(result)

        successes = sum(1 for r in results if r.success)
        logger.info(f"Today's collection complete: {successes}/{len(results)} succeeded")

        return results

    def get_status(self) -> Dict[str, Any]:
        """Get dispatcher status including all job states."""
        jobs = self.scheduler.get_jobs() if self._running else []

        job_info = []
        for job in jobs:
            job_info.append({
                'id': job.id,
                'name': job.name,
                'next_run': str(job.next_run_time) if job.next_run_time else None,
                'trigger': str(job.trigger),
            })

        return {
            'running': self._running,
            'job_count': len(jobs),
            'jobs': job_info,
            'registered_collectors': len(self.registry.list_collectors()),
        }

    def get_schedule_summary(self) -> str:
        """Get a human-readable summary of the schedule."""
        weekly = self.report_scheduler.get_weekly_schedule()
        lines = ["=== RLC-Agent Collection Schedule ===\n"]

        for day_name, collections in weekly.items():
            if not collections:
                continue
            lines.append(f"{day_name}:")
            for c in collections:
                time_str = (c.release_schedule.release_time.strftime('%H:%M')
                            if c.release_schedule.release_time else '-----')
                registered = '[OK]' if self.registry.is_registered(
                    # Find schedule key for this collector
                    next((k for k, v in RELEASE_SCHEDULES.items()
                          if v.collector_name == c.collector_name), '')
                ) else '[--]'
                lines.append(f"  {time_str}  [{c.priority}] {c.collector_name} {registered}")
            lines.append("")

        return '\n'.join(lines)
