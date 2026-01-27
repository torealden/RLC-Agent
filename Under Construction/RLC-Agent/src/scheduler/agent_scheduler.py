"""
RLC Agent Scheduler
====================
Central scheduler for all RLC data collection agents.
Tracks release calendars and triggers agents automatically.

Features:
- USDA report release calendar
- Configurable schedules per agent
- Automatic retries on failure
- Logging and notifications

Usage:
  python agent_scheduler.py run          # Start the scheduler daemon
  python agent_scheduler.py list         # List all scheduled jobs
  python agent_scheduler.py next         # Show next 10 upcoming jobs
  python agent_scheduler.py trigger <agent>  # Manually trigger an agent
"""

import json
import os
import sys
import subprocess
import logging
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Optional, Dict, List, Callable
import time

# Try to import schedule library
try:
    import schedule
except ImportError:
    print("Installing 'schedule' library...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "schedule"])
    import schedule

# =============================================================================
# US Federal Holidays (for schedule shifting)
# =============================================================================
# When a federal holiday falls on Monday, USDA reports shift to Tuesday

def get_federal_holidays(year: int) -> List[date]:
    """
    Returns list of US federal holidays for a given year.
    These cause USDA Monday reports to shift to Tuesday.
    """
    holidays = []

    # New Year's Day - Jan 1 (observed Mon if falls on Sun)
    nyd = date(year, 1, 1)
    if nyd.weekday() == 6:  # Sunday
        holidays.append(date(year, 1, 2))
    else:
        holidays.append(nyd)

    # MLK Day - 3rd Monday of January
    jan1 = date(year, 1, 1)
    first_monday = jan1 + timedelta(days=(7 - jan1.weekday()) % 7)
    holidays.append(first_monday + timedelta(weeks=2))

    # Presidents Day - 3rd Monday of February
    feb1 = date(year, 2, 1)
    first_monday = feb1 + timedelta(days=(7 - feb1.weekday()) % 7)
    holidays.append(first_monday + timedelta(weeks=2))

    # Memorial Day - Last Monday of May
    may31 = date(year, 5, 31)
    holidays.append(may31 - timedelta(days=(may31.weekday())))

    # Juneteenth - June 19 (observed Mon if falls on Sun)
    june19 = date(year, 6, 19)
    if june19.weekday() == 6:
        holidays.append(date(year, 6, 20))
    elif june19.weekday() == 5:
        holidays.append(date(year, 6, 18))
    else:
        holidays.append(june19)

    # Independence Day - July 4 (observed Mon if falls on Sun)
    july4 = date(year, 7, 4)
    if july4.weekday() == 6:
        holidays.append(date(year, 7, 5))
    elif july4.weekday() == 5:
        holidays.append(date(year, 7, 3))
    else:
        holidays.append(july4)

    # Labor Day - 1st Monday of September
    sep1 = date(year, 9, 1)
    first_monday = sep1 + timedelta(days=(7 - sep1.weekday()) % 7)
    holidays.append(first_monday)

    # Columbus Day - 2nd Monday of October
    oct1 = date(year, 10, 1)
    first_monday = oct1 + timedelta(days=(7 - oct1.weekday()) % 7)
    holidays.append(first_monday + timedelta(weeks=1))

    # Veterans Day - Nov 11 (observed Mon if falls on Sun)
    nov11 = date(year, 11, 11)
    if nov11.weekday() == 6:
        holidays.append(date(year, 11, 12))
    elif nov11.weekday() == 5:
        holidays.append(date(year, 11, 10))
    else:
        holidays.append(nov11)

    # Thanksgiving - 4th Thursday of November
    nov1 = date(year, 11, 1)
    first_thursday = nov1 + timedelta(days=(3 - nov1.weekday()) % 7)
    holidays.append(first_thursday + timedelta(weeks=3))

    # Christmas - Dec 25 (observed Mon if falls on Sun)
    dec25 = date(year, 12, 25)
    if dec25.weekday() == 6:
        holidays.append(date(year, 12, 26))
    elif dec25.weekday() == 5:
        holidays.append(date(year, 12, 24))
    else:
        holidays.append(dec25)

    return holidays


def is_federal_holiday(check_date: date) -> bool:
    """Check if a date is a US federal holiday."""
    holidays = get_federal_holidays(check_date.year)
    return check_date in holidays


def is_monday_holiday_week() -> bool:
    """Check if this Monday was a federal holiday (reports shift to Tuesday)."""
    today = date.today()
    # Find this week's Monday
    days_since_monday = today.weekday()
    this_monday = today - timedelta(days=days_since_monday)
    return is_federal_holiday(this_monday)


def has_holiday_before_in_week(target_weekday: int, check_date: date = None) -> bool:
    """
    Check if any federal holiday falls before the target weekday in that week.

    USDA Rule: If any holiday falls BEFORE the scheduled release day in a week,
    the release shifts back by one day. For example, if Monday is MLK Day,
    Thursday's Export Sales shifts to Friday.

    Args:
        target_weekday: The weekday of the scheduled release (0=Mon, 1=Tue, etc.)
        check_date: The date to check (defaults to today)

    Returns:
        True if any holiday falls on Monday through (target_weekday - 1)
    """
    if check_date is None:
        check_date = date.today()

    # Find this week's Monday
    days_since_monday = check_date.weekday()
    this_monday = check_date - timedelta(days=days_since_monday)

    # Check each day from Monday up to (but not including) target_weekday
    for day_offset in range(target_weekday):  # 0 to target_weekday-1
        day_to_check = this_monday + timedelta(days=day_offset)
        if is_federal_holiday(day_to_check):
            return True

    return False

# Configuration
CONFIG_DIR = Path(os.environ.get("RLC_CONFIG_DIR", Path.home() / "rlc_scheduler" / "config"))
LOG_DIR = Path(os.environ.get("RLC_LOG_DIR", Path.home() / "rlc_scheduler" / "logs"))
AGENTS_DIR = Path(os.environ.get("RLC_AGENTS_DIR", Path.home() / "commodity_pipeline"))

# Setup logging
LOG_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / "scheduler.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("RLC_Scheduler")


# =============================================================================
# USDA Release Calendar
# =============================================================================
# Source: https://www.usda.gov/media/agency-reports

USDA_RELEASES = {
    "export_sales": {
        "name": "Weekly Export Sales",
        "schedule": "thursday_holiday_aware",  # Shifts to Friday if Thursday is federal holiday
        "time": "08:30",
        "timezone": "America/New_York",
        "agent": "usda_export_sales_agent",
        "description": "Weekly export sales data for grains and oilseeds"
    },
    "export_inspections": {
        "name": "US Export Inspections",
        "schedule": "monday_holiday_aware",  # Shifts to Tuesday if Monday is federal holiday
        "time": "10:00",
        "timezone": "America/New_York",
        "agent": "usda_export_inspections_agent",
        "description": "Weekly grain export inspections at ports"
    },
    "wasde": {
        "name": "WASDE Report",
        "schedule": "specific_dates",
        "dates_2026": [
            (1, 12),   # Jan 12
            (2, 10),   # Feb 10
            (3, 10),   # Mar 10
            (4, 9),    # Apr 9
            (5, 12),   # May 12
            (6, 11),   # Jun 11
            (7, 10),   # Jul 10
            (8, 12),   # Aug 12
            (9, 11),   # Sep 11
            (10, 9),   # Oct 9
            (11, 10),  # Nov 10
            (12, 10),  # Dec 10
        ],
        "time": "12:00",
        "timezone": "America/New_York",
        "agent": "usda_wasde_agent",
        "description": "World Agricultural Supply and Demand Estimates"
    },
    "crop_progress": {
        "name": "Crop Progress",
        "schedule": "monday_holiday_aware",  # Shifts to Tuesday if Monday is federal holiday
        "time": "16:00",
        "timezone": "America/New_York",
        "agent": "usda_crop_progress_agent",
        "description": "Weekly crop progress and condition",
        "seasonal": {"start_month": 4, "end_month": 11}  # April-November
    },
    "grain_stocks": {
        "name": "Grain Stocks",
        "schedule": "specific_dates",
        # March & June: last business day (coincides with Prospective Plantings / Acreage)
        # September: last day of month
        # December: published with January WASDE (same day, same time)
        "dates_2026": [
            (1, 12),   # Jan 12 - December edition (with January WASDE)
            (3, 31),   # Mar 31 - with Prospective Plantings (Tue, last biz day)
            (6, 30),   # Jun 30 - with Acreage (Tue, last biz day)
            (9, 30),   # Sep 30 - last day of September (Wed)
        ],
        "time": "12:00",
        "timezone": "America/New_York",
        "agent": "usda_grain_stocks_agent",
        "description": "Quarterly grain stocks report"
    },
    "prospective_plantings": {
        "name": "Prospective Plantings",
        "schedule": "specific_dates",
        # Last business day of March - coincides with March Grain Stocks
        "dates_2026": [
            (3, 31),   # Mar 31, 2026 (Tuesday)
        ],
        "time": "12:00",
        "timezone": "America/New_York",
        "agent": "usda_plantings_agent",
        "description": "Annual planting intentions survey"
    },
    "acreage": {
        "name": "Acreage",
        "schedule": "specific_dates",
        # Last business day of June - coincides with June Grain Stocks
        "dates_2026": [
            (6, 30),   # Jun 30, 2026 (Tuesday)
        ],
        "time": "12:00",
        "timezone": "America/New_York",
        "agent": "usda_acreage_agent",
        "description": "Annual planted acreage report"
    },
    "cattle_on_feed": {
        "name": "Cattle on Feed",
        "schedule": "specific_dates",
        "dates_2026": [
            (1, 23),   # Jan 23
            (2, 20),   # Feb 20
            (3, 20),   # Mar 20
            (4, 17),   # Apr 17
            (5, 22),   # May 22
            (6, 18),   # Jun 18
            (7, 24),   # Jul 24
            (8, 21),   # Aug 21
            (9, 18),   # Sep 18
            (10, 23),  # Oct 23
            (11, 20),  # Nov 20
            (12, 18),  # Dec 18
        ],
        "time": "15:00",
        "timezone": "America/New_York",
        "agent": "usda_cattle_agent",
        "description": "Monthly cattle on feed report - inventory, placements, marketings"
    }
}

# Other data source schedules
OTHER_SCHEDULES = {
    "census_trade": {
        "name": "Census Bureau Export Data",
        "schedule": "specific_dates",
        # Format: (month, day) - add dates as Census publishes their schedule
        # First month listed = data month, release date shown below
        "dates_2026": [
            (1, 29),   # Jan 29 - November 2025 data
            # Add more as Census publishes:
            # (2, ?),  # Feb ? - December 2025 data
            # (3, ?),  # Mar ? - January 2026 data
        ],
        "time": "08:30",
        "timezone": "America/New_York",
        "agent": "census_trade_agent",
        "description": "Monthly US trade data from Census Bureau"
    },
    "eia_petroleum": {
        "name": "EIA Petroleum Status",
        "schedule": "wednesday_holiday_aware",  # Shifts to Thursday if Wednesday is federal holiday
        "time": "10:30",
        "timezone": "America/New_York",
        "agent": "eia_petroleum_agent",
        "description": "Weekly petroleum status report"
    },
    "conab_brazil": {
        "name": "CONAB Brazil Crop",
        "schedule": "monthly",
        "day": 10,
        "time": "09:00",
        "timezone": "America/Sao_Paulo",
        "agent": "conab_agent",
        "description": "Brazilian crop estimates"
    },
    "cme_settlements": {
        "name": "CME Daily Settlements",
        "schedule": "weekdays_holiday_aware",  # Skips federal holidays
        "time": "17:00",
        "timezone": "America/Chicago",
        "agent": "cme_settlements_agent",
        "description": "Daily futures settlement prices"
    },
    "weather_email": {
        "name": "Weather Email Agent",
        "schedule": "weather_custom",  # Custom schedule for weather emails
        # Weekdays: 7:30 AM, 1:00 PM, 8:00 PM ET
        # Saturday: 12:00 PM ET
        # Sunday: 9:00 AM, 7:00 PM ET
        "weekday_times": ["07:30", "13:00", "20:00"],
        "saturday_times": ["12:00"],
        "sunday_times": ["09:00", "19:00"],
        "timezone": "America/New_York",
        "agent": "weather_email_agent",
        "agent_path": "rlc_scheduler/agents/weather_email_agent.py",
        "description": "Forward meteorologist emails and generate weather summaries"
    }
}


# =============================================================================
# Agent Execution
# =============================================================================

class AgentRunner:
    """Handles agent execution with logging and error handling."""

    def __init__(self, agents_dir: Path):
        self.agents_dir = agents_dir
        self.results_log = LOG_DIR / "agent_results.json"

    def run_agent(self, agent_name: str, agent_path: str = None, **kwargs) -> dict:
        """Execute an agent and return results."""
        logger.info(f"Triggering agent: {agent_name}")
        start_time = datetime.now()

        result = {
            "agent": agent_name,
            "triggered_at": start_time.isoformat(),
            "status": "unknown",
            "duration_seconds": 0,
            "output": None,
            "error": None
        }

        try:
            # If agent_path provided (relative to RLC-Agent), use it
            if agent_path:
                # Resolve relative to RLC-Agent home folder
                rlc_agent_home = Path(__file__).parent.parent  # rlc_scheduler -> RLC-Agent
                resolved_path = rlc_agent_home / agent_path
                if resolved_path.exists():
                    found_path = resolved_path
                else:
                    found_path = None
            else:
                # Look for agent in various locations
                search_paths = [
                    self.agents_dir / agent_name / "main.py",
                    self.agents_dir / f"{agent_name}.py",
                    self.agents_dir / agent_name / "run.py",
                    Path.home() / "RLC-Agent" / agent_name / "main.py",
                    Path(__file__).parent / "agents" / f"{agent_name}.py",  # rlc_scheduler/agents/
                ]

                found_path = None
                for path in search_paths:
                    if path.exists():
                        found_path = path
                        break

            agent_path = found_path

            if agent_path is None:
                # Agent not yet implemented - log for tracking
                result["status"] = "not_implemented"
                result["output"] = f"Agent '{agent_name}' not found. Add to build queue."
                logger.warning(f"Agent not found: {agent_name}")
            else:
                # Execute the agent
                proc = subprocess.run(
                    [sys.executable, str(agent_path)],
                    capture_output=True,
                    text=True,
                    timeout=300,  # 5 minute timeout
                    cwd=agent_path.parent
                )

                result["status"] = "success" if proc.returncode == 0 else "failed"
                result["output"] = proc.stdout
                result["error"] = proc.stderr if proc.returncode != 0 else None

                if proc.returncode == 0:
                    logger.info(f"Agent {agent_name} completed successfully")
                else:
                    logger.error(f"Agent {agent_name} failed: {proc.stderr}")

        except subprocess.TimeoutExpired:
            result["status"] = "timeout"
            result["error"] = "Agent execution timed out after 5 minutes"
            logger.error(f"Agent {agent_name} timed out")

        except Exception as e:
            result["status"] = "error"
            result["error"] = str(e)
            logger.exception(f"Error running agent {agent_name}")

        finally:
            result["duration_seconds"] = (datetime.now() - start_time).total_seconds()
            self._log_result(result)

        return result

    def _log_result(self, result: dict):
        """Append result to log file."""
        try:
            if self.results_log.exists():
                with open(self.results_log, "r") as f:
                    results = json.load(f)
            else:
                results = []

            results.append(result)

            # Keep last 1000 results
            results = results[-1000:]

            with open(self.results_log, "w") as f:
                json.dump(results, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to log result: {e}")


# =============================================================================
# Scheduler
# =============================================================================

class RLCScheduler:
    """Main scheduler for RLC agents."""

    def __init__(self):
        self.runner = AgentRunner(AGENTS_DIR)
        self.schedules = {**USDA_RELEASES, **OTHER_SCHEDULES}

    def setup_schedules(self):
        """Configure all scheduled jobs."""
        logger.info("Setting up schedules...")

        for schedule_id, config in self.schedules.items():
            self._add_schedule(schedule_id, config)

        logger.info(f"Configured {len(schedule.get_jobs())} scheduled jobs")

    def _add_schedule(self, schedule_id: str, config: dict):
        """Add a single schedule."""
        sched_type = config.get("schedule", "daily")
        time_str = config.get("time", "09:00")
        agent_name = config.get("agent", schedule_id)

        job_func = lambda a=agent_name: self.runner.run_agent(a)

        if sched_type == "daily":
            if config.get("weekdays_only"):
                schedule.every().monday.at(time_str).do(job_func).tag(schedule_id)
                schedule.every().tuesday.at(time_str).do(job_func).tag(schedule_id)
                schedule.every().wednesday.at(time_str).do(job_func).tag(schedule_id)
                schedule.every().thursday.at(time_str).do(job_func).tag(schedule_id)
                schedule.every().friday.at(time_str).do(job_func).tag(schedule_id)
            else:
                schedule.every().day.at(time_str).do(job_func).tag(schedule_id)

        elif sched_type == "monday":
            schedule.every().monday.at(time_str).do(job_func).tag(schedule_id)

        elif sched_type == "monday_holiday_aware":
            # Runs Monday normally, but shifts to Tuesday if Monday is a federal holiday
            def monday_holiday_check(agent=agent_name, cfg=config):
                today = date.today()
                # Find this week's Monday
                days_since_monday = today.weekday()
                this_monday = today - timedelta(days=days_since_monday)

                # Check seasonal restriction
                seasonal = cfg.get("seasonal")
                if seasonal:
                    if not (seasonal["start_month"] <= today.month <= seasonal["end_month"]):
                        return None  # Outside seasonal window

                if today.weekday() == 0:  # Monday
                    if not is_federal_holiday(today):
                        logger.info(f"Running {agent} (Monday, no holiday)")
                        return self.runner.run_agent(agent)
                    else:
                        logger.info(f"Skipping {agent} - Monday is federal holiday, will run Tuesday")
                elif today.weekday() == 1:  # Tuesday
                    if is_federal_holiday(this_monday):
                        logger.info(f"Running {agent} (Tuesday, shifted from Monday holiday)")
                        return self.runner.run_agent(agent)
                return None

            # Schedule to check both Monday and Tuesday
            schedule.every().monday.at(time_str).do(monday_holiday_check).tag(schedule_id)
            schedule.every().tuesday.at(time_str).do(monday_holiday_check).tag(schedule_id)

        elif sched_type == "tuesday":
            schedule.every().tuesday.at(time_str).do(job_func).tag(schedule_id)

        elif sched_type == "wednesday":
            schedule.every().wednesday.at(time_str).do(job_func).tag(schedule_id)

        elif sched_type == "wednesday_holiday_aware":
            # Runs Wednesday normally, but shifts to Thursday if ANY holiday falls Mon-Tue
            def wednesday_holiday_check(agent=agent_name):
                today = date.today()

                # Check if any holiday fell earlier this week (Mon or Tue)
                holiday_earlier = has_holiday_before_in_week(2, today)  # 2 = Wednesday

                if today.weekday() == 2:  # Wednesday
                    if not holiday_earlier and not is_federal_holiday(today):
                        logger.info(f"Running {agent} (Wednesday, no holiday this week)")
                        return self.runner.run_agent(agent)
                    else:
                        reason = "Wednesday is holiday" if is_federal_holiday(today) else "holiday earlier in week"
                        logger.info(f"Skipping {agent} - {reason}, will run Thursday")
                elif today.weekday() == 3:  # Thursday
                    # Run Thursday if there was any holiday Mon-Wed
                    holiday_mon_tue = has_holiday_before_in_week(2, today)
                    holiday_wed = is_federal_holiday(today - timedelta(days=1))
                    if holiday_mon_tue or holiday_wed:
                        logger.info(f"Running {agent} (Thursday, shifted due to holiday earlier in week)")
                        return self.runner.run_agent(agent)
                return None

            schedule.every().wednesday.at(time_str).do(wednesday_holiday_check).tag(schedule_id)
            schedule.every().thursday.at(time_str).do(wednesday_holiday_check).tag(schedule_id)

        elif sched_type == "thursday":
            schedule.every().thursday.at(time_str).do(job_func).tag(schedule_id)

        elif sched_type == "thursday_holiday_aware":
            # Runs Thursday normally, but shifts to Friday if ANY holiday falls Mon-Wed
            def thursday_holiday_check(agent=agent_name):
                today = date.today()

                # Check if any holiday fell earlier this week (Mon, Tue, or Wed)
                holiday_earlier = has_holiday_before_in_week(3, today)  # 3 = Thursday

                if today.weekday() == 3:  # Thursday
                    if not holiday_earlier and not is_federal_holiday(today):
                        logger.info(f"Running {agent} (Thursday, no holiday this week)")
                        return self.runner.run_agent(agent)
                    else:
                        reason = "Thursday is holiday" if is_federal_holiday(today) else "holiday earlier in week"
                        logger.info(f"Skipping {agent} - {reason}, will run Friday")
                elif today.weekday() == 4:  # Friday
                    # Run Friday if there was any holiday Mon-Thu
                    holiday_mon_wed = has_holiday_before_in_week(3, today)
                    holiday_thu = is_federal_holiday(today - timedelta(days=1))
                    if holiday_mon_wed or holiday_thu:
                        logger.info(f"Running {agent} (Friday, shifted due to holiday earlier in week)")
                        return self.runner.run_agent(agent)
                return None

            schedule.every().thursday.at(time_str).do(thursday_holiday_check).tag(schedule_id)
            schedule.every().friday.at(time_str).do(thursday_holiday_check).tag(schedule_id)

        elif sched_type == "friday":
            schedule.every().friday.at(time_str).do(job_func).tag(schedule_id)

        elif sched_type == "weekdays_holiday_aware":
            # Runs on weekdays but skips federal holidays (no shift, just skip)
            def weekday_holiday_check(agent=agent_name):
                today = date.today()
                if today.weekday() < 5:  # Monday-Friday
                    if not is_federal_holiday(today):
                        logger.info(f"Running {agent} (weekday, no holiday)")
                        return self.runner.run_agent(agent)
                    else:
                        logger.info(f"Skipping {agent} - federal holiday")
                return None

            schedule.every().monday.at(time_str).do(weekday_holiday_check).tag(schedule_id)
            schedule.every().tuesday.at(time_str).do(weekday_holiday_check).tag(schedule_id)
            schedule.every().wednesday.at(time_str).do(weekday_holiday_check).tag(schedule_id)
            schedule.every().thursday.at(time_str).do(weekday_holiday_check).tag(schedule_id)
            schedule.every().friday.at(time_str).do(weekday_holiday_check).tag(schedule_id)

        elif sched_type == "monthly":
            # For monthly, we check daily if it's the right day
            def monthly_check(day=config.get("day", 1), agent=agent_name):
                if datetime.now().day == day:
                    return self.runner.run_agent(agent)
            schedule.every().day.at(time_str).do(monthly_check).tag(schedule_id)

        elif sched_type == "specific_dates":
            # For reports with known specific release dates per year
            def specific_date_check(cfg=config, agent=agent_name):
                today = date.today()
                year_key = f"dates_{today.year}"
                dates = cfg.get(year_key, [])

                # Check if today matches any scheduled date
                for month, day in dates:
                    if today.month == month and today.day == day:
                        logger.info(f"Running {agent} (specific date: {today})")
                        return self.runner.run_agent(agent)
                return None

            schedule.every().day.at(time_str).do(specific_date_check).tag(schedule_id)

        elif sched_type == "quarterly":
            # Check if we're in the right month and day
            def quarterly_check(months=config.get("months", [1,4,7,10]), day=config.get("day", 1), agent=agent_name):
                now = datetime.now()
                if now.month in months and now.day == day:
                    return self.runner.run_agent(agent)
            schedule.every().day.at(time_str).do(quarterly_check).tag(schedule_id)

        elif sched_type == "annual":
            def annual_check(month=config.get("month", 1), day=config.get("day", 1), agent=agent_name):
                now = datetime.now()
                if now.month == month and now.day == day:
                    return self.runner.run_agent(agent)
            schedule.every().day.at(time_str).do(annual_check).tag(schedule_id)

        elif sched_type == "weather_custom":
            # Custom schedule for weather emails:
            # Weekdays (Mon-Fri): multiple times per day
            # Saturday: specific times
            # Sunday: specific times
            agent_path = config.get("agent_path")

            def run_weather_agent(path=agent_path, agent=agent_name):
                logger.info(f"Running weather email agent")
                return self.runner.run_agent(agent, agent_path=path)

            # Weekday times (Mon-Fri)
            for t in config.get("weekday_times", []):
                schedule.every().monday.at(t).do(run_weather_agent).tag(schedule_id, "weekday")
                schedule.every().tuesday.at(t).do(run_weather_agent).tag(schedule_id, "weekday")
                schedule.every().wednesday.at(t).do(run_weather_agent).tag(schedule_id, "weekday")
                schedule.every().thursday.at(t).do(run_weather_agent).tag(schedule_id, "weekday")
                schedule.every().friday.at(t).do(run_weather_agent).tag(schedule_id, "weekday")

            # Saturday times
            for t in config.get("saturday_times", []):
                schedule.every().saturday.at(t).do(run_weather_agent).tag(schedule_id, "saturday")

            # Sunday times
            for t in config.get("sunday_times", []):
                schedule.every().sunday.at(t).do(run_weather_agent).tag(schedule_id, "sunday")

            times_str = f"Weekdays: {config.get('weekday_times')}, Sat: {config.get('saturday_times')}, Sun: {config.get('sunday_times')}"
            logger.info(f"Scheduled: {config.get('name', schedule_id)} ({times_str})")
            return  # Skip the default log message below

        logger.info(f"Scheduled: {config.get('name', schedule_id)} ({sched_type} at {time_str})")

    def run(self):
        """Run the scheduler daemon."""
        self.setup_schedules()
        logger.info("Scheduler running. Press Ctrl+C to stop.")

        try:
            while True:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
        except KeyboardInterrupt:
            logger.info("Scheduler stopped by user")

    def list_jobs(self):
        """List all configured jobs."""
        print("\nConfigured Schedules:")
        print("=" * 70)
        for schedule_id, config in self.schedules.items():
            print(f"\n{config.get('name', schedule_id)}")
            print(f"  Schedule: {config.get('schedule')} at {config.get('time', 'N/A')}")
            print(f"  Agent: {config.get('agent', schedule_id)}")
            print(f"  Description: {config.get('description', 'N/A')}")

    def show_next(self, count: int = 10):
        """Show next upcoming jobs."""
        self.setup_schedules()
        jobs = schedule.get_jobs()

        print(f"\nNext {count} Scheduled Runs:")
        print("=" * 50)

        # Sort by next run time
        sorted_jobs = sorted(jobs, key=lambda j: j.next_run or datetime.max)

        for i, job in enumerate(sorted_jobs[:count], 1):
            tags = ", ".join(job.tags) if job.tags else "untagged"
            next_run = job.next_run.strftime("%Y-%m-%d %H:%M") if job.next_run else "N/A"
            print(f"{i}. [{next_run}] {tags}")

    def trigger(self, agent_name: str):
        """Manually trigger an agent."""
        return self.runner.run_agent(agent_name)


# =============================================================================
# CLI
# =============================================================================

def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return

    scheduler = RLCScheduler()
    command = sys.argv[1].lower()

    if command == "run":
        scheduler.run()

    elif command == "list":
        scheduler.list_jobs()

    elif command == "next":
        count = int(sys.argv[2]) if len(sys.argv) > 2 else 10
        scheduler.show_next(count)

    elif command == "trigger":
        if len(sys.argv) < 3:
            print("Usage: python agent_scheduler.py trigger <agent_name>")
            return
        agent_name = sys.argv[2]
        result = scheduler.trigger(agent_name)
        print(f"\nResult: {result['status']}")
        if result.get('output'):
            print(f"Output: {result['output'][:500]}")
        if result.get('error'):
            print(f"Error: {result['error']}")

    else:
        print(f"Unknown command: {command}")
        print(__doc__)


if __name__ == "__main__":
    main()
