"""
Master Data Collection Scheduler

Manages timing and execution of all data collectors based on
official release schedules from each data source.

Key Features:
- Schedule-based collection triggered by report release times
- Pre-fetch mode to prepare for expected releases
- Retry logic for failed collections
- Priority-based collection ordering
- Holiday/non-trading day handling
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, date, time, timedelta
from enum import Enum
from typing import Dict, List, Optional, Any, Callable
import json

logger = logging.getLogger(__name__)


class DayOfWeek(Enum):
    """Days of week for scheduling"""
    MONDAY = 0
    TUESDAY = 1
    WEDNESDAY = 2
    THURSDAY = 3
    FRIDAY = 4
    SATURDAY = 5
    SUNDAY = 6


class ReleaseFrequency(Enum):
    """Data release frequencies"""
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    QUARTERLY = "quarterly"
    ANNUAL = "annual"
    ON_DEMAND = "on_demand"


@dataclass
class ReleaseSchedule:
    """Defines when a data source releases new data"""
    frequency: ReleaseFrequency
    day_of_week: Optional[DayOfWeek] = None        # For weekly releases
    day_of_month: Optional[int] = None             # For monthly (1-31, negative for end)
    release_time: Optional[time] = None            # Eastern Time
    timezone: str = "America/New_York"
    lag_days: int = 0                              # Days after period end data is released
    description: str = ""


@dataclass
class CollectorSchedule:
    """Complete schedule configuration for a collector"""
    collector_name: str
    collector_class: str
    release_schedule: ReleaseSchedule
    priority: int = 5                              # 1=highest, 10=lowest
    enabled: bool = True
    retry_attempts: int = 3
    retry_delay_minutes: int = 15
    commodities: List[str] = field(default_factory=list)
    dependencies: List[str] = field(default_factory=list)  # Other collectors that must run first


# =============================================================================
# OFFICIAL RELEASE SCHEDULES
# =============================================================================

RELEASE_SCHEDULES: Dict[str, CollectorSchedule] = {
    # -------------------------------------------------------------------------
    # DAILY RELEASES
    # -------------------------------------------------------------------------
    'cme_settlements': CollectorSchedule(
        collector_name='cme_settlements',
        collector_class='CMESettlementsCollector',
        release_schedule=ReleaseSchedule(
            frequency=ReleaseFrequency.DAILY,
            release_time=time(17, 0),  # 5:00 PM ET (after close)
            description="CME futures settlements released after market close"
        ),
        priority=2,
        commodities=['corn', 'wheat', 'soybeans', 'soy_oil', 'soy_meal',
                    'crude_oil', 'gasoline', 'diesel', 'natural_gas', 'ethanol'],
    ),

    # -------------------------------------------------------------------------
    # WEEKLY RELEASES - MONDAY
    # -------------------------------------------------------------------------
    'usda_nass_crop_progress': CollectorSchedule(
        collector_name='usda_nass',
        collector_class='NASSCollector',
        release_schedule=ReleaseSchedule(
            frequency=ReleaseFrequency.WEEKLY,
            day_of_week=DayOfWeek.MONDAY,
            release_time=time(16, 0),  # 4:00 PM ET
            description="USDA Crop Progress report (seasonal, Apr-Nov)"
        ),
        priority=1,
        commodities=['corn', 'soybeans', 'wheat', 'cotton', 'sorghum'],
    ),

    # -------------------------------------------------------------------------
    # WEEKLY RELEASES - WEDNESDAY
    # -------------------------------------------------------------------------
    'eia_petroleum': CollectorSchedule(
        collector_name='eia_petroleum',
        collector_class='EIAPetroleumCollector',
        release_schedule=ReleaseSchedule(
            frequency=ReleaseFrequency.WEEKLY,
            day_of_week=DayOfWeek.WEDNESDAY,
            release_time=time(10, 30),  # 10:30 AM ET
            description="EIA Weekly Petroleum Status Report"
        ),
        priority=1,
        commodities=['crude_oil', 'gasoline', 'diesel', 'jet_fuel', 'natural_gas'],
    ),

    'eia_ethanol': CollectorSchedule(
        collector_name='eia_ethanol',
        collector_class='EIAEthanolCollector',
        release_schedule=ReleaseSchedule(
            frequency=ReleaseFrequency.WEEKLY,
            day_of_week=DayOfWeek.WEDNESDAY,
            release_time=time(10, 30),  # 10:30 AM ET
            description="EIA Weekly Ethanol Production"
        ),
        priority=1,
        commodities=['ethanol', 'corn'],
    ),

    # -------------------------------------------------------------------------
    # WEEKLY RELEASES - THURSDAY
    # -------------------------------------------------------------------------
    'usda_fas_export_sales': CollectorSchedule(
        collector_name='usda_fas',
        collector_class='USDATFASCollector',
        release_schedule=ReleaseSchedule(
            frequency=ReleaseFrequency.WEEKLY,
            day_of_week=DayOfWeek.THURSDAY,
            release_time=time(8, 30),  # 8:30 AM ET
            description="USDA Export Sales Weekly Report"
        ),
        priority=1,
        commodities=['corn', 'wheat', 'soybeans', 'soy_oil', 'soy_meal', 'cotton'],
    ),

    'drought_monitor': CollectorSchedule(
        collector_name='drought',
        collector_class='DroughtCollector',
        release_schedule=ReleaseSchedule(
            frequency=ReleaseFrequency.WEEKLY,
            day_of_week=DayOfWeek.THURSDAY,
            release_time=time(8, 30),  # 8:30 AM ET
            description="US Drought Monitor Weekly Update"
        ),
        priority=2,
        commodities=['corn', 'soybeans', 'wheat', 'cotton'],
    ),

    'canada_cgc': CollectorSchedule(
        collector_name='canada_cgc',
        collector_class='CGCCollector',
        release_schedule=ReleaseSchedule(
            frequency=ReleaseFrequency.WEEKLY,
            day_of_week=DayOfWeek.THURSDAY,
            release_time=time(13, 30),  # 1:30 PM ET (varies)
            description="Canadian Grain Commission Weekly Report"
        ),
        priority=3,
        commodities=['wheat', 'canola', 'barley', 'oats'],
    ),

    # -------------------------------------------------------------------------
    # WEEKLY RELEASES - FRIDAY
    # -------------------------------------------------------------------------
    'cftc_cot': CollectorSchedule(
        collector_name='cftc_cot',
        collector_class='CFTCCOTCollector',
        release_schedule=ReleaseSchedule(
            frequency=ReleaseFrequency.WEEKLY,
            day_of_week=DayOfWeek.FRIDAY,
            release_time=time(15, 30),  # 3:30 PM ET
            description="CFTC Commitments of Traders (data as of Tuesday)"
        ),
        priority=1,
        commodities=['corn', 'wheat', 'soybeans', 'soy_oil', 'soy_meal',
                    'crude_oil', 'natural_gas'],
    ),

    # -------------------------------------------------------------------------
    # WEEKLY RELEASES - VARIABLE
    # -------------------------------------------------------------------------
    'usda_ams_feedstocks': CollectorSchedule(
        collector_name='usda_ams_tallow',
        collector_class='TallowProteinCollector',
        release_schedule=ReleaseSchedule(
            frequency=ReleaseFrequency.WEEKLY,
            day_of_week=DayOfWeek.FRIDAY,  # Varies
            release_time=time(14, 0),
            description="USDA AMS Tallow & Grease Weekly Prices"
        ),
        priority=4,
        commodities=['yellow_grease', 'tallow', 'cwg', 'lard', 'poultry_fat'],
    ),

    'usda_ams_ddgs': CollectorSchedule(
        collector_name='usda_ams_ddgs',
        collector_class='GrainCoProductsCollector',
        release_schedule=ReleaseSchedule(
            frequency=ReleaseFrequency.WEEKLY,
            day_of_week=DayOfWeek.FRIDAY,  # Varies
            release_time=time(14, 0),
            description="USDA AMS DDGS & Ethanol Co-Products"
        ),
        priority=4,
        commodities=['ddgs', 'dco', 'corn_gluten'],
    ),

    # -------------------------------------------------------------------------
    # MONTHLY RELEASES
    # -------------------------------------------------------------------------
    'usda_wasde': CollectorSchedule(
        collector_name='usda_wasde',
        collector_class='USDATFASCollector',  # Uses FAS API for WASDE data
        release_schedule=ReleaseSchedule(
            frequency=ReleaseFrequency.MONTHLY,
            day_of_month=12,  # Usually ~12th
            release_time=time(12, 0),  # 12:00 PM ET
            description="USDA World Agricultural Supply & Demand Estimates"
        ),
        priority=1,  # Highest priority - market-moving report
        commodities=['corn', 'wheat', 'soybeans', 'soy_oil', 'soy_meal', 'cotton'],
    ),

    'nopa_crush': CollectorSchedule(
        collector_name='nopa_crush',
        collector_class=None,  # Not yet implemented
        release_schedule=ReleaseSchedule(
            frequency=ReleaseFrequency.MONTHLY,
            day_of_month=15,  # Usually ~15th
            release_time=time(11, 0),  # 11:00 AM CT (12:00 ET)
            description="NOPA Monthly Soybean Crush Report"
        ),
        priority=2,
        enabled=False,  # Not yet implemented
        commodities=['soybeans', 'soy_oil', 'soy_meal'],
    ),

    'mpob': CollectorSchedule(
        collector_name='mpob',
        collector_class='MPOBCollector',
        release_schedule=ReleaseSchedule(
            frequency=ReleaseFrequency.MONTHLY,
            day_of_month=10,  # Usually ~10th
            release_time=time(4, 0),  # Early morning ET (daytime Malaysia)
            description="MPOB Monthly Palm Oil Statistics"
        ),
        priority=2,
        commodities=['palm_oil'],
    ),

    'census_trade': CollectorSchedule(
        collector_name='census_trade',
        collector_class='CensusTradeCollector',
        release_schedule=ReleaseSchedule(
            frequency=ReleaseFrequency.MONTHLY,
            day_of_month=-5,  # ~5th from end (about 6 week lag)
            release_time=time(10, 0),
            lag_days=45,
            description="US Census International Trade Monthly (6-week lag)"
        ),
        priority=3,
        commodities=['all'],
    ),

    'epa_rfs': CollectorSchedule(
        collector_name='epa_rfs',
        collector_class='EPARFSCollector',
        release_schedule=ReleaseSchedule(
            frequency=ReleaseFrequency.MONTHLY,
            day_of_month=15,  # Mid-month typically
            release_time=time(12, 0),
            description="EPA RFS RIN Generation Monthly Data"
        ),
        priority=3,
        commodities=['ethanol', 'biodiesel', 'renewable_diesel'],
    ),

    'canada_statscan': CollectorSchedule(
        collector_name='canada_statscan',
        collector_class='StatsCanCollector',
        release_schedule=ReleaseSchedule(
            frequency=ReleaseFrequency.MONTHLY,
            day_of_month=20,  # Varies by report
            release_time=time(8, 30),
            description="Statistics Canada Agricultural Reports"
        ),
        priority=3,
        commodities=['wheat', 'canola', 'barley', 'oats'],
    ),

    # -------------------------------------------------------------------------
    # QUARTERLY RELEASES
    # -------------------------------------------------------------------------
    'usda_nass_stocks': CollectorSchedule(
        collector_name='usda_nass_stocks',
        collector_class='NASSCollector',
        release_schedule=ReleaseSchedule(
            frequency=ReleaseFrequency.QUARTERLY,
            day_of_month=1,  # Late month releases (varies)
            release_time=time(12, 0),
            description="USDA Quarterly Grain Stocks (Jan, Mar, Jun, Sep)"
        ),
        priority=1,
        commodities=['corn', 'soybeans', 'wheat'],
    ),

    'canada_statscan_stocks': CollectorSchedule(
        collector_name='canada_statscan_stocks',
        collector_class='StatsCanCollector',
        release_schedule=ReleaseSchedule(
            frequency=ReleaseFrequency.QUARTERLY,
            release_time=time(8, 30),
            description="Statistics Canada Quarterly Grain Stocks"
        ),
        priority=2,
        commodities=['wheat', 'canola', 'barley', 'oats'],
    ),

    # -------------------------------------------------------------------------
    # PERIODIC / ON-DEMAND
    # -------------------------------------------------------------------------
    'usda_ers_feed_grains': CollectorSchedule(
        collector_name='usda_ers_feed_grains',
        collector_class='FeedGrainsCollector',
        release_schedule=ReleaseSchedule(
            frequency=ReleaseFrequency.ON_DEMAND,
            description="USDA ERS Feed Grains Database (updated periodically)"
        ),
        priority=5,
        commodities=['corn', 'sorghum', 'barley', 'oats'],
    ),

    'usda_ers_oil_crops': CollectorSchedule(
        collector_name='usda_ers_oil_crops',
        collector_class='OilCropsCollector',
        release_schedule=ReleaseSchedule(
            frequency=ReleaseFrequency.ON_DEMAND,
            description="USDA ERS Oil Crops Yearbook (updated periodically)"
        ),
        priority=5,
        commodities=['soybeans', 'canola', 'sunflower'],
    ),

    'usda_ers_wheat': CollectorSchedule(
        collector_name='usda_ers_wheat',
        collector_class='WheatDataCollector',
        release_schedule=ReleaseSchedule(
            frequency=ReleaseFrequency.ON_DEMAND,
            description="USDA ERS Wheat Data (updated periodically)"
        ),
        priority=5,
        commodities=['wheat'],
    ),
}


# =============================================================================
# HB WEEKLY REPORT SCHEDULE
# =============================================================================

# Data collection schedule for Tuesday HB Report generation
HB_REPORT_COLLECTION_SCHEDULE = {
    # Pre-report data check (Monday evening)
    'pre_report_check': {
        'day': DayOfWeek.MONDAY,
        'time': time(20, 0),  # 8:00 PM ET
        'collectors': [
            'cftc_cot',
            'usda_fas_export_sales',
            'eia_ethanol',
            'drought_monitor',
            'cme_settlements',
        ],
        'description': 'Verify all weekly data collected before Tuesday report'
    },

    # Report generation (Tuesday morning)
    'report_generation': {
        'day': DayOfWeek.TUESDAY,
        'time': time(6, 0),  # 6:00 AM ET
        'actions': [
            'aggregate_weekly_data',
            'run_analysis',
            'generate_report',
            'upload_to_dropbox',
        ],
        'description': 'Generate and distribute HB Weekly Report'
    },

    # Post-report data collection (Tuesday-Thursday)
    'mid_week_collection': {
        'collectors_by_day': {
            DayOfWeek.WEDNESDAY: ['eia_petroleum', 'eia_ethanol'],
            DayOfWeek.THURSDAY: ['usda_fas_export_sales', 'drought_monitor', 'canada_cgc'],
            DayOfWeek.FRIDAY: ['cftc_cot', 'usda_ams_feedstocks'],
        }
    }
}


# =============================================================================
# SCHEDULER CLASS
# =============================================================================

class ReportScheduler:
    """
    Master scheduler for commodity data collection.

    Coordinates collection timing based on official release schedules,
    handles retries, and manages dependencies between collectors.
    """

    def __init__(self, config: Dict = None):
        self.config = config or {}
        self.schedules = RELEASE_SCHEDULES.copy()
        self.collection_log: List[Dict] = []

    def get_todays_collections(self, as_of: datetime = None) -> List[CollectorSchedule]:
        """
        Get collectors scheduled to run today.

        Args:
            as_of: Reference datetime (default: now)

        Returns:
            List of CollectorSchedule objects sorted by priority
        """
        as_of = as_of or datetime.now()
        today = as_of.date()
        day_of_week = DayOfWeek(today.weekday())

        scheduled = []

        for name, schedule in self.schedules.items():
            if not schedule.enabled:
                continue

            release = schedule.release_schedule

            if release.frequency == ReleaseFrequency.DAILY:
                scheduled.append(schedule)

            elif release.frequency == ReleaseFrequency.WEEKLY:
                if release.day_of_week == day_of_week:
                    scheduled.append(schedule)

            elif release.frequency == ReleaseFrequency.MONTHLY:
                if self._is_monthly_release_day(today, release.day_of_month):
                    scheduled.append(schedule)

            elif release.frequency == ReleaseFrequency.QUARTERLY:
                if today.month in [1, 3, 6, 9]:  # Typical quarterly months
                    if self._is_monthly_release_day(today, release.day_of_month):
                        scheduled.append(schedule)

        # Sort by priority (lower = higher priority)
        return sorted(scheduled, key=lambda s: s.priority)

    def get_next_collection_time(
        self,
        collector_name: str,
        after: datetime = None
    ) -> Optional[datetime]:
        """
        Get next scheduled collection time for a collector.

        Args:
            collector_name: Name of collector
            after: Find next time after this (default: now)

        Returns:
            Next scheduled datetime or None
        """
        if collector_name not in self.schedules:
            return None

        schedule = self.schedules[collector_name]
        after = after or datetime.now()
        release = schedule.release_schedule

        if release.frequency == ReleaseFrequency.ON_DEMAND:
            return None

        # Build next occurrence
        if release.frequency == ReleaseFrequency.DAILY:
            next_date = after.date()
            if release.release_time and after.time() > release.release_time:
                next_date += timedelta(days=1)
            return datetime.combine(next_date, release.release_time or time(0, 0))

        elif release.frequency == ReleaseFrequency.WEEKLY:
            days_ahead = release.day_of_week.value - after.weekday()
            if days_ahead < 0:
                days_ahead += 7
            elif days_ahead == 0 and release.release_time and after.time() > release.release_time:
                days_ahead = 7

            next_date = after.date() + timedelta(days=days_ahead)
            return datetime.combine(next_date, release.release_time or time(0, 0))

        elif release.frequency == ReleaseFrequency.MONTHLY:
            next_date = self._next_monthly_date(after.date(), release.day_of_month)
            return datetime.combine(next_date, release.release_time or time(0, 0))

        return None

    def get_weekly_schedule(self) -> Dict[str, List[CollectorSchedule]]:
        """
        Get full weekly schedule organized by day.

        Returns:
            Dict mapping day names to list of scheduled collectors
        """
        schedule = {day.name: [] for day in DayOfWeek}

        for name, collector_schedule in self.schedules.items():
            if not collector_schedule.enabled:
                continue

            release = collector_schedule.release_schedule

            if release.frequency == ReleaseFrequency.DAILY:
                for day in DayOfWeek:
                    if day.value < 5:  # Weekdays only
                        schedule[day.name].append(collector_schedule)

            elif release.frequency == ReleaseFrequency.WEEKLY:
                if release.day_of_week:
                    schedule[release.day_of_week.name].append(collector_schedule)

        # Sort each day by priority
        for day in schedule:
            schedule[day] = sorted(schedule[day], key=lambda s: s.priority)

        return schedule

    def get_release_calendar(self, month: int = None, year: int = None) -> List[Dict]:
        """
        Get calendar of expected releases for a month.

        Args:
            month: Month number (default: current)
            year: Year (default: current)

        Returns:
            List of release events with dates and collectors
        """
        today = date.today()
        month = month or today.month
        year = year or today.year

        events = []

        # Generate dates for the month
        current = date(year, month, 1)
        while current.month == month:
            day_events = self.get_todays_collections(datetime.combine(current, time(0, 0)))

            for event in day_events:
                events.append({
                    'date': current.isoformat(),
                    'collector': event.collector_name,
                    'description': event.release_schedule.description,
                    'time': event.release_schedule.release_time.isoformat() if event.release_schedule.release_time else None,
                    'priority': event.priority,
                    'commodities': event.commodities,
                })

            current += timedelta(days=1)

        return sorted(events, key=lambda e: (e['date'], e['priority']))

    def _is_monthly_release_day(self, check_date: date, day_of_month: int) -> bool:
        """Check if date matches monthly release day"""
        if day_of_month is None:
            return False

        if day_of_month > 0:
            return check_date.day == day_of_month
        else:
            # Negative = days from end of month
            import calendar
            last_day = calendar.monthrange(check_date.year, check_date.month)[1]
            target_day = last_day + day_of_month + 1
            return check_date.day == target_day

    def _next_monthly_date(self, after: date, day_of_month: int) -> date:
        """Get next monthly release date"""
        import calendar

        if day_of_month > 0:
            target_day = min(day_of_month, calendar.monthrange(after.year, after.month)[1])
            candidate = after.replace(day=target_day)

            if candidate <= after:
                # Move to next month
                if after.month == 12:
                    candidate = date(after.year + 1, 1, min(day_of_month, 31))
                else:
                    last_day = calendar.monthrange(after.year, after.month + 1)[1]
                    candidate = date(after.year, after.month + 1, min(day_of_month, last_day))

            return candidate
        else:
            # Negative = from end of month
            last_day = calendar.monthrange(after.year, after.month)[1]
            target_day = last_day + day_of_month + 1
            candidate = after.replace(day=target_day)

            if candidate <= after:
                # Move to next month
                if after.month == 12:
                    last_day = calendar.monthrange(after.year + 1, 1)[1]
                    candidate = date(after.year + 1, 1, last_day + day_of_month + 1)
                else:
                    last_day = calendar.monthrange(after.year, after.month + 1)[1]
                    candidate = date(after.year, after.month + 1, last_day + day_of_month + 1)

            return candidate

    def to_json(self) -> str:
        """Export schedule as JSON"""
        export = {}
        for name, schedule in self.schedules.items():
            export[name] = {
                'collector_name': schedule.collector_name,
                'collector_class': schedule.collector_class,
                'priority': schedule.priority,
                'enabled': schedule.enabled,
                'commodities': schedule.commodities,
                'release': {
                    'frequency': schedule.release_schedule.frequency.value,
                    'day_of_week': schedule.release_schedule.day_of_week.name if schedule.release_schedule.day_of_week else None,
                    'day_of_month': schedule.release_schedule.day_of_month,
                    'time': schedule.release_schedule.release_time.isoformat() if schedule.release_schedule.release_time else None,
                    'description': schedule.release_schedule.description,
                }
            }
        return json.dumps(export, indent=2)


# =============================================================================
# CLI INTERFACE
# =============================================================================

def main():
    """CLI for report scheduler"""
    import argparse

    parser = argparse.ArgumentParser(description='Commodity Data Collection Scheduler')

    parser.add_argument(
        'command',
        choices=['today', 'week', 'calendar', 'next', 'export'],
        help='Command to execute'
    )

    parser.add_argument(
        '--collector',
        help='Collector name for "next" command'
    )

    parser.add_argument(
        '--month',
        type=int,
        help='Month for calendar'
    )

    parser.add_argument(
        '--year',
        type=int,
        help='Year for calendar'
    )

    parser.add_argument(
        '--output', '-o',
        help='Output file'
    )

    args = parser.parse_args()

    scheduler = ReportScheduler()

    if args.command == 'today':
        collections = scheduler.get_todays_collections()
        print(f"\n=== Collections Scheduled for Today ({date.today().strftime('%A, %B %d')}) ===\n")

        if not collections:
            print("No scheduled collections today.")
        else:
            for c in collections:
                time_str = c.release_schedule.release_time.strftime('%H:%M') if c.release_schedule.release_time else 'N/A'
                print(f"  [{c.priority}] {c.collector_name}")
                print(f"      Time: {time_str} ET")
                print(f"      {c.release_schedule.description}")
                print(f"      Commodities: {', '.join(c.commodities)}")
                print()

    elif args.command == 'week':
        schedule = scheduler.get_weekly_schedule()
        print("\n=== Weekly Collection Schedule ===\n")

        for day, collections in schedule.items():
            if not collections:
                continue

            print(f"{day}:")
            for c in collections:
                time_str = c.release_schedule.release_time.strftime('%H:%M') if c.release_schedule.release_time else '-----'
                print(f"  {time_str}  [{c.priority}] {c.collector_name}")
            print()

    elif args.command == 'calendar':
        events = scheduler.get_release_calendar(args.month, args.year)
        month = args.month or date.today().month
        year = args.year or date.today().year

        print(f"\n=== Release Calendar for {year}-{month:02d} ===\n")

        current_date = None
        for event in events:
            if event['date'] != current_date:
                current_date = event['date']
                print(f"\n{current_date}:")

            time_str = event['time'][:5] if event['time'] else '-----'
            print(f"  {time_str}  [{event['priority']}] {event['collector']}")

    elif args.command == 'next':
        if not args.collector:
            print("Error: --collector required for 'next' command")
            return

        next_time = scheduler.get_next_collection_time(args.collector)
        if next_time:
            print(f"Next collection for {args.collector}: {next_time.strftime('%Y-%m-%d %H:%M')} ET")
        else:
            print(f"No scheduled collection for {args.collector}")

    elif args.command == 'export':
        output = scheduler.to_json()

        if args.output:
            with open(args.output, 'w') as f:
                f.write(output)
            print(f"Schedule exported to {args.output}")
        else:
            print(output)


if __name__ == '__main__':
    main()
