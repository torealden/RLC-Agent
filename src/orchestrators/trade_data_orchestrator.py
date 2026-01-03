"""
Trade Data Orchestrator

Coordinates data collection across all South American country agents,
handles harmonization, and manages the complete data pipeline.

Also includes LineupDataOrchestrator for port line-up data collection.
"""

import logging
import json
from dataclasses import dataclass, field, asdict
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

from ..config.settings import SouthAmericaTradeConfig, default_config
from ..agents.argentina_agent import ArgentinaINDECAgent
from ..agents.brazil_agent import BrazilComexStatAgent
from ..agents.colombia_agent import ColombiaDANEAgent
from ..agents.uruguay_agent import UruguayDNAAgent
from ..agents.paraguay_agent import ParaguayAgent
from ..agents.brazil_lineup_agent import BrazilANECLineupAgent
from ..utils.harmonization import TradeDataHarmonizer, BalanceMatrixBuilder
from ..utils.quality import QualityValidator, OutlierDetector, CompletenessChecker

logger = logging.getLogger(__name__)


@dataclass
class PipelineResult:
    """Result of a pipeline run"""
    success: bool
    start_time: datetime
    end_time: Optional[datetime] = None
    periods_processed: List[str] = field(default_factory=list)
    countries_processed: List[str] = field(default_factory=list)
    total_records_fetched: int = 0
    total_records_loaded: int = 0
    total_errors: int = 0
    country_results: Dict[str, Any] = field(default_factory=dict)
    harmonization_results: Optional[Dict] = None
    quality_alerts: List[Dict] = field(default_factory=list)
    error_message: Optional[str] = None


class TradeDataOrchestrator:
    """
    Orchestrates data collection from all South American sources

    Responsibilities:
    1. Initialize and manage country-specific agents
    2. Coordinate parallel data fetching
    3. Apply harmonization across sources
    4. Perform quality validation
    5. Build balance matrices for reconciliation
    6. Log all operations
    """

    def __init__(self, config: SouthAmericaTradeConfig = None, db_session_factory=None):
        """
        Initialize orchestrator

        Args:
            config: Pipeline configuration
            db_session_factory: SQLAlchemy session factory for database operations
        """
        self.config = config or default_config
        self.db_session_factory = db_session_factory

        # Initialize agents
        self.agents = self._initialize_agents()

        # Initialize utilities
        self.harmonizer = TradeDataHarmonizer()
        self.balance_builder = BalanceMatrixBuilder()
        self.validator = QualityValidator()
        self.outlier_detector = OutlierDetector(
            zscore_threshold=self.config.quality.zscore_threshold,
            deviation_threshold_pct=self.config.quality.monthly_deviation_threshold_pct,
        )
        self.completeness_checker = CompletenessChecker()

        logger.info(f"Initialized {self.config.pipeline_name} v{self.config.pipeline_version}")

    def _initialize_agents(self) -> Dict[str, Any]:
        """Initialize country-specific agents"""
        agents = {}

        # Argentina
        if self.config.argentina.enabled:
            agents['ARG'] = ArgentinaINDECAgent(
                self.config.argentina,
                self.db_session_factory
            )

        # Brazil
        if self.config.brazil.enabled:
            agents['BRA'] = BrazilComexStatAgent(
                self.config.brazil,
                self.db_session_factory
            )

        # Colombia
        if self.config.colombia.enabled:
            agents['COL'] = ColombiaDANEAgent(
                self.config.colombia,
                self.db_session_factory
            )

        # Uruguay
        if self.config.uruguay.enabled:
            agents['URY'] = UruguayDNAAgent(
                self.config.uruguay,
                self.db_session_factory
            )

        # Paraguay
        if self.config.paraguay.enabled:
            agents['PRY'] = ParaguayAgent(
                self.config.paraguay,
                self.db_session_factory
            )

        logger.info(f"Initialized {len(agents)} country agents: {list(agents.keys())}")
        return agents

    def run_monthly_pipeline(
        self,
        year: int,
        month: int,
        countries: List[str] = None,
        flows: List[str] = None,
        parallel: bool = True
    ) -> PipelineResult:
        """
        Run the complete monthly data pipeline

        Args:
            year: Year to process
            month: Month to process
            countries: List of country codes (default: all enabled)
            flows: List of flows ['export', 'import'] (default: both)
            parallel: Run country agents in parallel

        Returns:
            PipelineResult with complete pipeline results
        """
        period = f"{year}-{month:02d}"
        start_time = datetime.now()

        result = PipelineResult(
            success=True,
            start_time=start_time,
            periods_processed=[period],
        )

        countries = countries or list(self.agents.keys())
        flows = flows or ['export', 'import']

        logger.info(f"Starting monthly pipeline for {period}")
        logger.info(f"Countries: {countries}, Flows: {flows}")

        # Fetch data from all sources
        all_records = []

        if parallel and len(countries) > 1:
            country_results = self._fetch_parallel(countries, year, month, flows)
        else:
            country_results = self._fetch_sequential(countries, year, month, flows)

        result.country_results = country_results
        result.countries_processed = countries

        # Collect all records
        for country, country_result in country_results.items():
            for flow, flow_result in country_result.items():
                result.total_records_fetched += flow_result.get('records_fetched', 0)
                result.total_records_loaded += flow_result.get('records_loaded', 0)
                result.total_errors += flow_result.get('errors', 0)

                if flow_result.get('records'):
                    all_records.extend(flow_result['records'])

                if not flow_result.get('success', False):
                    result.success = False

        # Harmonize data
        if all_records:
            logger.info(f"Harmonizing {len(all_records)} records...")
            harmonized = self.harmonizer.harmonize_records(all_records)

            result.harmonization_results = {
                'input_records': len(all_records),
                'harmonized_records': len(harmonized),
            }

            # Build balance matrix (if we have exports and imports)
            if len(harmonized) > 0:
                balance_entries = self.balance_builder.build_matrix(harmonized)
                discrepancies = self.balance_builder.identify_discrepancies(
                    balance_entries,
                    threshold_pct=10.0
                )

                result.harmonization_results['balance_entries'] = len(balance_entries)
                result.harmonization_results['discrepancies'] = len(discrepancies)

        # Quality validation
        if all_records:
            valid_records, invalid_records, alerts = self.validator.validate_batch(all_records)
            result.quality_alerts = [asdict(a) for a in alerts[:100]]  # Limit to first 100

        result.end_time = datetime.now()
        duration = (result.end_time - result.start_time).total_seconds()

        logger.info(
            f"Pipeline complete: {result.total_records_loaded} records loaded, "
            f"{result.total_errors} errors, {duration:.1f}s"
        )

        return result

    def _fetch_parallel(
        self,
        countries: List[str],
        year: int,
        month: int,
        flows: List[str]
    ) -> Dict[str, Dict]:
        """Fetch data from multiple countries in parallel"""
        results = {}

        with ThreadPoolExecutor(max_workers=min(len(countries), 4)) as executor:
            futures = {}

            for country in countries:
                if country in self.agents:
                    future = executor.submit(
                        self._fetch_country_data,
                        country, year, month, flows
                    )
                    futures[future] = country

            for future in as_completed(futures):
                country = futures[future]
                try:
                    results[country] = future.result()
                except Exception as e:
                    logger.error(f"Error fetching {country}: {e}")
                    results[country] = {
                        flow: {'success': False, 'error': str(e)}
                        for flow in flows
                    }

        return results

    def _fetch_sequential(
        self,
        countries: List[str],
        year: int,
        month: int,
        flows: List[str]
    ) -> Dict[str, Dict]:
        """Fetch data from countries sequentially"""
        results = {}

        for country in countries:
            if country in self.agents:
                try:
                    results[country] = self._fetch_country_data(country, year, month, flows)
                except Exception as e:
                    logger.error(f"Error fetching {country}: {e}")
                    results[country] = {
                        flow: {'success': False, 'error': str(e)}
                        for flow in flows
                    }

        return results

    def _fetch_country_data(
        self,
        country: str,
        year: int,
        month: int,
        flows: List[str]
    ) -> Dict[str, Dict]:
        """Fetch data for a single country"""
        agent = self.agents.get(country)
        if not agent:
            return {flow: {'success': False, 'error': 'Agent not found'} for flow in flows}

        results = {}

        for flow in flows:
            try:
                # Fetch data
                fetch_result = agent.fetch_data(year, month, flow)

                if not fetch_result.success:
                    results[flow] = {
                        'success': False,
                        'error': fetch_result.error_message,
                        'records_fetched': 0,
                        'records_loaded': 0,
                    }
                    continue

                # Transform to records
                if fetch_result.data is not None:
                    records = agent.transform_to_records(fetch_result.data, flow)
                else:
                    records = []

                results[flow] = {
                    'success': True,
                    'records_fetched': fetch_result.records_fetched,
                    'records_loaded': len(records),
                    'records': records,
                    'errors': 0,
                }

            except Exception as e:
                logger.error(f"Error processing {country} {flow}: {e}")
                results[flow] = {
                    'success': False,
                    'error': str(e),
                    'records_fetched': 0,
                    'records_loaded': 0,
                }

        return results

    def run_historical_backfill(
        self,
        start_year: int,
        start_month: int,
        end_year: int = None,
        end_month: int = None,
        countries: List[str] = None
    ) -> List[PipelineResult]:
        """
        Run historical backfill for a date range

        Args:
            start_year: Start year
            start_month: Start month
            end_year: End year (default: current year)
            end_month: End month (default: current month - 2)
            countries: Countries to process

        Returns:
            List of PipelineResult for each month
        """
        today = date.today()
        end_year = end_year or today.year
        end_month = end_month or max(1, today.month - 2)

        results = []

        current = date(start_year, start_month, 1)
        end = date(end_year, end_month, 1)

        while current <= end:
            logger.info(f"Processing {current.year}-{current.month:02d}")

            result = self.run_monthly_pipeline(
                current.year,
                current.month,
                countries=countries,
                parallel=True
            )
            results.append(result)

            # Move to next month
            if current.month == 12:
                current = date(current.year + 1, 1, 1)
            else:
                current = date(current.year, current.month + 1, 1)

        return results

    def get_agent_status(self) -> Dict[str, Dict]:
        """Get status of all agents"""
        status = {}

        for country, agent in self.agents.items():
            status[country] = agent.get_status()

        return status

    def get_pipeline_status(self) -> Dict:
        """Get overall pipeline status"""
        return {
            'pipeline_name': self.config.pipeline_name,
            'pipeline_version': self.config.pipeline_version,
            'enabled_countries': list(self.agents.keys()),
            'agent_status': self.get_agent_status(),
        }


# =============================================================================
# STANDALONE USAGE
# =============================================================================

def main():
    """Run orchestrator from command line"""
    import argparse

    parser = argparse.ArgumentParser(
        description='South America Trade Data Pipeline Orchestrator'
    )

    parser.add_argument(
        'command',
        choices=['monthly', 'backfill', 'status'],
        help='Command to execute'
    )

    parser.add_argument('--year', '-y', type=int, default=datetime.now().year)
    parser.add_argument('--month', '-m', type=int, default=max(1, datetime.now().month - 1))
    parser.add_argument('--start-year', type=int)
    parser.add_argument('--start-month', type=int, default=1)
    parser.add_argument('--end-year', type=int)
    parser.add_argument('--end-month', type=int)
    parser.add_argument('--countries', '-c', nargs='+', help='Country codes to process')
    parser.add_argument('--flows', '-f', nargs='+', choices=['export', 'import'])
    parser.add_argument('--sequential', action='store_true', help='Run sequentially instead of parallel')

    args = parser.parse_args()

    # Create orchestrator
    orchestrator = TradeDataOrchestrator()

    if args.command == 'monthly':
        result = orchestrator.run_monthly_pipeline(
            year=args.year,
            month=args.month,
            countries=args.countries,
            flows=args.flows,
            parallel=not args.sequential
        )
        print(json.dumps(asdict(result), indent=2, default=str))

    elif args.command == 'backfill':
        start_year = args.start_year or args.year
        start_month = args.start_month

        results = orchestrator.run_historical_backfill(
            start_year=start_year,
            start_month=start_month,
            end_year=args.end_year,
            end_month=args.end_month,
            countries=args.countries
        )

        summary = {
            'total_periods': len(results),
            'successful': sum(1 for r in results if r.success),
            'failed': sum(1 for r in results if not r.success),
            'total_records': sum(r.total_records_loaded for r in results),
        }
        print(json.dumps(summary, indent=2))

    elif args.command == 'status':
        status = orchestrator.get_pipeline_status()
        print(json.dumps(status, indent=2, default=str))


# =============================================================================
# LINEUP DATA ORCHESTRATOR
# =============================================================================

@dataclass
class LineupPipelineResult:
    """Result of a lineup pipeline run"""
    success: bool
    start_time: datetime
    end_time: Optional[datetime] = None
    report_weeks_processed: List[str] = field(default_factory=list)
    countries_processed: List[str] = field(default_factory=list)
    total_records_fetched: int = 0
    total_records_loaded: int = 0
    total_errors: int = 0
    total_volume_tons: float = 0.0
    country_results: Dict[str, Any] = field(default_factory=dict)
    quality_alerts: List[Dict] = field(default_factory=list)
    error_message: Optional[str] = None


class LineupDataOrchestrator:
    """
    Orchestrates port line-up data collection from South American sources

    Responsibilities:
    1. Initialize and manage lineup-specific agents (ANEC, NABSA, etc.)
    2. Coordinate weekly data fetching
    3. Aggregate lineup data by port and commodity
    4. Perform quality validation
    5. Log all operations
    """

    def __init__(self, config: SouthAmericaTradeConfig = None, db_session_factory=None):
        """
        Initialize lineup orchestrator

        Args:
            config: Pipeline configuration
            db_session_factory: SQLAlchemy session factory
        """
        self.config = config or default_config
        self.db_session_factory = db_session_factory

        # Initialize lineup agents
        self.agents = self._initialize_agents()

        logger.info(f"Initialized LineupDataOrchestrator with {len(self.agents)} agents")

    def _initialize_agents(self) -> Dict[str, Any]:
        """Initialize country-specific lineup agents"""
        agents = {}

        # Brazil ANEC
        if self.config.brazil_lineup.enabled:
            agents['BRA'] = BrazilANECLineupAgent(
                self.config.brazil_lineup,
                self.db_session_factory
            )

        # Argentina NABSA (placeholder - to be implemented)
        # if self.config.argentina_lineup.enabled:
        #     agents['ARG'] = ArgentinaNABSALineupAgent(
        #         self.config.argentina_lineup,
        #         self.db_session_factory
        #     )

        logger.info(f"Initialized {len(agents)} lineup agents: {list(agents.keys())}")
        return agents

    def run_weekly_pipeline(
        self,
        year: int = None,
        week: int = None,
        countries: List[str] = None
    ) -> LineupPipelineResult:
        """
        Run the complete weekly lineup data pipeline

        Args:
            year: Year to process (default: current)
            week: Week to process (default: current)
            countries: List of country codes (default: all enabled)

        Returns:
            LineupPipelineResult with complete pipeline results
        """
        # Default to current week
        today = date.today()
        if year is None:
            year, week, _ = today.isocalendar()
        elif week is None:
            week = today.isocalendar()[1]

        report_week = f"{year}-W{week:02d}"
        start_time = datetime.now()

        result = LineupPipelineResult(
            success=True,
            start_time=start_time,
            report_weeks_processed=[report_week],
        )

        countries = countries or list(self.agents.keys())

        logger.info(f"Starting weekly lineup pipeline for {report_week}")
        logger.info(f"Countries: {countries}")

        # Fetch data from all sources
        all_records = []

        for country in countries:
            if country not in self.agents:
                logger.warning(f"No lineup agent for {country}")
                continue

            try:
                country_result = self._fetch_country_lineup(country, year, week)
                result.country_results[country] = country_result

                result.total_records_fetched += country_result.get('records_fetched', 0)
                result.total_records_loaded += country_result.get('records_loaded', 0)
                result.total_errors += country_result.get('errors', 0)
                result.total_volume_tons += country_result.get('total_volume_tons', 0)

                if country_result.get('records'):
                    all_records.extend(country_result['records'])

                if not country_result.get('success', False):
                    result.success = False

            except Exception as e:
                logger.error(f"Error fetching {country} lineup: {e}")
                result.country_results[country] = {
                    'success': False,
                    'error': str(e)
                }
                result.success = False

        result.countries_processed = countries
        result.end_time = datetime.now()
        duration = (result.end_time - result.start_time).total_seconds()

        logger.info(
            f"Lineup pipeline complete: {result.total_records_loaded} records loaded, "
            f"{result.total_volume_tons:,.0f} tons total, {duration:.1f}s"
        )

        return result

    def _fetch_country_lineup(
        self,
        country: str,
        year: int,
        week: int
    ) -> Dict[str, Any]:
        """Fetch lineup data for a single country"""
        agent = self.agents.get(country)
        if not agent:
            return {'success': False, 'error': 'Agent not found'}

        try:
            # Fetch data
            fetch_result = agent.fetch_data(year=year, week=week)

            if not fetch_result.success:
                return {
                    'success': False,
                    'error': fetch_result.error_message,
                    'records_fetched': 0,
                    'records_loaded': 0,
                }

            # Transform to records
            if fetch_result.data is not None:
                records = agent.transform_to_records(
                    fetch_result.data,
                    fetch_result.report_week
                )
            else:
                records = []

            # Calculate totals
            total_volume = sum(r.get('volume_tons', 0) for r in records)

            return {
                'success': True,
                'records_fetched': fetch_result.records_fetched,
                'records_loaded': len(records),
                'records': records,
                'total_volume_tons': total_volume,
                'report_week': fetch_result.report_week,
                'errors': 0,
            }

        except Exception as e:
            logger.error(f"Error processing {country} lineup: {e}")
            return {
                'success': False,
                'error': str(e),
                'records_fetched': 0,
                'records_loaded': 0,
            }

    def run_historical_backfill(
        self,
        start_year: int,
        start_week: int,
        end_year: int = None,
        end_week: int = None,
        countries: List[str] = None
    ) -> List[LineupPipelineResult]:
        """
        Run historical backfill for a week range

        Args:
            start_year: Start year
            start_week: Start week number
            end_year: End year (default: current)
            end_week: End week (default: current)
            countries: Countries to process

        Returns:
            List of LineupPipelineResult for each week
        """
        today = date.today()
        if end_year is None:
            end_year, end_week, _ = today.isocalendar()
        elif end_week is None:
            end_week = today.isocalendar()[1]

        results = []

        current_year = start_year
        current_week = start_week

        while (current_year < end_year) or (current_year == end_year and current_week <= end_week):
            logger.info(f"Processing lineup week {current_year}-W{current_week:02d}")

            result = self.run_weekly_pipeline(
                year=current_year,
                week=current_week,
                countries=countries
            )
            results.append(result)

            # Move to next week
            current_week += 1
            last_week = date(current_year, 12, 28).isocalendar()[1]
            if current_week > last_week:
                current_week = 1
                current_year += 1

        return results

    def get_agent_status(self) -> Dict[str, Dict]:
        """Get status of all lineup agents"""
        status = {}

        for country, agent in self.agents.items():
            status[country] = agent.get_status()

        return status

    def get_pipeline_status(self) -> Dict:
        """Get overall lineup pipeline status"""
        return {
            'pipeline_name': 'SouthAmericaLineupPipeline',
            'pipeline_type': 'lineup',
            'enabled_countries': list(self.agents.keys()),
            'agent_status': self.get_agent_status(),
        }


# =============================================================================
# LINEUP STANDALONE USAGE
# =============================================================================

def lineup_main():
    """Run lineup orchestrator from command line"""
    import argparse

    parser = argparse.ArgumentParser(
        description='South America Port Lineup Data Pipeline'
    )

    parser.add_argument(
        'command',
        choices=['weekly', 'backfill', 'status'],
        help='Command to execute'
    )

    parser.add_argument('--year', '-y', type=int, default=datetime.now().year)
    parser.add_argument('--week', '-w', type=int, default=None)
    parser.add_argument('--start-year', type=int)
    parser.add_argument('--start-week', type=int, default=1)
    parser.add_argument('--end-year', type=int)
    parser.add_argument('--end-week', type=int)
    parser.add_argument('--countries', '-c', nargs='+', help='Country codes')

    args = parser.parse_args()

    # Create orchestrator
    orchestrator = LineupDataOrchestrator()

    if args.command == 'weekly':
        result = orchestrator.run_weekly_pipeline(
            year=args.year,
            week=args.week,
            countries=args.countries
        )
        print(json.dumps(asdict(result), indent=2, default=str))

    elif args.command == 'backfill':
        start_year = args.start_year or args.year
        start_week = args.start_week

        results = orchestrator.run_historical_backfill(
            start_year=start_year,
            start_week=start_week,
            end_year=args.end_year,
            end_week=args.end_week,
            countries=args.countries
        )

        summary = {
            'total_weeks': len(results),
            'successful': sum(1 for r in results if r.success),
            'failed': sum(1 for r in results if not r.success),
            'total_records': sum(r.total_records_loaded for r in results),
            'total_volume_tons': sum(r.total_volume_tons for r in results),
        }
        print(json.dumps(summary, indent=2))

    elif args.command == 'status':
        status = orchestrator.get_pipeline_status()
        print(json.dumps(status, indent=2, default=str))


if __name__ == '__main__':
    main()
