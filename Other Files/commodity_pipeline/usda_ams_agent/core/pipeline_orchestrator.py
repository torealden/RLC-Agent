"""
Master Orchestrator for Commodity Data Pipeline
Coordinates all agents: Collector, Database, Verification
Manages the complete ETL workflow from fetch to verified storage
Round Lakes Commodities
"""

import asyncio
import logging
import json
import sys
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from pathlib import Path
import uuid

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logger = logging.getLogger(__name__)


@dataclass
class PipelineRunResult:
    """Result of a complete pipeline run"""
    run_id: str
    start_time: str
    end_time: str
    status: str  # 'SUCCESS', 'PARTIAL_SUCCESS', 'FAILED'
    reports_processed: int
    reports_successful: int
    reports_failed: int
    total_records_fetched: int
    total_records_inserted: int
    total_records_skipped: int
    verification_status: str
    error_messages: List[str] = field(default_factory=list)
    report_details: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def duration_seconds(self) -> float:
        start = datetime.fromisoformat(self.start_time)
        end = datetime.fromisoformat(self.end_time)
        return (end - start).total_seconds()
    
    def to_dict(self) -> Dict:
        return {
            'run_id': self.run_id,
            'start_time': self.start_time,
            'end_time': self.end_time,
            'duration_seconds': round(self.duration_seconds, 2),
            'status': self.status,
            'reports_processed': self.reports_processed,
            'reports_successful': self.reports_successful,
            'reports_failed': self.reports_failed,
            'total_records_fetched': self.total_records_fetched,
            'total_records_inserted': self.total_records_inserted,
            'total_records_skipped': self.total_records_skipped,
            'verification_status': self.verification_status,
            'error_messages': self.error_messages,
            'report_details': self.report_details
        }
    
    def summary(self) -> str:
        return (
            f"Pipeline Run {self.run_id}\n"
            f"  Status: {self.status}\n"
            f"  Duration: {self.duration_seconds:.1f}s\n"
            f"  Reports: {self.reports_successful}/{self.reports_processed} successful\n"
            f"  Records: {self.total_records_inserted} inserted, {self.total_records_skipped} skipped\n"
            f"  Verification: {self.verification_status}"
        )


class PipelineOrchestrator:
    """
    Master orchestrator for the commodity data pipeline.
    Coordinates data collection, parsing, storage, and verification.
    """
    
    def __init__(self, 
                 collector=None,
                 database_agent=None,
                 verification_agent=None,
                 settings=None):
        """
        Initialize the pipeline orchestrator.
        
        Args:
            collector: USDACollector instance (or compatible collector)
            database_agent: DatabaseAgent instance
            verification_agent: VerificationAgent instance
            settings: Settings configuration object
        """
        self.collector = collector
        self.db = database_agent
        self.verifier = verification_agent
        self.settings = settings
        
        # Lazy initialization flags
        self._initialized = False
        
        logger.info("PipelineOrchestrator created")
    
    def initialize(self) -> bool:
        """
        Initialize all components if not already done.
        Called automatically before pipeline runs.
        
        Returns:
            True if initialization successful
        """
        if self._initialized:
            return True
        
        try:
            # Initialize components if not provided
            if self.settings is None:
                from config.settings import get_settings
                self.settings = get_settings()
            
            if self.db is None:
                from agents.database_agent import create_database_agent
                self.db = create_database_agent(self.settings)
            
            # Initialize database schema
            if not self.db.initialize_schema():
                logger.error("Failed to initialize database schema")
                return False
            
            if self.verifier is None:
                from agents.verification_agent import create_verification_agent
                self.verifier = create_verification_agent(
                    self.db, 
                    sample_size=self.settings.pipeline.verification_sample_size
                )
            
            # Collector is optional - can be passed externally
            if self.collector is None:
                logger.info("No collector provided - external collector required for data fetching")
            
            self._initialized = True
            logger.info("Pipeline orchestrator initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Pipeline initialization failed: {e}")
            return False
    
    async def run_daily_pipeline(self, 
                                  report_date: str = None,
                                  reports: List[Dict] = None) -> PipelineRunResult:
        """
        Run the complete daily data pipeline.
        
        Args:
            report_date: Date to collect data for (default: today)
            reports: List of report configs (uses collector's config if None)
            
        Returns:
            PipelineRunResult with complete run statistics
        """
        run_id = f"daily_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
        start_time = datetime.now().isoformat()
        
        logger.info(f"Starting daily pipeline run: {run_id}")
        
        if not self.initialize():
            return PipelineRunResult(
                run_id=run_id,
                start_time=start_time,
                end_time=datetime.now().isoformat(),
                status='FAILED',
                reports_processed=0,
                reports_successful=0,
                reports_failed=0,
                total_records_fetched=0,
                total_records_inserted=0,
                total_records_skipped=0,
                verification_status='NOT_RUN',
                error_messages=['Pipeline initialization failed']
            )
        
        if report_date is None:
            report_date = datetime.now().strftime('%m/%d/%Y')
        
        # Run the pipeline
        result = await self._execute_pipeline(
            run_id=run_id,
            start_time=start_time,
            report_date=report_date,
            reports=reports
        )
        
        logger.info(f"Pipeline run complete: {result.status}")
        logger.info(result.summary())
        
        return result
    
    async def run_historical_backfill(self,
                                       start_date: str = None,
                                       end_date: str = None,
                                       reports: List[Dict] = None) -> PipelineRunResult:
        """
        Run historical data backfill.
        
        Args:
            start_date: Start date (MM/DD/YYYY format)
            end_date: End date (MM/DD/YYYY format)
            reports: List of report configs
            
        Returns:
            PipelineRunResult with complete run statistics
        """
        run_id = f"backfill_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
        start_time = datetime.now().isoformat()
        
        logger.info(f"Starting historical backfill: {run_id}")
        
        if not self.initialize():
            return PipelineRunResult(
                run_id=run_id,
                start_time=start_time,
                end_time=datetime.now().isoformat(),
                status='FAILED',
                reports_processed=0,
                reports_successful=0,
                reports_failed=0,
                total_records_fetched=0,
                total_records_inserted=0,
                total_records_skipped=0,
                verification_status='NOT_RUN',
                error_messages=['Pipeline initialization failed']
            )
        
        # Use settings defaults if not provided
        if start_date is None:
            start_date = self.settings.pipeline.historical_start_date
        if end_date is None:
            end_date = self.settings.pipeline.historical_end_date
        
        # For historical, we need to modify the API call to include date range
        # This is passed through as a special date format
        date_range = f"{start_date}:{end_date}"
        
        result = await self._execute_pipeline(
            run_id=run_id,
            start_time=start_time,
            report_date=date_range,
            reports=reports,
            is_historical=True
        )
        
        logger.info(f"Historical backfill complete: {result.status}")
        logger.info(result.summary())
        
        return result
    
    async def _execute_pipeline(self,
                                 run_id: str,
                                 start_time: str,
                                 report_date: str,
                                 reports: List[Dict] = None,
                                 is_historical: bool = False) -> PipelineRunResult:
        """
        Internal method to execute the pipeline workflow.
        """
        reports_processed = 0
        reports_successful = 0
        reports_failed = 0
        total_fetched = 0
        total_inserted = 0
        total_skipped = 0
        error_messages = []
        report_details = {}
        verification_status = 'NOT_RUN'
        
        try:
            # Step 1: Collect data
            logger.info("Step 1: Collecting data from API...")
            
            if self.collector is None:
                error_messages.append("No collector configured")
                return PipelineRunResult(
                    run_id=run_id,
                    start_time=start_time,
                    end_time=datetime.now().isoformat(),
                    status='FAILED',
                    reports_processed=0,
                    reports_successful=0,
                    reports_failed=0,
                    total_records_fetched=0,
                    total_records_inserted=0,
                    total_records_skipped=0,
                    verification_status='NOT_RUN',
                    error_messages=error_messages
                )
            
            # Collect data using the async collector
            collected_data = await self.collector.collect_daily_prices(report_date)
            total_fetched = len(collected_data)
            
            logger.info(f"Collected {total_fetched} records")
            
            if total_fetched == 0:
                logger.warning("No data collected from API")
                return PipelineRunResult(
                    run_id=run_id,
                    start_time=start_time,
                    end_time=datetime.now().isoformat(),
                    status='SUCCESS',  # No data is not necessarily an error
                    reports_processed=len(reports) if reports else 0,
                    reports_successful=0,
                    reports_failed=0,
                    total_records_fetched=0,
                    total_records_inserted=0,
                    total_records_skipped=0,
                    verification_status='N/A',
                    error_messages=['No data returned from API (may be expected for non-trading days)']
                )
            
            # Step 2: Store in database
            logger.info("Step 2: Storing data in database...")
            
            if self.settings.pipeline.enable_database_output:
                # Group records by source report for better tracking
                records_by_source = {}
                for record in collected_data:
                    source = record.get('source_report', record.get('source', 'Unknown'))
                    if source not in records_by_source:
                        records_by_source[source] = []
                    records_by_source[source].append(record)
                
                # Insert records by source
                for source, records in records_by_source.items():
                    reports_processed += 1
                    try:
                        insert_result = self.db.insert_price_records(records)
                        
                        total_inserted += insert_result.inserted
                        total_skipped += insert_result.skipped
                        
                        report_details[source] = {
                            'fetched': len(records),
                            'inserted': insert_result.inserted,
                            'skipped': insert_result.skipped,
                            'errors': insert_result.errors
                        }
                        
                        if insert_result.errors > 0:
                            error_messages.extend(insert_result.error_messages[:3])  # Limit errors
                            reports_failed += 1
                        else:
                            reports_successful += 1
                        
                        # Log quality metrics
                        self.db.log_quality_result(
                            source_report=source,
                            records_fetched=len(records),
                            records_inserted=insert_result.inserted,
                            records_skipped=insert_result.skipped,
                            records_failed=insert_result.errors,
                            verification_status='PENDING',
                            error_messages=insert_result.error_messages[:5]
                        )
                        
                    except Exception as e:
                        reports_failed += 1
                        error_msg = f"Error inserting records for {source}: {e}"
                        error_messages.append(error_msg)
                        logger.error(error_msg)
            
            # Step 3: Verify data
            logger.info("Step 3: Verifying data integrity...")
            
            if self.settings.pipeline.verification_enabled and self.verifier:
                try:
                    # Run verification for each source
                    all_verifications_passed = True
                    
                    for source, records in records_by_source.items():
                        verification_report = self.verifier.verify_insert(
                            source_records=records,
                            source_report=source
                        )
                        
                        if verification_report.status != 'PASSED':
                            all_verifications_passed = False
                        
                        report_details[source]['verification'] = verification_report.status
                    
                    verification_status = 'PASSED' if all_verifications_passed else 'PARTIAL'
                    
                except Exception as e:
                    verification_status = 'ERROR'
                    error_messages.append(f"Verification error: {e}")
                    logger.error(f"Verification failed: {e}")
            else:
                verification_status = 'DISABLED'
            
            # Determine overall status
            if reports_failed == 0 and total_inserted > 0:
                status = 'SUCCESS'
            elif reports_successful > 0:
                status = 'PARTIAL_SUCCESS'
            else:
                status = 'FAILED'
            
        except Exception as e:
            status = 'FAILED'
            error_messages.append(f"Pipeline execution error: {e}")
            logger.error(f"Pipeline execution failed: {e}")
        
        return PipelineRunResult(
            run_id=run_id,
            start_time=start_time,
            end_time=datetime.now().isoformat(),
            status=status,
            reports_processed=reports_processed,
            reports_successful=reports_successful,
            reports_failed=reports_failed,
            total_records_fetched=total_fetched,
            total_records_inserted=total_inserted,
            total_records_skipped=total_skipped,
            verification_status=verification_status,
            error_messages=error_messages,
            report_details=report_details
        )
    
    def get_database_status(self) -> Dict[str, Any]:
        """
        Get current database status and statistics.
        
        Returns:
            Dictionary with database statistics
        """
        if not self.initialize():
            return {'error': 'Failed to initialize database'}
        
        return self.db.get_statistics()
    
    def verify_database_health(self) -> Dict[str, Any]:
        """
        Run database health verification.
        
        Returns:
            Verification report dictionary
        """
        if not self.initialize():
            return {'error': 'Failed to initialize'}
        
        report = self.verifier.verify_database_integrity()
        return report.to_dict()
    
    async def test_connection(self) -> bool:
        """
        Test API and database connections.
        
        Returns:
            True if all connections are working
        """
        if not self.initialize():
            return False
        
        try:
            # Test database
            count = self.db.get_record_count()
            logger.info(f"Database connection OK ({count} records)")
            
            # Test API if collector available
            if self.collector:
                reports = await self.collector.get_available_reports()
                logger.info(f"API connection OK ({len(reports)} reports available)")
            
            return True
            
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False


def create_pipeline_orchestrator(collector=None, settings=None) -> PipelineOrchestrator:
    """
    Factory function to create a fully configured PipelineOrchestrator.
    
    Args:
        collector: USDACollector instance (optional)
        settings: Settings configuration (uses global if None)
        
    Returns:
        Configured PipelineOrchestrator instance
    """
    if settings is None:
        from config.settings import get_settings
        settings = get_settings()
    
    from agents.database_agent import create_database_agent
    from agents.verification_agent import create_verification_agent
    
    db = create_database_agent(settings)
    verifier = create_verification_agent(db, settings.pipeline.verification_sample_size)
    
    return PipelineOrchestrator(
        collector=collector,
        database_agent=db,
        verification_agent=verifier,
        settings=settings
    )


# CLI entry point
async def main():
    """
    Command-line interface for the pipeline orchestrator.
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='Commodity Data Pipeline Orchestrator')
    parser.add_argument('command', choices=['daily', 'backfill', 'status', 'verify', 'test'],
                       help='Command to execute')
    parser.add_argument('--date', type=str, help='Report date (MM/DD/YYYY)')
    parser.add_argument('--start-date', type=str, help='Start date for backfill')
    parser.add_argument('--end-date', type=str, help='End date for backfill')
    parser.add_argument('--config', type=str, help='Path to report config file')
    
    args = parser.parse_args()
    
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create orchestrator (without collector for CLI - would need to import separately)
    orchestrator = create_pipeline_orchestrator()
    
    if args.command == 'status':
        stats = orchestrator.get_database_status()
        print("\nDatabase Status:")
        print(json.dumps(stats, indent=2, default=str))
        
    elif args.command == 'verify':
        result = orchestrator.verify_database_health()
        print("\nDatabase Verification:")
        print(json.dumps(result, indent=2, default=str))
        
    elif args.command == 'test':
        success = await orchestrator.test_connection()
        print(f"\nConnection Test: {'PASSED' if success else 'FAILED'}")
        
    elif args.command == 'daily':
        # Note: Requires collector to be configured
        print("\nNote: Daily collection requires USDACollector to be configured.")
        print("Use this with the main.py entry point that includes the collector.")
        
    elif args.command == 'backfill':
        print("\nNote: Historical backfill requires USDACollector to be configured.")
        print("Use this with the main.py entry point that includes the collector.")


if __name__ == "__main__":
    asyncio.run(main())