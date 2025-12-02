"""
Export Inspections Agent
Main orchestrator for FGIS export inspection data collection and processing
"""

import logging
import sys
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from sqlalchemy.orm import Session

from config.settings import AgentConfig, DatabaseType, default_config
from database.models import (
    init_database, InspectionRecord, DataLoadLog,
    Commodity, Country, Port, Grade, create_all_tables
)
from utils.download_manager import FGISDownloadManager, DownloadResult
from utils.csv_parser import FGISCSVParser, FGISDataTransformer, ParsedRecord
from services.aggregation_service import DataAggregationService

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('export_inspections_agent.log')
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class LoadResult:
    """Result of a data load operation"""
    success: bool
    source_file: str
    file_year: int
    records_read: int = 0
    records_inserted: int = 0
    records_updated: int = 0
    records_skipped: int = 0
    records_errored: int = 0
    weeks_processed: int = 0
    earliest_week: Optional[date] = None
    latest_week: Optional[date] = None
    duration_seconds: float = 0.0
    error_message: Optional[str] = None


class ExportInspectionsAgent:
    """
    Main agent for collecting and processing FGIS export inspection data
    
    This agent:
    1. Downloads CSV files from FGIS website
    2. Parses the 112-column CSV format
    3. Stores raw records in database
    4. Generates aggregated summary tables
    5. Supports incremental updates
    """
    
    def __init__(self, config: AgentConfig = None):
        """
        Initialize the agent
        
        Args:
            config: Agent configuration (uses default if not provided)
        """
        self.config = config or default_config
        
        # Initialize components
        self.download_manager = FGISDownloadManager(
            data_directory=self.config.data_source.data_directory,
            timeout=self.config.data_source.download_timeout,
            retry_attempts=self.config.data_source.retry_attempts,
            retry_delay=self.config.data_source.retry_delay
        )
        
        self.parser = FGISCSVParser()
        self.transformer = FGISDataTransformer(config=self.config)
        
        # Database connection (initialized lazily)
        self._session_factory = None
        self._engine = None
        
        logger.info(f"Initialized {self.config.agent_name} v{self.config.agent_version}")
    
    @property
    def session_factory(self):
        """Get database session factory, initializing if needed"""
        if self._session_factory is None:
            self._initialize_database()
        return self._session_factory
    
    def _initialize_database(self):
        """Initialize database connection and create tables"""
        connection_string = self.config.database.get_connection_string()
        logger.info(f"Connecting to database: {self.config.database.db_type.value}")
        
        self._session_factory, self._engine = init_database(
            connection_string,
            echo=self.config.log_level == "DEBUG"
        )
        
        logger.info("Database initialized successfully")
    
    def get_session(self) -> Session:
        """Get a new database session"""
        return self.session_factory()
    
    # =========================================================================
    # LOOKUP TABLE INITIALIZATION
    # =========================================================================
    
    def initialize_lookup_tables(self):
        """
        Populate lookup tables with reference data
        """
        session = self.get_session()
        try:
            self._init_commodities(session)
            self._init_countries(session)
            self._init_ports(session)
            self._init_grades(session)
            session.commit()
            logger.info("Lookup tables initialized")
        except Exception as e:
            session.rollback()
            logger.error(f"Error initializing lookup tables: {e}")
            raise
        finally:
            session.close()
    
    def _init_commodities(self, session: Session):
        """Initialize commodity reference table"""
        commodities = [
            ('SOYBEANS', 'Soybeans', 60.0, 9),
            ('CORN', 'Corn', 56.0, 9),
            ('WHEAT', 'Wheat (All Classes)', 60.0, 6),
            ('SORGHUM', 'Sorghum', 56.0, 9),
            ('BARLEY', 'Barley', 48.0, 6),
            ('OATS', 'Oats', 32.0, 6),
            ('RYE', 'Rye', 56.0, 6),
            ('FLAXSEED', 'Flaxseed', 56.0, 9),
            ('SUNFLOWER', 'Sunflower Seeds', 28.0, 10),
        ]
        
        for code, name, weight, my_start in commodities:
            existing = session.query(Commodity).filter_by(code=code).first()
            if not existing:
                commodity = Commodity(
                    code=code,
                    name=name,
                    bushel_weight_lbs=weight,
                    marketing_year_start_month=my_start
                )
                session.add(commodity)
    
    def _init_countries(self, session: Session):
        """Initialize country reference table with region mappings"""
        for country, region in self.config.regions.destination_regions.items():
            existing = session.query(Country).filter_by(name=country).first()
            if not existing:
                c = Country(name=country, region=region)
                session.add(c)
    
    def _init_ports(self, session: Session):
        """Initialize port reference table"""
        for port, region in self.config.regions.port_region_mapping.items():
            existing = session.query(Port).filter_by(code=port).first()
            if not existing:
                p = Port(code=port, name=port, region=region)
                session.add(p)
    
    def _init_grades(self, session: Session):
        """Initialize grade reference table"""
        grades = [
            ('US NO. 1', 'U.S. No. 1', 1),
            ('US NO. 2', 'U.S. No. 2', 2),
            ('US NO. 3', 'U.S. No. 3', 3),
            ('US NO. 4', 'U.S. No. 4', 4),
            ('US NO. 5', 'U.S. No. 5', 5),
            ('SAMPLE', 'Sample Grade', None),
            ('SMPL GR', 'Sample Grade', None),
        ]
        
        for code, name, num in grades:
            existing = session.query(Grade).filter_by(code=code).first()
            if not existing:
                g = Grade(code=code, name=name, numeric_grade=num)
                session.add(g)
    
    # =========================================================================
    # DATA DOWNLOAD
    # =========================================================================
    
    def download_data(self, year: int = None, force: bool = False) -> DownloadResult:
        """
        Download CSV file for specified year
        
        Args:
            year: Year to download (default: current year)
            force: Force download even if file exists
            
        Returns:
            DownloadResult with status
        """
        year = year or datetime.now().year
        logger.info(f"Downloading data for year {year}")
        
        result = self.download_manager.download_year(year, force=force)
        
        if result.success:
            logger.info(f"Download complete: {result}")
        else:
            logger.error(f"Download failed: {result.error_message}")
        
        return result
    
    def download_historical(self, start_year: int, end_year: int = None,
                           force: bool = False) -> List[DownloadResult]:
        """
        Download historical data for multiple years
        
        Args:
            start_year: First year to download
            end_year: Last year (default: current year)
            force: Force download even if files exist
            
        Returns:
            List of DownloadResult objects
        """
        end_year = end_year or datetime.now().year
        logger.info(f"Downloading historical data from {start_year} to {end_year}")
        
        results = self.download_manager.download_years(start_year, end_year, force)
        
        success_count = sum(1 for r in results if r.success)
        logger.info(f"Downloaded {success_count}/{len(results)} files successfully")
        
        return results
    
    # =========================================================================
    # DATA LOADING
    # =========================================================================
    
    def load_file(self, file_path: Path, incremental: bool = True) -> LoadResult:
        """
        Load data from a CSV file into the database
        
        Args:
            file_path: Path to CSV file
            incremental: If True, skip existing records
            
        Returns:
            LoadResult with statistics
        """
        file_path = Path(file_path)
        start_time = datetime.now()
        
        logger.info(f"Loading file: {file_path}")
        
        # Validate file structure
        is_valid, issues = self.parser.validate_file_structure(file_path)
        if not is_valid:
            return LoadResult(
                success=False,
                source_file=file_path.name,
                file_year=0,
                error_message=f"Invalid file structure: {issues}"
            )
        
        # Extract year from filename
        import re
        year_match = re.search(r'(?:CY)?(\d{4})', file_path.stem)
        file_year = int(year_match.group(1)) if year_match else datetime.now().year
        
        session = self.get_session()
        
        result = LoadResult(
            success=True,
            source_file=file_path.name,
            file_year=file_year
        )
        
        weeks_seen = set()
        batch = []
        batch_size = self.config.batch_size
        
        try:
            for record in self.parser.parse_file(file_path, file_year):
                result.records_read += 1
                
                if not record.is_valid:
                    result.records_errored += 1
                    continue
                
                # Transform record
                data = self.transformer.transform_record(record)
                
                # Track weeks
                if data.get('week_ending_date'):
                    weeks_seen.add(data['week_ending_date'])
                    
                    # Track date range
                    if result.earliest_week is None or data['week_ending_date'] < result.earliest_week:
                        result.earliest_week = data['week_ending_date']
                    if result.latest_week is None or data['week_ending_date'] > result.latest_week:
                        result.latest_week = data['week_ending_date']
                
                # Check for existing record if incremental
                if incremental:
                    existing = session.query(InspectionRecord).filter(
                        InspectionRecord.serial_number == data.get('serial_number'),
                        InspectionRecord.week_ending_date == data.get('week_ending_date'),
                        InspectionRecord.grain == data.get('grain')
                    ).first()
                    
                    if existing:
                        result.records_skipped += 1
                        continue
                
                # Add to batch
                batch.append(data)
                
                # Process batch
                if len(batch) >= batch_size:
                    inserted = self._insert_batch(session, batch)
                    result.records_inserted += inserted
                    batch = []
            
            # Insert remaining batch
            if batch:
                inserted = self._insert_batch(session, batch)
                result.records_inserted += inserted
            
            result.weeks_processed = len(weeks_seen)
            
            # Log the load
            self._log_data_load(session, result, start_time)
            
            session.commit()
            
        except Exception as e:
            session.rollback()
            result.success = False
            result.error_message = str(e)
            logger.error(f"Error loading file: {e}")
        finally:
            session.close()
        
        result.duration_seconds = (datetime.now() - start_time).total_seconds()
        
        logger.info(f"Load complete: {result.records_inserted} inserted, "
                   f"{result.records_skipped} skipped, {result.records_errored} errors")
        
        return result
    
    def _insert_batch(self, session: Session, batch: List[Dict]) -> int:
        """Insert a batch of records"""
        inserted = 0
        
        for data in batch:
            try:
                record = InspectionRecord(**data)
                session.add(record)
                inserted += 1
            except Exception as e:
                logger.warning(f"Error inserting record: {e}")
        
        session.flush()
        return inserted
    
    def _log_data_load(self, session: Session, result: LoadResult, start_time: datetime):
        """Log data load operation"""
        log_entry = DataLoadLog(
            load_timestamp=start_time,
            source_file=result.source_file,
            file_year=result.file_year,
            records_read=result.records_read,
            records_inserted=result.records_inserted,
            records_updated=result.records_updated,
            records_skipped=result.records_skipped,
            records_errored=result.records_errored,
            weeks_processed=result.weeks_processed,
            earliest_week=result.earliest_week,
            latest_week=result.latest_week,
            status='SUCCESS' if result.success else 'FAILED',
            error_message=result.error_message,
            duration_seconds=(datetime.now() - start_time).total_seconds()
        )
        session.add(log_entry)
    
    def load_all_local_files(self, incremental: bool = True) -> List[LoadResult]:
        """
        Load all CSV files from the data directory
        
        Returns:
            List of LoadResult objects
        """
        local_files = self.download_manager.get_local_files()
        results = []
        
        for year in sorted(local_files.keys()):
            file_path = local_files[year]
            logger.info(f"Loading {file_path}")
            result = self.load_file(file_path, incremental=incremental)
            results.append(result)
        
        return results
    
    # =========================================================================
    # DATA AGGREGATION
    # =========================================================================
    
    def run_aggregations(self, week_date: date = None) -> Dict[str, int]:
        """
        Run all aggregations for a specific week or all weeks
        
        Args:
            week_date: Specific week to aggregate, or None for all
            
        Returns:
            Dict with aggregation counts
        """
        session = self.get_session()
        
        try:
            agg_service = DataAggregationService(session, self.config)
            
            if week_date:
                results = agg_service.aggregate_week(week_date)
            else:
                results = agg_service.aggregate_all_weeks()
            
            return results
            
        finally:
            session.close()
    
    # =========================================================================
    # FULL UPDATE WORKFLOW
    # =========================================================================
    
    def run_weekly_update(self) -> Dict:
        """
        Run the weekly update workflow:
        1. Download latest data
        2. Load new records
        3. Run aggregations
        
        Returns:
            Dict with operation results
        """
        results = {
            'download': None,
            'load': None,
            'aggregations': None,
            'success': False
        }
        
        logger.info("Starting weekly update workflow")
        
        try:
            # Step 1: Download current year data
            download_result = self.download_data()
            results['download'] = {
                'success': download_result.success,
                'file': str(download_result.file_path) if download_result.file_path else None,
                'is_new_data': download_result.is_new_data
            }
            
            if not download_result.success:
                logger.error("Download failed, aborting update")
                return results
            
            # Step 2: Load if new data
            if download_result.is_new_data and download_result.file_path:
                load_result = self.load_file(download_result.file_path)
                results['load'] = {
                    'success': load_result.success,
                    'records_inserted': load_result.records_inserted,
                    'records_skipped': load_result.records_skipped,
                    'weeks_processed': load_result.weeks_processed
                }
                
                if not load_result.success:
                    logger.error("Load failed, aborting aggregations")
                    return results
                
                # Step 3: Run aggregations for new weeks
                if load_result.latest_week:
                    agg_results = self.run_aggregations(load_result.latest_week)
                    results['aggregations'] = agg_results
            else:
                results['load'] = {'skipped': True, 'reason': 'No new data'}
            
            results['success'] = True
            logger.info("Weekly update completed successfully")
            
        except Exception as e:
            logger.error(f"Weekly update failed: {e}")
            results['error'] = str(e)
        
        return results
    
    def run_full_historical_load(self, start_year: int = 2010) -> Dict:
        """
        Run a full historical load from start_year to present
        
        Args:
            start_year: First year to load
            
        Returns:
            Dict with operation results
        """
        results = {
            'downloads': [],
            'loads': [],
            'aggregations': None,
            'success': False
        }
        
        logger.info(f"Starting full historical load from {start_year}")
        
        try:
            # Initialize lookup tables
            self.initialize_lookup_tables()
            
            # Download all years
            download_results = self.download_historical(start_year)
            results['downloads'] = [
                {'year': r.url.split('/')[-1], 'success': r.success}
                for r in download_results
            ]
            
            # Load all files
            load_results = self.load_all_local_files(incremental=False)
            results['loads'] = [
                {
                    'file': r.source_file,
                    'success': r.success,
                    'inserted': r.records_inserted
                }
                for r in load_results
            ]
            
            # Run aggregations
            agg_results = self.run_aggregations()
            results['aggregations'] = agg_results
            
            results['success'] = True
            logger.info("Full historical load completed")
            
        except Exception as e:
            logger.error(f"Historical load failed: {e}")
            results['error'] = str(e)
        
        return results
    
    # =========================================================================
    # STATUS AND REPORTING
    # =========================================================================
    
    def get_status(self) -> Dict:
        """
        Get current agent status and data summary
        """
        session = self.get_session()
        
        try:
            from sqlalchemy import func
            
            # Count records
            total_records = session.query(func.count(InspectionRecord.id)).scalar()
            
            # Get date range
            date_range = session.query(
                func.min(InspectionRecord.week_ending_date),
                func.max(InspectionRecord.week_ending_date)
            ).first()
            
            # Count by commodity
            commodities = session.query(
                InspectionRecord.grain,
                func.count(InspectionRecord.id)
            ).group_by(InspectionRecord.grain).all()
            
            # Recent loads
            recent_loads = session.query(DataLoadLog).order_by(
                DataLoadLog.load_timestamp.desc()
            ).limit(5).all()
            
            return {
                'agent': self.config.agent_name,
                'version': self.config.agent_version,
                'database_type': self.config.database.db_type.value,
                'total_records': total_records,
                'date_range': {
                    'earliest': str(date_range[0]) if date_range[0] else None,
                    'latest': str(date_range[1]) if date_range[1] else None
                },
                'records_by_commodity': {c: count for c, count in commodities},
                'recent_loads': [
                    {
                        'file': l.source_file,
                        'timestamp': str(l.load_timestamp),
                        'records': l.records_inserted,
                        'status': l.status
                    }
                    for l in recent_loads
                ],
                'local_files': list(self.download_manager.get_local_files().keys())
            }
            
        finally:
            session.close()


# =============================================================================
# CLI INTERFACE
# =============================================================================

def main():
    """Command-line interface for the Export Inspections Agent"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='FGIS Export Inspections Data Collection Agent'
    )
    
    parser.add_argument(
        'command',
        choices=['download', 'load', 'aggregate', 'update', 'historical', 'status'],
        help='Command to execute'
    )
    
    parser.add_argument(
        '--year', '-y',
        type=int,
        help='Year to process (default: current year)'
    )
    
    parser.add_argument(
        '--start-year',
        type=int,
        default=2015,
        help='Start year for historical load'
    )
    
    parser.add_argument(
        '--force', '-f',
        action='store_true',
        help='Force operation (re-download, overwrite existing)'
    )
    
    parser.add_argument(
        '--db-type',
        choices=['sqlite', 'mysql', 'postgresql'],
        default='sqlite',
        help='Database type'
    )
    
    parser.add_argument(
        '--db-path',
        type=str,
        default='./data/export_inspections.db',
        help='Database path (for SQLite)'
    )
    
    args = parser.parse_args()
    
    # Configure database from args
    config = AgentConfig()
    config.database.db_type = DatabaseType(args.db_type)
    if args.db_type == 'sqlite':
        config.database.sqlite_path = Path(args.db_path)
    
    # Create agent
    agent = ExportInspectionsAgent(config)
    
    # Execute command
    if args.command == 'download':
        result = agent.download_data(year=args.year, force=args.force)
        print(f"Download: {result}")
        
    elif args.command == 'load':
        local_files = agent.download_manager.get_local_files()
        year = args.year or max(local_files.keys()) if local_files else None
        
        if year and year in local_files:
            result = agent.load_file(local_files[year], incremental=not args.force)
            print(f"Load result: {result.records_inserted} inserted, "
                  f"{result.records_skipped} skipped")
        else:
            print(f"No file found for year {year}")
            
    elif args.command == 'aggregate':
        results = agent.run_aggregations()
        print(f"Aggregation results: {results}")
        
    elif args.command == 'update':
        results = agent.run_weekly_update()
        print(f"Update results: {results}")
        
    elif args.command == 'historical':
        results = agent.run_full_historical_load(start_year=args.start_year)
        print(f"Historical load results: {results}")
        
    elif args.command == 'status':
        status = agent.get_status()
        import json
        print(json.dumps(status, indent=2, default=str))


if __name__ == '__main__':
    main()
