#!/usr/bin/env python3
"""
Commodity Data Pipeline - Main Application Entry Point
Complete ETL pipeline for USDA AMS commodity price data
Round Lakes Commodities

Usage:
    python main.py daily                    # Run daily data collection
    python main.py daily --date 11/25/2025  # Collect specific date
    python main.py backfill                 # Historical data backfill
    python main.py status                   # Show database status
    python main.py verify                   # Verify database integrity
    python main.py test                     # Test connections
"""

import asyncio
import argparse
import logging
import json
import sys
import os
from datetime import datetime
from pathlib import Path

# Add the project root to the path
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

# Configure logging
def setup_logging(log_dir: str = './logs', log_level: str = 'INFO'):
    """Set up logging configuration"""
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    
    log_file = Path(log_dir) / f"pipeline_{datetime.now().strftime('%Y%m%d')}.log"
    
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file)
        ]
    )
    
    return logging.getLogger(__name__)


# Import components after path setup
from config.settings import get_settings, Settings
from agents.database_agent import DatabaseAgent, create_database_agent
from agents.verification_agent import VerificationAgent, create_verification_agent
from core.pipeline_orchestrator import PipelineOrchestrator, create_pipeline_orchestrator


class USDACollectorAdapter:
    """
    Adapter class to integrate with the external USDA collector.
    This allows the pipeline to work with the existing usda_ams_collector_asynch.py
    """
    
    def __init__(self, collector_module_path: str = None, 
                 api_key: str = None,
                 output_dir: str = './data',
                 config_path: str = 'report_config.xlsx'):
        """
        Initialize the collector adapter.
        
        Args:
            collector_module_path: Path to the usda_ams_collector_asynch.py file
            api_key: USDA API key (reads from env if None)
            output_dir: Output directory for data files
            config_path: Path to report configuration Excel file
        """
        self.api_key = api_key or os.getenv('USDA_AMS_API_KEY')
        self.output_dir = output_dir
        self.config_path = config_path
        self.collector = None
        self._initialize_collector(collector_module_path)
    
    def _initialize_collector(self, module_path: str = None):
        """Initialize the USDA collector"""
        try:
            # Try to import from installed location first
            try:
                from usda_ams_collector_asynch import USDACollector
            except ImportError:
                # Try from common locations
                possible_paths = [
                    module_path,
                    './usda_ams_collector_asynch.py',
                    '../usda_ams_collector_asynch.py',
                    'usda_ams_collector_asynch.py'
                ]
                
                USDACollector = None
                for path in possible_paths:
                    if path and Path(path).exists():
                        import importlib.util
                        spec = importlib.util.spec_from_file_location("usda_collector", path)
                        module = importlib.util.module_from_spec(spec)
                        spec.loader.exec_module(module)
                        USDACollector = module.USDACollector
                        break
                
                if USDACollector is None:
                    raise ImportError("Could not find usda_ams_collector_asynch.py")
            
            self.collector = USDACollector(
                api_key=self.api_key,
                output_dir=self.output_dir,
                config_path=self.config_path
            )
            logging.getLogger(__name__).info("USDA Collector initialized successfully")
            
        except Exception as e:
            logging.getLogger(__name__).error(f"Failed to initialize USDA collector: {e}")
            self.collector = None
    
    async def collect_daily_prices(self, report_date: str = None):
        """Collect daily prices using the USDA collector"""
        if self.collector is None:
            raise RuntimeError("USDA Collector not initialized")
        return await self.collector.collect_daily_prices(report_date)
    
    async def get_available_reports(self):
        """Get available reports from USDA API"""
        if self.collector is None:
            return []
        return await self.collector.get_available_reports()
    
    def list_configured_reports(self):
        """Get configured reports"""
        if self.collector is None:
            return []
        return self.collector.list_configured_reports()


class CommodityPipeline:
    """
    Main application class that provides a simple interface for all pipeline operations.
    """
    
    def __init__(self, 
                 collector_path: str = None,
                 config_path: str = 'report_config.xlsx'):
        """
        Initialize the commodity pipeline.
        
        Args:
            collector_path: Path to the USDA collector script
            config_path: Path to report configuration file
        """
        self.settings = get_settings()
        self.logger = logging.getLogger(__name__)
        
        # Initialize collector
        self.collector = USDACollectorAdapter(
            collector_module_path=collector_path,
            output_dir=self.settings.pipeline.output_dir,
            config_path=config_path
        )
        
        # Initialize orchestrator with collector
        self.orchestrator = create_pipeline_orchestrator(
            collector=self.collector,
            settings=self.settings
        )
        
        self.logger.info("Commodity Pipeline initialized")
    
    async def run_daily(self, report_date: str = None):
        """
        Run daily data collection and storage.
        
        Args:
            report_date: Date to collect (MM/DD/YYYY), defaults to today
        """
        self.logger.info(f"Running daily pipeline for date: {report_date or 'today'}")
        
        result = await self.orchestrator.run_daily_pipeline(report_date=report_date)
        
        print("\n" + "="*60)
        print("PIPELINE RUN COMPLETE")
        print("="*60)
        print(result.summary())
        
        if result.error_messages:
            print("\nErrors:")
            for msg in result.error_messages[:10]:
                print(f"  - {msg}")
        
        return result
    
    async def run_backfill(self, start_date: str = None, end_date: str = None):
        """
        Run historical data backfill.
        
        Args:
            start_date: Start date (MM/DD/YYYY)
            end_date: End date (MM/DD/YYYY)
        """
        self.logger.info(f"Running historical backfill: {start_date} to {end_date}")
        
        result = await self.orchestrator.run_historical_backfill(
            start_date=start_date,
            end_date=end_date
        )
        
        print("\n" + "="*60)
        print("BACKFILL COMPLETE")
        print("="*60)
        print(result.summary())
        
        return result
    
    def show_status(self):
        """Display database status and statistics"""
        stats = self.orchestrator.get_database_status()
        
        print("\n" + "="*60)
        print("DATABASE STATUS")
        print("="*60)
        print(f"Total Records: {stats.get('total_records', 0):,}")
        print(f"Unique Commodities: {stats.get('unique_commodities', 0)}")
        print(f"Data Sources: {stats.get('unique_sources', 0)}")
        
        date_range = stats.get('date_range', (None, None))
        if date_range[0]:
            print(f"Date Range: {date_range[0]} to {date_range[1]}")
        else:
            print("Date Range: No data yet")
        
        if stats.get('commodities'):
            print(f"\nCommodities: {', '.join(stats['commodities'][:10])}")
            if len(stats['commodities']) > 10:
                print(f"  ... and {len(stats['commodities']) - 10} more")
        
        if stats.get('sources'):
            print(f"\nData Sources: {', '.join(stats['sources'])}")
        
        return stats
    
    def verify_data(self):
        """Run data verification checks"""
        result = self.orchestrator.verify_database_health()
        
        print("\n" + "="*60)
        print("DATA VERIFICATION")
        print("="*60)
        print(f"Status: {result.get('status', 'UNKNOWN')}")
        print(f"Checks Passed: {result.get('passed_checks', 0)}/{result.get('total_checks', 0)}")
        print(f"Summary: {result.get('summary', '')}")
        
        if result.get('results'):
            print("\nDetailed Results:")
            for check in result['results']:
                status = "✓" if check['passed'] else "✗"
                print(f"  {status} {check['check_type']}: {check['message']}")
        
        return result
    
    async def test_connections(self):
        """Test API and database connections"""
        print("\n" + "="*60)
        print("CONNECTION TEST")
        print("="*60)
        
        # Test database
        try:
            count = self.orchestrator.db.get_record_count()
            print(f"✓ Database: Connected ({count:,} records)")
        except Exception as e:
            print(f"✗ Database: Failed ({e})")
        
        # Test API
        try:
            if self.collector.collector:
                reports = await self.collector.get_available_reports()
                print(f"✓ USDA API: Connected ({len(reports)} reports available)")
            else:
                print("✗ USDA API: Collector not initialized")
        except Exception as e:
            print(f"✗ USDA API: Failed ({e})")
        
        # Show configured reports
        reports = self.collector.list_configured_reports()
        if reports:
            print(f"\nConfigured Reports ({len(reports)}):")
            for r in reports:
                print(f"  - {r.get('name', 'Unknown')} (ID: {r.get('id', 'N/A')})")
    
    def show_reports(self):
        """Show configured reports"""
        reports = self.collector.list_configured_reports()
        
        print("\n" + "="*60)
        print("CONFIGURED REPORTS")
        print("="*60)
        
        if not reports:
            print("No reports configured")
            return
        
        for r in reports:
            enabled = r.get('enabled', True)
            status = "✓" if enabled else "○"
            print(f"  {status} {r.get('name', 'Unknown')}")
            print(f"      ID: {r.get('id', 'N/A')}, Type: {r.get('type', 'generic')}, Freq: {r.get('frequency', 'daily')}")


async def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Commodity Data Pipeline - USDA AMS Data Collection',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py daily                     # Collect today's data
  python main.py daily --date 11/25/2025   # Collect specific date
  python main.py backfill                  # Full historical backfill
  python main.py backfill --start-date 01/01/2024 --end-date 12/31/2024
  python main.py status                    # Show database status
  python main.py verify                    # Verify data integrity
  python main.py test                      # Test all connections
  python main.py reports                   # Show configured reports
        """
    )
    
    parser.add_argument('command', 
                       choices=['daily', 'backfill', 'status', 'verify', 'test', 'reports'],
                       help='Command to execute')
    parser.add_argument('--date', type=str, 
                       help='Report date for daily collection (MM/DD/YYYY)')
    parser.add_argument('--start-date', type=str,
                       help='Start date for backfill (MM/DD/YYYY)')
    parser.add_argument('--end-date', type=str,
                       help='End date for backfill (MM/DD/YYYY)')
    parser.add_argument('--config', type=str, default='report_config.xlsx',
                       help='Path to report configuration file')
    parser.add_argument('--collector', type=str,
                       help='Path to USDA collector script')
    parser.add_argument('--log-level', type=str, default='INFO',
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       help='Logging level')
    parser.add_argument('--output', type=str,
                       help='Output file for results (JSON)')
    
    args = parser.parse_args()
    
    # Setup logging
    logger = setup_logging(log_level=args.log_level)
    
    print("\n" + "="*60)
    print("COMMODITY DATA PIPELINE")
    print("Round Lakes Commodities")
    print("="*60)
    
    # Initialize pipeline
    try:
        pipeline = CommodityPipeline(
            collector_path=args.collector,
            config_path=args.config
        )
    except Exception as e:
        logger.error(f"Failed to initialize pipeline: {e}")
        print(f"\nError: {e}")
        print("\nMake sure the USDA collector script (usda_ams_collector_asynch.py) is available")
        print("and your .env file contains USDA_AMS_API_KEY")
        sys.exit(1)
    
    # Execute command
    result = None
    
    if args.command == 'daily':
        result = await pipeline.run_daily(report_date=args.date)
        
    elif args.command == 'backfill':
        result = await pipeline.run_backfill(
            start_date=args.start_date,
            end_date=args.end_date
        )
        
    elif args.command == 'status':
        result = pipeline.show_status()
        
    elif args.command == 'verify':
        result = pipeline.verify_data()
        
    elif args.command == 'test':
        await pipeline.test_connections()
        
    elif args.command == 'reports':
        pipeline.show_reports()
    
    # Save output if requested
    if args.output and result:
        with open(args.output, 'w') as f:
            if hasattr(result, 'to_dict'):
                json.dump(result.to_dict(), f, indent=2, default=str)
            else:
                json.dump(result, f, indent=2, default=str)
        print(f"\nResults saved to: {args.output}")
    
    print("\n" + "="*60)
    print("Done")
    print("="*60 + "\n")


if __name__ == "__main__":
    asyncio.run(main())