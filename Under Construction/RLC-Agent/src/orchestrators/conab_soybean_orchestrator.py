"""
CONAB Soybean Pipeline Orchestrator

Orchestrates the full data pipeline for Brazilian soybean data from CONAB.
Provides integration with the Desktop LLM for supply/demand modeling.

Features:
- Scheduled data collection
- Pipeline state management
- Desktop LLM data provider interface
- Error handling and notifications

Round Lakes Commodities
"""

import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, date
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

from src.agents.collectors.south_america.conab_soybean_agent import (
    CONABSoybeanAgent,
    CONABSoybeanConfig,
    CollectionResult
)

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data"
DB_PATH = DATA_DIR / "rlc_commodities.db"


@dataclass
class PipelineState:
    """State of a pipeline run"""
    run_id: str
    status: str  # 'running', 'success', 'failed', 'partial'
    start_time: datetime
    end_time: Optional[datetime] = None

    bronze_records: int = 0
    silver_records: int = 0
    gold_outputs: int = 0

    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


@dataclass
class LLMDataRequest:
    """Request for data from the Desktop LLM"""
    commodity: str = "soybeans"
    country: str = "brazil"
    data_type: str = "supply_demand"  # 'supply_demand', 'production', 'prices', 'balance_sheet'
    start_year: Optional[int] = None
    end_year: Optional[int] = None
    state: Optional[str] = None
    format: str = "dataframe"  # 'dataframe', 'dict', 'json'


class CONABSoybeanOrchestrator:
    """
    Orchestrator for CONAB Soybean data pipeline.

    Provides:
    - Full pipeline execution (Bronze -> Silver -> Gold)
    - Scheduled updates
    - Data access interface for Desktop LLM
    - Pipeline state management
    - Error handling and recovery
    """

    def __init__(
        self,
        config: CONABSoybeanConfig = None,
        auto_initialize: bool = True
    ):
        self.config = config or CONABSoybeanConfig()
        self.agent = CONABSoybeanAgent(self.config)
        self.logger = logging.getLogger(f"{self.__class__.__name__}")

        # Pipeline state
        self.current_run: Optional[PipelineState] = None
        self.last_successful_run: Optional[PipelineState] = None

        if auto_initialize:
            self._ensure_database_ready()

        self.logger.info("CONABSoybeanOrchestrator initialized")

    def _ensure_database_ready(self):
        """Ensure database and tables exist"""
        # The agent handles table creation on first run
        # Just ensure the data directory exists
        DATA_DIR.mkdir(parents=True, exist_ok=True)

    # =========================================================================
    # PIPELINE EXECUTION
    # =========================================================================

    def run_full_pipeline(self) -> PipelineState:
        """
        Execute the full Bronze -> Silver -> Gold pipeline.

        Returns:
            PipelineState with run details
        """
        import uuid
        run_id = str(uuid.uuid4())

        self.current_run = PipelineState(
            run_id=run_id,
            status='running',
            start_time=datetime.now()
        )

        self.logger.info(f"Starting full pipeline. Run ID: {run_id}")

        try:
            # Run through the agent's pipeline
            results = self.agent.run_full_pipeline()

            # Update state
            self.current_run.bronze_records = results['bronze'].records_inserted
            self.current_run.silver_records = results['silver'].records_inserted
            self.current_run.gold_outputs = len(results['gold'].data.get('visualizations', [])) if results['gold'].data else 0

            # Collect warnings
            for layer, result in results.items():
                self.current_run.warnings.extend(result.warnings)

            # Determine status
            if all(r.success for r in results.values()):
                self.current_run.status = 'success'
                self.last_successful_run = self.current_run
            elif any(r.success for r in results.values()):
                self.current_run.status = 'partial'
            else:
                self.current_run.status = 'failed'

        except Exception as e:
            self.logger.error(f"Pipeline failed: {e}")
            self.current_run.status = 'failed'
            self.current_run.errors.append(str(e))

        self.current_run.end_time = datetime.now()
        self._log_pipeline_run(self.current_run)

        return self.current_run

    def run_incremental_update(self) -> PipelineState:
        """
        Run incremental update (typically for daily/weekly refreshes).
        """
        # For CONAB data, this is typically the same as full pipeline
        # since the data is historical series that gets updated
        return self.run_full_pipeline()

    def _log_pipeline_run(self, state: PipelineState):
        """Log pipeline run to database"""
        conn = sqlite3.connect(str(self.config.database_path))
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                id TEXT PRIMARY KEY,
                source TEXT NOT NULL,
                start_time TIMESTAMP NOT NULL,
                end_time TIMESTAMP,
                status TEXT NOT NULL,
                bronze_records INTEGER DEFAULT 0,
                silver_records INTEGER DEFAULT 0,
                gold_outputs INTEGER DEFAULT 0,
                errors TEXT,
                warnings TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        cursor.execute("""
            INSERT INTO pipeline_runs
            (id, source, start_time, end_time, status,
             bronze_records, silver_records, gold_outputs,
             errors, warnings)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            state.run_id,
            'CONAB_SOYBEAN',
            state.start_time.isoformat(),
            state.end_time.isoformat() if state.end_time else None,
            state.status,
            state.bronze_records,
            state.silver_records,
            state.gold_outputs,
            '\n'.join(state.errors) if state.errors else None,
            '\n'.join(state.warnings) if state.warnings else None,
        ))

        conn.commit()
        conn.close()

    # =========================================================================
    # DESKTOP LLM DATA INTERFACE
    # =========================================================================

    def get_data_for_llm(self, request: LLMDataRequest) -> Dict[str, Any]:
        """
        Get data formatted for Desktop LLM consumption.

        This is the main interface for the Desktop LLM to access
        Brazilian soybean supply/demand data for modeling.

        Args:
            request: LLMDataRequest specifying what data is needed

        Returns:
            Dictionary with data and metadata
        """
        self.logger.info(f"LLM data request: {request.data_type}")

        if request.data_type == 'production':
            return self._get_production_data(request)
        elif request.data_type == 'supply_demand' or request.data_type == 'balance_sheet':
            return self._get_balance_sheet_data(request)
        elif request.data_type == 'prices':
            return self._get_price_data(request)
        elif request.data_type == 'summary':
            return self._get_summary_data(request)
        else:
            return {
                'success': False,
                'error': f"Unknown data_type: {request.data_type}"
            }

    def _get_production_data(self, request: LLMDataRequest) -> Dict[str, Any]:
        """Get production data for LLM"""
        conn = sqlite3.connect(str(self.config.database_path))

        query = """
            SELECT
                crop_year,
                state,
                production_mmt,
                planted_area_ha / 1000000 as planted_area_mha,
                harvested_area_ha / 1000000 as harvested_area_mha,
                yield_mt_ha,
                production_yoy_pct,
                production_vs_5yr_avg,
                quality_flag
            FROM silver_conab_soybean_production
            WHERE 1=1
        """
        params = []

        if request.state and request.state.upper() != 'ALL':
            query += " AND state = ?"
            params.append(request.state.upper())
        else:
            query += " AND state = 'BRASIL'"

        if request.start_year:
            query += " AND CAST(SUBSTR(crop_year, 1, 4) AS INTEGER) >= ?"
            params.append(request.start_year)

        if request.end_year:
            query += " AND CAST(SUBSTR(crop_year, 1, 4) AS INTEGER) <= ?"
            params.append(request.end_year)

        query += " ORDER BY crop_year"

        if PANDAS_AVAILABLE:
            df = pd.read_sql(query, conn, params=params)

            if request.format == 'dataframe':
                data = df
            elif request.format == 'json':
                data = df.to_json(orient='records', date_format='iso')
            else:
                data = df.to_dict(orient='records')
        else:
            cursor = conn.cursor()
            cursor.execute(query, params)
            columns = [desc[0] for desc in cursor.description]
            data = [dict(zip(columns, row)) for row in cursor.fetchall()]

        conn.close()

        return {
            'success': True,
            'data_type': 'production',
            'commodity': 'soybeans',
            'country': 'brazil',
            'source': 'CONAB',
            'records': len(data) if isinstance(data, list) else len(data) if PANDAS_AVAILABLE else 0,
            'data': data,
            'units': {
                'production_mmt': 'Million Metric Tons',
                'planted_area_mha': 'Million Hectares',
                'yield_mt_ha': 'Metric Tons per Hectare'
            },
            'metadata': {
                'last_updated': self.last_successful_run.end_time.isoformat() if self.last_successful_run else None,
                'marketing_year': 'February - January',
            }
        }

    def _get_balance_sheet_data(self, request: LLMDataRequest) -> Dict[str, Any]:
        """Get supply/demand balance sheet for LLM"""
        conn = sqlite3.connect(str(self.config.database_path))

        query = """
            SELECT
                crop_year,
                beginning_stocks_mt / 1000000 as beginning_stocks_mmt,
                production_mt / 1000000 as production_mmt,
                imports_mt / 1000000 as imports_mmt,
                total_supply_mt / 1000000 as total_supply_mmt,
                domestic_consumption_mt / 1000000 as domestic_consumption_mmt,
                crush_mt / 1000000 as crush_mmt,
                exports_mt / 1000000 as exports_mmt,
                total_use_mt / 1000000 as total_use_mmt,
                ending_stocks_mt / 1000000 as ending_stocks_mmt,
                stocks_to_use_ratio,
                export_share_pct,
                quality_flag
            FROM silver_conab_soybean_balance_sheet
            WHERE 1=1
        """
        params = []

        if request.start_year:
            query += " AND CAST(SUBSTR(crop_year, 1, 4) AS INTEGER) >= ?"
            params.append(request.start_year)

        if request.end_year:
            query += " AND CAST(SUBSTR(crop_year, 1, 4) AS INTEGER) <= ?"
            params.append(request.end_year)

        query += " ORDER BY crop_year"

        if PANDAS_AVAILABLE:
            df = pd.read_sql(query, conn, params=params)

            if request.format == 'dataframe':
                data = df
            elif request.format == 'json':
                data = df.to_json(orient='records', date_format='iso')
            else:
                data = df.to_dict(orient='records')
        else:
            cursor = conn.cursor()
            cursor.execute(query, params)
            columns = [desc[0] for desc in cursor.description]
            data = [dict(zip(columns, row)) for row in cursor.fetchall()]

        conn.close()

        return {
            'success': True,
            'data_type': 'balance_sheet',
            'commodity': 'soybeans',
            'country': 'brazil',
            'source': 'CONAB',
            'records': len(data) if isinstance(data, list) else len(data) if PANDAS_AVAILABLE else 0,
            'data': data,
            'units': {
                'all_volume_fields': 'Million Metric Tons (MMT)',
                'stocks_to_use_ratio': 'Ratio (decimal)',
                'export_share_pct': 'Percentage'
            },
            'balance_sheet_structure': {
                'supply': ['beginning_stocks_mmt', 'production_mmt', 'imports_mmt'],
                'demand': ['domestic_consumption_mmt', 'crush_mmt', 'exports_mmt'],
                'balance': ['ending_stocks_mmt']
            },
            'metadata': {
                'last_updated': self.last_successful_run.end_time.isoformat() if self.last_successful_run else None,
                'marketing_year': 'February - January',
            }
        }

    def _get_price_data(self, request: LLMDataRequest) -> Dict[str, Any]:
        """Get price data for LLM"""
        # Price data not yet fully implemented
        return {
            'success': True,
            'data_type': 'prices',
            'commodity': 'soybeans',
            'country': 'brazil',
            'source': 'CONAB',
            'data': [],
            'message': 'Price data collection not yet implemented'
        }

    def _get_summary_data(self, request: LLMDataRequest) -> Dict[str, Any]:
        """Get summary statistics for LLM"""
        conn = sqlite3.connect(str(self.config.database_path))
        cursor = conn.cursor()

        # Get latest year data
        cursor.execute("""
            SELECT *
            FROM silver_conab_soybean_production
            WHERE state = 'BRASIL'
            ORDER BY crop_year DESC
            LIMIT 1
        """)
        latest = cursor.fetchone()

        # Get historical stats
        cursor.execute("""
            SELECT
                AVG(production_mmt) as avg_production,
                MAX(production_mmt) as max_production,
                MIN(production_mmt) as min_production,
                AVG(yield_mt_ha) as avg_yield,
                MAX(yield_mt_ha) as max_yield,
                COUNT(*) as years_of_data
            FROM silver_conab_soybean_production
            WHERE state = 'BRASIL'
        """)
        stats = cursor.fetchone()

        conn.close()

        return {
            'success': True,
            'data_type': 'summary',
            'commodity': 'soybeans',
            'country': 'brazil',
            'source': 'CONAB',
            'summary': {
                'latest_year': {
                    'crop_year': latest[1] if latest else None,
                    'production_mmt': latest[7] if latest else None,
                    'yield_mt_ha': latest[10] if latest else None,
                },
                'historical': {
                    'avg_production_mmt': stats[0] if stats else None,
                    'max_production_mmt': stats[1] if stats else None,
                    'min_production_mmt': stats[2] if stats else None,
                    'avg_yield_mt_ha': stats[3] if stats else None,
                    'max_yield_mt_ha': stats[4] if stats else None,
                    'years_of_data': stats[5] if stats else None,
                }
            }
        }

    def get_production_for_modeling(
        self,
        start_year: int = None,
        state: str = 'BRASIL'
    ) -> Optional[Any]:
        """
        Convenience method to get production data ready for modeling.

        Returns DataFrame (if pandas available) or list of dicts.
        """
        request = LLMDataRequest(
            data_type='production',
            start_year=start_year,
            state=state,
            format='dataframe' if PANDAS_AVAILABLE else 'dict'
        )
        result = self.get_data_for_llm(request)

        if result['success']:
            return result['data']
        return None

    def get_balance_sheet_for_modeling(
        self,
        start_year: int = None
    ) -> Optional[Any]:
        """
        Convenience method to get balance sheet data ready for modeling.

        Returns DataFrame (if pandas available) or list of dicts.
        """
        request = LLMDataRequest(
            data_type='balance_sheet',
            start_year=start_year,
            format='dataframe' if PANDAS_AVAILABLE else 'dict'
        )
        result = self.get_data_for_llm(request)

        if result['success']:
            return result['data']
        return None

    # =========================================================================
    # STATUS AND MONITORING
    # =========================================================================

    def get_status(self) -> Dict[str, Any]:
        """Get orchestrator status"""
        agent_status = self.agent.get_status()

        return {
            'orchestrator': 'CONABSoybeanOrchestrator',
            'agent': agent_status,
            'current_run': {
                'run_id': self.current_run.run_id if self.current_run else None,
                'status': self.current_run.status if self.current_run else None,
            } if self.current_run else None,
            'last_successful_run': {
                'run_id': self.last_successful_run.run_id if self.last_successful_run else None,
                'end_time': self.last_successful_run.end_time.isoformat() if self.last_successful_run else None,
                'bronze_records': self.last_successful_run.bronze_records if self.last_successful_run else 0,
                'silver_records': self.last_successful_run.silver_records if self.last_successful_run else 0,
            } if self.last_successful_run else None,
        }

    def get_data_quality_report(self) -> Dict[str, Any]:
        """Get data quality report"""
        conn = sqlite3.connect(str(self.config.database_path))
        cursor = conn.cursor()

        # Bronze counts
        cursor.execute("SELECT COUNT(*) FROM bronze_conab_soybean_production")
        bronze_prod = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM bronze_conab_soybean_supply_demand")
        bronze_sd = cursor.fetchone()[0]

        # Silver counts
        cursor.execute("SELECT COUNT(*) FROM silver_conab_soybean_production")
        silver_prod = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM silver_conab_soybean_balance_sheet")
        silver_bs = cursor.fetchone()[0]

        # Quality flags
        cursor.execute("""
            SELECT quality_flag, COUNT(*)
            FROM silver_conab_soybean_production
            GROUP BY quality_flag
        """)
        quality_breakdown = dict(cursor.fetchall())

        # Date coverage
        cursor.execute("""
            SELECT MIN(crop_year), MAX(crop_year)
            FROM silver_conab_soybean_production
        """)
        date_range = cursor.fetchone()

        conn.close()

        return {
            'record_counts': {
                'bronze_production': bronze_prod,
                'bronze_supply_demand': bronze_sd,
                'silver_production': silver_prod,
                'silver_balance_sheet': silver_bs,
            },
            'quality_flags': quality_breakdown,
            'coverage': {
                'earliest_year': date_range[0] if date_range else None,
                'latest_year': date_range[1] if date_range else None,
            },
            'completeness': {
                'bronze_to_silver_ratio': silver_prod / bronze_prod if bronze_prod > 0 else 0,
            }
        }


# =============================================================================
# FACTORY FUNCTION FOR DESKTOP LLM INTEGRATION
# =============================================================================

def create_conab_soybean_data_provider() -> CONABSoybeanOrchestrator:
    """
    Factory function to create CONAB Soybean data provider.

    Use this in the Desktop LLM to get access to Brazilian soybean data.

    Example usage in Desktop LLM:
        from src.orchestrators.conab_soybean_orchestrator import create_conab_soybean_data_provider

        provider = create_conab_soybean_data_provider()

        # Get production data for modeling
        prod_data = provider.get_production_for_modeling(start_year=2010)

        # Get balance sheet
        balance_sheet = provider.get_balance_sheet_for_modeling()

        # Custom query
        from src.orchestrators.conab_soybean_orchestrator import LLMDataRequest
        request = LLMDataRequest(
            data_type='production',
            start_year=2015,
            state='MT',  # Mato Grosso
            format='dataframe'
        )
        result = provider.get_data_for_llm(request)
    """
    return CONABSoybeanOrchestrator()


# =============================================================================
# CLI
# =============================================================================

def main():
    """CLI for CONAB Soybean Orchestrator"""
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    parser = argparse.ArgumentParser(
        description='CONAB Soybean Pipeline Orchestrator'
    )

    parser.add_argument(
        'command',
        choices=['run', 'status', 'quality', 'get-production', 'get-balance-sheet'],
        help='Command to execute'
    )

    parser.add_argument('--start-year', type=int, help='Start year for data queries')
    parser.add_argument('--state', type=str, default='BRASIL', help='State filter')
    parser.add_argument('--format', choices=['json', 'table'], default='table')

    args = parser.parse_args()

    orchestrator = CONABSoybeanOrchestrator()

    if args.command == 'run':
        state = orchestrator.run_full_pipeline()
        print(f"\nPipeline Status: {state.status}")
        print(f"Bronze Records: {state.bronze_records}")
        print(f"Silver Records: {state.silver_records}")
        print(f"Gold Outputs: {state.gold_outputs}")
        if state.warnings:
            print(f"Warnings: {len(state.warnings)}")
        if state.errors:
            print(f"Errors: {state.errors}")

    elif args.command == 'status':
        status = orchestrator.get_status()
        import json
        print(json.dumps(status, indent=2, default=str))

    elif args.command == 'quality':
        report = orchestrator.get_data_quality_report()
        import json
        print(json.dumps(report, indent=2))

    elif args.command == 'get-production':
        request = LLMDataRequest(
            data_type='production',
            start_year=args.start_year,
            state=args.state,
            format='dict'
        )
        result = orchestrator.get_data_for_llm(request)

        if args.format == 'json':
            import json
            print(json.dumps(result, indent=2))
        else:
            print(f"\nBrazil Soybean Production Data")
            print(f"Records: {result['records']}")
            print("-" * 70)
            for row in result['data'][:10]:
                print(f"{row['crop_year']}: {row.get('production_mmt', 'N/A'):.2f} MMT, "
                      f"Yield: {row.get('yield_mt_ha', 'N/A'):.2f} MT/ha")

    elif args.command == 'get-balance-sheet':
        request = LLMDataRequest(
            data_type='balance_sheet',
            start_year=args.start_year,
            format='dict'
        )
        result = orchestrator.get_data_for_llm(request)

        if args.format == 'json':
            import json
            print(json.dumps(result, indent=2))
        else:
            print(f"\nBrazil Soybean Balance Sheet")
            print("-" * 70)
            for row in result['data'][:5]:
                print(f"\n{row['crop_year']}:")
                print(f"  Production:      {row.get('production_mmt', 'N/A'):.2f} MMT")
                print(f"  Exports:         {row.get('exports_mmt', 'N/A'):.2f} MMT")
                print(f"  Ending Stocks:   {row.get('ending_stocks_mmt', 'N/A'):.2f} MMT")


if __name__ == '__main__':
    main()
