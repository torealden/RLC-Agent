"""
Test Suite for CONAB Soybean Pipeline

Tests the Bronze -> Silver -> Gold data pipeline for Brazilian soybean data.
"""

import logging
import os
import sqlite3
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.agents.collectors.south_america.conab_soybean_agent import (
    CONABSoybeanAgent,
    CONABSoybeanConfig,
    CollectionResult,
)
from src.orchestrators.conab_soybean_orchestrator import (
    CONABSoybeanOrchestrator,
    LLMDataRequest,
    PipelineState,
)

logging.basicConfig(level=logging.INFO)


class TestCONABSoybeanConfig(unittest.TestCase):
    """Test configuration"""

    def test_default_config(self):
        """Test default configuration values"""
        config = CONABSoybeanConfig()

        self.assertEqual(config.source_name, "CONAB_SOYBEAN")
        self.assertEqual(config.timeout, 60)
        self.assertEqual(config.retry_attempts, 3)
        self.assertEqual(config.start_year, 1976)

    def test_custom_config(self):
        """Test custom configuration"""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = CONABSoybeanConfig(
                database_path=Path(tmpdir) / "test.db",
                downloads_dir=Path(tmpdir) / "downloads",
                timeout=30,
            )

            self.assertEqual(config.timeout, 30)
            self.assertTrue(config.downloads_dir.exists())


class TestCONABSoybeanAgent(unittest.TestCase):
    """Test the CONAB Soybean Agent"""

    def setUp(self):
        """Set up test fixtures"""
        self.tmpdir = tempfile.mkdtemp()
        self.config = CONABSoybeanConfig(
            database_path=Path(self.tmpdir) / "test.db",
            downloads_dir=Path(self.tmpdir) / "downloads",
        )
        self.agent = CONABSoybeanAgent(self.config)

    def tearDown(self):
        """Clean up"""
        import shutil
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_agent_initialization(self):
        """Test agent initializes correctly"""
        self.assertIsNotNone(self.agent)
        self.assertIsNotNone(self.agent.session)
        self.assertEqual(self.agent.config.source_name, "CONAB_SOYBEAN")

    def test_bronze_schema_creation(self):
        """Test bronze schema is created correctly"""
        self.agent._initialize_bronze_schema()

        conn = sqlite3.connect(str(self.config.database_path))
        cursor = conn.cursor()

        # Check tables exist
        cursor.execute("""
            SELECT name FROM sqlite_master
            WHERE type='table' AND name LIKE 'bronze_%'
        """)
        tables = [row[0] for row in cursor.fetchall()]

        self.assertIn('bronze_conab_soybean_production', tables)
        self.assertIn('bronze_conab_soybean_supply_demand', tables)
        self.assertIn('bronze_conab_soybean_prices', tables)
        self.assertIn('bronze_ingest_run', tables)

        conn.close()

    def test_silver_schema_creation(self):
        """Test silver schema is created correctly"""
        self.agent._initialize_silver_schema()

        conn = sqlite3.connect(str(self.config.database_path))
        cursor = conn.cursor()

        cursor.execute("""
            SELECT name FROM sqlite_master
            WHERE type='table' AND name LIKE 'silver_%'
        """)
        tables = [row[0] for row in cursor.fetchall()]

        self.assertIn('silver_conab_soybean_production', tables)
        self.assertIn('silver_conab_soybean_balance_sheet', tables)

        conn.close()

    def test_normalize_crop_year(self):
        """Test crop year normalization"""
        # Already correct format
        self.assertEqual(self.agent._normalize_crop_year("2023/24"), "2023/24")

        # Full format
        self.assertEqual(self.agent._normalize_crop_year("2023/2024"), "2023/24")

        # Just year
        self.assertEqual(self.agent._normalize_crop_year("2023"), "2023/24")

    def test_safe_float_conversion(self):
        """Test safe float conversion"""
        # Normal numbers
        self.assertEqual(self.agent._safe_float("123.45"), 123.45)

        # Brazilian format (1.234,56)
        self.assertEqual(self.agent._safe_float("1.234,56"), 1234.56)

        # None/empty
        self.assertIsNone(self.agent._safe_float(None))
        self.assertIsNone(self.agent._safe_float(""))

    def test_parse_marketing_year_dates(self):
        """Test marketing year date parsing"""
        start, end = self.agent._parse_marketing_year_dates("2023/24")

        self.assertEqual(start, "2023-02-01")
        self.assertEqual(end, "2024-01-31")

    def test_store_bronze_production(self):
        """Test storing production records"""
        self.agent._initialize_bronze_schema()

        test_records = [
            {
                'crop_year': '2023/24',
                'state': 'BRASIL',
                'commodity': 'SOYBEANS',
                'production_1000t': 150000,
                'planted_area_1000ha': 45000,
                'yield_kg_ha': 3333,
            },
            {
                'crop_year': '2022/23',
                'state': 'BRASIL',
                'commodity': 'SOYBEANS',
                'production_1000t': 155000,
                'planted_area_1000ha': 43500,
                'yield_kg_ha': 3563,
            },
        ]

        inserted = self.agent._store_bronze_production(test_records, "test-run-id")
        self.assertEqual(inserted, 2)

        # Verify data
        conn = sqlite3.connect(str(self.config.database_path))
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM bronze_conab_soybean_production")
        count = cursor.fetchone()[0]
        self.assertEqual(count, 2)
        conn.close()

    def test_verify_bronze_data(self):
        """Test bronze data verification"""
        self.agent._initialize_bronze_schema()

        # Store test data
        test_records = [
            {'crop_year': '2023/24', 'state': 'BRASIL', 'production_1000t': 150000},
        ]
        self.agent._store_bronze_production(test_records, "test-run-id")

        # Verify
        result = self.agent._verify_bronze_data("test-run-id")

        self.assertEqual(result['production_records'], 1)
        self.assertIn(result['status'], ['PASSED', 'PASSED_WITH_WARNINGS'])


class TestCONABSoybeanOrchestrator(unittest.TestCase):
    """Test the pipeline orchestrator"""

    def setUp(self):
        """Set up test fixtures"""
        self.tmpdir = tempfile.mkdtemp()
        self.config = CONABSoybeanConfig(
            database_path=Path(self.tmpdir) / "test.db",
            downloads_dir=Path(self.tmpdir) / "downloads",
        )
        self.orchestrator = CONABSoybeanOrchestrator(
            config=self.config,
            auto_initialize=True
        )

    def tearDown(self):
        """Clean up"""
        import shutil
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_orchestrator_initialization(self):
        """Test orchestrator initializes correctly"""
        self.assertIsNotNone(self.orchestrator)
        self.assertIsNotNone(self.orchestrator.agent)

    def test_get_status(self):
        """Test status reporting"""
        status = self.orchestrator.get_status()

        self.assertEqual(status['orchestrator'], 'CONABSoybeanOrchestrator')
        self.assertIn('agent', status)

    def test_llm_data_request(self):
        """Test LLM data request structure"""
        request = LLMDataRequest(
            data_type='production',
            start_year=2020,
            state='MT',
            format='dict'
        )

        self.assertEqual(request.commodity, 'soybeans')
        self.assertEqual(request.country, 'brazil')
        self.assertEqual(request.data_type, 'production')

    def test_get_data_for_llm_empty(self):
        """Test LLM data request with empty database"""
        # Initialize schema
        self.orchestrator.agent._initialize_silver_schema()

        request = LLMDataRequest(data_type='production')
        result = self.orchestrator.get_data_for_llm(request)

        self.assertTrue(result['success'])
        self.assertEqual(result['data_type'], 'production')
        self.assertEqual(result['commodity'], 'soybeans')

    def test_get_summary_data(self):
        """Test summary data retrieval"""
        self.orchestrator.agent._initialize_silver_schema()

        request = LLMDataRequest(data_type='summary')
        result = self.orchestrator.get_data_for_llm(request)

        self.assertTrue(result['success'])
        self.assertEqual(result['data_type'], 'summary')
        self.assertIn('summary', result)


class TestCollectionResult(unittest.TestCase):
    """Test CollectionResult dataclass"""

    def test_result_creation(self):
        """Test creating a collection result"""
        result = CollectionResult(
            success=True,
            source="CONAB_SOYBEAN",
            records_fetched=100,
            records_inserted=95,
            records_skipped=5,
        )

        self.assertTrue(result.success)
        self.assertEqual(result.records_fetched, 100)
        self.assertEqual(result.records_inserted, 95)
        self.assertEqual(result.records_skipped, 5)

    def test_result_with_error(self):
        """Test result with error"""
        result = CollectionResult(
            success=False,
            source="CONAB_SOYBEAN",
            error_message="Connection failed"
        )

        self.assertFalse(result.success)
        self.assertEqual(result.error_message, "Connection failed")


class TestPipelineState(unittest.TestCase):
    """Test PipelineState dataclass"""

    def test_state_creation(self):
        """Test creating pipeline state"""
        state = PipelineState(
            run_id="test-123",
            status="running",
            start_time=datetime.now(),
        )

        self.assertEqual(state.run_id, "test-123")
        self.assertEqual(state.status, "running")
        self.assertEqual(state.bronze_records, 0)


class TestDataTransformations(unittest.TestCase):
    """Test data transformation functions"""

    def setUp(self):
        """Set up test fixtures"""
        self.tmpdir = tempfile.mkdtemp()
        self.config = CONABSoybeanConfig(
            database_path=Path(self.tmpdir) / "test.db",
            downloads_dir=Path(self.tmpdir) / "downloads",
        )
        self.agent = CONABSoybeanAgent(self.config)

    def tearDown(self):
        """Clean up"""
        import shutil
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_production_transformation(self):
        """Test transforming production data to silver"""
        # Initialize schemas
        self.agent._initialize_bronze_schema()
        self.agent._initialize_silver_schema()

        # Insert bronze data
        test_records = [
            {
                'crop_year': '2023/24',
                'state': 'BRASIL',
                'production_1000t': 150000,  # 150 MMT
                'planted_area_1000ha': 45000,  # 45 MHa
                'yield_kg_ha': 3333,
            },
        ]
        self.agent._store_bronze_production(test_records, "test-run")

        # Transform to silver
        result = self.agent._transform_production_to_silver()

        self.assertEqual(result['inserted'], 1)

        # Verify transformation
        conn = sqlite3.connect(str(self.config.database_path))
        cursor = conn.cursor()
        cursor.execute("""
            SELECT production_mt, production_mmt, planted_area_ha, yield_mt_ha
            FROM silver_conab_soybean_production
            WHERE crop_year = '2023/24'
        """)
        row = cursor.fetchone()
        conn.close()

        self.assertIsNotNone(row)
        self.assertEqual(row[0], 150000000)  # 150M MT
        self.assertEqual(row[1], 150.0)  # 150 MMT
        self.assertEqual(row[2], 45000000)  # 45M ha


class TestGoldLayerViews(unittest.TestCase):
    """Test Gold layer view creation"""

    def setUp(self):
        """Set up test fixtures"""
        self.tmpdir = tempfile.mkdtemp()
        self.config = CONABSoybeanConfig(
            database_path=Path(self.tmpdir) / "test.db",
            downloads_dir=Path(self.tmpdir) / "downloads",
        )
        self.agent = CONABSoybeanAgent(self.config)

    def tearDown(self):
        """Clean up"""
        import shutil
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_gold_views_creation(self):
        """Test that gold views are created"""
        # Initialize all schemas
        self.agent._initialize_bronze_schema()
        self.agent._initialize_silver_schema()
        self.agent._create_gold_views()

        conn = sqlite3.connect(str(self.config.database_path))
        cursor = conn.cursor()

        cursor.execute("""
            SELECT name FROM sqlite_master
            WHERE type='view' AND name LIKE 'gold_%'
        """)
        views = [row[0] for row in cursor.fetchall()]

        self.assertIn('gold_brazil_soybean_summary', views)
        self.assertIn('gold_brazil_soybean_national', views)
        self.assertIn('gold_brazil_soybean_by_state', views)

        conn.close()


class TestIntegration(unittest.TestCase):
    """Integration tests for the full pipeline"""

    def setUp(self):
        """Set up test fixtures"""
        self.tmpdir = tempfile.mkdtemp()
        self.config = CONABSoybeanConfig(
            database_path=Path(self.tmpdir) / "test.db",
            downloads_dir=Path(self.tmpdir) / "downloads",
        )

    def tearDown(self):
        """Clean up"""
        import shutil
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_full_pipeline_with_mock_data(self):
        """Test full pipeline with mock data (no network)"""
        agent = CONABSoybeanAgent(self.config)

        # Initialize schemas
        agent._initialize_bronze_schema()
        agent._initialize_silver_schema()

        # Mock bronze data
        test_records = [
            {'crop_year': '2020/21', 'state': 'BRASIL', 'production_1000t': 135000, 'planted_area_1000ha': 38500, 'yield_kg_ha': 3506},
            {'crop_year': '2021/22', 'state': 'BRASIL', 'production_1000t': 125000, 'planted_area_1000ha': 40800, 'yield_kg_ha': 3063},
            {'crop_year': '2022/23', 'state': 'BRASIL', 'production_1000t': 155000, 'planted_area_1000ha': 43500, 'yield_kg_ha': 3563},
            {'crop_year': '2023/24', 'state': 'BRASIL', 'production_1000t': 150000, 'planted_area_1000ha': 45000, 'yield_kg_ha': 3333},
            # State data
            {'crop_year': '2023/24', 'state': 'MT', 'production_1000t': 45000, 'planted_area_1000ha': 12000, 'yield_kg_ha': 3750},
            {'crop_year': '2023/24', 'state': 'PR', 'production_1000t': 22000, 'planted_area_1000ha': 5800, 'yield_kg_ha': 3793},
            {'crop_year': '2023/24', 'state': 'RS', 'production_1000t': 21000, 'planted_area_1000ha': 6200, 'yield_kg_ha': 3387},
        ]

        inserted = agent._store_bronze_production(test_records, "test-run")
        self.assertEqual(inserted, 7)

        # Transform to silver
        agent._transform_production_to_silver()
        agent._calculate_silver_metrics()

        # Create gold views
        agent._create_gold_views()

        # Generate report
        report = agent._generate_soybean_report()
        self.assertIn("BRAZIL SOYBEAN MARKET REPORT", report)
        self.assertIn("CONAB", report)

        # Test LLM interface
        orchestrator = CONABSoybeanOrchestrator(config=self.config)

        # Get production data
        request = LLMDataRequest(data_type='production', format='dict')
        result = orchestrator.get_data_for_llm(request)

        self.assertTrue(result['success'])
        self.assertEqual(result['commodity'], 'soybeans')
        self.assertEqual(len(result['data']), 4)  # 4 BRASIL records


if __name__ == '__main__':
    unittest.main(verbosity=2)
