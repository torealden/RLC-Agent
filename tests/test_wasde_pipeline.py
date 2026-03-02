"""
WASDE Pipeline Integration Tests (Phase 2)

Tests the delta summarizer WASDE handler, analysis template, prompt template,
and report pipeline orchestrator. All DB calls are mocked.
"""

import hashlib
import json
import sys
import unittest
import uuid
from datetime import datetime, date, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch, PropertyMock

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


# =========================================================================
# Helper: mock cursor rows as dicts
# =========================================================================

def _make_dict_row(d):
    """Create a dict-like object that supports both d['key'] and iteration."""
    return d


def _mock_cursor(rows):
    """Create a mock cursor whose fetchall() returns list of dicts."""
    cur = MagicMock()
    cur.fetchall.return_value = [_make_dict_row(r) for r in rows]
    cur.fetchone.return_value = _make_dict_row(rows[0]) if rows else None
    return cur


# =========================================================================
# 1. TestWASdeDeltaSummarizer
# =========================================================================
class TestWASdeDeltaSummarizer(unittest.TestCase):
    """Test the _delta_wasde handler in delta_summarizer.py."""

    def test_handler_registered(self):
        """usda_wasde handler is registered in compute_delta."""
        from src.dispatcher.delta_summarizer import compute_delta
        # compute_delta should not crash with an unregistered name
        # and should recognize usda_wasde
        source = open(compute_delta.__code__.co_filename).read()
        self.assertIn("'usda_wasde'", source)

    def test_returns_none_on_empty_data(self):
        """Returns None when no WASDE data exists."""
        from src.dispatcher.delta_summarizer import _delta_wasde

        conn = MagicMock()
        cur = _mock_cursor([])
        conn.cursor.return_value = cur

        result = _delta_wasde(conn)
        self.assertIsNone(result)

    def test_computes_ending_stocks_change(self):
        """Computes ending stocks change correctly."""
        from src.dispatcher.delta_summarizer import _delta_wasde

        rows = [{
            'commodity': 'corn',
            'marketing_year': 2024,
            'latest_date': date(2026, 3, 1),
            'ending_stocks': 50000,
            'production': 380000,
            'exports': 60000,
            'domestic_consumption': 310000,
            'total_supply': 420000,
            'feed_dom_consumption': 140000,
            'fsi_consumption': 170000,
            'prior_date': date(2026, 2, 1),
            'ending_stocks_prior': 52000,
            'production_prior': 380000,
            'exports_prior': 58000,
            'dom_consumption_prior': 310000,
        }]

        conn = MagicMock()
        cur = _mock_cursor(rows)
        conn.cursor.return_value = cur

        result = _delta_wasde(conn)
        self.assertIsNotNone(result)
        self.assertIn('data', result)
        self.assertIn('corn', result['data'])
        corn = result['data']['corn']
        self.assertEqual(corn['ending_stocks'], 50000.0)
        self.assertEqual(corn['ending_stocks_change'], -2000.0)
        # -2000 / 52000 * 100 = -3.8%
        self.assertAlmostEqual(corn['ending_stocks_change_pct'], -3.8, places=1)

    def test_flags_large_stocks_revision(self):
        """Flags notable change when ending stocks revision > 5%."""
        from src.dispatcher.delta_summarizer import _delta_wasde

        rows = [{
            'commodity': 'soybeans',
            'marketing_year': 2024,
            'latest_date': date(2026, 3, 1),
            'ending_stocks': 8000,
            'production': 120000,
            'exports': 55000,
            'domestic_consumption': 60000,
            'total_supply': 130000,
            'feed_dom_consumption': 30000,
            'fsi_consumption': 30000,
            'prior_date': date(2026, 2, 1),
            'ending_stocks_prior': 10000,  # 20% drop
            'production_prior': 120000,
            'exports_prior': 53000,
            'dom_consumption_prior': 60000,
        }]

        conn = MagicMock()
        cur = _mock_cursor(rows)
        conn.cursor.return_value = cur

        result = _delta_wasde(conn)
        self.assertIsNotNone(result)
        notable = result['notable_changes']
        self.assertTrue(len(notable) > 0)
        flags = [n['flag'] for n in notable]
        self.assertIn('large_stocks_revision', flags)

    def test_result_dict_structure(self):
        """Result has expected top-level keys."""
        from src.dispatcher.delta_summarizer import _delta_wasde

        rows = [{
            'commodity': 'wheat',
            'marketing_year': 2024,
            'latest_date': date(2026, 3, 1),
            'ending_stocks': 20000,
            'production': 50000,
            'exports': 25000,
            'domestic_consumption': 30000,
            'total_supply': 55000,
            'feed_dom_consumption': 5000,
            'fsi_consumption': 25000,
            'prior_date': date(2026, 2, 1),
            'ending_stocks_prior': 20000,
            'production_prior': 50000,
            'exports_prior': 25000,
            'dom_consumption_prior': 30000,
        }]

        conn = MagicMock()
        cur = _mock_cursor(rows)
        conn.cursor.return_value = cur

        result = _delta_wasde(conn)
        self.assertIn('notable_changes', result)
        self.assertIn('summary_parts', result)
        self.assertIn('data', result)
        self.assertIsInstance(result['notable_changes'], list)
        self.assertIsInstance(result['summary_parts'], list)
        self.assertIsInstance(result['data'], dict)


# =========================================================================
# 2. TestWASDeAnalysisTemplate
# =========================================================================
class TestWASDeAnalysisTemplate(unittest.TestCase):
    """Test WASDeAnalysisTemplate methods."""

    def _make_template(self):
        from src.analysis.templates.wasde_template import WASDeAnalysisTemplate
        return WASDeAnalysisTemplate()

    @patch('src.analysis.templates.base_template.BaseAnalysisTemplate._get_connection')
    def test_check_data_ready_true(self, mock_conn):
        """check_data_ready returns True when data exists."""
        conn = MagicMock()
        cur = MagicMock()
        cur.fetchone.return_value = {'cnt': 3}
        conn.cursor.return_value = cur
        conn.__enter__ = MagicMock(return_value=conn)
        conn.__exit__ = MagicMock(return_value=False)
        mock_conn.return_value = conn

        t = self._make_template()
        self.assertTrue(t.check_data_ready())

    @patch('src.analysis.templates.base_template.BaseAnalysisTemplate._get_connection')
    def test_check_data_ready_false(self, mock_conn):
        """check_data_ready returns False when no data."""
        conn = MagicMock()
        cur = MagicMock()
        cur.fetchone.return_value = {'cnt': 0}
        conn.cursor.return_value = cur
        conn.__enter__ = MagicMock(return_value=conn)
        conn.__exit__ = MagicMock(return_value=False)
        mock_conn.return_value = conn

        t = self._make_template()
        self.assertFalse(t.check_data_ready())

    @patch('src.analysis.templates.base_template.BaseAnalysisTemplate._get_connection')
    def test_gather_data_shapes(self, mock_conn):
        """gather_data returns expected keys."""
        us_rows = [
            {
                'commodity': 'corn', 'marketing_year': 2024, 'rn': 1,
                'report_date': date(2026, 3, 1),
                'production': 380000, 'total_supply': 420000,
                'beginning_stocks': 35000, 'imports': 5000,
                'domestic_consumption': 310000, 'feed_dom_consumption': 140000,
                'fsi_consumption': 170000, 'exports': 60000,
                'ending_stocks': 50000, 'crush': None,
                'stocks_use_pct': 13.5,
            },
            {
                'commodity': 'corn', 'marketing_year': 2024, 'rn': 2,
                'report_date': date(2026, 2, 1),
                'production': 380000, 'total_supply': 420000,
                'beginning_stocks': 35000, 'imports': 5000,
                'domestic_consumption': 310000, 'feed_dom_consumption': 140000,
                'fsi_consumption': 170000, 'exports': 58000,
                'ending_stocks': 52000, 'crush': None,
                'stocks_use_pct': 14.1,
            },
        ]
        global_rows = [
            {
                'commodity': 'corn', 'country_code': 'BR', 'rn': 1,
                'report_date': date(2026, 3, 1),
                'production': 130000, 'exports': 50000,
                'ending_stocks': 10000, 'domestic_consumption': 70000,
            },
        ]

        conn = MagicMock()
        cur = MagicMock()
        # fetchall called twice: US rows, then global rows
        cur.fetchall.side_effect = [us_rows, global_rows]
        conn.cursor.return_value = cur
        conn.__enter__ = MagicMock(return_value=conn)
        conn.__exit__ = MagicMock(return_value=False)
        mock_conn.return_value = conn

        t = self._make_template()
        data = t.gather_data()

        self.assertIn('us_balance_sheets', data)
        self.assertIn('global_sd', data)
        self.assertIn('report_date', data)
        self.assertIn('marketing_year', data)
        self.assertIn('corn', data['us_balance_sheets'])

    def test_compute_analysis_keys(self):
        """compute_analysis returns all required prompt variables."""
        t = self._make_template()
        data = {
            'us_balance_sheets': {
                'corn': {
                    'current': {
                        'ending_stocks': 50000, 'production': 380000,
                        'exports': 60000, 'domestic_consumption': 310000,
                        'total_supply': 420000, 'feed_dom_consumption': 140000,
                        'fsi_consumption': 170000, 'stocks_use_pct': 13.5,
                    },
                    'prior': {
                        'ending_stocks': 52000, 'production': 380000,
                        'exports': 58000, 'domestic_consumption': 310000,
                        'total_supply': 420000, 'feed_dom_consumption': 140000,
                        'fsi_consumption': 170000, 'stocks_use_pct': 14.1,
                    },
                },
            },
            'global_sd': {},
            'report_date': '2026-03-01',
            'prior_date': '2026-02-01',
            'marketing_year': 2024,
        }

        result = t.compute_analysis(data)
        self.assertIn('report_date', result)
        self.assertIn('marketing_year', result)
        self.assertIn('balance_sheet_table', result)
        self.assertIn('delta_summary', result)
        self.assertIn('global_context', result)
        self.assertIn('is_august_wasde', result)
        self.assertEqual(result['marketing_year'], '2024/25')

    def test_august_wasde_flag(self):
        """August report is flagged correctly."""
        t = self._make_template()
        data = {
            'us_balance_sheets': {},
            'global_sd': {},
            'report_date': '2025-08-12',
            'prior_date': '2025-07-11',
            'marketing_year': 2025,
        }

        result = t.compute_analysis(data)
        self.assertEqual(result['is_august_wasde'], 'Yes')

    def test_non_august_wasde_flag(self):
        """Non-August report is not flagged."""
        t = self._make_template()
        data = {
            'us_balance_sheets': {},
            'global_sd': {},
            'report_date': '2026-03-01',
            'prior_date': '2026-02-01',
            'marketing_year': 2025,
        }

        result = t.compute_analysis(data)
        self.assertEqual(result['is_august_wasde'], 'No')

    def test_template_attributes(self):
        """Template has correct ID and required_collectors."""
        t = self._make_template()
        self.assertEqual(t.template_id, 'wasde_monthly')
        self.assertEqual(t.required_collectors, ['usda_wasde'])
        self.assertEqual(t.prompt_template_id, 'wasde_analysis_v1')


# =========================================================================
# 3. TestWASDeAnalysisV1Prompt
# =========================================================================
class TestWASDeAnalysisV1Prompt(unittest.TestCase):
    """Test the WASDeAnalysisV1 prompt template."""

    def _make_template(self):
        from src.prompts.analysis.wasde_analysis_v1 import WASDeAnalysisV1
        return WASDeAnalysisV1()

    def _valid_vars(self):
        return {
            'report_date': '2026-03-01',
            'marketing_year': '2024/25',
            'balance_sheet_table': '| Item | Corn | ... |',
            'delta_summary': 'Corn ending stocks cut 2000',
            'global_context': 'Brazil production unchanged',
            'kg_context': 'No context',
            'is_august_wasde': 'No',
        }

    def test_registers_and_renders(self):
        """Template renders with all required variables."""
        from src.prompts.registry import PromptRegistry
        t = self._make_template()
        registry = PromptRegistry()
        registry.register(t)

        rendered = registry.render('wasde_analysis_v1', self._valid_vars())
        self.assertEqual(rendered.template_id, 'wasde_analysis_v1')
        self.assertEqual(rendered.task_type, 'analysis')
        self.assertIn('2026-03-01', rendered.user_prompt)
        self.assertIn('2024/25', rendered.user_prompt)

    def test_missing_variable_raises(self):
        """Missing required variable raises ValueError."""
        t = self._make_template()
        incomplete = {'report_date': '2026-03-01'}  # missing 6 vars
        with self.assertRaises(ValueError) as ctx:
            t.render(incomplete)
        self.assertIn('missing required variables', str(ctx.exception).lower())

    def test_hash_determinism(self):
        """Same variables produce same prompt_hash."""
        t = self._make_template()
        vars1 = self._valid_vars()
        vars2 = self._valid_vars()

        r1 = t.render(vars1)
        r2 = t.render(vars2)
        self.assertEqual(r1.prompt_hash, r2.prompt_hash)

    def test_different_vars_different_hash(self):
        """Different variables produce different prompt_hash."""
        t = self._make_template()
        vars1 = self._valid_vars()
        vars2 = self._valid_vars()
        vars2['report_date'] = '2026-04-01'

        r1 = t.render(vars1)
        r2 = t.render(vars2)
        self.assertNotEqual(r1.prompt_hash, r2.prompt_hash)

    def test_template_attributes(self):
        """Template has correct class attributes."""
        t = self._make_template()
        self.assertEqual(t.TEMPLATE_ID, 'wasde_analysis_v1')
        self.assertEqual(t.TASK_TYPE, 'analysis')
        self.assertEqual(t.DEFAULT_SENSITIVITY, 0)
        self.assertEqual(len(t.REQUIRED_VARIABLES), 7)

    def test_system_prompt_content(self):
        """System prompt contains analyst persona."""
        t = self._make_template()
        self.assertIn('grain', t.SYSTEM_PROMPT.lower())
        self.assertIn('analyst', t.SYSTEM_PROMPT.lower())


# =========================================================================
# 4. TestReportPipeline
# =========================================================================
class TestReportPipeline(unittest.TestCase):
    """Test ReportPipeline end-to-end with mocked DB and LLM."""

    def _make_pipeline(self):
        from src.analysis.templates.wasde_template import WASDeAnalysisTemplate
        from src.pipeline.report_pipeline import ReportPipeline
        return WASDeAnalysisTemplate(), ReportPipeline

    @patch('src.pipeline.report_pipeline.ReportPipeline._get_connection')
    @patch('src.services.llm.call_logger.CallLogger._get_connection')
    @patch('src.services.llm.call_logger.CallLogger._get_last_chain_hash')
    @patch('src.services.llm.llm_client.call_llm')
    @patch('src.analysis.templates.base_template.BaseAnalysisTemplate._get_connection')
    @patch('src.analysis.templates.base_template.BaseAnalysisTemplate._get_kg_manager')
    def test_full_run_success(self, mock_kg, mock_tmpl_conn, mock_call_llm,
                               mock_chain, mock_logger_conn, mock_pipe_conn):
        """Full pipeline run with mocked DB + LLM produces success."""
        from src.services.llm.llm_client import LLMResponse

        # Mock KG manager
        kg = MagicMock()
        kg.get_enriched_context.return_value = None
        mock_kg.return_value = kg

        # Mock template DB: check_data_ready
        ready_conn = MagicMock()
        ready_cur = MagicMock()
        ready_cur.fetchone.return_value = {'cnt': 3}
        ready_conn.cursor.return_value = ready_cur
        ready_conn.__enter__ = MagicMock(return_value=ready_conn)
        ready_conn.__exit__ = MagicMock(return_value=False)

        # Mock template DB: gather_data (US + global queries)
        gather_conn = MagicMock()
        gather_cur = MagicMock()
        us_rows = [
            {
                'commodity': 'corn', 'marketing_year': 2024, 'rn': 1,
                'report_date': date(2026, 3, 1),
                'production': 380000, 'total_supply': 420000,
                'beginning_stocks': 35000, 'imports': 5000,
                'domestic_consumption': 310000, 'feed_dom_consumption': 140000,
                'fsi_consumption': 170000, 'exports': 60000,
                'ending_stocks': 50000, 'crush': None,
                'stocks_use_pct': 13.5,
            },
            {
                'commodity': 'corn', 'marketing_year': 2024, 'rn': 2,
                'report_date': date(2026, 2, 1),
                'production': 380000, 'total_supply': 420000,
                'beginning_stocks': 35000, 'imports': 5000,
                'domestic_consumption': 310000, 'feed_dom_consumption': 140000,
                'fsi_consumption': 170000, 'exports': 58000,
                'ending_stocks': 52000, 'crush': None,
                'stocks_use_pct': 14.1,
            },
        ]
        gather_cur.fetchall.side_effect = [us_rows, []]  # US, then empty global
        gather_conn.cursor.return_value = gather_cur
        gather_conn.__enter__ = MagicMock(return_value=gather_conn)
        gather_conn.__exit__ = MagicMock(return_value=False)

        # Template connection returns ready_conn first, then gather_conn
        mock_tmpl_conn.side_effect = [ready_conn, gather_conn]

        # Mock LLM call
        mock_call_llm.return_value = LLMResponse(
            text=(
                "## Headline Changes\n\n"
                "USDA cut corn ending stocks by 2,000 thousand MT to 50,000.\n\n"
                "## US Corn\n\n"
                "Corn ending stocks were reduced to 50,000 thousand MT from 52,000, "
                "reflecting higher exports (+2,000). Stocks-to-use ratio declined "
                "to 13.5% from 14.1%. This is mildly bullish for corn futures.\n\n"
                "## US Soybeans\n\n"
                "Soybean balance sheet was unchanged this month.\n\n"
                "## US Wheat\n\n"
                "Wheat balance sheet was unchanged this month.\n\n"
                "## Global Context\n\n"
                "No significant changes to global balance sheets.\n\n"
                "## Market Implications\n\n"
                "- Corn stocks tightened modestly\n"
                "- Watch export pace for further adjustments\n"
                "- Soybeans and wheat unchanged, neutral"
            ),
            tokens_in=1200,
            tokens_out=350,
            latency_ms=2500,
            model_id='claude-opus-4-20250514',
            provider='anthropic',
        )

        # Mock chain hash
        mock_chain.return_value = None

        # Mock call logger DB
        log_conn = MagicMock()
        log_cur = MagicMock()
        log_conn.cursor.return_value = log_cur
        log_conn.__enter__ = MagicMock(return_value=log_conn)
        log_conn.__exit__ = MagicMock(return_value=False)
        mock_logger_conn.return_value = log_conn

        # Mock pipeline event log DB
        pipe_conn = MagicMock()
        pipe_cur = MagicMock()
        pipe_conn.cursor.return_value = pipe_cur
        pipe_conn.__enter__ = MagicMock(return_value=pipe_conn)
        pipe_conn.__exit__ = MagicMock(return_value=False)
        mock_pipe_conn.return_value = pipe_conn

        # Run pipeline
        template, PipelineClass = self._make_pipeline()
        pipeline = PipelineClass(template)
        result = pipeline.run(triggered_by='usda_wasde')

        self.assertTrue(result.success)
        self.assertIsNotNone(result.llm_narrative)
        self.assertTrue(result.validation_passed)
        self.assertIsNotNone(result.llm_call_id)
        self.assertEqual(result.triggered_by, 'usda_wasde')

    def test_validation_pass(self):
        """Narrative with >50 words mentioning commodities passes validation."""
        from src.pipeline.report_pipeline import ReportPipeline
        from src.analysis.templates.wasde_template import WASDeAnalysisTemplate

        pipeline = ReportPipeline(WASDeAnalysisTemplate())
        narrative = (
            "The USDA WASDE report showed corn ending stocks were reduced by "
            "2 million metric tons. Soybean production was revised higher. "
            "Wheat exports increased. The corn stocks-to-use ratio fell to "
            "13.5 percent from 14.1 percent. This represents a tighter balance "
            "sheet and is supportive for corn futures prices in the near term. "
            "Global context was mixed with Brazil production unchanged."
        )
        self.assertTrue(pipeline._validate_narrative(narrative))

    def test_validation_fail_too_short(self):
        """Narrative with <50 words fails validation."""
        from src.pipeline.report_pipeline import ReportPipeline
        from src.analysis.templates.wasde_template import WASDeAnalysisTemplate

        pipeline = ReportPipeline(WASDeAnalysisTemplate())
        self.assertFalse(pipeline._validate_narrative("Corn is up. Soybeans down."))

    def test_validation_fail_no_commodities(self):
        """Narrative without commodity mentions fails validation."""
        from src.pipeline.report_pipeline import ReportPipeline
        from src.analysis.templates.wasde_template import WASDeAnalysisTemplate

        pipeline = ReportPipeline(WASDeAnalysisTemplate())
        narrative = " ".join(["The report showed changes in the balance sheet."] * 10)
        self.assertFalse(pipeline._validate_narrative(narrative))

    def test_validation_fail_empty(self):
        """Empty narrative fails validation."""
        from src.pipeline.report_pipeline import ReportPipeline
        from src.analysis.templates.wasde_template import WASDeAnalysisTemplate

        pipeline = ReportPipeline(WASDeAnalysisTemplate())
        self.assertFalse(pipeline._validate_narrative(None))
        self.assertFalse(pipeline._validate_narrative(""))

    @patch('src.analysis.templates.base_template.BaseAnalysisTemplate._get_connection')
    @patch('src.analysis.templates.base_template.BaseAnalysisTemplate._get_kg_manager')
    def test_pipeline_catches_exceptions(self, mock_kg, mock_conn):
        """Pipeline run() never raises, stores error in result."""
        mock_conn.side_effect = Exception("DB connection failed")
        mock_kg.return_value = MagicMock()

        template, PipelineClass = self._make_pipeline()
        pipeline = PipelineClass(template)
        result = pipeline.run(triggered_by='test')

        self.assertFalse(result.success)
        self.assertIsNotNone(result.error_message)
        # check_data_ready catches the DB error and returns False,
        # then build_prompt_context raises RuntimeError about data not ready
        self.assertTrue(
            'DB connection failed' in result.error_message
            or 'Data not ready' in result.error_message
        )

    def test_pipeline_result_defaults(self):
        """PipelineResult has correct defaults."""
        from src.pipeline.report_pipeline import PipelineResult
        r = PipelineResult(
            pipeline_run_id=uuid.uuid4(),
            template_id='wasde_monthly',
            report_type='wasde',
            triggered_by='test',
            started_at=datetime.now(timezone.utc),
        )
        self.assertFalse(r.success)
        self.assertIsNone(r.llm_narrative)
        self.assertEqual(r.chart_paths, [])
        self.assertIsNone(r.docx_path)
        self.assertFalse(r.validation_passed)


if __name__ == '__main__':
    unittest.main()
