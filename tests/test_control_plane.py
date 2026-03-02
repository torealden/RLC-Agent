"""
Control Plane Integration Tests (Phase 1)

Tests the sensitivity config, model router, call logger, prompt registry,
and the full control-plane flow (route -> render -> mock LLM -> log).
"""

import hashlib
import json
import sys
import unittest
import uuid
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch, PropertyMock

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


# =========================================================================
# 1. SensitivityConfig
# =========================================================================
class TestSensitivityConfig(unittest.TestCase):
    """Test config/sensitivity.yaml loading and lookup methods."""

    def setUp(self):
        from src.config.sensitivity import SensitivityConfig
        self.config = SensitivityConfig()

    def test_loads_yaml(self):
        """YAML loads with expected structure."""
        self.assertGreater(len(self.config.levels), 0)
        self.assertGreater(len(self.config.data_source_defaults), 0)
        self.assertGreater(len(self.config.models), 0)

    def test_cloud_allowed_public(self):
        """Sensitivity 0 (public) allows cloud."""
        self.assertTrue(self.config.is_cloud_allowed(0))

    def test_cloud_allowed_licensed(self):
        """Sensitivity 1 (licensed) allows cloud."""
        self.assertTrue(self.config.is_cloud_allowed(1))

    def test_cloud_blocked_internal(self):
        """Sensitivity 2 (internal) blocks cloud."""
        self.assertFalse(self.config.is_cloud_allowed(2))

    def test_cloud_blocked_proprietary(self):
        """Sensitivity 3 (proprietary) blocks cloud."""
        self.assertFalse(self.config.is_cloud_allowed(3))

    def test_cloud_blocked_restricted(self):
        """Sensitivity 4 (restricted) blocks cloud."""
        self.assertFalse(self.config.is_cloud_allowed(4))

    def test_unknown_level_denies_cloud(self):
        """Unknown sensitivity level defaults to blocking cloud."""
        self.assertFalse(self.config.is_cloud_allowed(99))

    def test_data_source_sensitivity_public(self):
        """CFTC COT is public (0)."""
        self.assertEqual(self.config.get_data_source_sensitivity('cftc_cot'), 0)

    def test_data_source_sensitivity_licensed(self):
        """CME settlements is licensed (1)."""
        self.assertEqual(self.config.get_data_source_sensitivity('cme_settlements'), 1)

    def test_data_source_unknown_defaults_zero(self):
        """Unknown data source defaults to sensitivity 0."""
        self.assertEqual(self.config.get_data_source_sensitivity('nonexistent'), 0)

    def test_model_cost_anthropic(self):
        """Anthropic model has non-zero costs."""
        cost = self.config.get_model_cost('claude-sonnet-4-20250514')
        self.assertGreater(cost['cost_per_1k_in'], 0)
        self.assertGreater(cost['cost_per_1k_out'], 0)

    def test_model_cost_ollama(self):
        """Ollama model has zero costs."""
        cost = self.config.get_model_cost('llama3.1:70b')
        self.assertEqual(cost['cost_per_1k_in'], 0.0)
        self.assertEqual(cost['cost_per_1k_out'], 0.0)

    def test_model_tier(self):
        """Cloud and local tiers are correct."""
        self.assertEqual(self.config.get_model_tier('claude-sonnet-4-20250514'), 'cloud')
        self.assertEqual(self.config.get_model_tier('llama3.1:70b'), 'local')

    def test_all_22_sources_mapped(self):
        """All 22 data sources from collector registry are mapped."""
        self.assertEqual(len(self.config.data_source_defaults), 22)


# =========================================================================
# 2. ModelRouter
# =========================================================================
class TestModelRouter(unittest.TestCase):
    """Test model routing based on sensitivity and task type."""

    def setUp(self):
        from src.services.llm.model_router import ModelRouter
        self.router = ModelRouter()

    def test_public_routes_to_cloud(self):
        """Public sensitivity -> cloud model."""
        model = self.router.route('analysis', sensitivity=0)
        self.assertEqual(model.model_id, 'claude-sonnet-4-20250514')
        self.assertEqual(model.provider, 'anthropic')
        self.assertEqual(model.tier, 'cloud')

    def test_internal_routes_to_local(self):
        """Internal sensitivity -> local model."""
        model = self.router.route('analysis', sensitivity=2)
        self.assertEqual(model.model_id, 'llama3.1:32b')
        self.assertEqual(model.provider, 'ollama')
        self.assertEqual(model.tier, 'local')

    def test_narrative_cloud(self):
        """Narrative task uses sonnet on cloud."""
        model = self.router.route('narrative', sensitivity=0)
        self.assertEqual(model.model_id, 'claude-sonnet-4-20250514')

    def test_summary_cloud(self):
        """Summary task uses haiku on cloud."""
        model = self.router.route('summary', sensitivity=0)
        self.assertEqual(model.model_id, 'claude-haiku-4-5-20251001')

    def test_synthesis_cloud(self):
        """Synthesis task uses opus on cloud."""
        model = self.router.route('synthesis', sensitivity=0)
        self.assertEqual(model.model_id, 'claude-opus-4-20250514')

    def test_complexity_high_upgrades(self):
        """High complexity upgrades model one tier."""
        model = self.router.route('summary', sensitivity=0, complexity='high')
        self.assertEqual(model.model_id, 'claude-sonnet-4-20250514')

    def test_complexity_low_downgrades(self):
        """Low complexity downgrades model one tier."""
        model = self.router.route('analysis', sensitivity=0, complexity='low')
        self.assertEqual(model.model_id, 'claude-haiku-4-5-20251001')

    def test_preflight_blocks_cloud_high_sensitivity(self):
        """Preflight check blocks cloud models for sensitivity >= 2."""
        model = self.router.route('synthesis', sensitivity=3)
        self.assertEqual(model.tier, 'local')

    def test_unknown_task_raises(self):
        """Unknown task type raises ValueError."""
        with self.assertRaises(ValueError):
            self.router.route('unknown_task')

    def test_fallback_chain(self):
        """Fallback chain returns next model."""
        fb = self.router.get_fallback('claude-sonnet-4-20250514')
        self.assertIsNotNone(fb)
        self.assertEqual(fb.model_id, 'claude-haiku-4-5-20251001')

    def test_fallback_end_of_chain(self):
        """Last model in chain returns None."""
        fb = self.router.get_fallback('llama3.1:8b')
        self.assertIsNone(fb)

    def test_model_config_has_cost(self):
        """Routed model has cost fields."""
        model = self.router.route('analysis', sensitivity=0)
        self.assertGreater(model.cost_per_1k_in, 0)
        self.assertGreater(model.cost_per_1k_out, 0)

    def test_health_check_returns_dict(self):
        """health_check returns a dict with anthropic and ollama keys."""
        result = self.router.health_check()
        self.assertIn('anthropic', result)
        self.assertIn('ollama', result)


# =========================================================================
# 3. CallLogger
# =========================================================================
class TestCallLogger(unittest.TestCase):
    """Test LLM call logging and hash chaining."""

    def _mock_connection(self):
        """Create a mock connection context manager."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = None  # No prior chain hash

        ctx = MagicMock()
        ctx.__enter__ = MagicMock(return_value=mock_conn)
        ctx.__exit__ = MagicMock(return_value=False)
        return ctx, mock_conn, mock_cursor

    @patch('src.services.llm.call_logger.CallLogger._get_connection')
    def test_log_call_inserts_record(self, mock_get_conn):
        """log_call executes INSERT and returns a UUID."""
        ctx, conn, cursor = self._mock_connection()
        mock_get_conn.return_value = ctx

        from src.services.llm.call_logger import CallLogger
        logger = CallLogger()
        call_id = logger.log_call(
            task_type='analysis',
            model_id='claude-sonnet-4-20250514',
            tokens_in=500,
            tokens_out=1200,
        )

        self.assertIsInstance(call_id, uuid.UUID)
        # Should have been called for: get_last_chain_hash + log_call INSERT
        self.assertTrue(cursor.execute.called)

    @patch('src.services.llm.call_logger.CallLogger._get_connection')
    def test_log_error_inserts_record(self, mock_get_conn):
        """log_error writes an error record."""
        ctx, conn, cursor = self._mock_connection()
        mock_get_conn.return_value = ctx

        from src.services.llm.call_logger import CallLogger
        logger = CallLogger()
        call_id = logger.log_error(
            task_type='analysis',
            model_id='claude-sonnet-4-20250514',
            error_message='API timeout',
        )

        self.assertIsInstance(call_id, uuid.UUID)

    def test_compute_hash_deterministic(self):
        """Same input always produces same hash."""
        from src.services.llm.call_logger import CallLogger
        h1 = CallLogger.compute_hash("test content")
        h2 = CallLogger.compute_hash("test content")
        self.assertEqual(h1, h2)
        self.assertEqual(len(h1), 64)  # SHA-256 hex length

    def test_compute_hash_differs(self):
        """Different input produces different hash."""
        from src.services.llm.call_logger import CallLogger
        h1 = CallLogger.compute_hash("content A")
        h2 = CallLogger.compute_hash("content B")
        self.assertNotEqual(h1, h2)

    def test_chain_hash_first_row(self):
        """First row: chain_hash == record_hash (no prior)."""
        from src.services.llm.call_logger import CallLogger
        logger = CallLogger()
        record_hash = "abc123"
        chain = logger._compute_chain_hash(record_hash, None)
        self.assertEqual(chain, record_hash)

    def test_chain_hash_with_prior(self):
        """Subsequent rows: chain_hash = SHA-256(record + prior)."""
        from src.services.llm.call_logger import CallLogger
        logger = CallLogger()
        record_hash = "abc123"
        prior = "def456"
        chain = logger._compute_chain_hash(record_hash, prior)
        expected = hashlib.sha256(f"{record_hash}{prior}".encode()).hexdigest()
        self.assertEqual(chain, expected)

    def test_sanitize_details_low_sensitivity(self):
        """Sensitivity <= 1 keeps full details."""
        from src.services.llm.call_logger import CallLogger
        logger = CallLogger()
        details = {'prompt': 'full text', 'response': 'full response'}
        result = logger._sanitize_details(details, sensitivity=0)
        self.assertIn('prompt', result)

    def test_sanitize_details_high_sensitivity(self):
        """Sensitivity >= 2 strips text, keeps metadata."""
        from src.services.llm.call_logger import CallLogger
        logger = CallLogger()
        details = {
            'prompt': 'secret text',
            'response': 'secret response',
            'template_id': 'test',
            'tokens_in': 100,
        }
        result = logger._sanitize_details(details, sensitivity=2)
        self.assertNotIn('prompt', result)
        self.assertNotIn('response', result)
        self.assertIn('template_id', result)
        self.assertTrue(result.get('_redacted'))


# =========================================================================
# 4. PromptRegistry
# =========================================================================
class TestPromptRegistry(unittest.TestCase):
    """Test prompt template registration and rendering."""

    def _make_template(self, template_id='test_template'):
        """Create a minimal template for testing."""
        from src.prompts.base_template import BasePromptTemplate

        class TestTemplate(BasePromptTemplate):
            TEMPLATE_ID = template_id
            TEMPLATE_VERSION = '1.0'
            TASK_TYPE = 'analysis'
            DEFAULT_SENSITIVITY = 0
            SYSTEM_PROMPT = 'You are a test analyst.'
            USER_TEMPLATE = 'Analyze {commodity}: {data}'
            REQUIRED_VARIABLES = ['commodity', 'data']
            OPTIONAL_VARIABLES = ['notes']

        return TestTemplate()

    def test_register_and_get(self):
        """Register a template and retrieve it."""
        from src.prompts.registry import PromptRegistry
        registry = PromptRegistry()
        template = self._make_template()
        registry.register(template)

        got = registry.get('test_template')
        self.assertEqual(got.TEMPLATE_ID, 'test_template')

    def test_get_missing_raises(self):
        """Getting unregistered template raises KeyError."""
        from src.prompts.registry import PromptRegistry
        registry = PromptRegistry()
        with self.assertRaises(KeyError):
            registry.get('nonexistent')

    def test_render_succeeds(self):
        """Rendering with required variables produces a RenderedPrompt."""
        from src.prompts.registry import PromptRegistry
        registry = PromptRegistry()
        registry.register(self._make_template())

        rendered = registry.render('test_template', {
            'commodity': 'corn',
            'data': 'production: 15B bushels',
        })

        self.assertEqual(rendered.template_id, 'test_template')
        self.assertIn('corn', rendered.user_prompt)
        self.assertEqual(rendered.task_type, 'analysis')
        self.assertEqual(len(rendered.prompt_hash), 64)

    def test_render_missing_variable_raises(self):
        """Rendering without required variables raises ValueError."""
        from src.prompts.registry import PromptRegistry
        registry = PromptRegistry()
        registry.register(self._make_template())

        with self.assertRaises(ValueError):
            registry.render('test_template', {'commodity': 'corn'})

    def test_hash_determinism(self):
        """Same template + same variables -> same prompt_hash."""
        from src.prompts.registry import PromptRegistry
        registry = PromptRegistry()
        registry.register(self._make_template())
        vars_ = {'commodity': 'corn', 'data': 'test'}

        r1 = registry.render('test_template', vars_)
        r2 = registry.render('test_template', vars_)
        self.assertEqual(r1.prompt_hash, r2.prompt_hash)

    def test_hash_differs_for_different_input(self):
        """Different variables -> different prompt_hash."""
        from src.prompts.registry import PromptRegistry
        registry = PromptRegistry()
        registry.register(self._make_template())

        r1 = registry.render('test_template', {'commodity': 'corn', 'data': 'A'})
        r2 = registry.render('test_template', {'commodity': 'wheat', 'data': 'B'})
        self.assertNotEqual(r1.prompt_hash, r2.prompt_hash)

    def test_list_templates(self):
        """list_templates returns metadata for all registered templates."""
        from src.prompts.registry import PromptRegistry
        registry = PromptRegistry()
        registry.register(self._make_template('t1'))
        registry.register(self._make_template('t2'))

        items = registry.list_templates()
        self.assertEqual(len(items), 2)
        ids = [i['template_id'] for i in items]
        self.assertIn('t1', ids)
        self.assertIn('t2', ids)

    def test_register_empty_id_raises(self):
        """Template with empty ID raises ValueError."""
        from src.prompts.base_template import BasePromptTemplate
        from src.prompts.registry import PromptRegistry

        class BadTemplate(BasePromptTemplate):
            TEMPLATE_ID = ''

        registry = PromptRegistry()
        with self.assertRaises(ValueError):
            registry.register(BadTemplate())


# =========================================================================
# 5. Integration: Full Control-Plane Flow
# =========================================================================
class TestControlPlaneIntegration(unittest.TestCase):
    """End-to-end: route -> render prompt -> mock LLM -> log with hashes."""

    @patch('src.services.llm.call_logger.CallLogger._get_connection')
    def test_full_flow(self, mock_get_conn):
        """Complete flow: route model, render prompt, log call."""
        # Setup mock DB
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = None

        ctx = MagicMock()
        ctx.__enter__ = MagicMock(return_value=mock_conn)
        ctx.__exit__ = MagicMock(return_value=False)
        mock_get_conn.return_value = ctx

        from src.services.llm.model_router import ModelRouter
        from src.services.llm.call_logger import CallLogger
        from src.prompts.registry import PromptRegistry
        from src.prompts.base_template import BasePromptTemplate

        # 1. Create and register a template
        class TestAnalysis(BasePromptTemplate):
            TEMPLATE_ID = 'integration_test'
            TEMPLATE_VERSION = '1.0'
            TASK_TYPE = 'analysis'
            DEFAULT_SENSITIVITY = 0
            SYSTEM_PROMPT = 'You are a commodity analyst.'
            USER_TEMPLATE = 'Analyze {commodity} balance sheet: {data}'
            REQUIRED_VARIABLES = ['commodity', 'data']

        registry = PromptRegistry()
        registry.register(TestAnalysis())

        # 2. Route the model
        router = ModelRouter()
        model = router.route('analysis', sensitivity=0)
        self.assertEqual(model.provider, 'anthropic')

        # 3. Render the prompt
        rendered = registry.render('integration_test', {
            'commodity': 'corn',
            'data': 'production=15.1B bu, ending stocks=1.8B bu',
        })
        self.assertEqual(rendered.task_type, 'analysis')
        self.assertIn('corn', rendered.user_prompt)

        # 4. Log the call (simulating a successful LLM response)
        call_logger = CallLogger(pipeline_run_id=uuid.uuid4())
        output_text = "Corn stocks-to-use at 11.9% is historically tight."
        call_id = call_logger.log_call(
            task_type=rendered.task_type,
            model_id=model.model_id,
            provider=model.provider,
            sensitivity=rendered.sensitivity,
            prompt_hash=rendered.prompt_hash,
            output_hash=CallLogger.compute_hash(output_text),
            tokens_in=500,
            tokens_out=1200,
            cost_usd=0.02,
            latency_ms=3400,
            details={
                'template_id': rendered.template_id,
                'system_prompt': rendered.system_prompt,
                'user_prompt': rendered.user_prompt,
                'response': output_text,
            },
            context_keys=['corn', 'usda.wasde.revision_pattern'],
        )

        self.assertIsInstance(call_id, uuid.UUID)
        # Verify INSERT was called
        insert_calls = [
            c for c in mock_cursor.execute.call_args_list
            if 'INSERT INTO core.llm_call_log' in str(c)
        ]
        self.assertGreater(len(insert_calls), 0)

    def test_sensitivity_blocks_cloud_in_flow(self):
        """High sensitivity forces local routing in the full flow."""
        from src.services.llm.model_router import ModelRouter

        router = ModelRouter()
        model = router.route('analysis', sensitivity=3)
        self.assertEqual(model.tier, 'local')
        self.assertEqual(model.provider, 'ollama')


if __name__ == '__main__':
    unittest.main()
