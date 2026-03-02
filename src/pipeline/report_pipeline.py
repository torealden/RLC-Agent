"""
Report Pipeline Orchestrator

Wires together analysis template + model router + LLM client + call logger
to produce an end-to-end autonomous report. Each run goes through 6 stages:

  1. Build prompt context (template.build_prompt_context)
  2. Route model -> render prompt -> call LLM -> log call
  3. Generate charts (stub - Phase 2.5)
  4. Assemble .docx (stub - Phase 2.5)
  5. Validate narrative
  6. Write report_generated event to core.event_log

The run() method never raises. All exceptions are caught and stored in
PipelineResult for downstream inspection.
"""

import hashlib
import json
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class PipelineResult:
    """Full record of a pipeline run."""
    pipeline_run_id: uuid.UUID
    template_id: str
    report_type: str
    triggered_by: str
    started_at: datetime
    finished_at: Optional[datetime] = None
    success: bool = False
    error_message: Optional[str] = None
    prompt_context: Optional[Dict] = None
    llm_narrative: Optional[str] = None
    llm_call_id: Optional[uuid.UUID] = None
    chart_paths: List[str] = field(default_factory=list)   # Phase 2.5
    docx_path: Optional[str] = None                        # Phase 2.5
    validation_passed: bool = False
    event_log_id: Optional[int] = None


class ReportPipeline:
    """
    Orchestrates a single report generation run.

    Usage:
        from src.analysis.templates.wasde_template import WASDeAnalysisTemplate
        from src.pipeline.report_pipeline import ReportPipeline

        template = WASDeAnalysisTemplate()
        pipeline = ReportPipeline(template)
        result = pipeline.run(triggered_by='usda_wasde')
    """

    def __init__(self, template, router=None, call_logger=None):
        """
        Args:
            template: A BaseAnalysisTemplate subclass instance
            router: Optional ModelRouter (lazy-created if None)
            call_logger: Optional CallLogger (lazy-created if None)
        """
        self.template = template
        self._router = router
        self._call_logger = call_logger

    def _get_router(self):
        if self._router is None:
            from src.services.llm.model_router import ModelRouter
            self._router = ModelRouter()
        return self._router

    def _get_call_logger(self, pipeline_run_id):
        if self._call_logger is None:
            from src.services.llm.call_logger import CallLogger
            self._call_logger = CallLogger(pipeline_run_id=pipeline_run_id)
        return self._call_logger

    def _get_connection(self):
        from src.services.database.db_config import get_connection
        return get_connection()

    def run(self, triggered_by: str = 'manual') -> PipelineResult:
        """
        Execute the full pipeline. Never raises -- all errors stored in result.

        Args:
            triggered_by: Who/what triggered this run (e.g. collector name)

        Returns:
            PipelineResult with full execution details
        """
        run_id = uuid.uuid4()
        result = PipelineResult(
            pipeline_run_id=run_id,
            template_id=self.template.template_id,
            report_type=self.template.report_type,
            triggered_by=triggered_by,
            started_at=datetime.now(timezone.utc),
        )

        try:
            # Stage 1: Build prompt context
            logger.info("Pipeline %s stage 1: building prompt context", run_id)
            prompt_context = self.template.build_prompt_context()
            result.prompt_context = prompt_context

            # Stage 2: Route -> Render -> Call LLM -> Log
            logger.info("Pipeline %s stage 2: LLM call", run_id)
            self._stage_llm(result, prompt_context)

            # Stage 3: Chart generation (stub)
            logger.info("Pipeline %s stage 3: charts (stub)", run_id)
            # Phase 2.5: chart_paths = generate_charts(...)
            result.chart_paths = []

            # Stage 4: .docx assembly (stub)
            logger.info("Pipeline %s stage 4: docx (stub)", run_id)
            # Phase 2.5: docx_path = assemble_docx(...)
            result.docx_path = None

            # Stage 5: Validate narrative
            logger.info("Pipeline %s stage 5: validation", run_id)
            result.validation_passed = self._validate_narrative(result.llm_narrative)

            # Stage 6: Write event_log entry
            logger.info("Pipeline %s stage 6: event log", run_id)
            self._stage_event_log(result)

            result.success = True
            result.finished_at = datetime.now(timezone.utc)

        except Exception as e:
            result.success = False
            result.error_message = str(e)
            result.finished_at = datetime.now(timezone.utc)
            logger.error("Pipeline %s failed: %s", run_id, e, exc_info=True)

        return result

    # ------------------------------------------------------------------
    # Stage 2: LLM
    # ------------------------------------------------------------------

    def _stage_llm(self, result: PipelineResult, prompt_context: Dict):
        """Route model, render prompt, call LLM, log the call."""
        from src.prompts.analysis.wasde_analysis_v1 import WASDeAnalysisV1
        from src.services.llm.llm_client import call_llm

        # Build prompt variables from analysis output
        analysis = prompt_context.get('analysis', {})

        # Add KG context as formatted markdown
        kg_context = prompt_context.get('kg_context', {})
        kg_text = self._format_kg_context(kg_context)
        analysis['kg_context'] = kg_text

        # Render prompt
        template = WASDeAnalysisV1()
        rendered = template.render(
            analysis,
            context_keys=prompt_context.get('kg_node_keys', []),
        )

        # Route model: analysis task, sensitivity=0, complexity=high for WASDE
        router = self._get_router()
        model = router.route(
            task_type='analysis',
            sensitivity=rendered.sensitivity,
            complexity='high',
        )

        # Call LLM
        response = call_llm(
            model,
            rendered.system_prompt,
            rendered.user_prompt,
            max_tokens=1800,
        )
        result.llm_narrative = response.text

        # Log the call
        call_logger = self._get_call_logger(result.pipeline_run_id)
        output_hash = hashlib.sha256(response.text.encode('utf-8')).hexdigest()
        cost_in = (response.tokens_in / 1000) * model.cost_per_1k_in
        cost_out = (response.tokens_out / 1000) * model.cost_per_1k_out
        cost = round(cost_in + cost_out, 6)

        call_id = call_logger.log_call(
            task_type=rendered.task_type,
            model_id=model.model_id,
            provider=model.provider,
            sensitivity=rendered.sensitivity,
            prompt_hash=rendered.prompt_hash,
            output_hash=output_hash,
            tokens_in=response.tokens_in,
            tokens_out=response.tokens_out,
            cost_usd=cost,
            latency_ms=response.latency_ms,
            details={
                'template_id': rendered.template_id,
                'template_version': rendered.version,
                'response_preview': response.text[:500],
            },
            context_keys=rendered.context_keys,
            pipeline_run_id=result.pipeline_run_id,
        )
        result.llm_call_id = call_id

    # ------------------------------------------------------------------
    # Stage 5: Validation
    # ------------------------------------------------------------------

    def _validate_narrative(self, narrative: Optional[str]) -> bool:
        """
        Basic validation of the LLM narrative.

        Checks:
          - Narrative is not empty
          - At least 50 words
          - Mentions at least one of: corn, soybeans, wheat
        """
        if not narrative:
            return False

        words = narrative.split()
        if len(words) < 50:
            logger.warning("Narrative too short: %d words", len(words))
            return False

        text_lower = narrative.lower()
        commodities_mentioned = sum(
            1 for c in ['corn', 'soybean', 'wheat']
            if c in text_lower
        )
        if commodities_mentioned == 0:
            logger.warning("Narrative mentions no commodities")
            return False

        return True

    # ------------------------------------------------------------------
    # Stage 6: Event Log
    # ------------------------------------------------------------------

    def _stage_event_log(self, result: PipelineResult):
        """Write a report_generated event to core.event_log."""
        try:
            with self._get_connection() as conn:
                cur = conn.cursor()
                summary = (
                    f"WASDE report generated ({result.template_id}) "
                    f"triggered_by={result.triggered_by}"
                )
                if result.llm_narrative:
                    word_count = len(result.llm_narrative.split())
                    summary += f" | {word_count} words"
                if not result.validation_passed:
                    summary += " | VALIDATION FAILED"

                details = {
                    'pipeline_run_id': str(result.pipeline_run_id),
                    'template_id': result.template_id,
                    'triggered_by': result.triggered_by,
                    'validation_passed': result.validation_passed,
                    'word_count': len(result.llm_narrative.split()) if result.llm_narrative else 0,
                }
                if result.llm_call_id:
                    details['llm_call_id'] = str(result.llm_call_id)

                cur.execute(
                    "SELECT core.log_event(%s, %s, %s, %s, %s)",
                    ('report_generated', result.template_id, summary,
                     json.dumps(details), 1)
                )
                conn.commit()

                logger.info("Logged report_generated event for pipeline %s",
                            result.pipeline_run_id)
        except Exception as e:
            logger.error("Failed to log report_generated event: %s", e)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _format_kg_context(self, kg_context: Dict) -> str:
        """Format KG enriched context as compact markdown for the prompt."""
        if not kg_context:
            return "No knowledge graph context available."

        lines = []
        for node_key, enriched in kg_context.items():
            node = enriched.get('node', {})
            label = node.get('label', node_key)
            summary = enriched.get('summary', {})
            lines.append(f"**{label}** ({summary.get('context_count', 0)} contexts, "
                         f"{summary.get('edge_count', 0)} edges)")

            # Top 3 contexts, truncated
            for ctx in enriched.get('contexts', [])[:3]:
                val = ctx.get('context_value', {})
                if isinstance(val, dict):
                    val_str = json.dumps(val, default=str)
                else:
                    val_str = str(val)
                if len(val_str) > 300:
                    val_str = val_str[:297] + '...'
                lines.append(f"  - [{ctx.get('context_type', '')}] {val_str}")

        return "\n".join(lines)
