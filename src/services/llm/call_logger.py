"""
Call Logger

Tamper-evident audit trail for every LLM call. Writes to core.llm_call_log
with SHA-256 hash chaining: each record's chain_hash depends on the prior
row's chain_hash, making silent deletion/modification detectable.

Sensitivity-aware: sensitivity <= 1 stores full prompt/response in details
JSONB; sensitivity >= 2 stores metadata only (hashes, tokens, context keys).

Usage:
    from src.services.llm.call_logger import CallLogger

    logger = CallLogger(pipeline_run_id=run_id)

    # Direct logging
    call_id = logger.log_call(
        task_type='analysis', model_id='claude-sonnet-4-20250514',
        prompt_hash='abc...', output_hash='def...', tokens_in=500,
        tokens_out=1200, cost_usd=0.02, latency_ms=3400,
    )

    # Context manager (preferred)
    with logger.track('analysis', model_config) as tracker:
        response = call_llm(model, system_prompt, user_prompt)
        tracker.set_response(response)
"""

import hashlib
import json
import logging
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


class CallLogger:
    """
    Writes LLM call records to core.llm_call_log with hash chaining.

    Each call opens its own DB connection (following KGManager pattern).
    Uses get_connection() directly -- not execute_query() which blocks INSERT.
    """

    def __init__(self, pipeline_run_id: Optional[uuid.UUID] = None):
        self.pipeline_run_id = pipeline_run_id

    def _get_connection(self):
        from src.services.database.db_config import get_connection
        return get_connection()

    # ------------------------------------------------------------------
    # log_call
    # ------------------------------------------------------------------
    def log_call(
        self,
        task_type: str,
        model_id: str,
        provider: str = 'anthropic',
        sensitivity: int = 0,
        prompt_hash: Optional[str] = None,
        output_hash: Optional[str] = None,
        tokens_in: int = 0,
        tokens_out: int = 0,
        cost_usd: float = 0.0,
        latency_ms: int = 0,
        details: Optional[dict] = None,
        context_keys: Optional[List[str]] = None,
        pipeline_run_id: Optional[uuid.UUID] = None,
    ) -> uuid.UUID:
        """
        Insert a successful LLM call record with hash chaining.

        Returns the call's UUID.
        """
        call_id = uuid.uuid4()
        run_id = pipeline_run_id or self.pipeline_run_id
        called_at = datetime.now(timezone.utc)

        # Build record hash from body fields
        record_hash = self._compute_record_hash(
            call_id, model_id, prompt_hash, output_hash,
            tokens_in, tokens_out, called_at,
        )

        # Get prior chain hash for chaining
        prior_chain = self._get_last_chain_hash()
        chain_hash = self._compute_chain_hash(record_hash, prior_chain)

        # Sensitivity-aware details
        safe_details = self._sanitize_details(details, sensitivity)

        sql = """
            INSERT INTO core.llm_call_log (
                id, pipeline_run_id, task_type, model_id, provider,
                sensitivity, prompt_hash, output_hash,
                tokens_in, tokens_out, cost_usd, latency_ms,
                status, details, context_keys,
                record_hash, chain_hash, called_at
            ) VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s, %s,
                'success', %s, %s,
                %s, %s, %s
            )
        """
        params = (
            str(call_id), str(run_id) if run_id else None,
            task_type, model_id, provider,
            sensitivity, prompt_hash, output_hash,
            tokens_in, tokens_out, cost_usd, latency_ms,
            json.dumps(safe_details) if safe_details else None,
            context_keys,
            record_hash, chain_hash, called_at,
        )

        with self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute(sql, params)
            conn.commit()

        logger.debug("Logged LLM call %s (%s, %s)", call_id, task_type, model_id)
        return call_id

    # ------------------------------------------------------------------
    # log_error
    # ------------------------------------------------------------------
    def log_error(
        self,
        task_type: str,
        model_id: str,
        error_message: str,
        provider: str = 'anthropic',
        sensitivity: int = 0,
        prompt_hash: Optional[str] = None,
        tokens_in: int = 0,
        latency_ms: int = 0,
        details: Optional[dict] = None,
        context_keys: Optional[List[str]] = None,
        pipeline_run_id: Optional[uuid.UUID] = None,
    ) -> uuid.UUID:
        """Insert a failed LLM call record."""
        call_id = uuid.uuid4()
        run_id = pipeline_run_id or self.pipeline_run_id
        called_at = datetime.now(timezone.utc)

        record_hash = self._compute_record_hash(
            call_id, model_id, prompt_hash, None, tokens_in, 0, called_at,
        )
        prior_chain = self._get_last_chain_hash()
        chain_hash = self._compute_chain_hash(record_hash, prior_chain)

        safe_details = self._sanitize_details(details, sensitivity)

        sql = """
            INSERT INTO core.llm_call_log (
                id, pipeline_run_id, task_type, model_id, provider,
                sensitivity, prompt_hash,
                tokens_in, cost_usd, latency_ms,
                status, error_message, details, context_keys,
                record_hash, chain_hash, called_at
            ) VALUES (
                %s, %s, %s, %s, %s,
                %s, %s,
                %s, 0, %s,
                'error', %s, %s, %s,
                %s, %s, %s
            )
        """
        params = (
            str(call_id), str(run_id) if run_id else None,
            task_type, model_id, provider,
            sensitivity, prompt_hash,
            tokens_in, latency_ms,
            error_message,
            json.dumps(safe_details) if safe_details else None,
            context_keys,
            record_hash, chain_hash, called_at,
        )

        with self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute(sql, params)
            conn.commit()

        logger.warning("Logged LLM error %s (%s, %s): %s",
                        call_id, task_type, model_id, error_message[:100])
        return call_id

    # ------------------------------------------------------------------
    # track (context manager)
    # ------------------------------------------------------------------
    @contextmanager
    def track(self, task_type: str, model_config, sensitivity: int = 0,
              context_keys: Optional[List[str]] = None,
              prompt_text: Optional[str] = None):
        """
        Context manager that times an LLM call and logs it on exit.

        Usage:
            with call_logger.track('analysis', model_config) as tracker:
                response = call_llm(model, sys, user)
                tracker.set_response(response)
        """
        tracker = _CallTracker(
            logger=self,
            task_type=task_type,
            model_config=model_config,
            sensitivity=sensitivity,
            context_keys=context_keys,
            prompt_text=prompt_text,
        )
        tracker._start_time = time.perf_counter()
        try:
            yield tracker
            # Success path -- log_call
            tracker._finish()
        except Exception as e:
            # Error path -- log_error
            tracker._finish_error(str(e))
            raise

    # ------------------------------------------------------------------
    # Reporting
    # ------------------------------------------------------------------
    def get_pipeline_summary(self, pipeline_run_id: uuid.UUID) -> Dict:
        """Summary stats for a pipeline run: total calls, tokens, cost, errors."""
        sql = """
            SELECT
                COUNT(*) AS total_calls,
                COUNT(*) FILTER (WHERE status = 'error') AS errors,
                COALESCE(SUM(tokens_in), 0) AS total_tokens_in,
                COALESCE(SUM(tokens_out), 0) AS total_tokens_out,
                COALESCE(SUM(cost_usd), 0) AS total_cost_usd,
                COALESCE(AVG(latency_ms), 0) AS avg_latency_ms
            FROM core.llm_call_log
            WHERE pipeline_run_id = %s
        """
        with self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute(sql, (str(pipeline_run_id),))
            row = cur.fetchone()
            return dict(row) if row else {}

    def get_cost_report(self, start_date: str, end_date: str) -> Dict:
        """Cost breakdown by model between two dates."""
        sql = """
            SELECT
                model_id,
                COUNT(*) AS calls,
                COALESCE(SUM(tokens_in), 0) AS tokens_in,
                COALESCE(SUM(tokens_out), 0) AS tokens_out,
                COALESCE(SUM(cost_usd), 0) AS cost_usd
            FROM core.llm_call_log
            WHERE called_at >= %s AND called_at < %s
            GROUP BY model_id
            ORDER BY cost_usd DESC
        """
        with self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute(sql, (start_date, end_date))
            rows = [dict(r) for r in cur.fetchall()]
            total_cost = sum(float(r.get('cost_usd', 0)) for r in rows)
            return {'models': rows, 'total_cost_usd': total_cost}

    # ------------------------------------------------------------------
    # Hash helpers
    # ------------------------------------------------------------------
    @staticmethod
    def compute_hash(content: str) -> str:
        """SHA-256 hex digest of a string."""
        return hashlib.sha256(content.encode('utf-8')).hexdigest()

    def _compute_record_hash(
        self, call_id, model_id, prompt_hash, output_hash,
        tokens_in, tokens_out, called_at,
    ) -> str:
        """SHA-256 of concatenated record body fields."""
        body = f"{call_id}|{model_id}|{prompt_hash}|{output_hash}|{tokens_in}|{tokens_out}|{called_at.isoformat()}"
        return self.compute_hash(body)

    def _compute_chain_hash(self, record_hash: str, prior_chain_hash: Optional[str]) -> str:
        """SHA-256(record_hash + prior_chain_hash). If first row, just record_hash."""
        if prior_chain_hash:
            return self.compute_hash(record_hash + prior_chain_hash)
        return record_hash

    def _get_last_chain_hash(self) -> Optional[str]:
        """Fetch the most recent chain_hash from the log."""
        sql = """
            SELECT chain_hash FROM core.llm_call_log
            ORDER BY called_at DESC, created_at DESC
            LIMIT 1
        """
        try:
            with self._get_connection() as conn:
                cur = conn.cursor()
                cur.execute(sql)
                row = cur.fetchone()
                return row['chain_hash'] if row else None
        except Exception:
            # Table may not exist yet during testing
            return None

    def _sanitize_details(self, details: Optional[dict], sensitivity: int) -> Optional[dict]:
        """Strip prompt/response text from details if sensitivity >= 2."""
        if details is None:
            return None
        if sensitivity <= 1:
            return details
        # High sensitivity: keep metadata, strip text
        safe = {}
        for key in ('context_keys', 'template_id', 'template_version',
                     'task_type', 'prompt_hash', 'output_hash',
                     'tokens_in', 'tokens_out'):
            if key in details:
                safe[key] = details[key]
        safe['_redacted'] = True
        return safe


class _CallTracker:
    """Internal tracker used by CallLogger.track() context manager."""

    def __init__(self, logger_inst=None, task_type='', model_config=None,
                 sensitivity=0, context_keys=None, prompt_text=None,
                 **kwargs):
        # Accept both 'logger' and 'logger_inst' for flexibility
        self._logger = kwargs.get('logger', logger_inst)
        self._task_type = task_type
        self._model = model_config
        self._sensitivity = sensitivity
        self._context_keys = context_keys
        self._prompt_text = prompt_text
        self._response = None
        self._start_time = None

    def set_response(self, response):
        """Store the LLMResponse for logging on exit."""
        self._response = response

    def _finish(self):
        """Log a successful call."""
        if self._response is None:
            return
        latency = int((time.perf_counter() - self._start_time) * 1000)

        prompt_hash = None
        if self._prompt_text:
            prompt_hash = CallLogger.compute_hash(self._prompt_text)

        output_hash = CallLogger.compute_hash(self._response.text)
        cost = self._estimate_cost()

        details = None
        if self._sensitivity <= 1 and self._prompt_text:
            details = {
                'system_prompt': self._prompt_text[:500],
                'response_preview': self._response.text[:500],
            }

        self._logger.log_call(
            task_type=self._task_type,
            model_id=self._model.model_id,
            provider=self._model.provider,
            sensitivity=self._sensitivity,
            prompt_hash=prompt_hash,
            output_hash=output_hash,
            tokens_in=self._response.tokens_in,
            tokens_out=self._response.tokens_out,
            cost_usd=cost,
            latency_ms=latency,
            details=details,
            context_keys=self._context_keys,
        )

    def _finish_error(self, error_message: str):
        """Log a failed call."""
        latency = int((time.perf_counter() - self._start_time) * 1000)

        prompt_hash = None
        if self._prompt_text:
            prompt_hash = CallLogger.compute_hash(self._prompt_text)

        self._logger.log_error(
            task_type=self._task_type,
            model_id=self._model.model_id,
            provider=self._model.provider,
            sensitivity=self._sensitivity,
            prompt_hash=prompt_hash,
            latency_ms=latency,
            context_keys=self._context_keys,
            error_message=error_message,
        )

    def _estimate_cost(self) -> float:
        """Estimate USD cost from token counts and model config."""
        if self._response is None or self._model is None:
            return 0.0
        cost_in = (self._response.tokens_in / 1000) * self._model.cost_per_1k_in
        cost_out = (self._response.tokens_out / 1000) * self._model.cost_per_1k_out
        return round(cost_in + cost_out, 6)
