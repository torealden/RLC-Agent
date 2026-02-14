"""
Collector Runner

Executes a collector and logs the result to core.collection_status
and core.event_log. This is the bridge between the dispatcher and
the database CNS layer.

Workflow:
    1. Write collection_status row (status='running')
    2. Instantiate collector from registry
    3. Call collector.collect()
    4. Update collection_status (status='success'|'failed'|'partial')
    5. Write event_log entry (for LLM briefing)
    6. Return result
"""

import logging
import json
import time
from dataclasses import dataclass, field
from datetime import datetime, date
from typing import Optional, Dict, Any, List

from src.dispatcher.collector_registry import CollectorRegistry

logger = logging.getLogger(__name__)


@dataclass
class CollectorRunResult:
    """Result of a collector run through the runner."""
    collector_name: str
    success: bool
    status: str  # 'success', 'failed', 'partial'
    started_at: datetime
    finished_at: Optional[datetime] = None
    rows_collected: int = 0
    rows_inserted: int = 0
    data_period: Optional[str] = None
    is_new_data: bool = True
    error_message: Optional[str] = None
    warnings: List[str] = field(default_factory=list)
    triggered_by: str = 'manual'
    status_id: Optional[int] = None  # ID in collection_status table


class CollectorRunner:
    """
    Runs collectors with full CNS status logging.

    Each run:
    - Creates a 'running' entry in core.collection_status
    - Executes the collector
    - Updates the entry with results
    - Logs an event to core.event_log
    """

    def __init__(self, registry: CollectorRegistry = None):
        self.registry = registry or CollectorRegistry()

    def _get_connection(self):
        """Get database connection via db_config."""
        from src.services.database.db_config import get_connection
        return get_connection()

    def _insert_status_running(self, conn, collector_name: str,
                                triggered_by: str, commodities: List[str] = None) -> int:
        """Insert initial 'running' row into collection_status. Returns the row ID."""
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO core.collection_status
                (collector_name, run_started_at, status, triggered_by, commodities)
            VALUES (%s, NOW(), 'running', %s, %s)
            RETURNING id
        """, (collector_name, triggered_by, commodities))
        row = cursor.fetchone()
        conn.commit()
        return row['id'] if isinstance(row, dict) else row[0]

    def _update_status_result(self, conn, status_id: int, status: str,
                               rows_collected: int, rows_inserted: int,
                               data_period: str, is_new_data: bool,
                               error_message: str = None, notes: str = None):
        """Update collection_status row with final results."""
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE core.collection_status SET
                run_finished_at = NOW(),
                status = %s,
                rows_collected = %s,
                rows_inserted = %s,
                data_period = %s,
                is_new_data = %s,
                error_message = %s,
                notes = %s
            WHERE id = %s
        """, (status, rows_collected, rows_inserted, data_period,
              is_new_data, error_message, notes, status_id))
        conn.commit()

    def _log_event(self, conn, event_type: str, source: str,
                    summary: str, details: Dict = None, priority: int = 3):
        """Log an event to core.event_log using the log_event() function."""
        cursor = conn.cursor()
        cursor.execute(
            "SELECT core.log_event(%s, %s, %s, %s, %s)",
            (event_type, source, summary,
             json.dumps(details) if details else None,
             priority)
        )
        conn.commit()

    def _compute_data_period(self, result) -> Optional[str]:
        """Extract data period from collector result."""
        if hasattr(result, 'period_end') and result.period_end:
            return f"through_{result.period_end}"
        if hasattr(result, 'data_as_of') and result.data_as_of:
            return f"as_of_{result.data_as_of}"
        return f"collected_{date.today().isoformat()}"

    def run_collector(
        self,
        collector_name: str,
        triggered_by: str = 'manual',
        commodities: List[str] = None,
        **collector_kwargs
    ) -> CollectorRunResult:
        """
        Run a single collector with full status tracking.

        Args:
            collector_name: Registry key (e.g., 'cftc_cot')
            triggered_by: 'scheduler', 'manual', or 'backfill'
            commodities: Override commodity list (or use schedule default)
            **collector_kwargs: Passed to collector.collect()

        Returns:
            CollectorRunResult with full execution details
        """
        started_at = datetime.now()
        run_result = CollectorRunResult(
            collector_name=collector_name,
            success=False,
            status='failed',
            started_at=started_at,
            triggered_by=triggered_by,
        )

        # Step 1: Record the run as 'running' in the database
        status_id = None
        try:
            with self._get_connection() as conn:
                status_id = self._insert_status_running(
                    conn, collector_name, triggered_by, commodities
                )
                run_result.status_id = status_id
        except Exception as e:
            logger.error(f"Failed to write initial status for {collector_name}: {e}")
            # Continue anyway — the collector can still run even if logging fails

        # Step 2: Instantiate the collector
        collector = self.registry.get_collector(collector_name)
        if collector is None:
            error_msg = f"Collector '{collector_name}' not found in registry"
            logger.error(error_msg)
            run_result.error_message = error_msg
            run_result.finished_at = datetime.now()

            if status_id:
                try:
                    with self._get_connection() as conn:
                        self._update_status_result(
                            conn, status_id, 'failed', 0, 0, None, False,
                            error_message=error_msg
                        )
                        self._log_event(
                            conn, 'collection_failed', collector_name,
                            f"{collector_name} failed: {error_msg}",
                            {'error': error_msg}, priority=2
                        )
                except Exception:
                    pass

            return run_result

        # Step 3: Execute the collector
        logger.info(f"Running collector: {collector_name} (triggered_by={triggered_by})")
        try:
            result = collector.collect(**collector_kwargs)

            run_result.finished_at = datetime.now()
            run_result.rows_collected = result.records_fetched if hasattr(result, 'records_fetched') else 0
            run_result.data_period = self._compute_data_period(result)
            run_result.warnings = result.warnings if hasattr(result, 'warnings') else []

            if result.success:
                run_result.success = True
                run_result.status = 'success'
                run_result.is_new_data = run_result.rows_collected > 0
            else:
                run_result.status = 'failed'
                run_result.error_message = result.error_message if hasattr(result, 'error_message') else 'Unknown error'

            # Partial success: some data but also warnings/errors
            if result.success and run_result.warnings:
                run_result.status = 'partial'

        except Exception as e:
            run_result.finished_at = datetime.now()
            run_result.error_message = str(e)
            logger.error(f"Collector {collector_name} raised exception: {e}", exc_info=True)

        # Step 4: Update database with results
        if status_id:
            try:
                with self._get_connection() as conn:
                    self._update_status_result(
                        conn, status_id,
                        run_result.status,
                        run_result.rows_collected,
                        run_result.rows_inserted,
                        run_result.data_period,
                        run_result.is_new_data,
                        error_message=run_result.error_message,
                        notes='; '.join(run_result.warnings) if run_result.warnings else None
                    )
            except Exception as e:
                logger.error(f"Failed to update collection_status for {collector_name}: {e}")

        # Step 5: Log event for LLM briefing
        if status_id:
            try:
                with self._get_connection() as conn:
                    elapsed_s = (run_result.finished_at - run_result.started_at).total_seconds()

                    if run_result.success:
                        summary = (
                            f"{collector_name} collected {run_result.rows_collected} rows "
                            f"({run_result.data_period}) in {elapsed_s:.1f}s"
                        )
                        event_type = 'collection_complete'
                        priority = 3
                    else:
                        summary = (
                            f"{collector_name} FAILED: {run_result.error_message}"
                        )
                        event_type = 'collection_failed'
                        priority = 2

                    details = {
                        'collector': collector_name,
                        'status': run_result.status,
                        'rows_collected': run_result.rows_collected,
                        'data_period': run_result.data_period,
                        'elapsed_seconds': round(elapsed_s, 1),
                        'triggered_by': triggered_by,
                        'is_new_data': run_result.is_new_data,
                    }
                    if run_result.warnings:
                        details['warnings'] = run_result.warnings
                    if run_result.error_message:
                        details['error'] = run_result.error_message

                    # Enrich with KG context (best-effort)
                    if run_result.success:
                        try:
                            from src.knowledge_graph.kg_enricher import KGEnricher
                            enricher = KGEnricher()
                            enrichment = enricher.enrich_collection_event(
                                collector_name, run_result.rows_collected,
                                run_result.data_period
                            )
                            if enrichment:
                                details['kg_enrichment'] = enrichment
                                if enrichment.get('enriched_summary'):
                                    summary += f" | KG: {enrichment['enriched_summary']}"
                        except Exception as e:
                            logger.debug(f"KG enrichment skipped for {collector_name}: {e}")

                    # Recompute seasonal norms after relevant collectors (best-effort)
                    if run_result.success and run_result.is_new_data:
                        try:
                            from src.knowledge_graph.seasonal_calculator import SeasonalCalculator
                            calc = SeasonalCalculator()
                            if collector_name == 'cftc_cot':
                                calc_result = calc.compute_cftc_seasonal_norms()
                            elif collector_name == 'usda_nass_crop_progress':
                                calc_result = calc.compute_crop_condition_norms()
                            else:
                                calc_result = None
                            if calc_result and calc_result.success:
                                details['seasonal_calc'] = {
                                    'calculator': calc_result.calculator,
                                    'commodities': calc_result.commodities_computed,
                                    'written': calc_result.contexts_written,
                                    'updated': calc_result.contexts_updated,
                                }
                        except Exception as e:
                            logger.debug(f"Seasonal calc skipped for {collector_name}: {e}")

                    self._log_event(conn, event_type, collector_name,
                                     summary, details, priority)
            except Exception as e:
                logger.error(f"Failed to log event for {collector_name}: {e}")

        # Log summary
        elapsed = (run_result.finished_at - run_result.started_at).total_seconds()
        if run_result.success:
            logger.info(
                f"{collector_name}: {run_result.status} — "
                f"{run_result.rows_collected} rows in {elapsed:.1f}s"
            )
        else:
            logger.warning(
                f"{collector_name}: {run_result.status} — {run_result.error_message}"
            )

        return run_result

    def run_with_retry(
        self,
        collector_name: str,
        max_retries: int = 3,
        retry_delay_minutes: int = 15,
        triggered_by: str = 'scheduler',
        commodities: List[str] = None,
        **collector_kwargs
    ) -> CollectorRunResult:
        """
        Run a collector with retry logic.

        Args:
            collector_name: Registry key
            max_retries: Maximum retry attempts
            retry_delay_minutes: Delay between retries
            triggered_by: Who triggered this run
            commodities: Commodity list override
            **collector_kwargs: Passed to collector

        Returns:
            CollectorRunResult from the last attempt
        """
        for attempt in range(1, max_retries + 1):
            logger.info(f"Running {collector_name} (attempt {attempt}/{max_retries})")

            result = self.run_collector(
                collector_name,
                triggered_by=triggered_by,
                commodities=commodities,
                **collector_kwargs
            )

            if result.success:
                return result

            if attempt < max_retries:
                delay = retry_delay_minutes * 60
                logger.info(
                    f"{collector_name} failed, retrying in {retry_delay_minutes} minutes..."
                )
                time.sleep(delay)

        return result
