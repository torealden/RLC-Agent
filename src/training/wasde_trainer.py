"""
WASDE Report Trainer

Orchestrates iterative training of the LLM to produce publishable WASDE
analysis reports. Each iteration: gather data → prompt LLM → auto-score →
save for human review. Felipe reviews, adds feedback, and the next iteration
incorporates corrections.

Training Phases:
    Phase 1 — Data Accuracy:  Balance sheet numbers only. LLM summarizes facts.
    Phase 2 — Delta Analysis: Add MoM changes. LLM must get directions right.
    Phase 3 — Context:        Add KG context + global data. LLM draws inferences.
    Phase 4 — Full Report:    Complete publishable WASDE analysis.

Usage:
    from src.training.wasde_trainer import WASDeTrainer

    trainer = WASDeTrainer(phase=1)
    result = trainer.run_iteration()
    print(result.narrative[:200])
    print(f"Score: {result.eval_result.overall}")

    # Felipe reviews and adds feedback:
    trainer.add_feedback(result.iteration_id, human_rating=3,
                         feedback="Corn ending stocks number is wrong")

    # Next iteration incorporates feedback:
    result2 = trainer.run_iteration()

CLI:
    python -m src.training.wasde_trainer --phase 1
    python -m src.training.wasde_trainer --phase 2 --iterations 3
    python -m src.training.wasde_trainer --review          # show pending reviews
    python -m src.training.wasde_trainer --progress        # show training progress
"""

import argparse
import json
import logging
import sys
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

from src.training.evaluator import EvalResult, evaluate

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


# -------------------------------------------------------------------------
# Phase-specific prompt configurations
# -------------------------------------------------------------------------

PHASE_CONFIG = {
    1: {
        'name': 'Data Accuracy',
        'description': 'Summarize balance sheet numbers accurately. No inferences.',
        'system_prompt': (
            "You are a junior commodity analyst writing your first WASDE summary. "
            "Your ONLY job is to accurately report the numbers from the data provided. "
            "Do NOT make inferences, predictions, or market commentary. "
            "For each commodity (corn, soybeans, wheat), state the current balance sheet "
            "figures: production, ending stocks, exports, and stocks-to-use ratio. "
            "Use exact numbers. If a number is in 1000 MT, say so."
        ),
        'user_template': (
            "## USDA WASDE Data — {report_date}\n"
            "**Marketing Year:** {marketing_year}\n\n"
            "### US Balance Sheet (1000 MT)\n"
            "{balance_sheet_table}\n\n"
            "---\n"
            "Write a factual summary of the US balance sheet for each commodity. "
            "Include the exact numbers for production, ending stocks, exports, "
            "and stocks-to-use ratio. Keep it under 300 words."
        ),
        'max_tokens': 800,
        'include_deltas': False,
        'include_global': False,
        'include_kg': False,
    },
    2: {
        'name': 'Delta Analysis',
        'description': 'Accurately describe month-over-month changes.',
        'system_prompt': (
            "You are a commodity analyst writing a WASDE change summary. "
            "Focus on what CHANGED from last month. For each revision, state: "
            "(1) what changed, (2) by how much, (3) the direction (raised/cut). "
            "Use exact numbers. Do not speculate about causes — just report the changes."
        ),
        'user_template': (
            "## USDA WASDE Report — {report_date}\n"
            "**Marketing Year:** {marketing_year}\n\n"
            "### US Balance Sheet Changes (1000 MT)\n"
            "{balance_sheet_table}\n\n"
            "### Month-over-Month Revisions\n"
            "{delta_summary}\n\n"
            "---\n"
            "Write a structured analysis with these sections:\n"
            "1. **Headline Changes** — The 2-3 most significant revisions\n"
            "2. **US Corn** — What changed and by how much\n"
            "3. **US Soybeans** — What changed and by how much\n"
            "4. **US Wheat** — What changed and by how much\n\n"
            "Keep it under 400 words. Be precise with numbers and directions."
        ),
        'max_tokens': 1200,
        'include_deltas': True,
        'include_global': False,
        'include_kg': False,
    },
    3: {
        'name': 'Context & Inference',
        'description': 'Draw inferences using KG context and global data.',
        'system_prompt': (
            "You are a senior grain and oilseed analyst at a commodity trading firm. "
            "You write factual, concise market commentary. Use the Knowledge Graph context "
            "to inform your analysis — it contains expert rules, seasonal patterns, and "
            "cross-market relationships. Draw connections between US changes and global factors. "
            "Use exact numbers. Mark any inference clearly as such."
        ),
        'user_template': (
            "## USDA WASDE Report Analysis — {report_date}\n"
            "**Marketing Year:** {marketing_year}\n\n"
            "### US Balance Sheet Changes (1000 MT)\n"
            "{balance_sheet_table}\n\n"
            "### Month-over-Month Revisions\n"
            "{delta_summary}\n\n"
            "### Global Context\n"
            "{global_context}\n\n"
            "### Analyst Knowledge Base\n"
            "{kg_context}\n\n"
            "---\n"
            "Write a structured analysis:\n"
            "1. **Headline Changes** — 2-3 most significant revisions\n"
            "2. **US Corn** — Changes, implications, cross-market links\n"
            "3. **US Soybeans** — Changes, crush/export dynamics\n"
            "4. **US Wheat** — Changes, global competitiveness\n"
            "5. **Global Context** — Key changes in Brazil, Argentina, China\n\n"
            "Keep under 500 words. Use KG context to add depth."
        ),
        'max_tokens': 1500,
        'include_deltas': True,
        'include_global': True,
        'include_kg': True,
    },
    4: {
        'name': 'Full Report',
        'description': 'Publication-quality WASDE analysis.',
        'system_prompt': (
            "You are a senior grain and oilseed analyst at a commodity trading firm. "
            "You write factual, concise market commentary in a professional tone. "
            "Your output uses markdown formatting. You focus on what changed, why it "
            "matters, and what the implications are for the market. Never speculate "
            "beyond what the data supports. Use exact numbers from the data provided."
        ),
        'user_template': (
            "## USDA WASDE Report Analysis — {report_date}\n"
            "**Marketing Year:** {marketing_year}\n\n"
            "### US Balance Sheet Changes (1000 MT)\n"
            "{balance_sheet_table}\n\n"
            "### Month-over-Month Revisions\n"
            "{delta_summary}\n\n"
            "### Global Context\n"
            "{global_context}\n\n"
            "### Knowledge Graph Context\n"
            "{kg_context}\n\n"
            "### Special Considerations\n"
            "August WASDE (methodology shift to survey-based yield): {is_august_wasde}\n\n"
            "{feedback_section}"
            "---\n"
            "Write a structured analysis with these sections:\n"
            "1. **Headline Changes** — The 2-3 most significant revisions\n"
            "2. **US Corn** — Balance sheet changes, stocks-to-use, implications\n"
            "3. **US Soybeans** — Balance sheet changes, crush/export dynamics\n"
            "4. **US Wheat** — Balance sheet changes, global competitiveness\n"
            "5. **Global Context** — Key changes in Brazil, Argentina, China\n"
            "6. **Market Implications** — Bulleted takeaways for traders\n\n"
            "Keep the analysis under 600 words. Be specific with numbers."
        ),
        'max_tokens': 1800,
        'include_deltas': True,
        'include_global': True,
        'include_kg': True,
    },
}


@dataclass
class IterationResult:
    """Result of a single training iteration."""
    iteration_id: Optional[int] = None
    run_id: Optional[int] = None
    iteration_num: int = 0
    narrative: str = ''
    eval_result: Optional[EvalResult] = None
    data_snapshot: Optional[Dict] = None
    model_id: str = ''
    tokens_in: int = 0
    tokens_out: int = 0
    cost_usd: float = 0.0
    latency_ms: int = 0
    error: Optional[str] = None


class WASDeTrainer:
    """
    Iterative WASDE report trainer.

    Each run_iteration() call:
    1. Gathers current WASDE data from DB
    2. Builds phase-appropriate prompt (incorporating prior feedback)
    3. Calls LLM
    4. Auto-evaluates the output
    5. Saves iteration to DB for human review
    """

    def __init__(self, phase: int = 1, run_id: int = None, model_override: str = None):
        """
        Args:
            phase: Training phase 1-4
            run_id: Resume an existing run (or None to create new)
            model_override: Force a specific model (e.g., 'claude-sonnet-4-20250514')
        """
        if phase not in PHASE_CONFIG:
            raise ValueError(f"Invalid phase {phase}. Must be 1-4.")

        self.phase = phase
        self.phase_config = PHASE_CONFIG[phase]
        self.model_override = model_override
        self._run_id = run_id
        self._iteration_count = 0

    def _get_connection(self):
        from src.services.database.db_config import get_connection
        return get_connection()

    def _ensure_run(self):
        """Create or resume a training run."""
        if self._run_id is not None:
            return

        with self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO core.training_runs (report_type, phase, config)
                VALUES ('wasde', %s, %s)
                RETURNING run_id
            """, (self.phase, json.dumps({
                'phase_name': self.phase_config['name'],
                'model_override': self.model_override,
            })))
            self._run_id = cur.fetchone()['run_id']
            conn.commit()
            logger.info("Created training run %d (phase %d: %s)",
                        self._run_id, self.phase, self.phase_config['name'])

    def run_iteration(self) -> IterationResult:
        """
        Execute one training iteration.

        Returns:
            IterationResult with narrative, scores, and metadata
        """
        self._ensure_run()
        self._iteration_count += 1
        result = IterationResult(run_id=self._run_id, iteration_num=self._iteration_count)

        try:
            # 1. Gather data
            data, analysis = self._gather_data()
            result.data_snapshot = {**data, **analysis}

            # 2. Build prompt (with any prior feedback)
            system_prompt, user_prompt = self._build_prompt(analysis)

            # 3. Call LLM
            llm_response = self._call_llm(system_prompt, user_prompt)
            result.narrative = llm_response.text
            result.model_id = llm_response.model_id
            result.tokens_in = llm_response.tokens_in
            result.tokens_out = llm_response.tokens_out
            result.latency_ms = llm_response.latency_ms

            # Compute cost
            from src.services.llm.model_router import ModelRouter
            router = ModelRouter()
            model_cfg = router._build_config(llm_response.model_id)
            result.cost_usd = round(
                (llm_response.tokens_in / 1000) * model_cfg.cost_per_1k_in
                + (llm_response.tokens_out / 1000) * model_cfg.cost_per_1k_out,
                6,
            )

            # 4. Auto-evaluate
            result.eval_result = evaluate(result.narrative, result.data_snapshot, self.phase)

            # 5. Save to DB
            result.iteration_id = self._save_iteration(result)

            logger.info(
                "Iteration %d complete — overall=%.3f, accuracy=%.3f, delta=%.3f, "
                "hallucination=%.3f (%d issues)",
                self._iteration_count,
                result.eval_result.overall,
                result.eval_result.data_accuracy,
                result.eval_result.delta_accuracy,
                result.eval_result.no_hallucination,
                len(result.eval_result.issues),
            )

        except Exception as e:
            result.error = str(e)
            logger.error("Iteration %d failed: %s", self._iteration_count, e, exc_info=True)

        return result

    def add_feedback(
        self,
        iteration_id: int,
        human_rating: int,
        feedback: str = '',
        approved: bool = False,
        reviewed_by: str = 'felipe',
    ):
        """
        Record human review for an iteration.

        Args:
            iteration_id: The iteration to review
            human_rating: 1-5 scale (5 = publishable)
            feedback: Free-form corrections
            approved: True if report is ready to publish
            reviewed_by: Reviewer name
        """
        with self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                UPDATE core.training_iterations
                SET human_rating = %s,
                    human_feedback = %s,
                    human_approved = %s,
                    reviewed_at = NOW(),
                    reviewed_by = %s
                WHERE iteration_id = %s
            """, (human_rating, feedback, approved, reviewed_by, iteration_id))
            conn.commit()
            logger.info("Recorded feedback for iteration %d: rating=%d, approved=%s",
                        iteration_id, human_rating, approved)

    def add_structured_feedback(
        self,
        iteration_id: int,
        category: str,
        description: str,
        correction: str = '',
        severity: int = 2,
    ):
        """
        Add structured feedback for prompt refinement.

        Categories: wrong_number, missing_section, hallucination, tone, inference
        """
        with self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO core.training_feedback
                    (iteration_id, category, severity, description, correction)
                VALUES (%s, %s, %s, %s, %s)
            """, (iteration_id, category, severity, description, correction))
            conn.commit()

    def get_prior_feedback(self) -> List[Dict]:
        """Get all feedback from the current run for prompt injection."""
        if self._run_id is None:
            return []

        with self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT ti.iteration_num, ti.human_feedback, ti.human_rating,
                       tf.category, tf.description, tf.correction
                FROM core.training_iterations ti
                LEFT JOIN core.training_feedback tf ON tf.iteration_id = ti.iteration_id
                WHERE ti.run_id = %s
                  AND (ti.human_feedback IS NOT NULL OR tf.feedback_id IS NOT NULL)
                ORDER BY ti.iteration_num
            """, (self._run_id,))
            return [dict(row) for row in cur.fetchall()]

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _gather_data(self):
        """Use the existing WASDE template to gather data."""
        from src.analysis.templates.wasde_template import WASDeAnalysisTemplate

        template = WASDeAnalysisTemplate()
        data = template.gather_data()
        analysis = template.compute_analysis(data)

        # Add KG context if phase requires it
        if self.phase_config['include_kg']:
            kg_context = template.get_kg_context()
            kg_text = self._format_kg_context(kg_context)
            analysis['kg_context'] = kg_text
        else:
            analysis['kg_context'] = ''

        # Strip global context if not needed
        if not self.phase_config['include_global']:
            analysis['global_context'] = ''

        # Strip deltas if not needed
        if not self.phase_config['include_deltas']:
            analysis['delta_summary'] = ''

        return data, analysis

    def _build_prompt(self, analysis: Dict):
        """Build the phase-appropriate prompt, injecting prior feedback."""
        config = self.phase_config
        system_prompt = config['system_prompt']

        # Build feedback section for phase 4
        feedback_section = ''
        if self.phase >= 3:
            prior = self.get_prior_feedback()
            if prior:
                lines = ["### Prior Reviewer Feedback\n"]
                for fb in prior:
                    if fb.get('human_feedback'):
                        lines.append(f"- Iteration {fb['iteration_num']} "
                                     f"(rating {fb.get('human_rating', '?')}/5): "
                                     f"{fb['human_feedback']}")
                    if fb.get('category'):
                        lines.append(f"  - [{fb['category']}] {fb['description']}")
                        if fb.get('correction'):
                            lines.append(f"    Correct: {fb['correction']}")
                lines.append("\n**Address all feedback items above.**\n\n")
                feedback_section = "\n".join(lines)

        # Render user template
        render_vars = {
            'report_date': analysis.get('report_date', 'Unknown'),
            'marketing_year': analysis.get('marketing_year', 'Unknown'),
            'balance_sheet_table': analysis.get('balance_sheet_table', ''),
            'delta_summary': analysis.get('delta_summary', ''),
            'global_context': analysis.get('global_context', ''),
            'kg_context': analysis.get('kg_context', ''),
            'is_august_wasde': analysis.get('is_august_wasde', 'No'),
            'feedback_section': feedback_section,
        }

        user_prompt = config['user_template'].format(**render_vars)

        return system_prompt, user_prompt

    def _call_llm(self, system_prompt: str, user_prompt: str):
        """Call LLM via the standard client."""
        from src.services.llm.llm_client import call_llm
        from src.services.llm.model_router import ModelRouter

        router = ModelRouter()

        if self.model_override:
            model = router._build_config(self.model_override)
        else:
            model = router.route(
                task_type='analysis',
                sensitivity=0,
                complexity='high' if self.phase >= 3 else 'medium',
            )

        return call_llm(
            model,
            system_prompt,
            user_prompt,
            max_tokens=self.phase_config['max_tokens'],
        )

    def _save_iteration(self, result: IterationResult) -> int:
        """Persist iteration to the database."""
        import hashlib

        prompt_hash = hashlib.sha256(
            f"{self.phase_config['system_prompt']}\n{result.narrative[:100]}".encode()
        ).hexdigest()

        ev = result.eval_result

        with self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO core.training_iterations (
                    run_id, iteration_num, prompt_hash, model_id, narrative,
                    tokens_in, tokens_out, cost_usd, latency_ms,
                    score_data_accuracy, score_completeness, score_delta_accuracy,
                    score_no_hallucination, score_overall, data_snapshot
                ) VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s
                ) RETURNING iteration_id
            """, (
                self._run_id, result.iteration_num, prompt_hash, result.model_id,
                result.narrative, result.tokens_in, result.tokens_out,
                result.cost_usd, result.latency_ms,
                ev.data_accuracy if ev else None,
                ev.completeness if ev else None,
                ev.delta_accuracy if ev else None,
                ev.no_hallucination if ev else None,
                ev.overall if ev else None,
                json.dumps(result.data_snapshot, default=str) if result.data_snapshot else None,
            ))
            iteration_id = cur.fetchone()['iteration_id']
            conn.commit()

        return iteration_id

    @staticmethod
    def _format_kg_context(kg_context: Dict) -> str:
        """Format KG context as compact markdown."""
        if not kg_context:
            return "No knowledge graph context available."

        lines = []
        for node_key, enriched in kg_context.items():
            node = enriched.get('node', {})
            label = node.get('label', node_key)
            summary = enriched.get('summary', {})
            lines.append(f"**{label}** ({summary.get('context_count', 0)} contexts)")

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


# -------------------------------------------------------------------------
# CLI helpers
# -------------------------------------------------------------------------

def show_progress():
    """Display training progress from the database."""
    from src.services.database.db_config import get_connection

    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM core.training_progress ORDER BY started_at DESC LIMIT 10")
        rows = cur.fetchall()

    if not rows:
        print("No training runs found.")
        return

    print(f"\n{'Run':>4} {'Phase':>5} {'Type':>8} {'Iters':>5} {'Approved':>8} "
          f"{'Best':>6} {'Avg':>6} {'Rating':>6} {'Status':>10}")
    print("-" * 75)
    for r in rows:
        print(f"{r['run_id']:>4} {r['phase']:>5} {r['report_type']:>8} "
              f"{r['total_iterations']:>5} {r['approved_count']:>8} "
              f"{r['best_score'] or 0:>6.3f} {r['avg_score'] or 0:>6.3f} "
              f"{r['best_human_rating'] or '-':>6} {r['status']:>10}")


def show_pending_reviews():
    """Show iterations awaiting human review."""
    from src.services.database.db_config import get_connection

    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT ti.iteration_id, ti.run_id, ti.iteration_num,
                   tr.phase, ti.score_overall, ti.model_id,
                   LEFT(ti.narrative, 150) as preview,
                   ti.created_at
            FROM core.training_iterations ti
            JOIN core.training_runs tr ON tr.run_id = ti.run_id
            WHERE ti.human_rating IS NULL
            ORDER BY ti.created_at DESC
            LIMIT 20
        """)
        rows = cur.fetchall()

    if not rows:
        print("No pending reviews.")
        return

    for r in rows:
        print(f"\n--- Iteration {r['iteration_id']} (run {r['run_id']}, "
              f"phase {r['phase']}, #{r['iteration_num']}) ---")
        print(f"Model: {r['model_id']} | Score: {r['score_overall'] or 0:.3f} "
              f"| {r['created_at']}")
        print(f"Preview: {r['preview']}...")


def show_iteration(iteration_id: int):
    """Show full detail of a specific iteration."""
    from src.services.database.db_config import get_connection

    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT ti.*, tr.phase
            FROM core.training_iterations ti
            JOIN core.training_runs tr ON tr.run_id = ti.run_id
            WHERE ti.iteration_id = %s
        """, (iteration_id,))
        row = cur.fetchone()

    if not row:
        print(f"Iteration {iteration_id} not found.")
        return

    print(f"\n{'='*60}")
    print(f"Iteration {row['iteration_id']} | Run {row['run_id']} | "
          f"Phase {row['phase']} | #{row['iteration_num']}")
    print(f"Model: {row['model_id']} | Cost: ${row['cost_usd']:.4f} | "
          f"Latency: {row['latency_ms']}ms")
    print(f"{'='*60}")
    print(f"\nScores:")
    print(f"  Data Accuracy:    {row['score_data_accuracy'] or 0:.3f}")
    print(f"  Completeness:     {row['score_completeness'] or 0:.3f}")
    print(f"  Delta Accuracy:   {row['score_delta_accuracy'] or 0:.3f}")
    print(f"  No Hallucination: {row['score_no_hallucination'] or 0:.3f}")
    print(f"  Overall:          {row['score_overall'] or 0:.3f}")
    print(f"\n{'—'*60}")
    print(row['narrative'])
    print(f"{'—'*60}")

    if row['human_feedback']:
        print(f"\nHuman Rating: {row['human_rating']}/5")
        print(f"Feedback: {row['human_feedback']}")
        print(f"Approved: {row['human_approved']}")


# -------------------------------------------------------------------------
# Main
# -------------------------------------------------------------------------

def main():
    import os
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / '.env')

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
    )

    parser = argparse.ArgumentParser(description='WASDE Report Training')
    parser.add_argument('--phase', type=int, default=1, choices=[1, 2, 3, 4],
                        help='Training phase (1-4)')
    parser.add_argument('--iterations', type=int, default=1,
                        help='Number of iterations to run')
    parser.add_argument('--run-id', type=int, default=None,
                        help='Resume an existing run')
    parser.add_argument('--model', type=str, default=None,
                        help='Override model (e.g., claude-sonnet-4-20250514)')
    parser.add_argument('--progress', action='store_true',
                        help='Show training progress')
    parser.add_argument('--review', action='store_true',
                        help='Show pending reviews')
    parser.add_argument('--show', type=int, default=None,
                        help='Show full iteration detail by ID')
    parser.add_argument('--feedback', type=int, default=None,
                        help='Add feedback to iteration ID')
    parser.add_argument('--rating', type=int, default=None,
                        help='Human rating (1-5) for --feedback')
    parser.add_argument('--comment', type=str, default='',
                        help='Feedback comment for --feedback')
    args = parser.parse_args()

    if args.progress:
        show_progress()
        return

    if args.review:
        show_pending_reviews()
        return

    if args.show is not None:
        show_iteration(args.show)
        return

    if args.feedback is not None:
        if args.rating is None:
            print("Error: --rating is required with --feedback")
            sys.exit(1)
        trainer = WASDeTrainer(phase=args.phase)
        trainer.add_feedback(args.feedback, human_rating=args.rating, feedback=args.comment)
        print(f"Feedback recorded for iteration {args.feedback}")
        return

    # Run training iterations
    trainer = WASDeTrainer(
        phase=args.phase,
        run_id=args.run_id,
        model_override=args.model,
    )

    print(f"\nWASDE Training — Phase {args.phase}: {PHASE_CONFIG[args.phase]['name']}")
    print(f"Running {args.iterations} iteration(s)...\n")

    for i in range(args.iterations):
        print(f"--- Iteration {i + 1}/{args.iterations} ---")
        result = trainer.run_iteration()

        if result.error:
            print(f"ERROR: {result.error}")
            continue

        ev = result.eval_result
        print(f"Score: {ev.overall:.3f} (accuracy={ev.data_accuracy:.3f}, "
              f"delta={ev.delta_accuracy:.3f}, halluc={ev.no_hallucination:.3f})")
        print(f"Model: {result.model_id} | Cost: ${result.cost_usd:.4f}")

        if ev.issues:
            print(f"Issues ({len(ev.issues)}):")
            for issue in ev.issues[:5]:
                print(f"  - {issue}")

        print(f"\nSaved as iteration_id={result.iteration_id}")
        print(f"Preview: {result.narrative[:200]}...\n")

    print("Done. Use --review to see iterations awaiting human review.")
    print("Use --show <id> to read a full iteration.")
    print("Use --feedback <id> --rating <1-5> --comment '...' to add feedback.")


if __name__ == '__main__':
    main()
