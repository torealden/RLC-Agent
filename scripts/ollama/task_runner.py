"""
Ollama Task Runner — Feeds structured tasks to the local LLM and captures output.

Reads task definitions from a YAML job file, sends each to Ollama,
validates the output, and writes results to the output directory.

Usage:
    python scripts/ollama/task_runner.py jobs/gold_views.yaml
    python scripts/ollama/task_runner.py jobs/hs_codes.yaml --dry-run
    python scripts/ollama/task_runner.py jobs/gold_views.yaml --task 3
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import requests
import yaml

# ── Configuration ──────────────────────────────────────────────────
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434")
DEFAULT_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5-coder:14b-instruct")
OUTPUT_DIR = Path("scripts/ollama/output")
LOG_DIR = Path("scripts/ollama/logs")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "task_runner.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("task_runner")


os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)


def ensure_dirs():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)


def check_ollama():
    """Verify Ollama is running and the model is available."""
    try:
        r = requests.get(f"{OLLAMA_URL}/api/tags", timeout=5)
        r.raise_for_status()
        models = [m["name"] for m in r.json().get("models", [])]
        return models
    except Exception as e:
        logger.error(f"Cannot reach Ollama at {OLLAMA_URL}: {e}")
        return None


def ask_ollama(prompt, model=DEFAULT_MODEL, temperature=0.1, timeout=900):
    """Send a prompt to Ollama and return the response text."""
    payload = {
        "model": model,
        "prompt": prompt,
        "stream": False,
        "options": {
            "temperature": temperature,
            "num_predict": 4096,
        },
    }

    try:
        r = requests.post(
            f"{OLLAMA_URL}/api/generate",
            json=payload,
            timeout=timeout,
        )
        r.raise_for_status()
        data = r.json()
        return {
            "response": data.get("response", ""),
            "total_duration_ms": data.get("total_duration", 0) / 1e6,
            "eval_count": data.get("eval_count", 0),
        }
    except requests.exceptions.Timeout:
        logger.error(f"Ollama request timed out after {timeout}s")
        return None
    except Exception as e:
        logger.error(f"Ollama request failed: {e}")
        return None


def validate_output(response_text, validation):
    """Run basic validation checks on model output."""
    if not validation:
        return True, []

    errors = []
    text = response_text.strip()

    # Check required strings are present
    for must_contain in validation.get("must_contain", []):
        if must_contain.lower() not in text.lower():
            errors.append(f"Missing required string: '{must_contain}'")

    # Check forbidden strings are absent
    for must_not_contain in validation.get("must_not_contain", []):
        if must_not_contain.lower() in text.lower():
            errors.append(f"Contains forbidden string: '{must_not_contain}'")

    # Check output starts with expected prefix
    starts_with = validation.get("starts_with")
    if starts_with and not text.startswith(starts_with):
        errors.append(f"Does not start with: '{starts_with}'")

    # Check minimum length
    min_length = validation.get("min_length", 0)
    if len(text) < min_length:
        errors.append(f"Output too short: {len(text)} < {min_length}")

    return len(errors) == 0, errors


def load_job_file(path):
    """Load and parse a YAML job definition file."""
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def build_prompt(task, job_context):
    """Assemble the full prompt from task definition and job context."""
    parts = []

    # System-level instructions from the job
    if job_context.get("system_prompt"):
        parts.append(job_context["system_prompt"])

    # Template content if referenced
    if task.get("template_file"):
        tpl_path = Path(task["template_file"])
        if tpl_path.exists():
            parts.append(f"--- TEMPLATE FILE: {tpl_path.name} ---")
            parts.append(tpl_path.read_text(encoding="utf-8"))
            parts.append("--- END TEMPLATE ---")
        else:
            logger.warning(f"Template file not found: {tpl_path}")

    # The task-specific prompt
    parts.append(task["prompt"])

    # Output format instructions
    if task.get("output_format"):
        parts.append(f"\nOutput format: {task['output_format']}")

    return "\n\n".join(parts)


def run_task(task, job_context, task_index, dry_run=False):
    """Execute a single task."""
    task_name = task.get("name", f"task_{task_index:03d}")
    output_file = task.get("output_file", f"{task_name}.sql")
    output_path = OUTPUT_DIR / output_file

    logger.info(f"{'[DRY RUN] ' if dry_run else ''}Task {task_index}: {task_name}")

    prompt = build_prompt(task, job_context)

    if dry_run:
        logger.info(f"  Prompt length: {len(prompt)} chars")
        logger.info(f"  Output would be: {output_path}")
        print(f"\n{'='*60}")
        print(f"TASK {task_index}: {task_name}")
        print(f"{'='*60}")
        print(prompt[:500] + ("..." if len(prompt) > 500 else ""))
        return True

    # Call Ollama
    model = task.get("model", job_context.get("model", DEFAULT_MODEL))
    temperature = task.get("temperature", job_context.get("temperature", 0.1))

    logger.info(f"  Sending to {model} (temp={temperature})...")
    start = time.time()
    result = ask_ollama(prompt, model=model, temperature=temperature)
    elapsed = time.time() - start

    if result is None:
        logger.error(f"  FAILED: No response from Ollama")
        return False

    response_text = result["response"]
    logger.info(
        f"  Got {len(response_text)} chars in {elapsed:.1f}s "
        f"({result['eval_count']} tokens)"
    )

    # Validate
    valid, errors = validate_output(response_text, task.get("validation"))
    if not valid:
        logger.warning(f"  VALIDATION FAILED:")
        for err in errors:
            logger.warning(f"    - {err}")
        # Still save but mark as failed
        output_path = output_path.with_suffix(".FAILED" + output_path.suffix)

    # Strip markdown code fences if present
    clean = response_text.strip()
    if clean.startswith("```"):
        # Remove first line (```sql or ```) and last line (```)
        lines = clean.split("\n")
        if lines[-1].strip() == "```":
            lines = lines[1:-1]
        else:
            lines = lines[1:]
        clean = "\n".join(lines)

    # Save output
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(clean, encoding="utf-8")
    logger.info(f"  Saved to {output_path}")

    # Log result
    log_entry = {
        "task": task_name,
        "model": model,
        "timestamp": datetime.now().isoformat(),
        "elapsed_s": round(elapsed, 1),
        "tokens": result["eval_count"],
        "output_file": str(output_path),
        "valid": valid,
        "errors": errors,
    }
    log_path = LOG_DIR / "results.jsonl"
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(log_entry) + "\n")

    return valid


def main():
    parser = argparse.ArgumentParser(description="Ollama Task Runner")
    parser.add_argument("job_file", help="Path to YAML job definition")
    parser.add_argument("--dry-run", action="store_true", help="Show prompts without running")
    parser.add_argument("--task", type=int, help="Run only this task number (1-based)")
    parser.add_argument("--model", help="Override model for all tasks")
    args = parser.parse_args()

    ensure_dirs()

    # Check Ollama
    if not args.dry_run:
        models = check_ollama()
        if models is None:
            sys.exit(1)
        logger.info(f"Ollama available. Models: {', '.join(models)}")

    # Load job file
    job = load_job_file(args.job_file)
    job_context = {
        "system_prompt": job.get("system_prompt", ""),
        "model": args.model or job.get("model", DEFAULT_MODEL),
        "temperature": job.get("temperature", 0.1),
    }

    tasks = job.get("tasks", [])
    logger.info(f"Loaded {len(tasks)} tasks from {args.job_file}")

    # Run tasks
    results = {"passed": 0, "failed": 0, "skipped": 0}

    for i, task in enumerate(tasks, 1):
        if args.task and i != args.task:
            results["skipped"] += 1
            continue

        success = run_task(task, job_context, i, dry_run=args.dry_run)
        if success:
            results["passed"] += 1
        else:
            results["failed"] += 1

    # Summary
    logger.info(
        f"\nDone: {results['passed']} passed, {results['failed']} failed, "
        f"{results['skipped']} skipped"
    )


if __name__ == "__main__":
    main()
