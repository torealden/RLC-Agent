"""
kg_callable invoker — validates inputs against signature, executes the backing
implementation, logs every call to core.kg_callable_invocation, and returns
the result with citations (source_context_id, source_note).

This is the single entry point used by the MCP tool + any internal callers.
"""

from __future__ import annotations

import importlib
import inspect
import json
import os
import time
from pathlib import Path
from typing import Any, Dict, Optional

import psycopg2
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent.parent / '.env')


def _connect():
    return psycopg2.connect(
        host=os.environ['RLC_PG_HOST'],
        port=os.environ.get('RLC_PG_PORT', 5432),
        database=os.environ.get('RLC_PG_DATABASE', 'rlc_commodities'),
        user=os.environ['RLC_PG_USER'],
        password=os.environ['RLC_PG_PASSWORD'],
        sslmode='require',
    )


class CallableNotFound(Exception):
    pass


class InputValidationError(Exception):
    pass


class InvocationError(Exception):
    pass


def _validate_inputs(inputs: Dict[str, Any], signature: Dict[str, Any]) -> list:
    """Return list of warnings; raise InputValidationError for hard failures."""
    warnings = []
    spec = signature.get('inputs', {})

    for name, rules in spec.items():
        required = rules.get('required', False)
        if name not in inputs or inputs[name] is None:
            if required:
                raise InputValidationError(f"Missing required input: {name}")
            continue

        val = inputs[name]

        # Enum check
        if rules.get('type') == 'enum':
            allowed = rules.get('values', [])
            if val not in allowed:
                raise InputValidationError(
                    f"Input {name}={val!r} not in allowed values {allowed}"
                )

        # Range check (float/int)
        if rules.get('type') in ('float', 'int') and 'range' in rules:
            lo, hi = rules['range']
            if not (lo <= val <= hi):
                warnings.append(
                    f"Input {name}={val} outside documented range [{lo}, {hi}]"
                )

    # Flag unexpected inputs (non-fatal)
    for name in inputs:
        if name not in spec:
            warnings.append(f"Unexpected input {name!r} (not in signature); forwarded anyway")

    return warnings


def _resolve_callable(implementation: str):
    """
    implementation = 'module.submodule.function_name'
    Returns the callable.
    """
    module_path, _, func_name = implementation.rpartition('.')
    if not module_path:
        raise InvocationError(f"Bad implementation string: {implementation!r}")
    module = importlib.import_module(module_path)
    try:
        return getattr(module, func_name)
    except AttributeError:
        raise InvocationError(f"{implementation}: function {func_name!r} not found in {module_path}")


def _get_citations(cur, callable_id: int) -> Dict[str, Any]:
    """Build a citations bundle from kg_callable.source_context_id + provenance."""
    cur.execute("""
        SELECT c.source_context_id, c.source_note, c.confidence,
               ctx.context_type, ctx.context_key, n.node_key, n.label AS node_label
        FROM core.kg_callable c
        JOIN core.kg_node n ON n.id = c.node_id
        LEFT JOIN core.kg_context ctx ON ctx.id = c.source_context_id
        WHERE c.id = %s
    """, (callable_id,))
    row = cur.fetchone()
    if not row:
        return {}
    return {
        'parent_node': {'node_key': row[5], 'label': row[6]},
        'source_context': {
            'id': row[0], 'type': row[3], 'key': row[4],
        } if row[0] else None,
        'source_note': row[1],
        'callable_confidence': float(row[2]) if row[2] is not None else None,
    }


def invoke(
    callable_key: str,
    inputs: Dict[str, Any],
    mode: str = 'scenario',
    invoked_by: str = 'mcp',
) -> Dict[str, Any]:
    """
    Execute a kg_callable.

    Parameters
    ----------
    callable_key : unique key from core.kg_callable
    inputs       : dict of input values (validated against signature)
    mode         : 'scenario' (default) or 'self_exploration'
    invoked_by   : free-form tag for the invocation log

    Returns
    -------
    { 'output': <whatever the callable returned>,
      'warnings': [...],
      'citations': {...},
      'duration_ms': int,
      'callable_key': str,
      'mode': str }

    Raises CallableNotFound / InputValidationError / InvocationError.
    """
    if mode not in ('scenario', 'self_exploration'):
        raise ValueError(f"mode must be 'scenario' or 'self_exploration', got {mode!r}")

    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT id, signature, implementation, defaults, self_exploration, status
            FROM core.kg_callable
            WHERE callable_key = %s
        """, (callable_key,))
        row = cur.fetchone()
        if not row:
            raise CallableNotFound(f"callable_key={callable_key!r} not registered in kg_callable")

        callable_id, signature, implementation, defaults, self_exploration, status = row
        if status == 'retired':
            raise InvocationError(f"callable {callable_key} is retired")

        # Merge defaults first, then caller inputs override
        merged_inputs: Dict[str, Any] = {}
        if defaults:
            merged_inputs.update(defaults)
        merged_inputs.update(inputs)

        # Pick entry point: self_exploration specifies an alt function name
        if mode == 'self_exploration':
            if not self_exploration:
                raise InvocationError(
                    f"{callable_key} does not declare a self_exploration block"
                )
            alt_func = self_exploration.get('function', 'self_explore')
            module_path, _, _ = implementation.rpartition('.')
            entry_impl = f"{module_path}.{alt_func}"
        else:
            _validate_inputs(merged_inputs, signature)
            entry_impl = implementation

        func = _resolve_callable(entry_impl)
        warnings = _validate_inputs(merged_inputs, signature) if mode == 'scenario' else []

        # Filter merged_inputs to kwargs the target function actually accepts.
        # self_explore() and run() have different signatures; without this, defaults
        # added for run() (e.g. soil_moisture_pct=None) crash self_explore().
        sig = inspect.signature(func)
        accepted = set(sig.parameters.keys())
        call_kwargs = {k: v for k, v in merged_inputs.items() if k in accepted}

        start = time.time()
        try:
            output = func(**call_kwargs)
            err = None
        except Exception as e:
            output = None
            err = f"{type(e).__name__}: {e}"
        duration_ms = int((time.time() - start) * 1000)

        citations = _get_citations(cur, callable_id)

        # Log invocation
        cur.execute("""
            INSERT INTO core.kg_callable_invocation
                (callable_id, invoked_by, mode, inputs, output, warnings, error_message, duration_ms, citations)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            callable_id, invoked_by, mode,
            json.dumps(merged_inputs, default=str),
            json.dumps(output, default=str) if output is not None else None,
            json.dumps(warnings) if warnings else None,
            err,
            duration_ms,
            json.dumps(citations, default=str),
        ))
        conn.commit()

        if err:
            raise InvocationError(err)

        return {
            'callable_key': callable_key,
            'mode': mode,
            'output': output,
            'warnings': warnings,
            'citations': citations,
            'duration_ms': duration_ms,
        }
    finally:
        conn.close()


if __name__ == '__main__':
    # End-to-end smoke test
    result = invoke(
        'weather_adjusted_yield',
        inputs={
            'commodity': 'corn',
            'region': 'us.corn_belt',
            'growth_stage': 'pollination',
            'current_yield_bpa': 183.0,
            'forecast_rain_in_30d': 1.5,
            'forecast_temp_f_avg_30d': 92.0,
            'forecast_month': 7,
            'soil_moisture_pct': 32,
        },
        mode='scenario',
        invoked_by='test',
    )
    print("=== Scenario invocation ===")
    print(json.dumps(result, indent=2, default=str))

    print("\n=== Self-exploration invocation ===")
    result2 = invoke(
        'weather_adjusted_yield',
        inputs={
            'commodity': 'corn',
            'region': 'us.corn_belt',
            'growth_stage': 'pollination',
            'current_yield_bpa': 183.0,
            'forecast_month': 7,
        },
        mode='self_exploration',
        invoked_by='test',
    )
    exp = result2['output']
    print(f"rain_sensitivity = {exp['rain_sensitivity_bpa_per_inch']} bpa/in")
    print(f"temp_sensitivity = {exp['temp_sensitivity_bpa_per_deg_f']} bpa/degF")
    print(f"dry_breakpoint = {exp['dry_breakpoint_rain_in']} in")
    print(f"hot_breakpoint = {exp['hot_breakpoint_temp_f']} F")
    print(f"Citations: {json.dumps(result2['citations'], indent=2, default=str)}")
