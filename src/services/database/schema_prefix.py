"""Schema-name helper for sandbox/production isolation (Felipe onboarding, per user-guide §8).

New code targets a medallion layer via `sch('bronze')` instead of hardcoding `'bronze'`. In
production the prefix is empty, so `sch('bronze') == 'bronze'`. In Felipe's sandbox,
`RLC_SANDBOX_PREFIX=sandbox_` redirects writes to `sandbox_bronze` — the SAME code runs in both, so
nothing is retyped on promotion ("promote code, not data"). Existing production code is unaffected;
this is opt-in for new work.
"""
import os


def sch(layer: str) -> str:
    """Schema name for a medallion layer, honoring RLC_SANDBOX_PREFIX.

    Production (no prefix): sch('bronze') -> 'bronze'.
    Felipe sandbox (RLC_SANDBOX_PREFIX='sandbox_'): sch('bronze') -> 'sandbox_bronze'.
    """
    return os.getenv("RLC_SANDBOX_PREFIX", "") + layer


def qualified(layer: str, table: str) -> str:
    """Schema-qualified table name, prefix-aware. e.g. qualified('bronze','fas_psd')."""
    return f"{sch(layer)}.{table}"
