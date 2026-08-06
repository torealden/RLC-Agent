"""IFVS-008 compliance gates — renderer-enforced, non-negotiable.

Every gate failure raises GateError (hard error, not a warning). The renderer
must call run_gates() on the final rendered artifacts and refuse to write any
output when the gate list is non-empty.
"""

from __future__ import annotations

import re
from typing import Dict, List, Optional

from src.reports.feedstock_report.report_config import (
    CITATION_WHITELIST, EXCHANGE_WHITELIST, LICENSED_LEVEL_SOURCES,
    LICENSED_LEVELS_OK, BANNED_OUTPUT_STRINGS,
)


class GateError(RuntimeError):
    def __init__(self, failures: List[str]):
        self.failures = failures
        super().__init__(
            f"IFVS-008 gate failure ({len(failures)}):\n  - " + "\n  - ".join(failures))


# Numeric-value patterns forbidden in the free-mode IFV section: currency,
# decimals, cents marks, per-unit quotes. Bare 1-2 digit ranks are allowed.
_IFV_NUMERIC_RE = re.compile(r'\$\s*\d|\d+\.\d+|¢|/lb|/gal|per\s+lb|per\s+gal', re.I)


def check_free_mode_ifv(ifv_section_text: str, free_mode: bool) -> List[str]:
    if not free_mode:
        return []
    m = _IFV_NUMERIC_RE.search(ifv_section_text)
    if m:
        return [f"free_mode IFV section contains a numeric value "
                f"(matched {m.group(0)!r}) — rank + direction arrows only"]
    return []


def check_citation_whitelist(rendered_sources: List[str]) -> List[str]:
    allowed = CITATION_WHITELIST | EXCHANGE_WHITELIST
    failures = []
    for s in sorted(set(rendered_sources)):
        if s and s not in allowed:
            failures.append(f"source string {s!r} not in citation whitelist "
                            f"{sorted(allowed)}")
    return failures


def check_banned_strings(artifacts: Dict[str, str]) -> List[str]:
    """artifacts: {artifact_name: full_text}. Case-sensitive except where the
    banned entry is lowercase (then case-insensitive)."""
    failures = []
    for name, text in artifacts.items():
        for banned in BANNED_OUTPUT_STRINGS:
            hay = text if not banned.islower() else text.lower()
            needle = banned if not banned.islower() else banned.lower()
            if needle in hay:
                failures.append(f"banned string {banned!r} appears in {name}")
    return failures


def check_licensed_levels(rows: List[Dict]) -> List[str]:
    """rows: [{'source': str, 'renders_level': bool, 'label': str}]. Argus/OPIS
    rows must render as w/w change or base-100 index until licensed_levels_ok."""
    if LICENSED_LEVELS_OK:
        return []
    failures = []
    for r in rows:
        if r.get('source') in LICENSED_LEVEL_SOURCES and r.get('renders_level'):
            failures.append(f"{r.get('label', '?')}: {r['source']} level rendered "
                            f"while licensed_levels_ok=False (w/w or index only)")
    return failures


def run_gates(*, free_mode: bool, ifv_section_text: str,
              rendered_sources: List[str], artifacts: Dict[str, str],
              level_rows: Optional[List[Dict]] = None) -> None:
    failures = []
    failures += check_free_mode_ifv(ifv_section_text, free_mode)
    failures += check_citation_whitelist(rendered_sources)
    failures += check_banned_strings(artifacts)
    failures += check_licensed_levels(level_rows or [])
    if failures:
        raise GateError(failures)
