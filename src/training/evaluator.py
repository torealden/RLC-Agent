"""
WASDE Report Evaluator

Auto-scores LLM-generated WASDE narratives against the source data.
Catches hallucinated numbers, missing sections, wrong MoM directions,
and factual errors before a human ever sees the report.

Scores:
    data_accuracy      — numbers in text match the DB data
    completeness       — all required sections are present
    delta_accuracy     — MoM change directions are correct
    no_hallucination   — no numbers appear that aren't in the source data
    overall            — weighted composite
"""

import logging
import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


@dataclass
class EvalResult:
    """Evaluation scores for a single narrative."""
    data_accuracy: float = 0.0       # 0-1
    completeness: float = 0.0        # 0-1
    delta_accuracy: float = 0.0      # 0-1
    no_hallucination: float = 0.0    # 0-1
    overall: float = 0.0             # weighted composite
    issues: List[str] = field(default_factory=list)

    # Weights for composite score
    _WEIGHTS = {
        'data_accuracy': 0.35,
        'completeness': 0.15,
        'delta_accuracy': 0.30,
        'no_hallucination': 0.20,
    }

    def compute_overall(self):
        self.overall = round(
            self.data_accuracy * self._WEIGHTS['data_accuracy']
            + self.completeness * self._WEIGHTS['completeness']
            + self.delta_accuracy * self._WEIGHTS['delta_accuracy']
            + self.no_hallucination * self._WEIGHTS['no_hallucination'],
            3,
        )


# Required section headers (case-insensitive partial match)
REQUIRED_SECTIONS = [
    'headline',
    'corn',
    'soybean',
    'wheat',
    'global',
    'market implications',
]


def evaluate(narrative: str, data_snapshot: Dict, phase: int = 4) -> EvalResult:
    """
    Score a WASDE narrative against its source data.

    Args:
        narrative: LLM-generated markdown text
        data_snapshot: The exact data dict passed to the LLM (from gather_data + compute_analysis)
        phase: Training phase (1-4). Lower phases have relaxed section requirements.

    Returns:
        EvalResult with per-dimension scores and issue list
    """
    result = EvalResult()

    if not narrative or not narrative.strip():
        result.issues.append("Empty narrative")
        result.compute_overall()
        return result

    # --- Completeness ---
    result.completeness = _score_completeness(narrative, phase, result.issues)

    # --- Data Accuracy & Delta Accuracy ---
    us_bs = data_snapshot.get('us_balance_sheets', {})
    if us_bs:
        result.data_accuracy = _score_data_accuracy(narrative, us_bs, result.issues)
        result.delta_accuracy = _score_delta_accuracy(narrative, us_bs, result.issues)
    else:
        result.data_accuracy = 0.5  # can't check without data
        result.delta_accuracy = 0.5
        result.issues.append("No US balance sheet in snapshot — cannot verify numbers")

    # --- Hallucination check ---
    result.no_hallucination = _score_hallucination(narrative, data_snapshot, result.issues)

    result.compute_overall()
    return result


# -------------------------------------------------------------------------
# Scoring Functions
# -------------------------------------------------------------------------

def _score_completeness(narrative: str, phase: int, issues: List[str]) -> float:
    """Check that required sections are present in the narrative."""
    text_lower = narrative.lower()

    if phase == 1:
        # Phase 1: just needs commodity mentions and some structure
        required = ['corn', 'soybean', 'wheat']
    elif phase == 2:
        required = ['corn', 'soybean', 'wheat', 'change']
    elif phase == 3:
        required = REQUIRED_SECTIONS[:5]  # headline through global
    else:
        required = REQUIRED_SECTIONS

    found = 0
    for section in required:
        if section.lower() in text_lower:
            found += 1
        else:
            issues.append(f"Missing section/topic: '{section}'")

    return round(found / len(required), 3) if required else 1.0


def _score_data_accuracy(
    narrative: str,
    us_balance_sheets: Dict,
    issues: List[str],
) -> float:
    """
    Check that key numbers mentioned in the narrative match the source data.

    Strategy: extract numbers from the narrative, match them against known
    balance sheet values. High match rate = high accuracy.
    """
    # Build a set of "known good" numbers from the balance sheet
    known_numbers = set()
    for commodity, periods in us_balance_sheets.items():
        for period_key in ('current', 'prior'):
            period = periods.get(period_key, {})
            for col in ('production', 'ending_stocks', 'exports', 'domestic_consumption',
                        'total_supply', 'feed_dom_consumption', 'fsi_consumption',
                        'crush', 'stocks_use_pct'):
                val = period.get(col)
                if val is not None:
                    try:
                        fval = float(val)
                        # Add the number and common rounded forms
                        known_numbers.add(round(fval, 0))
                        known_numbers.add(round(fval, 1))
                        if fval != 0:
                            known_numbers.add(round(fval / 1000, 1))  # MMT if in 000s
                    except (ValueError, TypeError):
                        pass

    # Also add deltas (current - prior)
    for commodity, periods in us_balance_sheets.items():
        cur = periods.get('current', {})
        pri = periods.get('prior', {})
        for col in ('production', 'ending_stocks', 'exports', 'domestic_consumption'):
            c = _safe_float(cur.get(col))
            p = _safe_float(pri.get(col))
            if c is not None and p is not None:
                diff = round(c - p, 0)
                known_numbers.add(diff)
                known_numbers.add(abs(diff))

    if not known_numbers:
        return 0.5  # can't verify

    # Extract numbers from the narrative
    narrative_numbers = _extract_numbers(narrative)
    if not narrative_numbers:
        issues.append("No numbers found in narrative")
        return 0.3  # narrative should contain numbers

    # Check what fraction of narrative numbers are in the known set
    matched = 0
    checked = 0
    for num in narrative_numbers:
        # Skip very small numbers (likely not balance sheet values)
        if abs(num) < 5:
            continue
        checked += 1
        if num in known_numbers or round(num, 0) in known_numbers:
            matched += 1
        else:
            # Check within 1% tolerance (rounding differences)
            close_match = any(
                abs(num - k) / max(abs(k), 1) < 0.01
                for k in known_numbers if k != 0
            )
            if close_match:
                matched += 1
            else:
                issues.append(f"Unverified number in narrative: {num}")

    if checked == 0:
        return 0.5

    return round(matched / checked, 3)


def _score_delta_accuracy(
    narrative: str,
    us_balance_sheets: Dict,
    issues: List[str],
) -> float:
    """
    Check that MoM change directions are correct.

    Looks for phrases like "raised production", "cut exports", "stocks increased"
    and verifies against actual current vs prior data.
    """
    # Compute actual deltas
    actual_deltas = {}  # (commodity, attribute) -> direction ('up', 'down', 'unchanged')
    for commodity, periods in us_balance_sheets.items():
        cur = periods.get('current', {})
        pri = periods.get('prior', {})
        for col in ('production', 'ending_stocks', 'exports', 'domestic_consumption'):
            c = _safe_float(cur.get(col))
            p = _safe_float(pri.get(col))
            if c is not None and p is not None:
                if c > p:
                    direction = 'up'
                elif c < p:
                    direction = 'down'
                else:
                    direction = 'unchanged'
                actual_deltas[(commodity, col)] = direction

    if not actual_deltas:
        return 0.5

    # Pattern-match directional language in the narrative
    text_lower = narrative.lower()

    UP_WORDS = {'raised', 'increased', 'higher', 'up', 'added', 'grew', 'expanded', 'lifted'}
    DOWN_WORDS = {'cut', 'lowered', 'reduced', 'decreased', 'down', 'fell', 'dropped', 'trimmed', 'shrank'}
    UNCHANGED_WORDS = {'unchanged', 'steady', 'flat', 'maintained'}

    ATTR_SYNONYMS = {
        'production': ['production', 'output', 'crop'],
        'ending_stocks': ['ending stocks', 'stocks', 'carryout', 'inventories', 'carry-out'],
        'exports': ['exports', 'export'],
        'domestic_consumption': ['domestic use', 'domestic consumption', 'total use', 'demand'],
    }

    checked = 0
    correct = 0

    for (commodity, attr), actual_dir in actual_deltas.items():
        # Find sentences that mention BOTH this commodity AND this attribute
        found_claim = False
        for synonym in ATTR_SYNONYMS.get(attr, [attr]):
            if found_claim:
                break
            # Search for attribute synonym near commodity name
            for syn_match in re.finditer(re.escape(synonym), text_lower):
                # Look in a tight window around the attribute mention
                start = max(0, syn_match.start() - 80)
                end = min(len(text_lower), syn_match.end() + 80)
                window = text_lower[start:end]

                # The commodity must also appear in this window
                if commodity.lower() not in window:
                    continue

                # Determine claimed direction from the immediate context
                claimed_dir = None
                for word in UP_WORDS:
                    if word in window:
                        claimed_dir = 'up'
                        break
                if claimed_dir is None:
                    for word in DOWN_WORDS:
                        if word in window:
                            claimed_dir = 'down'
                            break
                if claimed_dir is None:
                    for word in UNCHANGED_WORDS:
                        if word in window:
                            claimed_dir = 'unchanged'
                            break

                if claimed_dir is not None:
                    checked += 1
                    if claimed_dir == actual_dir:
                        correct += 1
                    else:
                        issues.append(
                            f"Delta direction wrong: {commodity} {attr} "
                            f"claimed '{claimed_dir}' but actual '{actual_dir}'"
                        )
                    found_claim = True
                    break  # one check per (commodity, attr)

    if checked == 0:
        # No directional claims found — neutral score
        return 0.5

    return round(correct / checked, 3)


def _score_hallucination(
    narrative: str,
    data_snapshot: Dict,
    issues: List[str],
) -> float:
    """
    Detect hallucinated numbers — numbers in the narrative that don't appear
    anywhere in the source data.

    A stricter version of data_accuracy focused on false positives.
    """
    # Build comprehensive set of all numbers in the data snapshot
    all_known = set()
    _collect_numbers_from_dict(data_snapshot, all_known)

    # Add computed deltas (current - prior) which the LLM may cite
    us_bs = data_snapshot.get('us_balance_sheets', {})
    for commodity, periods in us_bs.items():
        cur = periods.get('current', {})
        pri = periods.get('prior', {})
        for col in ('production', 'ending_stocks', 'exports', 'domestic_consumption',
                     'total_supply', 'feed_dom_consumption', 'fsi_consumption', 'crush'):
            c = _safe_float(cur.get(col))
            p = _safe_float(pri.get(col))
            if c is not None and p is not None:
                diff = c - p
                all_known.add(round(diff, 0))
                all_known.add(round(abs(diff), 0))

    # Also add some universal known values (years, percentages, etc.)
    import datetime
    current_year = datetime.datetime.now().year
    for y in range(current_year - 5, current_year + 3):
        all_known.add(float(y))

    narrative_numbers = _extract_numbers(narrative)

    if not narrative_numbers:
        return 1.0  # no numbers = no hallucinations

    # Also add plausible percentage values from stocks-to-use
    us_bs = data_snapshot.get('us_balance_sheets', {})
    for commodity, periods in us_bs.items():
        cur = periods.get('current', {})
        stu = _safe_float(cur.get('stocks_use_pct'))
        if stu is not None:
            all_known.add(round(stu, 1))
            all_known.add(round(stu, 0))
        # Compute STU if not stored
        es = _safe_float(cur.get('ending_stocks'))
        dc = _safe_float(cur.get('domestic_consumption'))
        ex = _safe_float(cur.get('exports'))
        if es and dc and ex and (dc + ex) > 0:
            computed_stu = round(es / (dc + ex) * 100, 1)
            all_known.add(computed_stu)
            all_known.add(round(computed_stu, 0))

    # Check large numbers (>100) that could be balance sheet figures
    suspicious = []
    for num in narrative_numbers:
        if abs(num) < 100:
            continue  # skip small numbers, percentages, years
        if num in all_known or round(num, 0) in all_known:
            continue
        # Tolerance check
        close = any(
            abs(num - k) / max(abs(k), 1) < 0.02
            for k in all_known if abs(k) > 50
        )
        if not close:
            suspicious.append(num)

    if suspicious:
        for s in suspicious[:5]:  # limit noise
            issues.append(f"Possible hallucinated number: {s}")
        # Score: penalize by fraction of suspicious numbers
        total_large = sum(1 for n in narrative_numbers if abs(n) > 100)
        if total_large > 0:
            return round(max(0, 1.0 - len(suspicious) / total_large), 3)

    return 1.0


# -------------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------------

def _extract_numbers(text: str) -> List[float]:
    """Extract all numeric values from text."""
    # Match integers and decimals, with optional comma separators
    pattern = r'(?<![a-zA-Z])(\d{1,3}(?:,\d{3})*(?:\.\d+)?|\d+(?:\.\d+)?)(?![a-zA-Z])'
    matches = re.findall(pattern, text)
    numbers = []
    for m in matches:
        try:
            numbers.append(float(m.replace(',', '')))
        except ValueError:
            pass
    return numbers


def _collect_numbers_from_dict(d, number_set: set, depth: int = 0):
    """Recursively collect all numeric values from a nested dict/list."""
    if depth > 10:
        return
    if isinstance(d, dict):
        for v in d.values():
            _collect_numbers_from_dict(v, number_set, depth + 1)
    elif isinstance(d, (list, tuple)):
        for item in d:
            _collect_numbers_from_dict(item, number_set, depth + 1)
    elif isinstance(d, (int, float)):
        number_set.add(float(d))
        number_set.add(round(float(d), 0))
        number_set.add(round(float(d), 1))
    elif isinstance(d, str):
        # Try to parse as number
        try:
            val = float(d.replace(',', ''))
            number_set.add(val)
            number_set.add(round(val, 0))
        except (ValueError, AttributeError):
            pass


def _safe_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None
