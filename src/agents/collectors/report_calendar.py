"""Report calendar — expectation-aware monthly/quarterly collection.

Instead of "did any new row appear?", a scheduled collector should know *which specific reference
period* a report on a given run date is supposed to contain, and verify THAT period landed. If it
didn't (NASS API lag on release day, etc.), mark the run 'incomplete' and retry/alert rather than
reporting silent success. This module is the shared source of that expectation.

Confirmed lag for Fats & Oils: the 2026-07-01 report carried May 2026 data (2-month lag).
"""
from __future__ import annotations
from dataclasses import dataclass
from datetime import date

# report_type -> (frequency, lag). monthly lag = months back; quarterly lag = months back to a
# date that lands in the expected (most-recently-published) quarter.
REPORT_LAG = {
    'fats_oils':     ('monthly', 2),
    'grain_crush':   ('monthly', 2),
    'peanut':        ('monthly', 2),
    'flour_milling': ('quarterly', 4),   # as of early July, latest published quarter is Q1 (ends Mar)
}

@dataclass(frozen=True)
class ExpectedPeriod:
    report_type: str
    period_type: str      # 'month' | 'quarter'
    year: int
    period: int           # month 1-12, or quarter 1-4
    label: str            # human: 'May 2026' | 'Q1 2026 (Jan-Mar)'

def _shift_month(y: int, m: int, back: int) -> tuple[int, int]:
    idx = (y * 12 + (m - 1)) - back
    return idx // 12, idx % 12 + 1

_QLABEL = {1: 'Jan-Mar', 2: 'Apr-Jun', 3: 'Jul-Sep', 4: 'Oct-Dec'}

def expected_period(report_type: str, run_date: date) -> ExpectedPeriod | None:
    """The reference period a report of this type, run on run_date, should contain."""
    cfg = REPORT_LAG.get(report_type)
    if not cfg:
        return None
    freq, lag = cfg
    y, m = _shift_month(run_date.year, run_date.month, lag)
    if freq == 'monthly':
        month_name = date(y, m, 1).strftime('%b')
        return ExpectedPeriod(report_type, 'month', y, m, f'{month_name} {y}')
    q = (m - 1) // 3 + 1
    return ExpectedPeriod(report_type, 'quarter', y, q, f'Q{q} {y} ({_QLABEL[q]})')


if __name__ == '__main__':
    # validate against the case we diagnosed + a few cadence points
    for rt, d, want in [
        ('fats_oils',     date(2026, 7, 1),  'May 2026'),
        ('fats_oils',     date(2026, 8, 1),  'Jun 2026'),
        ('grain_crush',   date(2026, 7, 1),  'May 2026'),
        ('flour_milling', date(2026, 7, 2),  'Q1 2026 (Jan-Mar)'),
        ('flour_milling', date(2026, 10, 1), 'Q2 2026 (Apr-Jun)'),
    ]:
        got = expected_period(rt, d)
        ok = got and got.label == want
        print(f"  {'OK ' if ok else 'BAD'} {rt:13} run {d} -> {got.label if got else None} (want {want})")
