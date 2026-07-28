"""Unit test for the FRED FX parser against a stored real-payload fixture.

Brief DoD: "Unit test on the parser against a stored fixture of the real payload."
Fixture: tests/fixtures/fred_fx/dexuseu.json -- a real FRED /series/observations slice for DEXUSEU
(USD per EUR), captured 2026-07-28, with one synthetic '.' missing-value row appended.
"""
import datetime
import json
from pathlib import Path

import importlib

# 'global' is a Python keyword, so the package cannot be imported with a normal from-import;
# load it by string the same way the dispatcher's lazy import does.
parse_fred_observations = importlib.import_module(
    "src.agents.collectors.global.fred_fx_collector"
).parse_fred_observations

FIX = Path(__file__).parent / "fixtures" / "fred_fx" / "dexuseu.json"


def _payload():
    return json.loads(FIX.read_text(encoding="utf-8"))


def test_missing_value_sentinel_skipped():
    marks = parse_fred_observations("FX_EURUSD", "USD/EUR", "USD", _payload())
    # Fixture has 7 observations, exactly one of which is the '.' sentinel.
    assert len(marks) == 6
    assert all(m.value > 0 for m in marks)


def test_metadata_propagated():
    marks = parse_fred_observations("FX_EURUSD", "USD/EUR", "USD", _payload())
    assert all(m.series_key == "FX_EURUSD" for m in marks)
    assert all(m.unit == "USD/EUR" and m.currency == "USD" for m in marks)
    assert all(isinstance(m.obs_date, datetime.date) for m in marks)


def test_eurusd_value_direction():
    marks = parse_fred_observations("FX_EURUSD", "USD/EUR", "USD", _payload())
    latest = max(marks, key=lambda m: m.obs_date)
    assert latest.obs_date == datetime.date(2026, 7, 24)
    # USD per EUR sits near 1.0-1.2, never inverted to ~0.85.
    assert 1.0 < latest.value < 1.3


def test_empty_payload():
    assert parse_fred_observations("FX_USDMYR", "MYR/USD", "MYR", {}) == []


if __name__ == "__main__":
    m = parse_fred_observations("FX_EURUSD", "USD/EUR", "USD", _payload())
    print("marks:", len(m), "latest:", max(m, key=lambda x: x.obs_date))
    for fn in [test_missing_value_sentinel_skipped, test_metadata_propagated,
               test_eurusd_value_direction, test_empty_payload]:
        fn(); print("PASS", fn.__name__)
