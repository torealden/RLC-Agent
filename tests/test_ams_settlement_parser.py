"""Unit test for the AMS settlement-block parser against a stored real-payload fixture.

Brief DoD: "Unit test on the parser against a stored fixture of the real payload."
Fixture: tests/fixtures/ams_settlement/ams_3192_illinois.txt (real pdfplumber extraction, 2026-07-28).
"""
import datetime
from pathlib import Path

from src.agents.collectors.us.ams_settlement_collector import parse_settlement_block

FIX = Path(__file__).parent / "fixtures" / "ams_settlement" / "ams_3192_illinois.txt"


def _parse():
    return parse_settlement_block(FIX.read_text(encoding="utf-8"))


def test_report_date():
    d, _ = _parse()
    assert d == datetime.date(2026, 7, 28)


def test_all_six_series_present():
    _, marks = _parse()
    assert {m.series_key for m in marks} == {"ZC", "ZS", "ZW", "ZO", "KE", "MWE"}


def test_contract_code_mapping():
    _, marks = _parse()
    zc = {m.contract_month: m.value for m in marks if m.series_key == "ZC"}
    # "Sep 26" -> U26, "Dec 26" -> Z26, "Mar 27" -> H27 (from the fixture header block)
    assert zc["U26"] == 458.50
    assert zc["Z26"] == 480.50
    assert zc["H27"] == 495.75


def test_mgex_normalized_from_mge():
    _, marks = _parse()
    mwe = [m for m in marks if m.series_key == "MWE"]
    assert mwe and all(m.exchange == "MGEX" for m in mwe)
    assert mwe[0].value == 702.50  # MGE Wheat Sep 26


def test_full_strip_seven_contracts_each():
    _, marks = _parse()
    from collections import Counter
    counts = Counter(m.series_key for m in marks)
    # Each board prints a 7-contract deferred strip in this report.
    for series in ("ZC", "ZW", "KE", "MWE"):
        assert counts[series] == 7, f"{series} had {counts[series]} contracts"


def test_no_bare_or_bad_values():
    _, marks = _parse()
    assert marks, "parser returned no marks"
    assert all(m.value > 0 for m in marks)
    assert all(m.contract == f"{m.series_key}_{m.contract_month}" for m in marks)


if __name__ == "__main__":
    d, marks = _parse()
    print("report_date:", d, "marks:", len(marks),
          "series:", sorted({m.series_key for m in marks}))
    for fn in [test_report_date, test_all_six_series_present, test_contract_code_mapping,
               test_mgex_normalized_from_mge, test_full_strip_seven_contracts_each,
               test_no_bare_or_bad_values]:
        fn(); print("PASS", fn.__name__)
