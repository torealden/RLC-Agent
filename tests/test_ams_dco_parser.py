"""Unit test for the AMS DCO parser against a stored real-payload fixture.

Brief DoD: "Unit test on the parser against a stored fixture of the real payload."
Fixture: tests/fixtures/ams_dco/ams_3618_dco.json -- a trimmed 2-week slice of the real MARS
'Report Detail' JSON for slug 3618, commodity Distillers Corn Oil (captured 2026-07-28).
"""
import datetime
import json
from pathlib import Path

from src.agents.collectors.us.ams_dco_collector import parse_dco_rows

FIX = Path(__file__).parent / "fixtures" / "ams_dco" / "ams_3618_dco.json"


def _payload():
    return json.loads(FIX.read_text(encoding="utf-8"))


def _parse():
    return parse_dco_rows(_payload())


def test_all_eight_regions_present():
    marks = _parse()
    assert {m.series_key for m in marks} == {
        "DCO_IA", "DCO_KS", "DCO_WI", "DCO_MO",
        "DCO_NE", "DCO_SD", "DCO_MN", "DCO_ECB",
    }


def test_two_weeks_eight_regions_each():
    marks = _parse()
    from collections import Counter
    per_date = Counter(m.obs_date for m in marks)
    # 8 regions on each of the two survey weeks in the fixture.
    assert all(n == 8 for n in per_date.values()), dict(per_date)
    assert len(per_date) == 2


def test_obs_date_is_week_end_friday():
    marks = _parse()
    dates = sorted({m.obs_date for m in marks})
    assert dates == [datetime.date(2026, 7, 17), datetime.date(2026, 7, 24)]
    # AMS survey weeks close Friday.
    assert all(d.weekday() == 4 for d in dates)


def test_values_are_plausible_cents_per_lb():
    marks = _parse()
    assert marks
    # DCO trades in the 60-90 cents/lb range; guards against a $/bu credit sneaking in as the price.
    assert all(40.0 < m.value < 120.0 for m in marks), \
        sorted({(m.series_key, m.value) for m in marks})


def test_eastern_cornbelt_maps_to_ecb():
    marks = _parse()
    ecb = [m for m in marks if m.trade_loc == "Eastern Cornbelt"]
    assert ecb and all(m.series_key == "DCO_ECB" for m in ecb)


def test_unit_drift_guard_skips_wrong_unit():
    # A row whose price_unit is not 'Cents Per Lb' (e.g. AMS switches to $/cwt) must be dropped,
    # never stored at the wrong scale.
    payload = {"results": [{
        "commodity": "Distillers Corn Oil", "trade_loc": "Iowa",
        "report_date": "07/20/2026", "report_end_date": "07/24/2026",
        "price": 8.5, "price_unit": "$ Per Cwt",
    }]}
    assert parse_dco_rows(payload) == []


def test_null_price_and_unmapped_region_dropped():
    payload = {"results": [
        {"commodity": "Distillers Corn Oil", "trade_loc": "Iowa",
         "report_date": "07/20/2026", "report_end_date": "07/24/2026",
         "price": None, "price_unit": "Cents Per Lb"},
        {"commodity": "Distillers Corn Oil", "trade_loc": "Atlantis",
         "report_date": "07/20/2026", "report_end_date": "07/24/2026",
         "price": 77.0, "price_unit": "Cents Per Lb"},
        {"commodity": "Soybean Oil", "trade_loc": "Iowa",
         "report_date": "07/20/2026", "report_end_date": "07/24/2026",
         "price": 77.6, "price_unit": "Cents Per Lb"},
    ]}
    assert parse_dco_rows(payload) == []


if __name__ == "__main__":
    marks = _parse()
    print("marks:", len(marks), "regions:", sorted({m.series_key for m in marks}),
          "dates:", sorted({str(m.obs_date) for m in marks}))
    for fn in [test_all_eight_regions_present, test_two_weeks_eight_regions_each,
               test_obs_date_is_week_end_friday, test_values_are_plausible_cents_per_lb,
               test_eastern_cornbelt_maps_to_ecb, test_unit_drift_guard_skips_wrong_unit,
               test_null_price_and_unmapped_region_dropped]:
        fn(); print("PASS", fn.__name__)
