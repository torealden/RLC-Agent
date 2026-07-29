"""Unit test for the Barchart futures-prices CSV parser.

Brief DoD: "Unit test on the parser against a stored fixture of the real payload."
The fixture here is SYNTHETIC (fabricated EUR/t values, root BDO -> RSO) with the exact real export
format -- header, an N/A open-interest row, a real open-interest row, a zero/dead placeholder row, and
the trailing "Downloaded from Barchart.com" footer. It is synthetic on purpose: the real Barchart export
is personal-use/licensed data (can_republish=FALSE) and must not enter version control. The real file is
loaded locally from the untracked edition folder.
"""
import datetime
from pathlib import Path

from src.agents.collectors.market.barchart_csv_loader import parse_barchart_csv

FIX = Path(__file__).parent / "fixtures" / "barchart" / "rso_bdo_futures_prices_synthetic.csv"


def _marks():
    return parse_barchart_csv(FIX.read_text(encoding="utf-8-sig"))


def test_root_maps_to_rso_with_units():
    marks = _marks()
    assert marks
    assert all(m.series_key == "RSO" for m in marks)
    assert all(m.unit == "EUR/t" and m.currency == "EUR" for m in marks)


def test_dead_row_and_footer_dropped():
    marks = _marks()
    # 3 priced rows survive; the all-zero BDOH00 dead row and the footer line are dropped.
    assert len(marks) == 3, [m.tenor for m in marks]
    assert all(m.value > 0 for m in marks)


def test_symbol_to_tenor_and_obs_date():
    by_month = {m.contract_month: m for m in _marks()}
    z99 = by_month["Z99"]
    assert z99.tenor == "RSO_Z99"
    assert z99.value == 1000.0
    assert z99.obs_date == datetime.date(2026, 7, 28)


def test_open_interest_na_vs_value_and_volume_null():
    by_month = {m.contract_month: m for m in _marks()}
    assert by_month["Z99"].open_interest == 150     # real OI parsed to int
    assert by_month["F00"].open_interest is None     # 'N/A' -> None
    assert by_month["G00"].open_interest == 25
    assert all(m.volume is None for m in by_month.values())  # Volume 'N/A' throughout


def test_fractional_price_preserved():
    by_month = {m.contract_month: m for m in _marks()}
    assert by_month["G00"].value == 980.5


if __name__ == "__main__":
    ms = _marks()
    print("marks:", len(ms), "months:", [m.contract_month for m in ms])
    for fn in [test_root_maps_to_rso_with_units, test_dead_row_and_footer_dropped,
               test_symbol_to_tenor_and_obs_date, test_open_interest_na_vs_value_and_volume_null,
               test_fractional_price_preserved]:
        fn(); print("PASS", fn.__name__)
