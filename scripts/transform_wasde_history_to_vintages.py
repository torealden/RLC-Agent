"""Transform bronze.wasde_historical world tables -> silver.wasde_historical_vintage.

Scope (Tore ruling 2026-08-03): the comp-tab commodities carried by WASDE world
tables — corn, wheat, soybeans, soybean meal, soybean oil, rice, cotton — for the
individually-named world-table countries + the World aggregate. NOT covered here
(recorded, not silent): sorghum (no world table; US bushel table only) and wheat
classes (US table only, not on the PSD ladder). Extending scope later = add map
entries below and re-run; nothing structural.

Units: MMT -> 1000 MT (x1000); cotton stays native, Million 480-lb Bales ->
1000 480-lb Bales (x1000). WASDE-published 2-dp rounding throughout
(vintage_source='WASDE_ARCHIVE' in the gold view flags it).

Binding assertions (abort before writing if any fails):
  T1  every attribute in the seven world tables is mapped or explicitly excluded
  T2  every region label is mapped to a country code or whitelisted as an aggregate
  T3  every one of the 193 reports contributes rows for all seven commodities
  T4  balance identity |supply - distribution - ending| <= tolerance, BINDING for
      US rows only: many countries' world-table exports are trade-year basis vs
      local-MY supply, and the World row absorbs the global import/export
      imbalance, so the identity legitimately fails there (verified 2026-08-03:
      corn WD residual == world trade imbalance). Non-US rows get a breach-share
      ceiling instead of a per-row gate.
  T5  tie-out vs live gold.psd_wasde_vintages on shared cycles: country rows on
      all mapped attributes, World rows on production/stocks only (PSD's WD
      domestic is identity-derived with a different trade-imbalance treatment);
      unit-aware for cotton. >=99% within tolerance and zero gross (>10x) outliers.
Post-write:
  T6  DB row count == transformed row count

Usage:
  python scripts/transform_wasde_history_to_vintages.py            # transform + load
  python scripts/transform_wasde_history_to_vintages.py --dry-run  # assertions only
"""
from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

load_dotenv(ROOT / ".env")

import psycopg2.extras

from src.services.database.db_config import get_connection

TITLES = {
    "World Corn Supply and Use": "corn",
    "World Wheat Supply and Use": "wheat",
    "World Soybean Supply and Use": "soybeans",
    "World Soybean Meal Supply and Use": "soybean_meal",
    "World Soybean Oil Supply and Use": "soybean_oil",
    "World Cotton Supply and Use": "cotton",
    "World Rice Supply and Use  (Milled Basis)": "rice",  # double space is in-source
}

# WASDE region -> PSD/FAS country code. FIPS-style codes, matching the dominant
# live-row convention (CLAUDE.md; reference_psd_country_codes). Traps verified
# 2026-08-03 against live values: FIPS ZA = ZAMBIA (South Africa is SF), FIPS
# NG = NIGER (Nigeria is NI), FIPS AU = AUSTRIA (Australia is AS). The stray
# live rows under ISO codes (AU/ZA/NG name-mismatches) are the pending
# ISO-orphan cleanup, not the convention.
REGION_CODE = {
    "United States": "US", "Argentina": "AR", "Australia": "AS", "Bangladesh": "BG",
    "Brazil": "BR", "Burma": "BU", "Canada": "CA", "China": "CH", "Egypt": "EG",
    "European Union": "E4", "India": "IN", "Indonesia": "ID", "Japan": "JA",
    "Kazakhstan": "KZ", "Mexico": "MX", "Nigeria": "NI", "Pakistan": "PK",
    "Paraguay": "PA", "Philippines": "RP", "Russia": "RS", "Saudi Arabia": "SA",
    "South Africa": "SF", "South Korea": "KS", "Thailand": "TH", "Turkey": "TU",
    "Ukraine": "UP", "United Kingdom": "UK", "Vietnam": "VM", "World": "WD",
}
# Multi-country aggregates the world tables print that have no PSD country row.
# Kept in bronze; deliberately not transformed. Compared case-insensitively
# ('Major exporters' vs 'Major Exporters' both occur). Notable entries:
#   * EU-27+UK — WASDE #608-611 (Jan-Apr 2021 Brexit transition) printed this
#     INSTEAD of 'European Union'; it includes the UK, so mapping it to E4
#     (EU-27) would contaminate the series. Consequence: the E4 vintage history
#     has an honest 4-cycle gap. (Pre-2021 'European Union' = EU-28 as
#     published; composition changes are part of vintage history.)
#   * FSU-12 — 2010-era Former Soviet Union aggregate.
AGGREGATE_REGIONS = {
    "afr. fr. zone", "c. amer & carib", "central asia", "foreign",
    "major exporters", "major importers", "n. afr & mideast", "n. africa",
    "north africa", "s. hemis.", "s. hemis", "sel. mideast", "southeast asia",
    "total foreign", "world less china", "sub-saharan africa", "others",
    "eu-27", "eu-27+uk", "fsu-12", "middle east",
}

ATTR_MAP = {
    "Beginning Stocks": "beginning_stocks",
    "Production": "production",
    "Imports": "imports",
    "Exports": "exports",
    "Ending Stocks": "ending_stocks",
    "Domestic Total": "domestic_consumption",
    "Total Domestic": "domestic_consumption",   # rice prints 'Total  Domestic '
    "Domestic Use": "domestic_consumption",     # cotton
    "Domestic Feed": "feed_dom_consumption",
    "Domestic Crush": "crush",
    "Loss": "loss",  # cotton only; folded into total_distribution, not stored
}
STORED_COLS = [
    "beginning_stocks", "production", "imports", "exports", "ending_stocks",
    "domestic_consumption", "feed_dom_consumption", "crush",
]

IDENTITY_TOL = 60     # 1000 MT / 1000 bales: 2-dp rounding across up to 7 terms
TIE_OUT_TOL = 6       # per-value vs live PSD rows


def norm_attr(a: str) -> str | None:
    return ATTR_MAP.get(" ".join(a.split()))


def parse_my(label: str) -> int:
    my = int(label[:4])
    assert 1959 < my < 2035, f"implausible marketing year {label!r}"
    return my


def fetch_bronze(cur):
    cur.execute("""
        SELECT report_title, attribute, commodity AS src_commodity, region,
               market_year, value, unit, wasde_number, release_date
        FROM bronze.wasde_historical
        WHERE report_title = ANY(%s) AND market_year <> ''
    """, (list(TITLES),))
    return cur.fetchall()


def transform(rows):
    """bronze rows -> {(commodity, code, my, cycle): {col: val, ...}} + T1-T3."""
    unmapped_attrs = defaultdict(int)
    unmapped_regions = defaultdict(int)
    reports_by_commodity = defaultdict(set)
    out: dict[tuple, dict] = {}

    for r in rows:
        commodity = TITLES[r["report_title"]]
        attr = norm_attr(r["attribute"])
        if attr is None:
            unmapped_attrs[(r["report_title"], r["attribute"])] += 1
            continue
        region = " ".join(r["region"].split())
        code = REGION_CODE.get(region)
        if code is None:
            if region.lower() not in AGGREGATE_REGIONS:
                unmapped_regions[(r["report_title"], region)] += 1
            continue
        my = parse_my(r["market_year"])
        release = r["release_date"]
        cycle = date(release.year, release.month, 1)
        reports_by_commodity[commodity].add(r["wasde_number"])
        key = (commodity, code, my, cycle)
        rec = out.setdefault(key, {
            "country": region, "wasde_number": r["wasde_number"],
            "release_date": release,
            "unit": "1000 480-lb Bales" if commodity == "cotton" else "1000 MT",
        })
        val = None if r["value"] is None else float(r["value"]) * 1000.0
        rec[attr] = val

    assert not unmapped_attrs, f"T1 FAIL: unmapped attributes: {dict(unmapped_attrs)}"
    assert not unmapped_regions, (
        f"T2 FAIL: region labels neither mapped nor whitelisted: {dict(unmapped_regions)}"
        " — map them (REGION_CODE) or whitelist (AGGREGATE_REGIONS), with code verified"
        " against live PSD values, not guessed (ZA/NG/AU traps).")

    n_reports = {c: len(s) for c, s in reports_by_commodity.items()}
    assert set(n_reports) == set(TITLES.values()), (
        f"T3 FAIL: commodities missing entirely: {set(TITLES.values()) - set(n_reports)}")
    total_reports = max(n_reports.values())
    short = {c: n for c, n in n_reports.items() if n != total_reports}
    assert not short, (
        f"T3 FAIL: commodities missing from some reports: {short} (full={total_reports})")

    # derived columns + T4 identity
    bad_us, n_breach, n_checked = [], 0, 0
    for key, rec in out.items():
        beg, prod, imp = rec.get("beginning_stocks"), rec.get("production"), rec.get("imports")
        dom, exp, end = rec.get("domestic_consumption"), rec.get("exports"), rec.get("ending_stocks")
        loss = rec.pop("loss", None)
        if None in (beg, prod, imp, dom, exp, end):
            rec["total_supply"] = rec["total_distribution"] = None
            continue
        rec["total_supply"] = beg + prod + imp
        rec["total_distribution"] = dom + exp + (loss or 0.0)
        resid = rec["total_supply"] - rec["total_distribution"] - end
        n_checked += 1
        if abs(resid) > IDENTITY_TOL:
            n_breach += 1
            if key[1] == "US":
                bad_us.append((key, round(resid, 1)))
        feed = rec.get("feed_dom_consumption")
        rec["fsi_consumption"] = (dom - feed) if feed is not None else None
    assert not bad_us, (
        f"T4 FAIL: {len(bad_us)} US rows break the supply-distribution-ending "
        f"identity beyond +/-{IDENTITY_TOL}; worst: "
        f"{sorted(bad_us, key=lambda x: -abs(x[1]))[:5]}")
    breach_pct = 100.0 * n_breach / n_checked
    assert breach_pct <= 5.0, (
        f"T4 FAIL: {breach_pct:.1f}% of all rows break the identity — trade-year "
        f"basis explains a few percent, not this; suspect the attribute map")
    print(f"T4 PASS: US identity clean; {n_breach}/{n_checked} rows "
          f"({breach_pct:.1f}%) breach globally (trade-year/WD, expected)")

    print(f"T1-T4 PASS: {len(out):,} vintage rows, {total_reports} reports, "
          f"{len(n_reports)} commodities")
    return out


def tie_out_vs_live(cur, out):
    """T5: shared (commodity, code, MY, cycle) rows must match live PSD values."""
    cur.execute("""
        SELECT commodity, country_code, marketing_year, psd_cycle, unit,
               beginning_stocks, production, imports, exports, ending_stocks,
               domestic_consumption, feed_dom_consumption, crush
        FROM gold.psd_wasde_vintages
        WHERE vintage LIKE 'WASDE%%' AND vintage_source = 'PSD'
          AND commodity = ANY(%s)
    """, (list(TITLES.values()),))
    live = {(r["commodity"], r["country_code"], r["marketing_year"],
             r["psd_cycle"]): r for r in cur.fetchall()}

    n_cmp = n_ok = 0
    gross, near = [], []
    for key, rec in out.items():
        lr = live.get(key)
        if lr is None:
            continue
        commodity, code = key[0], key[1]
        wd = code == "WD"
        for col in STORED_COLS:
            if wd and col not in ("production", "beginning_stocks", "ending_stocks"):
                continue  # PSD WD dom/fsi/trade are definitionally different
            hv, lv = rec.get(col), lr[col]
            if hv is None or lv is None:
                continue
            lv = float(lv)
            # Cotton compares 1:1: ALL live cotton values are 1000 480-lb bales;
            # rows labeled '1000 MT' are mislabeled (verified 2026-08-03: US
            # 13,918 identical under both labels; China 34,500 only works as
            # bales). Live-collector label fix is a queued cleanup.
            n_cmp += 1
            d = abs(hv - lv)
            if d <= TIE_OUT_TOL:
                n_ok += 1
            elif d > 10 * TIE_OUT_TOL:
                gross.append((key, col, round(hv, 1), round(lv, 1)))
            else:
                near.append((key, col, round(hv, 1), round(lv, 1)))
    assert n_cmp > 500, f"T5 FAIL: only {n_cmp} comparable values — overlap too thin to trust"
    pct = 100.0 * n_ok / n_cmp
    assert not gross, f"T5 FAIL: gross mismatches (mapping/unit error): {gross[:8]}"
    assert pct >= 99.0, (
        f"T5 FAIL: only {pct:.1f}% of {n_cmp} shared values within +/-{TIE_OUT_TOL}; "
        f"sample: {near[:8]}")
    print(f"T5 PASS: {n_cmp:,} shared values vs live PSD, {pct:.2f}% within "
          f"+/-{TIE_OUT_TOL} ({len(near)} near-misses, 0 gross)")


INSERT_SQL = """
INSERT INTO silver.wasde_historical_vintage (
    commodity, country, country_code, marketing_year, psd_cycle, wasde_number,
    release_date, beginning_stocks, production, imports, exports, ending_stocks,
    domestic_consumption, feed_dom_consumption, fsi_consumption, crush,
    total_supply, total_distribution, unit
) VALUES %s
ON CONFLICT (commodity, country_code, marketing_year, psd_cycle) DO UPDATE SET
    country = EXCLUDED.country,
    wasde_number = EXCLUDED.wasde_number,
    release_date = EXCLUDED.release_date,
    beginning_stocks = EXCLUDED.beginning_stocks,
    production = EXCLUDED.production,
    imports = EXCLUDED.imports,
    exports = EXCLUDED.exports,
    ending_stocks = EXCLUDED.ending_stocks,
    domestic_consumption = EXCLUDED.domestic_consumption,
    feed_dom_consumption = EXCLUDED.feed_dom_consumption,
    fsi_consumption = EXCLUDED.fsi_consumption,
    crush = EXCLUDED.crush,
    total_supply = EXCLUDED.total_supply,
    total_distribution = EXCLUDED.total_distribution,
    unit = EXCLUDED.unit,
    last_touched_at = now()
"""


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true",
                    help="run assertions only, write nothing")
    args = ap.parse_args()
    started_at = datetime.now(timezone.utc)

    with get_connection() as conn:
        with conn.cursor() as cur:
            rows = fetch_bronze(cur)
            print(f"{len(rows):,} bronze world-table rows")
            out = transform(rows)
            tie_out_vs_live(cur, out)

            if args.dry_run:
                print("dry run: assertions passed, nothing written")
                return 0

            values = [
                (c, rec["country"], code, my, cycle, rec["wasde_number"],
                 rec["release_date"], rec.get("beginning_stocks"), rec.get("production"),
                 rec.get("imports"), rec.get("exports"), rec.get("ending_stocks"),
                 rec.get("domestic_consumption"), rec.get("feed_dom_consumption"),
                 rec.get("fsi_consumption"), rec.get("crush"), rec.get("total_supply"),
                 rec.get("total_distribution"), rec["unit"])
                for (c, code, my, cycle), rec in out.items()
            ]
            psycopg2.extras.execute_values(cur, INSERT_SQL, values, page_size=5000)

            cur.execute("SELECT COUNT(*) AS n FROM silver.wasde_historical_vintage")
            n_db = cur.fetchone()["n"]
            assert n_db == len(out), f"T6 FAIL: DB rows {n_db:,} != transformed {len(out):,}"

            cur.execute("""
                INSERT INTO core.collection_status
                    (collector_name, run_started_at, run_finished_at, status,
                     rows_collected, rows_inserted, triggered_by, notes)
                VALUES (%s, %s, %s, 'SUCCESS', %s, %s, 'cli', %s)
            """, ("wasde_historical_transform", started_at, datetime.now(timezone.utc),
                  len(out), len(out),
                  "world tables -> silver.wasde_historical_vintage (mig 168)"))
        conn.commit()
    print(f"T6 PASS: silver.wasde_historical_vintage holds {len(out):,} rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
