"""One-shot backfill loader: USDA OCE historical WASDE archive -> bronze.wasde_historical.

Reads data/raw/wasde_historical/ (two consolidated CSVs extracted from the git-committed
ZIPs + monthly_csv/ 2021-01 onward, fetched by scripts/download_wasde_historical.py) and
upserts into bronze.wasde_historical (migration 167). Idempotent on the 10-column natural
key; every touch stamps last_touched_at (feedback_timestamp_every_touch).

The archive-integrity checks from the source hunt are BINDING ASSERTIONS here, not prose
(CLAUDE.md checks-in-code rule). The load aborts before writing a single row if any fails:
  A1  exact expected CSV header on every file
  A2  natural-key uniqueness within every file
  A3  exactly one file per release month after errata -V2/-V3 preference
  A4  WasdeNumber sequence contiguous, starting at 481, zero gaps
  A5  the only missing calendar months are the government shutdowns
      (2013-10, 2019-01, 2025-10) — proven by A4's contiguity across them
  A6  one report number per release month, strictly increasing in month order
  A7  rows per report within sanity band [3000, 7000]
Post-load (DB) assertions:
  A8  DB distinct report count / min / max / total rows match the files
  A9  tie-out: newest report's US corn world-table values match
      gold.psd_wasde_vintages for the same cycle within +/-5 (1000 MT)
      (WASDE prints MMT at 2 dp; spec precision caveat)

Usage:
  python scripts/load_wasde_historical.py            # load
  python scripts/load_wasde_historical.py --dry-run  # file assertions only, no DB write
"""
from __future__ import annotations

import argparse
import csv
import re
import sys
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

load_dotenv(ROOT / ".env")

import psycopg2.extras

from src.services.database.db_config import get_connection

ARCHIVE_DIR = ROOT / "data" / "raw" / "wasde_historical"
CSV_DIRS = ["monthly_csv_2010_2015", "monthly_csv_2016_2020", "monthly_csv"]

EXPECTED_HEADER = [
    "WasdeNumber", "ReportDate", "ReportTitle", "Attribute", "ReliabilityProjection",
    "Commodity", "Region", "MarketYear", "ProjEstFlag", "AnnualQuarterFlag",
    "Value", "Unit", "ReleaseDate", "ReleaseTime", "ForecastYear", "ForecastMonth",
]
# Positions (0-based) of the natural-key columns within EXPECTED_HEADER order:
# wasde_number, report_title, attribute, reliability_projection, commodity,
# region, market_year, proj_est_flag, annual_quarter_flag, unit
KEY_IDX = [0, 2, 3, 4, 5, 6, 7, 8, 9, 11]

FIRST_WASDE = 481          # April 2010, start of the OCE archive
FIRST_MONTH = "2010-04"
SHUTDOWN_SKIPS = {"2013-10", "2019-01", "2025-10"}  # report never existed (sequence proves it)
ROWS_PER_REPORT_BAND = (3000, 7000)

MONTH_RE = re.compile(r"oce-wasde-report-data-(\d{4}-\d{2})(-V\d)?\.csv$", re.IGNORECASE)

TIE_OUT_TOLERANCE = 5  # 1000 MT; WASDE world tables print MMT at 2 dp

MON_ABBR = ["", "JAN", "FEB", "MAR", "APR", "MAY", "JUN",
            "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]


def discover_files() -> list[Path]:
    """All archive CSVs; assert one file per release month after -V preference (A3)."""
    files: list[Path] = []
    by_month: dict[str, list[Path]] = defaultdict(list)
    for d in CSV_DIRS:
        for p in sorted((ARCHIVE_DIR / d).glob("*.csv")):
            files.append(p)
            m = MONTH_RE.search(p.name)
            if m and d == "monthly_csv":
                by_month[m.group(1)].append(p)
    dupes = {ym: [p.name for p in ps] for ym, ps in by_month.items() if len(ps) > 1}
    assert not dupes, f"A3 FAIL: multiple files for the same release month: {dupes}"
    assert files, f"no CSVs found under {ARCHIVE_DIR}"
    return files


def parse_release_date(s: str) -> date:
    """Consolidated CSVs use ISO dates; the monthly CSVs use MM/DD/YYYY."""
    try:
        return date.fromisoformat(s)
    except ValueError:
        return datetime.strptime(s, "%m/%d/%Y").date()


def parse_value(s: str):
    s = s.strip().replace(",", "")
    if s in ("", "."):
        return None
    return float(s)


def read_and_check_files(files: list[Path]):
    """Read every CSV; run assertions A1/A2/A4-A7. Returns (rows, report_months)."""
    all_rows: list[tuple] = []
    seen_keys: set[tuple] = set()
    report_months: dict[int, str] = {}      # wasde_number -> YYYY-MM of release
    rows_per_report: Counter = Counter()

    for path in files:
        with open(path, newline="", encoding="utf-8-sig") as f:
            reader = csv.reader(f)
            header = next(reader)
            assert header == EXPECTED_HEADER, (
                f"A1 FAIL: unexpected header in {path.name}: {header}")
            file_keys: set[tuple] = set()
            for raw in reader:
                key = tuple(raw[i] for i in KEY_IDX)
                assert key not in file_keys, (
                    f"A2 FAIL: duplicate natural key in {path.name}: {key}")
                file_keys.add(key)
                # The consolidated CSVs and monthlies never overlap by construction
                # (different report ranges), but assert instead of trusting it.
                assert key not in seen_keys, (
                    f"A2 FAIL: key appears in more than one file: {key} ({path.name})")

                wn = int(raw[0])
                release = parse_release_date(raw[12])
                ym = f"{release.year:04d}-{release.month:02d}"
                prev = report_months.setdefault(wn, ym)
                assert prev == ym, (
                    f"A6 FAIL: WASDE #{wn} carries two release months: {prev} vs {ym}")
                rows_per_report[wn] += 1

                all_rows.append((
                    wn, raw[1], raw[2], raw[3], raw[4], raw[5], raw[6], raw[7],
                    raw[8], raw[9], parse_value(raw[10]), raw[11], release,
                    raw[13] or None,
                    int(raw[14]) if raw[14] else None,
                    int(raw[15]) if raw[15] else None,
                    path.name,
                ))
            seen_keys |= file_keys
        print(f"  read {path.name}: {len(file_keys):,} rows")

    # A4: contiguous report-number sequence from 481, zero gaps
    nums = sorted(report_months)
    assert nums[0] == FIRST_WASDE, f"A4 FAIL: first report is #{nums[0]}, expected #{FIRST_WASDE}"
    gaps = sorted(set(range(nums[0], nums[-1] + 1)) - set(nums))
    assert not gaps, f"A4 FAIL: missing WASDE numbers: {gaps}"

    # A5: calendar-month gaps are exactly the shutdown skips
    months_present = set(report_months.values())
    y, m = map(int, FIRST_MONTH.split("-"))
    last = max(months_present)
    expected_missing = set()
    while f"{y:04d}-{m:02d}" <= last:
        ym = f"{y:04d}-{m:02d}"
        if ym not in months_present:
            expected_missing.add(ym)
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    assert expected_missing == SHUTDOWN_SKIPS, (
        f"A5 FAIL: month gaps {sorted(expected_missing)} != known shutdowns "
        f"{sorted(SHUTDOWN_SKIPS)} — a report is missing from the archive (or a new "
        f"shutdown happened; update SHUTDOWN_SKIPS only with in-data proof, cf. "
        f"reference_govt_shutdown_data_handling)")

    # A6: report number strictly increasing with release month
    by_month_order = sorted(report_months.items(), key=lambda kv: kv[1])
    for (n1, m1), (n2, m2) in zip(by_month_order, by_month_order[1:]):
        assert n2 == n1 + 1, (
            f"A6 FAIL: release-month order breaks the sequence: #{n1} ({m1}) -> #{n2} ({m2})")

    # A7: rows-per-report sanity band
    lo, hi = ROWS_PER_REPORT_BAND
    off = {n: c for n, c in rows_per_report.items() if not lo <= c <= hi}
    assert not off, f"A7 FAIL: reports outside {ROWS_PER_REPORT_BAND} rows: {off}"

    print(f"file assertions A1-A7 PASS: {len(nums)} reports "
          f"#{nums[0]}->#{nums[-1]}, {len(all_rows):,} rows, "
          f"shutdown skips verified {sorted(SHUTDOWN_SKIPS)}")
    return all_rows, report_months


INSERT_SQL = """
INSERT INTO bronze.wasde_historical (
    wasde_number, report_date, report_title, attribute, reliability_projection,
    commodity, region, market_year, proj_est_flag, annual_quarter_flag,
    value, unit, release_date, release_time, forecast_year, forecast_month, source_file
) VALUES %s
ON CONFLICT ON CONSTRAINT wasde_historical_natural_key DO UPDATE SET
    value = EXCLUDED.value,
    report_date = EXCLUDED.report_date,
    release_date = EXCLUDED.release_date,
    release_time = EXCLUDED.release_time,
    forecast_year = EXCLUDED.forecast_year,
    forecast_month = EXCLUDED.forecast_month,
    source_file = EXCLUDED.source_file,
    last_touched_at = now()
"""


def post_load_assertions(cur, report_months: dict[int, str], n_rows: int) -> None:
    # A8: DB totals match the files (get_connection uses RealDictCursor — access by
    # alias, and alias EVERY column: duplicate names silently collapse in dict rows)
    cur.execute("""
        SELECT COUNT(DISTINCT wasde_number) AS n_rep, MIN(wasde_number) AS wn_min,
               MAX(wasde_number) AS wn_max, COUNT(*) AS total
        FROM bronze.wasde_historical
    """)
    r = cur.fetchone()
    nums = sorted(report_months)
    assert (r["n_rep"], r["wn_min"], r["wn_max"]) == (len(nums), nums[0], nums[-1]), (
        f"A8 FAIL: DB has {r['n_rep']} reports #{r['wn_min']}->#{r['wn_max']}, "
        f"files have {len(nums)} #{nums[0]}->#{nums[-1]}")
    assert r["total"] == n_rows, f"A8 FAIL: DB rows {r['total']:,} != file rows {n_rows:,}"

    # A9: newest report US corn world-table tie-out vs the live vintage ladder.
    # Only possible for cycles the live collector has captured (2026+); skip with a
    # loud note otherwise, fail on numeric mismatch.
    newest = nums[-1]
    ym = report_months[newest]
    yy, mm = int(ym[:4]), int(ym[5:])
    vintage = f"WASDE_{MON_ABBR[mm]}_{yy % 100}"
    cur.execute("""
        SELECT h.market_year AS my_label, h.attribute AS attr, h.value * 1000.0 AS kt
        FROM bronze.wasde_historical h
        WHERE h.wasde_number = %s AND h.region = 'United States'
          AND h.report_title ILIKE 'World Corn%%'
          AND h.attribute IN ('Production', 'Ending Stocks')
    """, (newest,))
    csv_vals = {(r["my_label"], r["attr"]): float(r["kt"]) for r in cur.fetchall()}
    assert csv_vals, f"A9 FAIL: no US corn world-table rows in WASDE #{newest}"

    cur.execute("""
        SELECT marketing_year, production, ending_stocks
        FROM gold.psd_wasde_vintages
        WHERE commodity = 'corn' AND country_code = 'US' AND vintage = %s
    """, (vintage,))
    live = {r["marketing_year"]: (r["production"], r["ending_stocks"])
            for r in cur.fetchall()}
    if not live:
        print(f"A9 SKIPPED: live ladder has no vintage {vintage} "
              f"(nothing to tie out against — NOT verified)")
        return
    checked = 0
    for (my_label, attr), kt in csv_vals.items():
        my = int(my_label[:4])  # '2025/26' -> 2025 == PSD marketing_year
        if my not in live:
            continue
        live_val = live[my][0] if attr == "Production" else live[my][1]
        if live_val is None:
            continue
        diff = abs(float(live_val) - kt)
        assert diff <= TIE_OUT_TOLERANCE, (
            f"A9 FAIL: {vintage} US corn MY{my} {attr}: archive {kt:,.0f} vs "
            f"live {float(live_val):,.0f} (diff {diff:,.1f} > {TIE_OUT_TOLERANCE})")
        checked += 1
    assert checked >= 2, f"A9 FAIL: only {checked} comparable values for {vintage}"
    print(f"A8-A9 PASS: DB totals match; {vintage} US corn ties out "
          f"on {checked} values within +/-{TIE_OUT_TOLERANCE} (1000 MT)")


def log_status(cur, started_at, n_rows: int, n_reports: int, status: str,
               error: str | None = None) -> None:
    cur.execute("""
        INSERT INTO core.collection_status
            (collector_name, run_started_at, run_finished_at, status,
             rows_collected, rows_inserted, triggered_by, notes, error_message)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, ("wasde_historical_backfill", started_at, datetime.now(timezone.utc), status,
          n_rows, n_rows, "cli",
          f"{n_reports} reports from data/raw/wasde_historical/", error))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true",
                    help="run file assertions only, write nothing")
    args = ap.parse_args()

    started_at = datetime.now(timezone.utc)
    files = discover_files()
    print(f"{len(files)} archive files under {ARCHIVE_DIR}")
    rows, report_months = read_and_check_files(files)

    if args.dry_run:
        print("dry run: assertions passed, nothing written")
        return 0

    with get_connection() as conn:
        with conn.cursor() as cur:
            try:
                print(f"upserting {len(rows):,} rows ...")
                psycopg2.extras.execute_values(cur, INSERT_SQL, rows, page_size=5000)
                post_load_assertions(cur, report_months, len(rows))
                log_status(cur, started_at, len(rows), len(report_months), "SUCCESS")
            except Exception as e:
                conn.rollback()
                with conn.cursor() as cur2:
                    log_status(cur2, started_at, 0, len(report_months), "FAILED", str(e)[:500])
                conn.commit()
                raise
        conn.commit()
    print("bronze.wasde_historical load complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
