"""
Barchart futures-prices CSV loader -- Helios price-feed layer, CME cash-settled veg-oil family.

Guidance Report price-feed layer (brief v1.1 §B; inventory:
clients/Contracts/Helios/CME_settled_veg_oil_futures_inventory.md). Loads the human-exported Barchart
"futures-prices" CSV for CME-listed cash-settled contracts (RSO now; UCO/UCOME/... next) into the
price layer.

WHY A LOADER, NOT A SCRAPER (route verified 2026-07-29):
    * CME's own settlement endpoint returns HTTP 403 with an explicit anti-scraping / Data Terms of Use
      block -- automated access is prohibited (they point to a paid GCC feed).
    * Barchart's data API lives under /proxies/, which robots.txt DISALLOWS (and it needs a session);
      the public futures-prices HTML page is robots-allowed but does NOT contain the strip (it loads via
      the disallowed XHR). So there is no robots/ToS-compliant automated free route.
    * The compliant free path is the per-page "Download" button (personal-use ToS) -- a human exports the
      CSV into the edition folder. This module parses those exports. The fetch stays human; everything
      after it is automated. Rows are can_republish=FALSE (Barchart personal-use), quality SETTLE_OFFICIAL
      (they ARE real exchange cash-settlements), source 'barchart_cme'.

CSV FORMAT (real export header):
    Contract,Latest,Change,Open,High,Low,Previous,Volume,Open Int,Time
    e.g.  BDON26 (Jul '26),1275.5,-0.5,0,1275.5,1275.5,1276,N/A,150,7/28/2026
    - Contract: Barchart symbol (root+MonthCode+YY) + a human month label. Root -> series_key via ROOT_MAP.
    - Latest: the mark. Time: the session date of that mark -> obs_date. 'N/A' -> null. Trailing footer
      line "Downloaded from Barchart.com ..." is ignored.
    NB the export Tore provided is the *intraday* variant; for these near-zero-volume marked-to-assessment
    contracts the intraday Latest at its Time date is effectively the daily mark. Prefer the EOD/settlement
    export where possible -- this loader reads both identically (Latest @ Time).

DEAD-CONTRACT / THIN handling (brief v1.1 §D): rows with a zero/blank Latest (e.g. the duplicate
    'BDOV26 (Oct 27)' placeholder that prints all zeros) are skipped and logged -- they must not land as
    a real mark. Full PLACEHOLDER tagging (zero vol AND zero OI AND uniform change) is the curve-module's
    job; here we just refuse the obviously-empty rows. Open interest is captured into curve_snapshot so
    the thin-OI guard can read it.
"""

from __future__ import annotations

import csv
import logging
import re
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# Barchart root symbol -> (series_key, unit, currency). Extend as CSVs for UCO/UCOME/... arrive.
ROOT_MAP: dict[str, tuple[str, str, str]] = {
    "BDO": ("RSO", "EUR/t", "EUR"),          # European FOB Dutch Mill Rapeseed Oil (Argus) = CME RSO
    # "JHJ": ("UCO_ARA",  "USD/t", "USD"),   # UCO FOB ARA (Argus) -- enable when a CSV is provided
    # "UCR": ("UCOME_ARA","USD/t", "USD"),   # UCOME FOB ARA (Argus) -- symbol/CSV to confirm
}

SOURCE = "barchart_cme"
QUALITY_RANK = "SETTLE_OFFICIAL"
CAN_REPUBLISH = False  # Barchart personal-use ToS + assessment pending license

_MONTH_CODES = set("FGHJKMNQUVXZ")
# "BDON26 (Jul '26)" -> capture symbol "BDON26"
_SYMBOL_RE = re.compile(r"^([A-Z]{1,4}[FGHJKMNQUVXZ]\d{2})\b")


@dataclass
class BarchartMark:
    series_key: str
    contract_month: str      # e.g. 'N26'
    tenor: str               # e.g. 'RSO_N26'
    obs_date: date
    value: float
    unit: str
    currency: str
    open_interest: Optional[int]
    volume: Optional[int]


def _num(s: str) -> Optional[float]:
    s = (s or "").strip()
    if s in ("", "N/A", "n/a", "NA"):
        return None
    try:
        return float(s.replace(",", ""))
    except ValueError:
        return None


def _int(s: str) -> Optional[int]:
    v = _num(s)
    return int(v) if v is not None else None


def _parse_date(s: str) -> Optional[date]:
    s = (s or "").strip()
    if s in ("", "N/A"):
        return None
    for fmt in ("%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def parse_barchart_csv(text: str, root_map: dict[str, tuple[str, str, str]] = ROOT_MAP) -> list[BarchartMark]:
    """Pure parser: a Barchart futures-prices CSV -> marks. No I/O; unit-tested against a real fixture.

    Skips the footer line, unknown roots (logged), zero/blank-price rows (dead placeholders), and rows
    with no parseable Time (obs_date)."""
    marks: list[BarchartMark] = []
    reader = csv.DictReader(text.splitlines())
    for row in reader:
        contract = (row.get("Contract") or "").strip()
        if not contract or contract.lower().startswith("downloaded from"):
            continue
        m = _SYMBOL_RE.match(contract)
        if not m:
            continue
        symbol = m.group(1)
        # Longest-matching known root (roots vary in length).
        root = next((r for r in sorted(root_map, key=len, reverse=True) if symbol.startswith(r)), None)
        if root is None:
            logger.debug("unmapped Barchart root for symbol %s", symbol)
            continue
        series_key, unit, currency = root_map[root]
        rest = symbol[len(root):]                 # e.g. 'N26'
        if len(rest) < 3 or rest[0] not in _MONTH_CODES:
            logger.warning("odd contract code %r in %r", rest, contract)
            continue
        contract_month = rest                     # 'N26'

        value = _num(row.get("Latest", ""))
        if value is None or value == 0:
            logger.info("skip empty/dead row: %s (Latest=%r)", contract, row.get("Latest"))
            continue
        obs_date = _parse_date(row.get("Time", ""))
        if obs_date is None:
            logger.info("skip row with no Time/obs_date: %s", contract)
            continue

        marks.append(BarchartMark(
            series_key=series_key, contract_month=contract_month,
            tenor=f"{series_key}_{contract_month}", obs_date=obs_date,
            value=value, unit=unit, currency=currency,
            open_interest=_int(row.get("Open Int", "")), volume=_int(row.get("Volume", "")),
        ))
    return marks


class BarchartCSVLoader:
    """Load a hand-exported Barchart futures-prices CSV into silver.price_mark + silver.curve_snapshot."""

    COLLECTOR_NAME = "barchart_csv"

    def load_file(self, path: str | Path, triggered_by: str = "manual") -> dict:
        started = datetime.now()
        path = Path(path)
        try:
            text = path.read_text(encoding="utf-8-sig")
        except OSError as e:
            logger.error("cannot read %s: %s", path, e)
            self._log_run(started, "FAILED", 0, 0, None, error=str(e), triggered_by=triggered_by)
            return {"success": False, "error": str(e)}

        marks = parse_barchart_csv(text)
        if not marks:
            self._log_run(started, "FAILED", 0, 0, None, error="no marks parsed",
                          triggered_by=triggered_by, notes=path.name)
            return {"success": False, "error": "no marks parsed", "file": path.name}

        inserted = self._write(marks)
        max_date = max(m.obs_date for m in marks)
        series = sorted({m.series_key for m in marks})
        self._log_run(started, "SUCCESS", len(marks), inserted, max_date, triggered_by=triggered_by,
                      notes=f"{path.name}: {series}, latest {max_date}")
        return {"success": True, "file": path.name, "series": series,
                "latest_obs_date": str(max_date), "marks": len(marks), "rows_written": inserted}

    def _write(self, marks: list[BarchartMark]) -> int:
        from psycopg2.extras import execute_values
        from src.services.database.db_config import get_connection
        pm = [(m.series_key, m.obs_date, "CONTRACT", m.tenor, m.value, m.unit, m.currency,
               SOURCE, QUALITY_RANK, CAN_REPUBLISH) for m in marks]
        cs = [(m.series_key, m.obs_date, m.tenor, m.value, m.volume, m.open_interest,
               m.unit, m.currency, SOURCE, QUALITY_RANK) for m in marks]
        with get_connection() as conn:
            cur = conn.cursor()
            before = self._count(cur)
            execute_values(cur,
                """INSERT INTO silver.price_mark
                   (series_key,obs_date,tenor_type,tenor,value,unit,currency,source,quality_rank,can_republish)
                   VALUES %s
                   ON CONFLICT (series_key,obs_date,tenor_type,tenor,source)
                   DO UPDATE SET value=EXCLUDED.value, quality_rank=EXCLUDED.quality_rank,
                                 can_republish=EXCLUDED.can_republish, collected_at=now()""", pm)
            execute_values(cur,
                """INSERT INTO silver.curve_snapshot
                   (series_key,obs_date,contract,settle,volume,open_interest,unit,currency,source,quality_rank)
                   VALUES %s
                   ON CONFLICT (series_key,obs_date,contract,source)
                   DO UPDATE SET settle=EXCLUDED.settle, volume=EXCLUDED.volume,
                                 open_interest=EXCLUDED.open_interest, collected_at=now()""", cs)
            after = self._count(cur)
            conn.commit()
        return after - before

    def _count(self, cur) -> int:
        cur.execute("SELECT count(*) AS n FROM silver.price_mark WHERE source=%s", (SOURCE,))
        return cur.fetchone()["n"]

    def _log_run(self, started, status, rows_collected, rows_inserted, data_period,
                 error=None, notes=None, triggered_by="manual"):
        try:
            from src.services.database.db_config import get_connection
            with get_connection() as conn:
                cur = conn.cursor()
                cur.execute(
                    """INSERT INTO core.collection_status
                       (collector_name, run_started_at, run_finished_at, status, rows_collected,
                        rows_inserted, error_message, data_period, commodities, is_new_data,
                        triggered_by, notes)
                       VALUES (%s,%s,now(),%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (self.COLLECTOR_NAME, started, status, rows_collected, rows_inserted, error,
                     str(data_period) if data_period else None, ["rapeseed_oil"],
                     rows_inserted > 0, triggered_by, notes))
                conn.commit()
        except Exception as e:  # pragma: no cover
            logger.error("collection_status log failed: %s", e)


def main():
    import sys
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    if len(sys.argv) < 2:
        print("usage: python -m src.agents.collectors.market.barchart_csv_loader <csv_path> [more.csv ...]")
        raise SystemExit(2)
    loader = BarchartCSVLoader()
    for p in sys.argv[1:]:
        print(loader.load_file(p, triggered_by="cli"))


if __name__ == "__main__":
    main()
