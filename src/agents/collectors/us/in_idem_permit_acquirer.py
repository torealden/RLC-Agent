"""
IN IDEM air-permit acquirer — second document-spine client (after IA DNR).

⚠️ STATUS 2026-06-21: WIP / BLOCKED ON RELIABLE TARGETING. The ECM DOWNLOAD path
is proven (GET_FILE returns real permit PDFs anonymously). What is NOT solved is
reliably finding a given facility's CURRENT operating permit via ECM search:
  - xSourceID is returned on docs (e.g. "059-00044") but is NOT search-indexed
    (xSourceID <substring>/<matches> queries return 0 rows).
  - dDocTitle <substring> `<sourcenum>` only matches OLD docs whose dDocName
    embedded the source number; NEWER permits have numeric dDocTitles and are
    MISSED (Cargill test returned a stale 2008 permit, not the current one).
  - Docs carry no facility name, so name matching is impossible at the doc level.
  - ECHO->IDEM crosswalk needs EPA FRS get_program_facilities (source id), which
    was returning HTTP 503 intermittently during dev — unvalidated.
Net: IDEM is downloadable but its search metadata is too inconsistent for clean
bulk targeting. Next options (see docs/planning/state_permit_data_source_inventory):
(a) drive targeting through the CAATS JSF facility search (authoritative
name->source->current permit), or (b) build a one-time full-index of OAQ Final
permit docs (read xSourceID off each of ~87k docs) then join on source id, or
(c) deprioritize IN and grind a metadata-clean state first (PA open directory).
The download + HDA-parse mechanics below are correct and reusable regardless.


IDEM permit documents live in an anonymous Oracle WebCenter Content ECM at
ecm.idem.in.gov/cs/idcplg (the research-agent's permits.air.idem.in.gov pattern
is wrong; CAATS is a JSF portal — both ignored). See memory
reference_idem_oracle_webcenter_permits for the cracked recipe.

Pipeline shape mirrors IA: bespoke FETCH here, generic downstream (registry ->
parse_spine -> publish).

  ECM recipe (proven + download-verified 2026-06-21):
    1. GET_SEARCH_RESULTS, QueryText in UCM syntax; rows in @ResultSet SearchResults
       (HDA: line1=ncol, ncol col-names, then values cycling ncol-wide).
    2. The facility key on docs is xSourceID (county-source, e.g. "157-34376") /
       xAIID. Docs carry NO facility name, so ECHO<->IDEM is bridged by source id.
    3. Operating permit = xIDEMDocumentType=Permit, xProgram=OAQ, xPermitType=Final.
       Newest by dInDate.
    4. Download: GET_FILE&dDocName=<id>&RevisionSelectionMethod=LatestReleased.

  Source-id crosswalk: ECHO gives frs_registry_id; EPA FRS get_program_facilities
  links the IDEM state program id (= source id). FRS REST is flaky (503s) so the
  resolver retries + caches to data/permits/in_idem/_source_id_cache.json.
"""

import json
import re
import sys
import time
import logging
import urllib.parse
import urllib.request
from pathlib import Path
from datetime import datetime
from typing import Optional

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT))
import requests
from dotenv import load_dotenv
load_dotenv(ROOT / ".env")
from src.services.documents import registry
from src.services.database.db_config import get_connection

logger = logging.getLogger(__name__)

ECM = "https://ecm.idem.in.gov/cs/idcplg"
FRS = "https://ofmpub.epa.gov/frs_public2/frs_rest_services.get_program_facilities"
INBOX = ROOT / "data" / "permits" / "in_idem"          # gitignored; DB tracks provenance
CACHE = INBOX / "_source_id_cache.json"
HEADERS = {"User-Agent": "Mozilla/5.0 (RLC-Agent permit acquirer)"}


def _hda_resultset(hda: str, name: str = "SearchResults") -> list:
    """Parse one HDA @ResultSet block into a list of dict rows."""
    m = re.search(r"@ResultSet " + re.escape(name) + r"\n(.*?)@end", hda, re.S)
    if not m:
        return []
    body = m.group(1).split("\n")
    ncol = int(body[0])
    if ncol == 0:
        return []
    cols = body[1:1 + ncol]
    vals = body[1 + ncol:]
    while vals and vals[-1] == "":
        vals.pop()
    return [dict(zip(cols, vals[i:i + ncol]))
            for i in range(0, len(vals) - ncol + 1, ncol)]


class IDEMPermitAcquirer:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    # --- ECM (proven) -----------------------------------------------------
    def _search(self, query: str, count: int = 50) -> str:
        params = {"IdcService": "GET_SEARCH_RESULTS", "QueryText": query,
                  "ResultCount": str(count), "IsJava": "1",
                  "SortField": "dInDate", "SortOrder": "Desc"}
        r = self.session.get(ECM, params=params, timeout=60)
        r.raise_for_status()
        return r.text

    def latest_operating_permit(self, source_id: str) -> Optional[dict]:
        """Return {dDocName, dInDate, xPermitNum, xCounty} for the newest issued
        (Final) OAQ operating permit of a facility, or None."""
        q = (f"xSourceID <matches> `{source_id}` <AND> "
             f"xIDEMDocumentType <matches> `Permit` <AND> "
             f"xProgram <matches> `OAQ` <AND> xPermitType <matches> `Final`")
        rows = _hda_resultset(self._search(q, count=50))
        if not rows:
            return None
        rows.sort(key=lambda r: r.get("dInDate", ""), reverse=True)
        top = rows[0]
        return {"dDocName": top.get("dDocName"), "dInDate": top.get("dInDate"),
                "xPermitNum": top.get("xPermitNum"), "xCounty": top.get("xCounty"),
                "n_candidates": len(rows)}

    def download(self, ddocname: str, dest: Path) -> bool:
        params = {"IdcService": "GET_FILE", "dDocName": ddocname,
                  "RevisionSelectionMethod": "LatestReleased", "Rendition": "Primary"}
        r = self.session.get(ECM, params=params, timeout=120)
        if r.status_code != 200 or not r.content[:4] == b"%PDF":
            return False
        dest.write_bytes(r.content)
        return True

    # --- source-id crosswalk (FRS, flaky -> cached) -----------------------
    def _echo_in_majors(self) -> list:
        with get_connection() as c:
            cur = c.cursor()
            cur.execute(
                "SELECT facility_name, frs_registry_id, county_name, search_profile "
                "FROM bronze.epa_echo_facility "
                "WHERE state='IN' AND air_universe ILIKE '%major%' "
                "AND frs_registry_id IS NOT NULL")
            return [dict(r) if isinstance(r, dict) else
                    dict(zip(['facility_name', 'frs_registry_id', 'county_name',
                              'search_profile'], r)) for r in cur.fetchall()]

    def _frs_source_id(self, frs_id: str, retries: int = 4) -> Optional[str]:
        """FRS get_program_facilities -> IDEM state program id (= source id).
        Retries on 503 (FRS is intermittently down)."""
        url = f"{FRS}?registry_id={frs_id}&output=JSON"
        for attempt in range(retries):
            try:
                raw = urllib.request.urlopen(
                    urllib.request.Request(url, headers=HEADERS), timeout=30
                ).read().decode("utf-8", "replace")
                d = json.loads(raw)
                # find the IN/IDEM air program link; source id format CCC-NNNNN
                for f in (d.get("Results", {}).get("FRSFacility", []) or []):
                    for pf in (f.get("ProgramFacility", []) or []):
                        acr = (pf.get("ProgramSystemAcronym") or "").upper()
                        pid = pf.get("ProgramSystemId") or ""
                        if "IDEM" in acr or acr in ("IN-RCRAINFO", "AIRS/AFS", "ICIS-AIR"):
                            m = re.search(r"\b(\d{3}-\d{4,5})\b", pid)
                            if m:
                                return m.group(1)
                return None
            except urllib.error.HTTPError as e:
                if e.code == 503 and attempt < retries - 1:
                    time.sleep(2 * (attempt + 1))
                    continue
                return None
            except Exception:
                return None
        return None

    def resolve_source_ids(self, refresh: bool = False) -> dict:
        """Map ECHO IN majors -> IDEM source id, cached to disk."""
        INBOX.mkdir(parents=True, exist_ok=True)
        cache = {}
        if CACHE.exists() and not refresh:
            cache = json.loads(CACHE.read_text(encoding="utf-8"))
        for fac in self._echo_in_majors():
            frs = fac["frs_registry_id"]
            if frs in cache and cache[frs].get("source_id"):
                continue
            sid = self._frs_source_id(frs)
            cache[frs] = {"facility_name": fac["facility_name"],
                          "county": fac["county_name"],
                          "profile": fac["search_profile"], "source_id": sid}
            self.logger.info(f"FRS {frs} {fac['facility_name'][:30]} -> {sid}")
        CACHE.write_text(json.dumps(cache, indent=2), encoding="utf-8")
        return cache

    # --- orchestration ----------------------------------------------------
    def acquire(self, limit: Optional[int] = None) -> dict:
        INBOX.mkdir(parents=True, exist_ok=True)
        crosswalk = self.resolve_source_ids()
        targets = [(v["facility_name"], v["source_id"], v["profile"])
                   for v in crosswalk.values() if v.get("source_id")]
        if limit:
            targets = targets[:limit]
        downloaded = registered = skipped = failed = no_permit = 0
        for name, sid, profile in targets:
            try:
                key = re.sub(r"[^a-z0-9]+", "_", f"in_{name}".lower()).strip("_")[:80]
                path = INBOX / f"{key}.pdf"
                if not path.exists() or path.stat().st_size == 0:
                    perm = self.latest_operating_permit(sid)
                    if not perm:
                        no_permit += 1
                        continue
                    if not self.download(perm["dDocName"], path):
                        failed += 1
                        continue
                    downloaded += 1
                else:
                    skipped += 1
                _id, is_new = registry.register(
                    "in_idem_titlev", "title_v_permit", source_key=key,
                    url=f"{ECM}?IdcService=GET_FILE&dDocName=lookup", local_path=str(path),
                    title=name)
                if is_new:
                    registered += 1
            except Exception as e:
                failed += 1
                self.logger.warning(f"error on {name}: {e}")
        return {"targets": len(targets), "downloaded": downloaded,
                "already_had": skipped, "newly_registered": registered,
                "no_permit_found": no_permit, "failed": failed,
                "unresolved_source_id": sum(1 for v in crosswalk.values()
                                            if not v.get("source_id"))}

    def collect(self, **kwargs):
        from dataclasses import dataclass, field as dc_field
        @dataclass
        class _Result:
            success: bool = False
            source: str = "in_idem_permit_acquirer"
            records_fetched: int = 0
            error_message: Optional[str] = None
            warnings: list = dc_field(default_factory=list)
            collected_at: datetime = dc_field(default_factory=datetime.now)
        res = _Result()
        try:
            stats = self.acquire()
            res.records_fetched = stats["newly_registered"]
            res.success = stats["targets"] > 0
            res.warnings = [f"{stats}"]
        except Exception as e:
            res.error_message = str(e)
        return res


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--resolve-only", action="store_true", help="just build the source-id crosswalk")
    ap.add_argument("--test-source", help="test ECM: fetch latest permit for one source id (e.g. 157-34376)")
    args = ap.parse_args()
    a = IDEMPermitAcquirer()
    if args.test_source:
        print(a.latest_operating_permit(args.test_source))
    elif args.resolve_only:
        cw = a.resolve_source_ids()
        ok = sum(1 for v in cw.values() if v.get("source_id"))
        print(f"resolved {ok}/{len(cw)} source ids")
    else:
        print(a.acquire(limit=args.limit))


if __name__ == "__main__":
    main()
