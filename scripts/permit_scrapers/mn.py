"""
Minnesota MPCA permit scraper.

Strategy:
    1. Bulk facility lookup via gisdata.mn.gov CSV (no auth, no captcha).
       Cached at data/raw/state_air_permits/mn/my_neighborhood_sites.csv.
       Used for fuzzy facility resolution by name + city.

    2. For a chosen site, drive Playwright through the WIMN SPA to:
        - Navigate /wimn/site/<siteId> → loads facility metadata API
        - Click into Documents tab → triggers permit-actions API
       The API responses (200 JSON) are intercepted via page.on("response").

    3. Parse permit-actions JSON to extract Air Quality permit history.

Known MPCA limitation (2026-05-09):
    The /api/v1/wimn/sites/documents endpoint returns HTTP 500 globally
    — for AGP Dawson, MnSP Brewster, every site we've tested. The WIMN
    UI shows "Documents (N)" but renders nothing on click because of
    this server-side bug. Therefore actual permit PDFs cannot currently
    be fetched via WIMN. Path forward for PDFs:
        - Submit MPCA Information Request, OR
        - Wait for MPCA to fix the endpoint, OR
        - Hand-grab when MPCA support provides direct URLs.

    This scraper still produces high-value metadata: permit ID, all
    amendment dates, inspection history, federal program flags,
    geographic + activity details. That's enough to populate the FIC
    and surface gaps systematically.
"""
from __future__ import annotations

import csv
import json
import re
import urllib.request
import zipfile
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import Optional

from playwright.sync_api import Page

from scripts.permit_scrapers.base import (
    DocumentRef,
    FacilityRef,
    PermitScraperBase,
    capture_responses,
    jitter_sleep,
)


WIMN_HOME = "https://webapp.pca.state.mn.us/wimn/"
WIMN_SITE_TPL = "https://webapp.pca.state.mn.us/wimn/site/{site_id}"
API_PATTERN = re.compile(r"services\.pca\.state\.mn\.us/api/v\d+/wimn/")
BULK_CSV_URL = (
    "https://resources.gisdata.mn.gov/pub/gdrs/data/pub/us_mn_state_pca/"
    "env_my_neighborhood/csv_env_my_neighborhood.zip"
)


# ---------------------------------------------------------------------------
# Bulk facility CSV (no scraping needed for this part)
# ---------------------------------------------------------------------------

class WIMNBulkCSV:
    """In-memory index over the MN-MPCA bulk facility CSV."""

    def __init__(self, csv_path: Path):
        self.csv_path = csv_path
        self.rows: list[dict] = []
        if csv_path.exists():
            with csv_path.open(encoding="utf-8") as f:
                self.rows = list(csv.DictReader(f))

    @classmethod
    def ensure_downloaded(cls, csv_path: Path) -> "WIMNBulkCSV":
        if not csv_path.exists():
            csv_path.parent.mkdir(parents=True, exist_ok=True)
            with urllib.request.urlopen(BULK_CSV_URL, timeout=120) as resp:
                z = zipfile.ZipFile(BytesIO(resp.read()))
                with z.open("my_neighborhood_sites.csv") as src:
                    csv_path.write_bytes(src.read())
        return cls(csv_path)

    def search(
        self,
        *,
        name_substr: Optional[str] = None,
        city: Optional[str] = None,
        program: Optional[str] = None,
    ) -> list[dict]:
        out = []
        name_substr_lc = (name_substr or "").lower()
        city_lc = (city or "").lower()
        for row in self.rows:
            if name_substr and name_substr_lc not in (row.get("name") or "").lower():
                continue
            if city and city_lc not in (row.get("address_city") or "").lower():
                continue
            if program and program not in (row.get("program_name_list") or ""):
                continue
            out.append(row)
        return out


# ---------------------------------------------------------------------------
# WIMN navigator (Playwright)
# ---------------------------------------------------------------------------

@dataclass
class WIMNApiCapture:
    bodies: dict[str, str]  # url → JSON text


def _navigate_and_capture(page: Page, site_id: str) -> WIMNApiCapture:
    """
    Navigate WIMN to /site/<id>, let the SPA fire its XHRs, return all
    intercepted JSON responses keyed by URL.

    Sequence (observed):
        1. /wimn/                        — app shell
        2. /wimn/site/<id>               — facility detail
        3. SPA fires:
            sites?siteId=<id>            — facility metadata (200)
            sites/activities?siteId=<id> — programs (200)
            sites/permit-actions?siteId  — permit history (200)
            sites/inspection-actions?... — inspection history (200)
            sites/cerclis-ids?...        — cleanup IDs (200, often empty)
            sites/enforcement-actions?.. — enforcement history (200)
            sites/documents?siteId=<id>  — DOCUMENTS (currently 500)
    """
    capture = WIMNApiCapture(bodies={})

    def _on_match(resp):
        if resp.status == 200:
            try:
                capture.bodies[resp.url] = resp.text()
            except Exception:
                pass

    capture_responses(page, url_pattern=API_PATTERN, on_match=_on_match)

    # Visit home first to warm up the SPA + get any cookies set
    page.goto(WIMN_HOME, timeout=60000)
    page.wait_for_load_state("networkidle", timeout=60000)
    jitter_sleep(0.6, 1.2)

    page.goto(WIMN_SITE_TPL.format(site_id=site_id), timeout=60000)
    page.wait_for_load_state("networkidle", timeout=60000)
    jitter_sleep(2.0, 3.5)

    return capture


def _extract_site_data(capture: WIMNApiCapture, endpoint: str) -> Optional[list]:
    for url, body in capture.bodies.items():
        if endpoint in url:
            try:
                data = json.loads(body)
            except json.JSONDecodeError:
                continue
            return data.get("data") if isinstance(data, dict) else data
    return None


# ---------------------------------------------------------------------------
# Scraper
# ---------------------------------------------------------------------------

class MNScraper(PermitScraperBase):
    state_code = "MN"

    def __init__(
        self,
        *,
        output_dir: Path,
        storage_state_path: Optional[Path] = None,
        headless: bool = False,
        bulk_csv_path: Optional[Path] = None,
    ):
        super().__init__(
            output_dir=output_dir,
            storage_state_path=storage_state_path,
            headless=headless,
        )
        self.bulk_csv_path = bulk_csv_path or (
            output_dir / "my_neighborhood_sites.csv"
        )
        self._bulk: Optional[WIMNBulkCSV] = None

    @property
    def bulk(self) -> WIMNBulkCSV:
        if self._bulk is None:
            self._bulk = WIMNBulkCSV.ensure_downloaded(self.bulk_csv_path)
        return self._bulk

    # ------ subclass hooks ------

    def find_facility(
        self,
        name: str,
        city: Optional[str] = None,
        state: Optional[str] = None,
    ) -> Optional[FacilityRef]:
        # Filter to Air Quality programs only — we only care about
        # facilities with a Title V or other air permit.
        candidates = self.bulk.search(
            name_substr=name,
            city=city,
            program="Air Quality",
        )
        if not candidates:
            return None
        # If multiple, pick the first ACTIVE one
        active = [c for c in candidates if (c.get("active_flag") or "").upper() == "Y"]
        chosen = active[0] if active else candidates[0]
        return FacilityRef(
            state="MN",
            site_id=chosen["site_id"],
            name=chosen["name"],
            city=chosen.get("address_city"),
            address=chosen.get("address_street"),
            extra={
                "mpca_id_list": chosen.get("mpca_id_list"),
                "program_name_list": chosen.get("program_name_list"),
                "industrial_classification": chosen.get("industrial_classification"),
                "lat": chosen.get("latitude"),
                "lon": chosen.get("longitude"),
                "site_url": chosen.get("site_url"),
                "_match_count": len(candidates),
            },
        )

    def fetch_metadata(self, ref: FacilityRef) -> dict:
        with self.open_session() as sess:
            page = sess.new_page()
            cap = _navigate_and_capture(page, ref.site_id)

        meta: dict = {
            "site_id": ref.site_id,
            "site_url": WIMN_SITE_TPL.format(site_id=ref.site_id),
            "captured_endpoints": list(cap.bodies),
        }
        for tag, endpoint in [
            ("site", "sites?"),
            ("activities", "sites/activities"),
            ("permit_actions", "sites/permit-actions"),
            ("inspection_actions", "sites/inspection-actions"),
            ("enforcement_actions", "sites/enforcement-actions"),
            ("cerclis_ids", "sites/cerclis-ids"),
        ]:
            items = _extract_site_data(cap, endpoint)
            if items is not None:
                meta[tag] = items
        return meta

    def fetch_documents(self, ref: FacilityRef) -> list[DocumentRef]:
        """
        Documents are derived from permit-actions metadata. We can't
        currently fetch the actual PDFs because the /sites/documents
        endpoint returns 500 globally (MPCA bug, see module docstring).
        Instead we emit DocumentRef stubs with permit_id + action_date
        so downstream code knows what to file Information Requests for.
        """
        meta = self.fetch_metadata(ref)
        permit_actions = meta.get("permit_actions") or []
        docs: list[DocumentRef] = []
        for pa in permit_actions:
            if pa.get("programName") != "Air Quality":
                continue
            docs.append(
                DocumentRef(
                    title=(
                        f"{pa.get('actionTypeName', '')} "
                        f"({pa.get('actionDate', '')}) "
                        f"{pa.get('activitySystemId', '')}"
                    ).strip(),
                    url=None,  # MPCA documents endpoint broken
                    program="Air Quality",
                    permit_id=pa.get("activitySystemId"),
                    action_date=pa.get("actionDate"),
                    action_type=pa.get("actionTypeName"),
                    extra={
                        "effectiveStartDate": pa.get("effectiveStartDate"),
                        "effectiveEndDate": pa.get("effectiveEndDate"),
                        "activityName": pa.get("activityName"),
                    },
                )
            )
        return docs

    def download_document(self, doc: DocumentRef, dest: Path) -> Optional[Path]:
        if doc.url is None:
            return None
        # For when MPCA fixes the endpoint or we add Information-Request
        # auto-fetch support; left as a TODO.
        raise NotImplementedError(
            "MPCA documents endpoint broken (HTTP 500). "
            "File Information Request for permit "
            f"{doc.permit_id}, action {doc.action_type} {doc.action_date}."
        )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Scrape MN MPCA WIMN for facility + permit metadata."
    )
    parser.add_argument("--name", required=True,
                        help="Facility name substring (e.g. 'Ag Processing')")
    parser.add_argument("--city",
                        help="City filter (e.g. 'Dawson')")
    parser.add_argument("--output-dir", type=Path,
                        default=Path("data/raw/state_air_permits/mn"))
    parser.add_argument("--headless", action="store_true",
                        help="Run browser headless (less reliable on first run)")
    parser.add_argument("--storage-state",
                        default="data/raw/state_air_permits/mn/.session.json",
                        help="Where to persist Playwright storage state")
    args = parser.parse_args()

    scraper = MNScraper(
        output_dir=args.output_dir,
        storage_state_path=Path(args.storage_state),
        headless=args.headless,
    )

    print(f"Searching MN bulk CSV for {args.name!r} in {args.city!r}...")
    ref = scraper.find_facility(args.name, args.city)
    if ref is None:
        print("  Not found.")
        return 1

    print(f"Found: {ref.name} (site_id={ref.site_id}, city={ref.city})")
    print(f"  matches in bulk CSV: {ref.extra.get('_match_count')}")
    print(f"  programs: {ref.extra.get('program_name_list')}")
    print()
    print("Fetching metadata from WIMN...")
    meta = scraper.fetch_metadata(ref)
    meta_path = scraper.write_metadata(ref, meta)
    print(f"  wrote {meta_path}")
    for k in ("activities", "permit_actions", "inspection_actions"):
        items = meta.get(k) or []
        print(f"  {k}: {len(items)} records")

    print()
    print("Deriving Air Quality document index...")
    docs = scraper.fetch_documents(ref)
    docs_path = scraper.write_documents_index(ref, docs)
    print(f"  wrote {docs_path}")
    for d in docs:
        print(f"  - {d.action_date}  {d.action_type:<35}  {d.permit_id}")

    print()
    print("Note: MPCA /documents endpoint returns 500 globally as of 2026-05-09.")
    print("PDFs require an Information Request to MPCA. Use the index above.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
