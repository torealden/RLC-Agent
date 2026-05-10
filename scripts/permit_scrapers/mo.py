"""
Missouri DNR Air Pollution Control Program permit scraper.

Strategy:
    1. Search the public Issued Air Permits index by company name:
       https://dnr.mo.gov/air/business-industry/permits/issued?combine=<name>
       Returns an HTML list of permit detail pages.

    2. For each permit detail page, extract:
        - Permit Number, Application/Project Number, Site ID
        - Permit Display Type (e.g. "Operating Permit: Part 70")
        - Permit Status, Date Issued
        - City, County
        - **Direct PDF URL** (no Information Request needed!)

    3. Download the PDF directly to data/raw/state_air_permits/mo/...

MO DNR is much friendlier than MN MPCA:
    - Static HTML (no Angular SPA)
    - No Radware bot wall (raw curl/requests works)
    - PDFs are publicly downloadable from /sites/dnr/files/vfc/<year>/<month>/main/

This means a single MO scraper run can fetch full permit text for the
existing Ollama equipment-extractor pipeline — no IR queue needed.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from urllib.parse import quote, urljoin

import requests
from bs4 import BeautifulSoup

from scripts.permit_scrapers.base import (
    DocumentRef,
    FacilityRef,
    PermitScraperBase,
    jitter_sleep,
)


SITE = "https://dnr.mo.gov"
SEARCH_URL = SITE + "/air/business-industry/permits/issued"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


# ---------------------------------------------------------------------------
# Search + parse helpers (no browser needed)
# ---------------------------------------------------------------------------

@dataclass
class PermitRef:
    """A single MO DNR permit. Each permit is its own URL/page."""
    title: str
    href: str
    permit_number: Optional[str] = None
    sub_facility: Optional[str] = None  # "main" / "biodiesel" / etc.


def search_permits(session: requests.Session, query: str) -> list[PermitRef]:
    """Hit the issued-permits index with combine=<query>."""
    r = session.get(SEARCH_URL, params={"combine": query}, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    refs: list[PermitRef] = []
    # Permit detail links match /air/business-industry/air-permits/...
    for a in soup.select('a[href^="/air/business-industry/air-permits/"]'):
        href = a.get("href")
        title = (a.get_text() or "").strip()
        # Permit number is the last comma-delimited token in the title
        m = re.search(r",\s*([A-Z0-9\-]+)\s*$", title)
        permit_number = m.group(1) if m else None
        # Sub-facility hint (e.g. " Biodiesel ")
        sub = None
        if "biodiesel" in title.lower():
            sub = "biodiesel"
        refs.append(PermitRef(
            title=title,
            href=urljoin(SITE, href),
            permit_number=permit_number,
            sub_facility=sub,
        ))
    return refs


@dataclass
class PermitDetail:
    """Parsed metadata from a MO DNR permit detail page."""
    permit_number: Optional[str] = None
    application_number: Optional[str] = None
    site_id: Optional[str] = None
    permit_type: Optional[str] = None
    permit_status: Optional[str] = None
    date_issued: Optional[str] = None
    city: Optional[str] = None
    county: Optional[str] = None
    pdf_url: Optional[str] = None
    page_url: Optional[str] = None


_FIELD_LABELS = {
    "Permit Number": "permit_number",
    "Application/ Project Number": "application_number",
    "Application/Project Number": "application_number",
    "Site ID": "site_id",
    "Permit Display Type": "permit_type",
    "Permit Status": "permit_status",
    "Date issued": "date_issued",
    "City": "city",
    "County": "county",
}


def parse_permit_page(html: str, page_url: str) -> PermitDetail:
    soup = BeautifulSoup(html, "html.parser")
    detail = PermitDetail(page_url=page_url)

    # Each metadata field is in a div like:
    #   <div class="field__label">Permit Number</div>
    #   <div class="field__item">OP2020-020</div>
    for label_div in soup.select("div.field__label"):
        label = (label_div.get_text() or "").strip()
        attr = _FIELD_LABELS.get(label)
        if not attr:
            continue
        # Find the sibling/parent that contains the value
        parent = label_div.parent
        if parent is None:
            continue
        items = parent.select("div.field__item")
        if not items:
            continue
        # First item — could be a <time> or plain text
        text = items[0].get_text(strip=True)
        if text and getattr(detail, attr) is None:
            setattr(detail, attr, text)

    # PDF URL is in the iframe's data-src or in a direct <a> link
    iframe = soup.select_one("iframe.pdf")
    if iframe:
        detail.pdf_url = iframe.get("data-src")
        if not detail.pdf_url:
            # Sometimes the URL is embedded in src= as a viewer query param
            src = iframe.get("src") or ""
            m = re.search(r"file=([^#&]+)", src)
            if m:
                from urllib.parse import unquote
                detail.pdf_url = unquote(m.group(1))

    return detail


# ---------------------------------------------------------------------------
# Scraper
# ---------------------------------------------------------------------------

class MOScraper(PermitScraperBase):
    state_code = "MO"

    def __init__(self, *, output_dir: Path, **kw):
        super().__init__(output_dir=output_dir, **kw)
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": USER_AGENT})

    def find_facility(
        self,
        name: str,
        city: Optional[str] = None,
        state: Optional[str] = None,
    ) -> Optional[FacilityRef]:
        """For MO we group all permits matching `name` (and optional `city`)
        into a single FacilityRef. The detail walking happens in
        fetch_documents."""
        permits = search_permits(self._session, name)
        if city:
            permits = [
                p for p in permits if city.lower() in p.title.lower()
            ]
        if not permits:
            return None
        # Use first permit's site as canonical; the caller can group
        # by sub-facility downstream.
        # Site ID we'll fetch from the first permit detail.
        first = permits[0]
        r = self._session.get(first.href, timeout=30)
        r.raise_for_status()
        d = parse_permit_page(r.text, first.href)
        site_id = d.site_id or "unknown"
        return FacilityRef(
            state="MO",
            site_id=site_id,
            name=first.title.split(",")[0].strip(),
            city=d.city,
            address=None,
            extra={
                "permit_count": len(permits),
                "permits": [
                    {"title": p.title, "href": p.href,
                     "permit_number": p.permit_number,
                     "sub_facility": p.sub_facility}
                    for p in permits
                ],
                "first_permit_detail": {
                    "permit_number": d.permit_number,
                    "site_id": d.site_id,
                    "permit_type": d.permit_type,
                    "date_issued": d.date_issued,
                    "county": d.county,
                },
            },
        )

    def fetch_metadata(self, ref: FacilityRef) -> dict:
        """Walk every permit page and collect detail metadata."""
        permits = ref.extra.get("permits") or []
        details = []
        for p in permits:
            r = self._session.get(p["href"], timeout=30)
            r.raise_for_status()
            d = parse_permit_page(r.text, p["href"])
            details.append({
                "title": p["title"],
                "page_url": d.page_url,
                "pdf_url": d.pdf_url,
                "permit_number": d.permit_number,
                "application_number": d.application_number,
                "site_id": d.site_id,
                "permit_type": d.permit_type,
                "permit_status": d.permit_status,
                "date_issued": d.date_issued,
                "city": d.city,
                "county": d.county,
                "sub_facility": p["sub_facility"],
            })
            jitter_sleep(0.3, 0.8)  # be polite
        return {
            "site_id": ref.site_id,
            "facility_name": ref.name,
            "city": ref.city,
            "permits": details,
        }

    def fetch_documents(self, ref: FacilityRef) -> list[DocumentRef]:
        meta = self.fetch_metadata(ref)
        out: list[DocumentRef] = []
        for p in meta["permits"]:
            out.append(DocumentRef(
                title=p["title"],
                url=p["pdf_url"],
                program="Air Quality",
                permit_id=p["permit_number"],
                action_date=p["date_issued"],
                action_type=p["permit_type"],
                extra={
                    "page_url": p["page_url"],
                    "site_id": p["site_id"],
                    "application_number": p["application_number"],
                    "county": p["county"],
                    "permit_status": p["permit_status"],
                    "sub_facility": p["sub_facility"],
                },
            ))
        return out

    def download_document(self, doc: DocumentRef, dest: Path) -> Optional[Path]:
        if not doc.url:
            return None
        r = self._session.get(doc.url, timeout=120, stream=True)
        r.raise_for_status()
        dest.parent.mkdir(parents=True, exist_ok=True)
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=64 * 1024):
                if chunk:
                    f.write(chunk)
        return dest


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Scrape MO DNR for air permit metadata + PDFs."
    )
    parser.add_argument("--name", required=True,
                        help="Facility name search keyword (e.g. 'Ag Processing')")
    parser.add_argument("--city",
                        help="City filter (e.g. 'St. Joseph')")
    parser.add_argument("--output-dir", type=Path,
                        default=Path("data/raw/state_air_permits/mo"))
    parser.add_argument("--download-pdfs", action="store_true",
                        help="Also download each permit's PDF")
    parser.add_argument("--latest-only", action="store_true",
                        help="When downloading, fetch only the most recent "
                             "permit per sub-facility (crush vs biodiesel)")
    args = parser.parse_args()

    scraper = MOScraper(output_dir=args.output_dir)

    print(f"Searching MO DNR for {args.name!r}{' city='+args.city if args.city else ''}...")
    ref = scraper.find_facility(args.name, args.city)
    if ref is None:
        print("  No match.")
        return 1

    print(f"Found: {ref.name}  site_id={ref.site_id}  city={ref.city}")
    print(f"  permits: {ref.extra.get('permit_count')}")
    print(f"  first permit: {ref.extra.get('first_permit_detail')}")
    print()
    print("Fetching all permit detail pages...")
    meta = scraper.fetch_metadata(ref)
    meta_path = scraper.write_metadata(ref, meta)
    print(f"  wrote {meta_path}")

    docs = scraper.fetch_documents(ref)
    docs_path = scraper.write_documents_index(ref, docs)
    print(f"  wrote {docs_path}")
    print()
    print("Permits:")
    for d in docs:
        sub = d.extra.get("sub_facility") or "main"
        url_short = (d.url or "")[:80]
        print(f"  [{sub:>9}] {d.action_date}  {d.permit_id:<15} "
              f"{d.action_type:<35}  pdf: {url_short}")

    if args.download_pdfs:
        # Group by sub_facility, optionally filter to latest Part 70 only.
        # Note: MO date strings are MM/DD/YYYY; convert to YYYY-MM-DD
        # before comparing or string sort is wrong (e.g. "12/24/2014"
        # would beat "10/28/2024").
        def _date_key(d_str: Optional[str]) -> str:
            if not d_str:
                return ""
            m = re.match(r"(\d{2})/(\d{2})/(\d{4})", d_str)
            if m:
                return f"{m.group(3)}-{m.group(1)}-{m.group(2)}"
            return d_str

        if args.latest_only:
            # Prefer the most recent Operating Permit: Part 70 per sub.
            # Fall back to most recent of any type if no Part 70 exists.
            def _is_part70(d: DocumentRef) -> bool:
                t = (d.action_type or "").lower()
                return "operating permit: part 70" in t

            by_sub_p70: dict[str, DocumentRef] = {}
            by_sub_any: dict[str, DocumentRef] = {}
            for d in docs:
                sub = d.extra.get("sub_facility") or "main"
                if _is_part70(d):
                    if sub not in by_sub_p70 or _date_key(d.action_date) > _date_key(by_sub_p70[sub].action_date):
                        by_sub_p70[sub] = d
                if sub not in by_sub_any or _date_key(d.action_date) > _date_key(by_sub_any[sub].action_date):
                    by_sub_any[sub] = d
            to_download = []
            for sub in sorted(set(list(by_sub_p70) + list(by_sub_any))):
                to_download.append(by_sub_p70.get(sub) or by_sub_any[sub])
        else:
            to_download = [d for d in docs if d.url]
        print()
        print(f"Downloading {len(to_download)} PDFs...")
        for d in to_download:
            sub = d.extra.get("sub_facility") or "main"
            slug = re.sub(r"[^a-zA-Z0-9]+", "_", d.permit_id or "unknown")
            dest = scraper.facility_dir(ref.site_id) / "permit_pdfs" / sub / f"{slug}.pdf"
            try:
                scraper.download_document(d, dest)
                size = dest.stat().st_size
                print(f"  {sub}/{slug}.pdf  ({size:,} bytes)")
            except Exception as e:
                print(f"  FAILED {sub}/{slug}.pdf: {e}")
            jitter_sleep(0.5, 1.2)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
