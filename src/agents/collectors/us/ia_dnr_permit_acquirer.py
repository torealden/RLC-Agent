"""
IA DNR Title V permit acquirer — first client of the document spine.

The IA DNR draft/final permits page is a STATIC list of facility -> direct PDF
links (/media/<id>/download). No portal/Playwright needed (unlike MN). This
acquirer scrapes that list, downloads each Title V PDF to the inbox, and
register()s it in bronze.source_documents as parse_status='pending'. The
generalized parse spine then drains the queue (local-LLM best-of-N -> bronze).

Bespoke FETCH (this file) + generalized everything-downstream (registry + spine).
"""

import re
import sys
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT))
import requests
from dotenv import load_dotenv
load_dotenv(ROOT / ".env")
from src.services.documents import registry

logger = logging.getLogger(__name__)

PAGE = "https://www.iowadnr.gov/environmental-protection/air-quality/operating-permits/draft-final-permits"
BASE = "https://www.iowadnr.gov"
INBOX = ROOT / "data" / "permits" / "ia_dnr"          # gitignored; DB tracks provenance
HEADERS = {"User-Agent": "Mozilla/5.0 (RLC-Agent permit acquirer)"}
_LINK = re.compile(r'<a[^>]+href="([^"]*media/\d+[^"]*download[^"]*)"[^>]*>(.*?)</a>', re.I | re.S)


def _slug(name: str) -> str:
    s = re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")
    return s[:80] or "unknown"


def _facility_name(raw_anchor: str) -> str:
    return re.sub(r"<[^>]+>", "", raw_anchor).strip()


class IADNRPermitAcquirer:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def list_permits(self):
        """Return [(facility_name, absolute_pdf_url)] from the static page."""
        html = self.session.get(PAGE, timeout=60).text
        out = []
        for href, anchor in _LINK.findall(html):
            name = _facility_name(anchor)
            if not name:
                continue
            url = href if href.startswith("http") else BASE + href
            out.append((name, url))
        return out

    def acquire(self, limit: Optional[int] = None):
        INBOX.mkdir(parents=True, exist_ok=True)
        permits = self.list_permits()
        if limit:
            permits = permits[:limit]
        downloaded = registered = skipped = failed = 0
        for name, url in permits:
            key = _slug(name)
            path = INBOX / f"{key}.pdf"
            try:
                if not path.exists() or path.stat().st_size == 0:
                    r = self.session.get(url, timeout=120)
                    if r.status_code != 200 or not r.content:
                        failed += 1
                        self.logger.warning(f"download failed {r.status_code}: {name}")
                        continue
                    path.write_bytes(r.content)
                    downloaded += 1
                else:
                    skipped += 1
                # each register() opens/commits/closes its own connection so we
                # never hold a DB connection open across the slow downloads
                _id, is_new = registry.register(
                    "ia_dnr_titlev", "title_v_permit", source_key=key,
                    url=url, local_path=str(path), title=name)
                if is_new:
                    registered += 1
            except requests.RequestException as e:
                failed += 1
                self.logger.warning(f"error on {name}: {e}")
        return {"listed": len(permits), "downloaded": downloaded,
                "already_had": skipped, "newly_registered": registered, "failed": failed}

    def collect(self, **kwargs):
        from dataclasses import dataclass, field as dc_field
        @dataclass
        class _Result:
            success: bool = False
            source: str = "ia_dnr_permit_acquirer"
            records_fetched: int = 0
            error_message: Optional[str] = None
            warnings: list = dc_field(default_factory=list)
            collected_at: datetime = dc_field(default_factory=datetime.now)
        res = _Result()
        try:
            stats = self.acquire()
            res.records_fetched = stats["newly_registered"]
            res.success = stats["listed"] > 0
            res.warnings = [f"{stats}"]
        except Exception as e:
            res.error_message = str(e)
        return res


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=None)
    args = ap.parse_args()
    print(IADNRPermitAcquirer().acquire(limit=args.limit))


if __name__ == "__main__":
    main()
