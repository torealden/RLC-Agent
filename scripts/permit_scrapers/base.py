"""
Base classes + helpers for state-portal permit scrapers.

Each state subclasses PermitScraperBase and implements:
    - find_facility(name, city, state) -> FacilityRef
    - fetch_metadata(facility_ref) -> dict
    - fetch_documents(facility_ref) -> list[DocumentRef]
    - download_document(doc_ref, dest_path) -> Path

Common infrastructure (here):
    - Playwright browser + context with bot-friendly defaults.
    - Network response interception (so we can grab JSON from XHR).
    - Storage state persistence (reuse session across runs).
    - Random jitter and retry on transient errors.

Why Playwright instead of requests:
    - Many state environmental portals (MN MPCA in particular) sit
      behind Radware/PerimeterX bot walls. A real browser context
      with the JS engine and proper fingerprint passes; raw HTTP
      doesn't.
"""
from __future__ import annotations

import contextlib
import json
import random
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional

from playwright.sync_api import (
    Browser,
    BrowserContext,
    Page,
    Playwright,
    Response,
    sync_playwright,
)


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------

@dataclass
class FacilityRef:
    """Pointer to a facility in a state portal."""
    state: str
    site_id: str
    name: str
    city: Optional[str] = None
    address: Optional[str] = None
    extra: dict = field(default_factory=dict)


@dataclass
class DocumentRef:
    """Pointer to a permit document on a state portal."""
    title: str
    url: Optional[str]
    program: str  # e.g. "Air Quality"
    permit_id: Optional[str] = None
    action_date: Optional[str] = None
    action_type: Optional[str] = None
    extra: dict = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Browser session
# ---------------------------------------------------------------------------

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


class ScraperSession:
    """Wraps a Playwright Browser + Context with response capture."""

    def __init__(
        self,
        *,
        headless: bool = False,
        storage_state_path: Optional[Path] = None,
        user_agent: str = DEFAULT_USER_AGENT,
        viewport: tuple[int, int] = (1280, 900),
    ):
        # Note: many state portals' bot detection (Radware) accept
        # headless Chrome but reject raw HTTP. Headless=False is more
        # robust on first run; headless=True works once a storage
        # state is cached.
        self.headless = headless
        self.storage_state_path = storage_state_path
        self.user_agent = user_agent
        self.viewport = viewport
        self._pw: Optional[Playwright] = None
        self._browser: Optional[Browser] = None
        self._ctx: Optional[BrowserContext] = None

    def __enter__(self):
        self._pw = sync_playwright().start()
        self._browser = self._pw.chromium.launch(headless=self.headless)
        ctx_kwargs: dict = {
            "user_agent": self.user_agent,
            "viewport": {"width": self.viewport[0], "height": self.viewport[1]},
        }
        if self.storage_state_path and self.storage_state_path.exists():
            ctx_kwargs["storage_state"] = str(self.storage_state_path)
        self._ctx = self._browser.new_context(**ctx_kwargs)
        return self

    def __exit__(self, exc_type, exc, tb):
        if self.storage_state_path and self._ctx is not None:
            self.storage_state_path.parent.mkdir(parents=True, exist_ok=True)
            self._ctx.storage_state(path=str(self.storage_state_path))
        with contextlib.suppress(Exception):
            if self._ctx:
                self._ctx.close()
        with contextlib.suppress(Exception):
            if self._browser:
                self._browser.close()
        with contextlib.suppress(Exception):
            if self._pw:
                self._pw.stop()

    @property
    def context(self) -> BrowserContext:
        assert self._ctx is not None, "ScraperSession not entered"
        return self._ctx

    def new_page(self) -> Page:
        return self.context.new_page()


def capture_responses(
    page: Page,
    *,
    url_pattern: re.Pattern,
    on_match: Callable[[Response], None],
):
    """Attach a response listener that calls on_match for matching URLs."""

    def _handler(resp: Response):
        if url_pattern.search(resp.url):
            try:
                on_match(resp)
            except Exception:
                pass

    page.on("response", _handler)


# ---------------------------------------------------------------------------
# Polite-pacing helpers
# ---------------------------------------------------------------------------

def jitter_sleep(min_s: float = 0.8, max_s: float = 2.4):
    """Sleep a random interval. Helps avoid triggering rate limiters."""
    time.sleep(random.uniform(min_s, max_s))


def with_retry(
    fn: Callable,
    *,
    attempts: int = 3,
    backoff_s: float = 4.0,
    transient_predicate: Callable[[Exception], bool] = lambda e: True,
):
    """Retry fn() up to `attempts` times with exponential backoff."""
    last: Optional[Exception] = None
    for i in range(attempts):
        try:
            return fn()
        except Exception as e:
            last = e
            if not transient_predicate(e):
                raise
            if i + 1 < attempts:
                time.sleep(backoff_s * (2 ** i))
    assert last is not None
    raise last


# ---------------------------------------------------------------------------
# Scraper base class
# ---------------------------------------------------------------------------

class PermitScraperBase:
    """
    Subclasses implement:
        find_facility(name, city, state) -> FacilityRef
        fetch_metadata(facility_ref) -> dict
        fetch_documents(facility_ref) -> list[DocumentRef]
        download_document(doc_ref, dest_path) -> Path
    """

    state_code: str = ""

    def __init__(
        self,
        *,
        output_dir: Path,
        storage_state_path: Optional[Path] = None,
        headless: bool = False,
    ):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.storage_state_path = storage_state_path
        self.headless = headless

    def open_session(self) -> ScraperSession:
        return ScraperSession(
            headless=self.headless,
            storage_state_path=self.storage_state_path,
        )

    # --- Subclass hooks ---
    def find_facility(
        self, name: str, city: Optional[str] = None, state: Optional[str] = None
    ) -> Optional[FacilityRef]:
        raise NotImplementedError

    def fetch_metadata(self, ref: FacilityRef) -> dict:
        raise NotImplementedError

    def fetch_documents(self, ref: FacilityRef) -> list[DocumentRef]:
        raise NotImplementedError

    def download_document(self, doc: DocumentRef, dest: Path) -> Optional[Path]:
        raise NotImplementedError

    # --- Helpers ---
    def facility_dir(self, facility_id: str) -> Path:
        d = self.output_dir / facility_id
        d.mkdir(parents=True, exist_ok=True)
        return d

    def write_metadata(self, ref: FacilityRef, meta: dict):
        path = self.facility_dir(ref.site_id) / "metadata.json"
        path.write_text(json.dumps(meta, indent=2, default=str), encoding="utf-8")
        return path

    def write_documents_index(self, ref: FacilityRef, docs: list[DocumentRef]):
        path = self.facility_dir(ref.site_id) / "documents_index.json"
        items = [
            {
                "title": d.title,
                "url": d.url,
                "program": d.program,
                "permit_id": d.permit_id,
                "action_date": d.action_date,
                "action_type": d.action_type,
                "extra": d.extra,
            }
            for d in docs
        ]
        path.write_text(json.dumps(items, indent=2), encoding="utf-8")
        return path
