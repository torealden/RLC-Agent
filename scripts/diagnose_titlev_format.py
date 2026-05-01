"""
Diagnostic tool — given a Title V PDF, dump the document's anatomical
structure: section headings, page-by-page summaries, equipment list
sources. Used to compare permit formats across operators (AGP vs Cargill
vs Bunge, etc.) before patching the extraction filter.

Usage:
    python scripts/diagnose_titlev_format.py collectors/epa_echo/raw/agp_emmetsburg_titlev.pdf
"""
from __future__ import annotations

import argparse
import re
from pathlib import Path

import pdfplumber


HEADING_HINTS = [
    (r"^\s*Section\s+[\dIVXLC]+", "Section"),
    (r"^\s*\d+\.\d{1,3}\b", "Numbered subsection"),
    (r"^\s*Emission\s+Unit", "Emission Unit"),
    (r"^\s*EU[-\s]?\d", "EU-x"),
    (r"^\s*EP[-\s]?\d", "EP-x"),
    (r"^\s*Equipment\s+Description", "Equipment Description"),
    (r"^\s*Operating\s+Conditions", "Operating Conditions"),
    (r"^\s*Process\s+Description", "Process Description"),
    (r"^\s*Applicable\s+Requirements", "Applicable Requirements"),
    (r"^\s*Compliance", "Compliance"),
    (r"^\s*Monitoring", "Monitoring"),
    (r"^\s*Recordkeeping", "Recordkeeping"),
    (r"^\s*Reporting", "Reporting"),
    (r"^\s*Permit\s+Conditions", "Permit Conditions"),
    (r"^\s*Table\s+of\s+Contents", "TOC"),
    (r"^\s*Facility\s+Description", "Facility Description"),
    (r"^\s*Permittee", "Permittee"),
    (r"^\s*Insignificant", "Insignificant Activities"),
    (r"^\s*Generally\s+Applicable", "Generally Applicable"),
    (r"^\s*Title\s+V", "Title V Header"),
]

CAPACITY_HINTS = [
    (r"\d+(?:\.\d+)?\s*tons?/(?:hr|hour|day|year|yr)", "tons-rate"),
    (r"\d+(?:\.\d+)?\s*(?:bushels?|bu)/(?:hr|day)", "bushels-rate"),
    (r"\d+(?:\.\d+)?\s*MMBtu/hr", "MMBtu/hr"),
    (r"\d+(?:\.\d+)?\s*(?:gallons?|gal)/(?:hr|yr|year)", "gal-rate"),
    (r"\d+(?:\.\d+)?\s*(?:lb|pound)/(?:hr|day)", "lb-rate"),
    (r"\bRated\s+Capacity", "phrase: Rated Capacity"),
    (r"\bMaximum\s+(?:Design|Hourly)", "phrase: Maximum Design/Hourly"),
    (r"\bThroughput\s+limit", "phrase: Throughput limit"),
    (r"\bdesign\s+rate", "phrase: design rate"),
]

EQUIPMENT_KEYWORDS = [
    "boiler", "extractor", "desolventizer", "expander", "dehuller",
    "conditioner", "dryer", "scrubber", "baghouse", "cyclone", "RTO",
    "loadout", "receiving", "crusher", "deodorizer", "bleacher",
    "evaporator", "centrifuge", "press", "packaging",
]


def dump(pdf_path: Path, max_pages: int = 0) -> None:
    print(f"\n{'='*80}\n{pdf_path.name}\n{'='*80}")
    with pdfplumber.open(pdf_path) as pdf:
        n_total = len(pdf.pages)
        pages = pdf.pages if not max_pages else pdf.pages[:max_pages]
        print(f"Total pages: {n_total}")

        # Per-page summary: count of heading hits, capacity hits, equipment mentions
        for i, page in enumerate(pages):
            text = page.extract_text() or ""
            if not text.strip():
                continue
            heading_hits = []
            for pat, label in HEADING_HINTS:
                for m in re.finditer(pat, text, re.IGNORECASE | re.MULTILINE):
                    heading_hits.append((label, m.group(0).strip()[:60]))
                    if len(heading_hits) >= 3:
                        break
            cap_hits = sum(1 for pat,_ in CAPACITY_HINTS if re.search(pat, text, re.IGNORECASE))
            eq_kws = [kw for kw in EQUIPMENT_KEYWORDS if re.search(rf"\b{kw}\w*", text, re.IGNORECASE)]

            if heading_hits or cap_hits or eq_kws:
                print(f"\n--- Page {i+1} ---")
                if heading_hits:
                    for label, sample in heading_hits[:3]:
                        print(f"  HEADING [{label}]: {sample!r}")
                if cap_hits:
                    print(f"  capacity-pattern hits: {cap_hits}")
                if eq_kws:
                    print(f"  equipment keywords: {', '.join(eq_kws[:8])}")
                # First non-blank line of page (often the real heading)
                for line in text.splitlines():
                    if line.strip():
                        print(f"  first line: {line.strip()[:100]!r}")
                        break


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("pdf", type=Path)
    ap.add_argument("--max-pages", type=int, default=0)
    args = ap.parse_args()
    dump(args.pdf, args.max_pages)


if __name__ == "__main__":
    main()
