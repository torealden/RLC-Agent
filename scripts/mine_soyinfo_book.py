"""
Mine a SoyInfo Center bibliography PDF for facts about specific
operators / plants / topics.

The SoyInfo Center publishes 200+ free annotated bibliographies, one
per major topic (cooperative soybean processing, individual companies,
individual oils, individual countries). Each book is structured as a
numbered bibliography:

    1. Citation. Author. Year. Title. ...
       Summary: [paragraph of facts]
    2. ...

This tool extracts entries matching one or more keywords and outputs
them grouped by plant / company. Designed to be reusable for ADM,
Cargill, Bunge, etc. once we download their books.

Usage:
    python scripts/mine_soyinfo_book.py \
        --input domain_knowledge/company_reports/agp/soyinfo_full_text.txt \
        --output domain_knowledge/company_reports/agp/soyinfo_entries.json \
        --keywords "AGP,Ag Processing,Boone Valley,Dawson Mills" \
        --plant-keywords "Eagle Grove,Sergeant Bluff,Algona,..."

The output is a JSON file with one entry per matched bibliographic
record:
    {
      "id": 817,
      "citation": "AGP News (Omaha, Nebraska). 1997. Grand opening...",
      "year": 1997,
      "matched_keywords": ["Emmetsburg", "AGP"],
      "matched_plants": ["ia.agp_emmetsburg"],
      "summary": "...",
      "source_pages": [56, 57]
    }
"""
from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass, asdict, field
from pathlib import Path


@dataclass
class Entry:
    id: int
    citation: str
    year: int | None
    summary: str
    matched_keywords: list[str] = field(default_factory=list)
    matched_plants: list[str] = field(default_factory=list)
    source_pages: list[int] = field(default_factory=list)


# Per-operator plant keyword → facility_id maps. Each phrase is a
# substring match; longer/more specific phrases first. Use carefully —
# bare city names trigger false positives (e.g., "Decatur" matches
# both ADM Decatur IL and ADM Decatur AL).
AGP_PLANT_MAP = {
    "Eagle Grove": "ia.agp_eagle_grove",
    "Sergeant Bluff": "ia.agp_sergeant_bluff",
    "Algona": "ia.agp_algona",
    "Mason City": "ia.agp_mason_city",
    "Manning": "ia.agp_manning",
    "Sheldon": "ia.agp_sheldon",
    "Emmetsburg": "ia.agp_emmetsburg",
    "Dawson, Minn": "mn.agp_dawson",
    "Dawson Mills": "mn.agp_dawson",
    "Tri-County Soy Bean": "mn.agp_dawson",
    "Tri-Country Soy Bean": "mn.agp_dawson",
    "St. Joseph, Mo": "mo.agp_st_joseph",
    "St Joseph, Mo": "mo.agp_st_joseph",
    "Hastings, Neb": "ne.agp_hastings",
    "David City": "ne.agp_david_city",
    "Aberdeen, S.D": "sd.agp_aberdeen",
    "Aberdeen, South Dakota": "sd.agp_aberdeen",
    "Van Buren, Ark": "ar.ag_processing_van_buren",
}

# ADM plants in our DB (24). City names paired with state to reduce
# collisions (e.g., Decatur IL vs Decatur AL).
ADM_PLANT_MAP = {
    "Helena, Ark": "ar.adm_helena",
    "Little Rock": "ar.adm_little_rock",
    "Valdosta": "ga.adm_valdosta",
    "Des Moines, Iowa": "ia.adm_des_moines",
    "Champaign, Ill": "il.adm_champaign",
    "Decatur, Ill": "il.adm_decatur",
    "Decatur, Illinois": "il.adm_decatur",
    "Galesburg, Ill": "il.adm_galesburg",
    "Granite City": "il.adm_granite_city",
    "Quincy, Ill": "il.adm_quincy",
    "Taylorville, Ill": "il.adm_taylorville",
    "Frankfort, Ind": "in.adm_frankfort",
    "Fredonia, Kan": "ks.adm_fredonia",
    "Mankato, Minn": "mn.adm_mankato",
    "Deerfield, Mo": "mo.adm_deerfield",
    "Kansas City, Mo": "mo.adm_kansas_city",
    "Mexico, Mo": "mo.adm_mexico",
    "Mexico, Missouri": "mo.adm_mexico",
    "Clarksville, Miss": "ms.adm_clarksville",
    "Enderlin": "nd.adm_enderlin",
    "Spiritwood": "nd.adm_spiritwood",
    "Velva": "nd.adm_velva",
    "Fremont, Neb": "ne.adm_fremont",
    "Lincoln, Neb": "ne.adm_lincoln",
    "Fostoria": "oh.adm_fostoria",
    "Kershaw": "sc.adm_kershaw",
}

OPERATOR_PLANT_MAPS = {
    "agp": AGP_PLANT_MAP,
    "adm": ADM_PLANT_MAP,
}

DEFAULT_PLANT_MAP = AGP_PLANT_MAP  # backward-compat


def parse_page_index(text: str) -> dict[int, int]:
    """Build char-offset → page-number index from === PAGE N === markers."""
    page_starts: dict[int, int] = {}  # char-offset → page
    for m in re.finditer(r"=== PAGE (\d+) ===", text):
        page_starts[m.start()] = int(m.group(1))
    return page_starts


def offset_to_page(offset: int, page_starts: dict[int, int]) -> int:
    last = 0
    for k in sorted(page_starts):
        if k > offset:
            return last
        last = page_starts[k]
    return last


def extract_entries(
    text: str, *, min_summary_len: int = 60
) -> list[Entry]:
    """
    Walk text looking for `^\\d+\\.\\s+[A-Z]` line starts and extract
    each entry up to the next entry boundary.
    """
    page_starts = parse_page_index(text)
    # Find all entry starts (numbered)
    starts = list(re.finditer(r"(?<=\n)(\d{1,4})\.\s+([A-Z][^\n]+)", text))

    # Also find section break markers (the book uses ALL CAPS title
    # blocks between bibliography sections; those reset numbering)
    entries: list[Entry] = []
    for i, m in enumerate(starts):
        eid = int(m.group(1))
        first_line = m.group(2)
        # Skip front-matter "1. Read the Introduction" style entries
        if eid <= 5 and any(kw in first_line for kw in [
            "Read the Introduction", "Search the book", "Use the indexes",
        ]):
            continue
        body_start = m.start()
        body_end = starts[i + 1].start() if i + 1 < len(starts) else len(text)
        block = text[body_start:body_end]
        # Strip page-break artifacts
        block = re.sub(r"=== PAGE \d+ ===\s*", " ", block)
        # Try to split citation from summary on "•" or "Summary:" markers
        m_summary = re.search(r"(?:•\s*Summary:|Summary:)\s*", block)
        if m_summary:
            citation = block[:m_summary.start()].strip()
            summary = block[m_summary.end():].strip()
        else:
            citation = block.strip()
            summary = ""
        # Compress whitespace
        citation = re.sub(r"\s+", " ", citation).strip()
        summary = re.sub(r"\s+", " ", summary).strip()
        # Extract year: look for 4-digit year in citation
        year_match = re.search(r"\b(19\d{2}|20\d{2})\b", citation[:200])
        year = int(year_match.group(1)) if year_match else None
        page = offset_to_page(body_start, page_starts)
        entries.append(Entry(
            id=eid,
            citation=citation[:600],
            year=year,
            summary=summary[:3000],
            source_pages=[page],
        ))
    return entries


def filter_by_keywords(
    entries: list[Entry],
    *,
    keywords: list[str],
    plant_map: dict[str, str],
) -> list[Entry]:
    """Tag entries with matched_keywords + matched_plants. Return matches."""
    matched: list[Entry] = []
    for e in entries:
        haystack = (e.citation + " " + e.summary).lower()
        kw_hits = [kw for kw in keywords if kw.lower() in haystack]
        plant_hits = []
        for plant_kw, fid in plant_map.items():
            if plant_kw.lower() in haystack:
                plant_hits.append(fid)
        if kw_hits or plant_hits:
            e.matched_keywords = kw_hits
            e.matched_plants = sorted(set(plant_hits))
            matched.append(e)
    return matched


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", type=Path, required=True,
                    help="Path to extracted PDF text")
    ap.add_argument("--output", type=Path, required=True,
                    help="Output JSON path")
    ap.add_argument("--keywords", type=str, required=True,
                    help="Comma-separated keywords (e.g. 'AGP,Ag Processing')")
    ap.add_argument("--operator", choices=list(OPERATOR_PLANT_MAPS),
                    default="agp",
                    help="Which operator's plant map to use")
    args = ap.parse_args()

    text = args.input.read_text(encoding="utf-8")
    print(f"Read {len(text):,} chars from {args.input}")

    all_entries = extract_entries(text)
    print(f"Parsed {len(all_entries)} bibliographic entries")

    keywords = [k.strip() for k in args.keywords.split(",") if k.strip()]
    plant_map = OPERATOR_PLANT_MAPS[args.operator]
    matches = filter_by_keywords(
        all_entries,
        keywords=keywords,
        plant_map=plant_map,
    )
    print(f"Filtered to {len(matches)} matching entries")

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8") as f:
        json.dump([asdict(e) for e in matches], f, indent=2, default=str)
    print(f"Wrote {args.output}")

    # Summary breakdown by plant
    from collections import Counter
    plant_counts = Counter()
    for e in matches:
        for p in e.matched_plants:
            plant_counts[p] += 1
    print()
    print("Match counts by plant:")
    for p, c in sorted(plant_counts.items(), key=lambda x: -x[1]):
        print(f"  {p:35} {c}")


if __name__ == "__main__":
    main()
