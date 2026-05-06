"""
Extract handwritten annotations from price-chart PDF scans via Claude vision.

Walks either a single multi-page PDF or a directory of single-page PDFs,
renders each page to an image, sends to Claude Sonnet 4.6 with a structured-
output prompt, persists results to bronze.handwritten_chart_annotation +
silver.market_event_annotation.

Idempotent on (source_file_hash, page_number, extractor_version) — re-running
upserts existing rows.

Usage:
    # Single multi-page PDF (e.g., user scans all charts to one document)
    python -m scripts.extract_chart_annotations --pdf path/to/all_charts.pdf

    # Directory of single-page PDFs (e.g., one chart per file)
    python -m scripts.extract_chart_annotations --dir domain_knowledge/special_situations

    # Smoke-test on a single page
    python -m scripts.extract_chart_annotations --pdf X.pdf --pages 1 --dry-run

Cost: ~$0.05-0.10 per chart on Sonnet 4.6 vision.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import io
import json
import logging
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
load_dotenv()

import psycopg2.extras

try:
    import anthropic
except ImportError:
    sys.exit('pip install anthropic')

try:
    import fitz  # PyMuPDF
except ImportError:
    sys.exit('pip install PyMuPDF')

try:
    from PIL import Image
except ImportError:
    sys.exit('pip install Pillow')

from src.services.database.db_config import get_connection


logger = logging.getLogger('extract_chart_annotations')
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


MODEL = 'claude-sonnet-4-6'
EXTRACTOR_VERSION = 'v1'
MARKET_ID = 'us_oilseed_crush'

# Render PDF pages at this DPI for the vision call. 200 is a good balance —
# higher catches faint pencil but bumps token cost; lower loses detail.
RENDER_DPI = 200

SYSTEM_PROMPT = """You are an analyst at Round Lakes Commodities, transcribing handwritten annotations on commodity-futures price charts.

The chart is a daily price chart for a specific futures contract (e.g., July 2015 soybeans = SN15). The user has annotated significant price moves with handwritten notes describing the market-moving event/news at that date — USDA reports, weather, fund positioning, crush pace, geopolitics, etc.

Your job: read the chart and return STRUCTURED JSON with:

1. chart_metadata
2. top_strip_numbers — numbers along the TOP edge of the chart (typically USDA report releases with deviation from expectations, e.g., "3927 +14" means USDA report was 3,927 with a +14 deviation from prior expected)
3. bottom_strip_numbers — numbers along the BOTTOM edge (typically running carryout/ending-stocks estimates over time, in million bushels)
4. annotations — a list of the diagonal/cursive notes inside the chart, with:
   - verbatim_text: as written, even if abbreviated
   - approximate_date_label: nearest x-axis label (e.g. "Aug-14")
   - approximate_date: best-effort YYYY-MM-DD (use day 15 of the month if you can only narrow to month)
   - estimated_topic: ONE of [weather, soybean_supply, veg_oil_demand, meal_livestock_demand, policy_federal, policy_state_local, policy_industry, competitor_activity, positioning, other]. Use "positioning" for fund / managed money / commitment-of-traders content. Use "other" for anything that doesn't fit the eight market_field topics + positioning.
   - estimated_polarity: -1 (very bearish) to +1 (very bullish) for the commodity in question
   - estimated_intensity: 0 to 1 (how much this single event would move the market)
   - confidence: 0 to 1 (how certain you are in your reading + classification)

Return ONLY a JSON object, no preamble:

{
  "chart_metadata": {
    "contract": "SN15",
    "commodity": "soybeans",
    "period_start": "2014-05-01",
    "period_end": "2015-07-31",
    "source": "DTN ProphetX",
    "price_scale_min": 925.0,
    "price_scale_max": 1275.0
  },
  "top_strip_numbers": [
    {"approximate_date_label": "May-14", "raw_number": 3635, "raw_deviation": null, "notes": "earliest report"},
    {"approximate_date_label": "Jul-14", "raw_number": 3800, "raw_deviation": 165, "notes": "+165 vs expected"}
  ],
  "bottom_strip_numbers": [
    {"approximate_date_label": "May-14", "value": 330},
    {"approximate_date_label": "Jun-14", "value": 325}
  ],
  "annotations": [
    {
      "verbatim_text": "Higher than Expected average",
      "approximate_date_label": "Jul-14",
      "approximate_date": "2014-07-15",
      "estimated_topic": "weather",
      "estimated_polarity": -0.4,
      "estimated_intensity": 0.6,
      "confidence": 0.8,
      "notes": "yield/weather call ahead of harvest"
    }
  ]
}

If a number, label, or date is illegible, set the field to null and lower the confidence accordingly. Do not invent values.
"""


def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    h.update(path.read_bytes())
    return h.hexdigest()


def render_page_to_b64(pdf_path: Path, page_num_0based: int, dpi: int = RENDER_DPI) -> str:
    """Render one page to a base64-encoded PNG."""
    doc = fitz.open(pdf_path)
    try:
        page = doc[page_num_0based]
        zoom = dpi / 72.0
        mat = fitz.Matrix(zoom, zoom)
        pix = page.get_pixmap(matrix=mat)
        png_bytes = pix.tobytes('png')
    finally:
        doc.close()
    return base64.standard_b64encode(png_bytes).decode('ascii')


def extract_one_page(client, pdf_path: Path, page_num_0based: int) -> Optional[dict]:
    """Send one page to Claude, return parsed JSON or None on failure."""
    img_b64 = render_page_to_b64(pdf_path, page_num_0based)
    logger.info(f'  rendered {pdf_path.name} page {page_num_0based + 1} '
                f'({len(img_b64) * 3 // 4 // 1024} KB)')
    try:
        resp = client.messages.create(
            model=MODEL,
            max_tokens=4000,
            system=SYSTEM_PROMPT,
            messages=[{
                'role': 'user',
                'content': [
                    {'type': 'image',
                     'source': {'type': 'base64', 'media_type': 'image/png',
                                'data': img_b64}},
                    {'type': 'text',
                     'text': 'Extract chart_metadata, top_strip_numbers, '
                             'bottom_strip_numbers, and annotations as the '
                             'JSON schema in the system prompt.'},
                ],
            }],
        )
    except anthropic.APIError as e:
        logger.error(f'  Claude error on {pdf_path.name} page {page_num_0based + 1}: {e}')
        return None

    text = resp.content[0].text.strip() if resp.content else ''
    if not text.startswith('{'):
        i, j = text.find('{'), text.rfind('}')
        if i >= 0 and j > i:
            text = text[i:j + 1]
    try:
        parsed = json.loads(text)
        parsed['_usage'] = {
            'input_tokens': resp.usage.input_tokens,
            'output_tokens': resp.usage.output_tokens,
        }
        return parsed
    except json.JSONDecodeError as e:
        logger.warning(f'  JSON parse failed for {pdf_path.name} page '
                       f'{page_num_0based + 1}: {e}')
        return None


def parse_date_label(label: Optional[str]) -> Optional[str]:
    """'Jul-14' -> '2014-07-15' (day 15 placeholder). Returns YYYY-MM-DD or None."""
    if not label:
        return None
    m = re.match(r'^([A-Za-z]+)-(\d{2,4})$', label.strip())
    if not m:
        return None
    month_str, year_str = m.groups()
    try:
        month = datetime.strptime(month_str[:3], '%b').month
    except ValueError:
        return None
    year = int(year_str)
    if year < 100:
        year = 2000 + year if year < 70 else 1900 + year
    return f'{year:04d}-{month:02d}-15'


def persist(pdf_path: Path, page_num: int, file_hash: str, parsed: dict) -> int:
    """Write annotations + numbers to bronze, then derive silver event rows.
    Returns number of bronze rows persisted."""
    meta = parsed.get('chart_metadata') or {}
    annotations = parsed.get('annotations') or []
    top_strip = parsed.get('top_strip_numbers') or []
    bottom_strip = parsed.get('bottom_strip_numbers') or []

    # Single bronze row per "annotation entity" — three sources:
    #  1. diagonal annotations (richest content)
    #  2. top strip numbers (USDA reports)
    #  3. bottom strip numbers (carryout estimates)
    rows = []
    idx = 0
    for a in annotations:
        rows.append({
            'position_on_chart': 'diagonal',
            'verbatim_text': a.get('verbatim_text') or '',
            'approximate_date_label': a.get('approximate_date_label'),
            'approximate_date': a.get('approximate_date') or parse_date_label(
                a.get('approximate_date_label')),
            'raw_number': None, 'raw_deviation': None,
            'topic': a.get('estimated_topic'),
            'polarity': a.get('estimated_polarity'),
            'intensity': a.get('estimated_intensity'),
            'confidence': a.get('confidence'),
        })
    for n in top_strip:
        idx += 1
        rows.append({
            'position_on_chart': 'top',
            'verbatim_text': f"USDA report {n.get('raw_number')} "
                             f"deviation {n.get('raw_deviation', '')}".strip(),
            'approximate_date_label': n.get('approximate_date_label'),
            'approximate_date': parse_date_label(n.get('approximate_date_label')),
            'raw_number': n.get('raw_number'),
            'raw_deviation': n.get('raw_deviation'),
            'topic': 'soybean_supply',
            'polarity': None, 'intensity': None, 'confidence': None,
        })
    for n in bottom_strip:
        idx += 1
        rows.append({
            'position_on_chart': 'bottom',
            'verbatim_text': f"carryout estimate {n.get('value')}",
            'approximate_date_label': n.get('approximate_date_label'),
            'approximate_date': parse_date_label(n.get('approximate_date_label')),
            'raw_number': n.get('value'),
            'raw_deviation': None,
            'topic': 'soybean_supply',
            'polarity': None, 'intensity': None, 'confidence': None,
        })

    bronze_ids = []
    with get_connection() as conn:
        cur = conn.cursor()
        for i, r in enumerate(rows):
            cur.execute("""
                INSERT INTO bronze.handwritten_chart_annotation
                    (source_file, source_file_hash, page_number,
                     chart_contract, chart_commodity, chart_period_start,
                     chart_period_end, chart_source,
                     annotation_index, verbatim_text, position_on_chart,
                     approximate_date, approximate_date_label,
                     raw_number, raw_deviation,
                     extractor_model, extractor_version, raw_response, extracted_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, NOW())
                ON CONFLICT (source_file_hash, page_number, annotation_index, extractor_version)
                DO UPDATE SET
                    verbatim_text = EXCLUDED.verbatim_text,
                    position_on_chart = EXCLUDED.position_on_chart,
                    approximate_date = EXCLUDED.approximate_date,
                    raw_number = EXCLUDED.raw_number,
                    raw_deviation = EXCLUDED.raw_deviation,
                    raw_response = EXCLUDED.raw_response,
                    extracted_at = NOW()
                RETURNING id
            """, (
                str(pdf_path), file_hash, page_num,
                meta.get('contract'), meta.get('commodity'),
                meta.get('period_start'), meta.get('period_end'),
                meta.get('source'),
                i, r['verbatim_text'], r['position_on_chart'],
                r['approximate_date'], r['approximate_date_label'],
                r['raw_number'], r['raw_deviation'],
                MODEL, EXTRACTOR_VERSION, json.dumps(parsed, default=str),
            ))
            ret = cur.fetchone()
            bronze_id = ret['id'] if isinstance(ret, dict) else ret[0]
            bronze_ids.append((bronze_id, r))

        # Silver rows for diagonal annotations only (numerical strips not events)
        for bronze_id, r in bronze_ids:
            if r['position_on_chart'] != 'diagonal':
                continue
            if not r['approximate_date'] or not r['topic']:
                continue
            cur.execute("""
                INSERT INTO silver.market_event_annotation
                    (bronze_id, market_id, event_date, event_text, topic_key,
                     estimated_polarity, estimated_intensity,
                     propagation_scope, confidence)
                VALUES (%s, %s, %s, %s, %s, %s, %s, 'national', %s)
                ON CONFLICT (bronze_id) DO UPDATE SET
                    event_date         = EXCLUDED.event_date,
                    event_text         = EXCLUDED.event_text,
                    topic_key          = EXCLUDED.topic_key,
                    estimated_polarity = EXCLUDED.estimated_polarity,
                    estimated_intensity = EXCLUDED.estimated_intensity,
                    confidence         = EXCLUDED.confidence,
                    cleaned_at         = NOW()
            """, (
                bronze_id, MARKET_ID, r['approximate_date'],
                r['verbatim_text'][:500], r['topic'],
                r['polarity'], r['intensity'], r['confidence'],
            ))
        conn.commit()

    return len(rows)


def find_pdfs(arg_pdf: Optional[Path], arg_dir: Optional[Path]) -> list[Path]:
    if arg_pdf and arg_dir:
        sys.exit('Pass either --pdf OR --dir, not both.')
    if arg_pdf:
        return [arg_pdf]
    if arg_dir:
        return sorted(arg_dir.rglob('*.pdf'))
    sys.exit('Pass --pdf <file.pdf> or --dir <directory>')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--pdf', type=Path, help='Single PDF (multi-page OK)')
    ap.add_argument('--dir', type=Path, help='Directory of PDFs')
    ap.add_argument('--pages', type=str, default='all',
                    help='Pages to process: "all" or comma-separated 1-based (e.g. "1,2,3")')
    ap.add_argument('--runs', type=int, default=1,
                    help='Run extraction N times per page for best-of-N variance '
                         'reduction. Each run uses extractor_version=v1-r<N>; the '
                         'consolidation script later picks consensus across runs.')
    ap.add_argument('--dry-run', action='store_true',
                    help='Show extracted JSON, do not write to DB')
    args = ap.parse_args()

    pdfs = find_pdfs(args.pdf, args.dir)
    if not pdfs:
        sys.exit('No PDFs found.')

    client = anthropic.Anthropic()
    total_pages = total_rows = 0
    total_in = total_out = 0

    for pdf_path in pdfs:
        try:
            file_hash = file_sha256(pdf_path)
            doc = fitz.open(pdf_path)
            n_pages = len(doc)
            doc.close()
        except Exception as e:
            logger.error(f'Skipping {pdf_path.name}: {e}')
            continue

        if args.pages == 'all':
            page_indices = list(range(n_pages))
        else:
            page_indices = [int(p.strip()) - 1 for p in args.pages.split(',')]

        logger.info(f'{pdf_path.name}: {n_pages} pages, processing {len(page_indices)} '
                    f'with {args.runs} run(s) each')

        for pi in page_indices:
            for run_idx in range(1, args.runs + 1):
                global EXTRACTOR_VERSION
                EXTRACTOR_VERSION = f'v1-r{run_idx}' if args.runs > 1 else 'v1'
                parsed = extract_one_page(client, pdf_path, pi)
                if parsed is None:
                    continue
                usage = parsed.get('_usage', {})
                total_in += usage.get('input_tokens') or 0
                total_out += usage.get('output_tokens') or 0

                if args.dry_run:
                    print(f'\n--- {pdf_path.name} page {pi + 1} run {run_idx}/{args.runs} ---')
                    print(json.dumps({k: v for k, v in parsed.items()
                                      if k != '_usage'}, indent=2, default=str))
                else:
                    n = persist(pdf_path, pi + 1, file_hash, parsed)
                    total_rows += n
                    logger.info(f'  page {pi + 1} run {run_idx}/{args.runs}: {n} bronze rows')
                total_pages += 1

    cost = total_in / 1_000_000 * 3.0 + total_out / 1_000_000 * 15.0
    logger.info(f'Done. {total_pages} pages processed, {total_rows} bronze rows persisted. '
                f'Tokens in={total_in:,} out={total_out:,}  ~${cost:.3f}')


if __name__ == '__main__':
    main()
