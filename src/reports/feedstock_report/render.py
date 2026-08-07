"""The Feedstock Report — renderer.

Two outputs per issue (handoff spec 2026-08-06):
  1. issue_{n}.html  — single file, inline CSS, INK/GOLD/PAPER brand,
     Georgia headers / Calibri body, ~720px content width.
  2. LinkedIn kit    — issue_{n}_linkedin/ with body.md ([IMAGE: name]
     placeholders) + brand-styled PNG exports of every table/chart at
     1400px wide (2x for 700px display).

Staleness rules (renderer-enforced, ruled 2026-08-06):
  - carried rows render the last actual print WITH its as-of date visible,
    never as a current-week price; w/w renders "—" across carried values
  - rows older than STALE_EXCLUDE_DAYS at the coverage close are excluded
    and listed on the one-line "coverage expanding" note
IFVS-008 gates run on the final artifacts; any failure aborts the write.
"""

from __future__ import annotations

import html as _html
import logging
import re
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / '.env')
from src.services.database.db_config import get_connection

from src.reports.feedstock_report.report_config import (
    INK, GOLD, PAPER, FONT_HEADER, FONT_BODY, MAX_CONTENT_WIDTH_PX,
    PNG_EXPORT_WIDTH_PX, SECTION_REGISTRY, STALE_EXCLUDE_DAYS,
    FEEDSTOCK_LABELS, CREDIT_INSTRUMENTS,
)
from src.reports.feedstock_report.gates import run_gates, GateError
from src.reports.feedstock_report.snapshot import get_issue

logger = logging.getLogger(__name__)

OUTPUT_ROOT = PROJECT_ROOT / 'output' / 'reports' / 'feedstock_report' / 'issues'

ARROW = {'up': '▲', 'down': '▼', 'flat': '—', 'new': '•'}
ARROW_WORD = {'up': 'up', 'down': 'down', 'flat': 'unchanged', 'new': 'new'}


# =============================================================
# Data loading
# =============================================================

def _load(issue_no: int) -> Dict[str, Any]:
    issue = get_issue(issue_no)
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""SELECT * FROM reports.feedstock_price_dashboard_snapshot
                           WHERE issue_id = %s ORDER BY feedstock_code, location""",
                        (issue['id'],))
            prices = [dict(r) for r in cur.fetchall()]
            cur.execute("""SELECT * FROM reports.feedstock_credit_stack_snapshot
                           WHERE issue_id = %s AND instrument IS NOT NULL
                           ORDER BY instrument""", (issue['id'],))
            credits = [dict(r) for r in cur.fetchall()]
            cur.execute("""SELECT * FROM reports.feedstock_section_content
                           WHERE issue_id = %s""", (issue['id'],))
            sections = {r['section_code']: dict(r) for r in cur.fetchall()}
            cur.execute("""SELECT * FROM reports.feedstock_news_items
                           WHERE issue_id = %s ORDER BY sort_order NULLS LAST, id""",
                        (issue['id'],))
            news = [dict(r) for r in cur.fetchall()]
    return {'issue': issue, 'prices': prices, 'credits': credits,
            'sections': sections, 'news': news}


def _classify(rows: List[Dict], close: date) -> Tuple[List[Dict], List[Dict], List[Dict]]:
    """Split rows into (renderable, excluded_stale, gaps)."""
    renderable, excluded, gaps = [], [], []
    cutoff = close - timedelta(days=STALE_EXCLUDE_DAYS)
    for r in rows:
        if r.get('is_placeholder') or r.get('weekly_avg') is None and r.get('price') is None:
            gaps.append(r)
        elif r.get('last_observed') and r['last_observed'] < cutoff:
            excluded.append(r)
        else:
            renderable.append(r)
    return renderable, excluded, gaps


# =============================================================
# Minimal markdown -> HTML (paragraphs, bold, italics, links)
# =============================================================

def _md(text: str) -> str:
    if not text:
        return ''
    out = _html.escape(text)
    out = re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', out)
    out = re.sub(r'(?<!\*)\*([^*\n]+)\*(?!\*)', r'<em>\1</em>', out)
    out = re.sub(r'\[([^\]]+)\]\((https?://[^)]+)\)', r'<a href="\2">\1</a>', out)
    paras = [p.strip().replace('\n', '<br>') for p in out.split('\n\n') if p.strip()]
    return ''.join(f'<p>{p}</p>' for p in paras)


def _fmt_px(v, unit: str) -> str:
    if v is None:
        return '—'
    return f"{float(v):,.2f} {unit}".strip()


def _fmt_pct(v) -> str:
    if v is None:
        return '—'
    v = float(v)
    sign = '+' if v > 0 else ''
    return f"{sign}{v:.1f}%"


def _fmt_date(d: Optional[date]) -> str:
    return d.strftime('%b %d') if d else '—'


# =============================================================
# HTML building blocks
# =============================================================

_TH = (f"background:{INK};color:{PAPER};font-family:{FONT_BODY};font-size:13px;"
       f"text-align:left;padding:7px 10px;border-bottom:2px solid {GOLD};")
_TD = (f"font-family:{FONT_BODY};font-size:14px;color:{INK};padding:7px 10px;"
       f"border-bottom:1px solid {GOLD}55;")


def _table(headers: List[str], rows: List[List[str]], right_cols: set) -> str:
    h = ''.join(
        f'<th style="{_TH}{"text-align:right;" if i in right_cols else ""}">{c}</th>'
        for i, c in enumerate(headers))
    body = ''
    for row in rows:
        tds = ''.join(
            f'<td style="{_TD}{"text-align:right;" if i in right_cols else ""}">{c}</td>'
            for i, c in enumerate(row))
        body += f'<tr>{tds}</tr>'
    return (f'<div style="overflow-x:auto"><table style="border-collapse:collapse;'
            f'width:100%;background:{PAPER};">'
            f'<tr>{h}</tr>{body}</table></div>')


def _section_title(title: str) -> str:
    return (f'<h2 style="font-family:{FONT_HEADER};color:{INK};font-size:21px;'
            f'margin:34px 0 4px 0;border-bottom:2px solid {GOLD};'
            f'padding-bottom:5px;">{title}</h2>')


def _asof_note(d: date) -> str:
    return (f'<span style="color:{INK};opacity:.65;font-size:12px;"> '
            f'(as of {_fmt_date(d)})</span>')


# =============================================================
# Section renderers (HTML). Each also contributes to the kit.
# =============================================================

def _render_masthead(issue: Dict) -> str:
    tag = 'PILOT · FREE EDITION' if issue['free_mode'] else 'SUBSCRIBER EDITION'
    cov = (f"Coverage: {_fmt_date(issue['coverage_start'])} – "
           f"{issue['week_ending'].strftime('%b %d, %Y')} weekly close")
    return f'''
<div style="border-top:6px solid {GOLD};border-bottom:2px solid {INK};padding:26px 0 14px 0;">
  <div style="font-family:{FONT_BODY};font-size:11px;letter-spacing:3px;color:{GOLD};
              text-transform:uppercase;">Round Lakes Commodities · {tag}</div>
  <h1 style="font-family:{FONT_HEADER};color:{INK};font-size:38px;margin:6px 0 4px 0;">
    The Feedstock Report</h1>
  <div style="font-family:{FONT_BODY};font-size:14px;color:{INK};">
    Issue {issue['issue_number']} · {issue['issue_date'].strftime('%B %d, %Y')}
    &nbsp;·&nbsp; {cov}</div>
</div>'''


def _render_dashboard(prices: List[Dict], close: date) -> Tuple[str, List[List[str]], List[str], str]:
    """Returns (html, png_rows, rendered_sources, coverage_note_text)."""
    renderable, excluded, gaps = _classify(prices, close)
    headers = ['Feedstock', 'Location', 'Price', 'As of', 'W/W', '52-wk range', 'Source']
    html_rows, png_rows, sources = [], [], []
    for r in renderable:
        px = _fmt_px(r['weekly_avg'], r['unit'] or '')
        asof = _fmt_date(r['last_observed'])
        wow = _fmt_pct(r['wow_change_pct'])
        if r['is_carried_forward']:
            asof += ' †'
            wow = '—'
        rng = (f"{float(r['range_52w_low']):,.2f}–{float(r['range_52w_high']):,.2f}"
               if r['range_52w_low'] is not None else '—')
        label = FEEDSTOCK_LABELS.get(r['feedstock_code'], r['product'])
        row = [label, r['location'] or '', px, asof, wow, rng, r['source'] or '']
        html_rows.append(row)
        png_rows.append(list(row))
        if r['source']:
            sources.append(r['source'])

    note_bits = []
    gap_codes = sorted({FEEDSTOCK_LABELS.get(g['feedstock_code'], g['product'])
                        for g in gaps})
    if gap_codes:
        note_bits.append("coverage expanding: " + ", ".join(gap_codes))
    for r in excluded:
        note_bits.append(f"{FEEDSTOCK_LABELS.get(r['feedstock_code'], r['product'])} "
                         f"(last print {_fmt_date(r['last_observed'])})")
    note = ('Rows pending reliable public sourcing — ' + '; '.join(note_bits) + '.'
            if note_bits else '')

    # The dagger legend travels with the table into the kit PNG as well — a kit
    # image is posted standalone, where an unexplained † reads as a defect.
    legend = ('† No print in the coverage week — last actual print shown with '
              'its date; changes not computed across carried values.'
              if any(r['is_carried_forward'] for r in renderable) else '')

    html = _table(headers, html_rows, right_cols={2, 3, 4, 5})
    if legend:
        html += (f'<p style="font-family:{FONT_BODY};font-size:12px;color:{INK};'
                 f'opacity:.7;margin:6px 0 0 0;">{_html.escape(legend)}</p>')
    if note:
        html += (f'<p style="font-family:{FONT_BODY};font-size:12px;color:{INK};'
                 f'opacity:.7;margin:8px 0 0 0;">{_html.escape(note)}</p>')
    return html, [headers] + png_rows, sources, note, legend


def _render_credit_stack(credits: List[Dict], close: date,
                         annotation_html: str) -> Tuple[str, Optional[List[List[str]]], List[str]]:
    inst_labels = {c['instrument']: c['label'] for c in CREDIT_INSTRUMENTS}
    priced = [c for c in credits if c.get('price') is not None]
    renderable, excluded, _ = _classify(
        [{**c, 'weekly_avg': c.get('price')} for c in priced], close)
    headers = ['Instrument', 'Price', 'W/W', 'Source']
    rows, sources = [], []
    for c in renderable:
        asof = _asof_note(c['last_observed']) if c['is_carried_forward'] else ''
        wow = '—' if c['is_carried_forward'] else _fmt_pct(c.get('wow_change'))
        rows.append([inst_labels.get(c['instrument'], c['instrument']),
                     _fmt_px(c['price'], c['unit'] or '') + asof, wow, c['source'] or ''])
        if c['source']:
            sources.append(c['source'])

    pending = [inst_labels.get(c['instrument'], c['instrument'])
               for c in credits if c.get('price') is None]
    pending += [inst_labels.get(c['instrument'], c['instrument']) for c in excluded]

    if rows:
        html = _table(headers, rows, right_cols={1, 2})
        png = [headers] + [[re.sub(r'<[^>]+>', '', c) for c in row] for row in rows]
    else:
        html, png = '', None
    if pending:
        html += (f'<p style="font-family:{FONT_BODY};font-size:12px;color:{INK};'
                 f'opacity:.7;margin:8px 0 0 0;">Coverage expanding: '
                 f'{_html.escape(", ".join(sorted(set(pending))))} — public-source '
                 f'feeds in progress.</p>')
    html += ('<p style="font-family:' + FONT_BODY + ';font-size:12px;color:' + INK +
             ';opacity:.8;margin:8px 0 0 0;">45Z clean fuel production credit: '
             'statutory formula (base credit × (50 − CI)/50); value varies by '
             'pathway carbon intensity — no market print exists.</p>')
    html += annotation_html
    return html, png, sources


def _render_ifv(section: Dict, free_mode: bool) -> Tuple[str, Optional[List[List[str]]], str]:
    """Free mode: rank + direction arrows ONLY."""
    snap = (section or {}).get('data_snapshot') or {}
    entries = snap.get('entries', [])
    if not entries:
        return ('<p style="font-family:%s;font-size:14px;">IFV leaderboard '
                'returns next issue.</p>' % FONT_BODY), None, ''
    headers = ['Rank', 'Feedstock', 'vs last issue']
    rows = []
    for e in entries:
        arrow = ARROW.get(e.get('direction', 'new'), '•')
        rows.append([str(e['rank']), e['label'], arrow])
    if not free_mode:
        headers.append('Implied value')
        for row, e in zip(rows, entries):
            row.append(f"{e['ifv_per_lb'] * 100:.1f} ¢/lb")
    caption = _md((section or {}).get('prose') or '')
    html = _table(headers, rows, right_cols={0}) + caption
    ifv_text_for_gate = re.sub(r'<[^>]+>', ' ', html)
    return html, [headers] + rows, ifv_text_for_gate


def _render_news(news: List[Dict]) -> Tuple[str, List[str]]:
    if not news:
        return f'<p style="font-family:{FONT_BODY};font-size:14px;">Quiet week.</p>', []
    items, sources = [], []
    for nitem in news:
        src = f" <span style='color:{GOLD};font-size:12px;'>[{_html.escape(nitem['source'])}]</span>" \
              if nitem.get('source') else ''
        link = (f'<a href="{_html.escape(nitem["url"])}" style="color:{INK};">'
                f'{_html.escape(nitem["headline"])}</a>' if nitem.get('url')
                else _html.escape(nitem['headline']))
        take = (f'<div style="font-size:13px;opacity:.8;margin:2px 0 0 0;">'
                f'{_html.escape(nitem.get("short_take") or "")}</div>')
        items.append(f'<li style="margin:0 0 12px 0;font-family:{FONT_BODY};'
                     f'font-size:14px;color:{INK};"><strong>{link}</strong>{src}{take}</li>')
        if nitem.get('source'):
            sources.append(nitem['source'])
    return f'<ol style="padding-left:20px;margin:10px 0;">{"".join(items)}</ol>', sources


def _render_footer(issue: Dict, coverage_note: str) -> str:
    return f'''
<div style="border-top:2px solid {INK};margin-top:36px;padding-top:14px;
            font-family:{FONT_BODY};font-size:12px;color:{INK};opacity:.85;">
  <p><strong>Methodology.</strong> Prices are the most recent actual prints from the
  named public sources as of the coverage close; a visible as-of date marks any value
  carried from an earlier week. Week-over-week changes are computed only between
  actual prints — never across carried values. Series whose last print is more than
  {STALE_EXCLUDE_DAYS} days old are withheld from the dashboard.
  {_html.escape(coverage_note) if coverage_note else ''}</p>
  <p><strong>Disclosures.</strong> The Feedstock Report is published by Round Lakes
  Commodities LLC for informational purposes only and is not investment, trading, or
  legal advice. Exchange data are the property of their respective exchanges.
  © {issue['issue_date'].year} Round Lakes Commodities LLC.</p>
</div>'''


# =============================================================
# Charts (matplotlib, brand-styled)
# =============================================================

def _brand_chart_history(cur, close: date) -> Optional[Dict]:
    """52w BFT Chicago daily history for the in-focus/teaser chart (USDA AMS)."""
    cur.execute("""
        SELECT price_date, AVG(price_per_lb) * 100 AS px
        FROM silver.feedstock_prices_consolidated
        WHERE feedstock_code = 'BFT' AND region = 'chicago'
          AND source LIKE 'USDA AMS%%'
          AND price_date BETWEEN %s AND %s AND price_per_lb > 0
        GROUP BY price_date ORDER BY price_date
    """, (close - timedelta(weeks=52), close))
    rows = cur.fetchall()
    if len(rows) < 5:
        return None
    return {'dates': [r['price_date'] for r in rows],
            'px': [float(r['px']) for r in rows],
            'label': 'Bleachable Fancy Tallow — Chicago (USDA)',
            'unit': '¢/lb'}


def _write_chart_png(series: Dict, out_path) -> None:
    """out_path: filesystem path or file-like buffer (for data-URI embedding)."""
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    fig, ax = plt.subplots(figsize=(PNG_EXPORT_WIDTH_PX / 100, 7.2), dpi=100)
    fig.patch.set_facecolor(PAPER)
    ax.set_facecolor(PAPER)
    ax.plot(series['dates'], series['px'], color=GOLD, linewidth=3.0)
    ax.set_title(series['label'], color=INK, fontsize=22,
                 fontfamily='Georgia', pad=16, loc='left')
    ax.set_ylabel(series['unit'], color=INK, fontsize=15)
    ax.tick_params(colors=INK, labelsize=14)
    for s in ('top', 'right'):
        ax.spines[s].set_visible(False)
    for s in ('left', 'bottom'):
        ax.spines[s].set_color(INK)
    ax.grid(axis='y', color=INK, alpha=.12, linewidth=.8)
    last = series['px'][-1]
    ax.annotate(f"{last:,.2f} {series['unit']} · {series['dates'][-1].strftime('%b %d')}",
                xy=(series['dates'][-1], last), xytext=(-10, 14),
                textcoords='offset points', ha='right', color=INK, fontsize=15,
                fontweight='bold')
    fig.text(0.01, 0.01, 'The Feedstock Report · Round Lakes Commodities',
             color=INK, alpha=.55, fontsize=11)
    fig.tight_layout(rect=(0, 0.03, 1, 1))
    fig.savefig(out_path, facecolor=PAPER, format='png')
    plt.close(fig)


def _write_table_png(table_rows: List[List[str]], title: str, out_path: Path,
                     note: str = '') -> None:
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    headers, body = table_rows[0], table_rows[1:]
    nrows = len(body)
    fig_h = 1.35 + 0.62 * max(nrows, 1) + (0.55 if note else 0.0)
    fig, ax = plt.subplots(figsize=(PNG_EXPORT_WIDTH_PX / 100, fig_h), dpi=100)
    fig.patch.set_facecolor(PAPER)
    ax.axis('off')
    # Proportional column widths from content length so nothing clips.
    ncols = len(headers)
    char_w = [max(len(str(headers[c])),
                  *(len(str(row[c])) for row in body)) + 2 for c in range(ncols)]
    total = sum(char_w)
    widths = [w / total for w in char_w]
    # With a note the table is lifted to make room beneath it; its height is
    # reduced by the same amount so the top edge never reaches the title.
    tbl_y0, tbl_h = (0.13, 0.74) if note else (0.0, 0.80)
    tbl = ax.table(cellText=body, colLabels=headers, cellLoc='left',
                   colWidths=widths, bbox=[0, tbl_y0, 1, tbl_h])
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(16)
    for (r, c), cell in tbl.get_celld().items():
        cell.set_edgecolor(GOLD)
        cell.set_linewidth(0.8)
        cell.PAD = 0.03
        if r == 0:
            cell.set_facecolor(INK)
            cell.set_text_props(color=PAPER, fontweight='bold', fontsize=15)
        else:
            cell.set_facecolor(PAPER)
            cell.set_text_props(color=INK)
    ax.text(0, 0.93, title, color=INK, fontsize=24, fontfamily='Georgia',
            transform=ax.transAxes, va='bottom')
    if note:
        ax.text(0, 0.045, note, color=INK, alpha=.75, fontsize=12,
                transform=ax.transAxes, va='bottom', wrap=True)
    fig.text(0.01, 0.012, 'The Feedstock Report · Round Lakes Commodities',
             color=INK, alpha=.55, fontsize=11)
    fig.savefig(out_path, facecolor=PAPER, bbox_inches='tight', pad_inches=0.25)
    plt.close(fig)


# =============================================================
# Top-level render
# =============================================================

def render_issue(issue_no: int, inject_ifv_numeric_test: bool = False) -> Dict[str, str]:
    """Render both outputs. Runs all IFVS-008 gates; refuses to write on failure.

    inject_ifv_numeric_test: test hook for acceptance criterion 4 — force a
    numeric IFV value into the free-mode IFV section and confirm the gate
    hard-fails. Never use outside testing.
    """
    data = _load(issue_no)
    issue = data['issue']
    close: date = issue['week_ending']
    free_mode: bool = issue['free_mode']

    out_dir = OUTPUT_ROOT / f"issue_{issue_no}"
    kit_dir = out_dir / f"issue_{issue_no}_linkedin"
    out_dir.mkdir(parents=True, exist_ok=True)
    kit_dir.mkdir(parents=True, exist_ok=True)

    rendered_sources: List[str] = []
    kit_images: List[Tuple[str, str]] = []   # (placeholder_name, filename)
    png_jobs: List[Tuple[str, Any, str, Path]] = []   # deferred until gates pass

    # --- build section HTML in registry order ---
    body_parts: List[str] = []
    kit_md: List[str] = []

    sections = data['sections']

    # masthead
    body_parts.append(_render_masthead(issue))
    kit_md.append(f"THE FEEDSTOCK REPORT — Issue {issue['issue_number']} · "
                  f"{issue['issue_date'].strftime('%B %d, %Y')}\n"
                  f"Round Lakes Commodities"
                  + (" · Pilot (free) edition" if free_mode else ""))

    # signal
    sig = sections.get('signal', {})
    if sig.get('prose'):
        body_parts.append(_section_title('The Signal') + _md(sig['prose']))
        kit_md.append("THE SIGNAL\n\n" + sig['prose'])

    # credit stack
    cs_annot = _md(sections.get('credit_stack', {}).get('prose') or '')
    cs_html, cs_png, cs_sources = _render_credit_stack(data['credits'], close, cs_annot)
    rendered_sources += cs_sources
    body_parts.append(_section_title('Credit Stack Monitor') + cs_html)
    kit_md.append("CREDIT STACK MONITOR\n\n[IMAGE: credit_stack]"
                  if cs_png else
                  "CREDIT STACK MONITOR\n\nPublic-source credit feeds are in "
                  "progress; the stack table joins the dashboard shortly.")
    if cs_png:
        png_jobs.append(('table', cs_png, 'Credit Stack Monitor',
                         kit_dir / 'credit_stack.png', ''))
        kit_images.append(('credit_stack', 'credit_stack.png'))

    # dashboard
    dash_html, dash_png, dash_sources, coverage_note, dash_legend = _render_dashboard(
        data['prices'], close)
    rendered_sources += dash_sources
    body_parts.append(_section_title('Feedstock Price Dashboard') + dash_html)
    kit_md.append("FEEDSTOCK PRICE DASHBOARD\n\n[IMAGE: dashboard]")
    png_jobs.append(('table', dash_png, 'Feedstock Price Dashboard',
                     kit_dir / 'dashboard.png', dash_legend))
    kit_images.append(('dashboard', 'dashboard.png'))

    # IFV leaderboard
    ifv_html, ifv_png, ifv_gate_text = _render_ifv(
        sections.get('ifv_leaderboard'), free_mode)
    if inject_ifv_numeric_test:
        ifv_html += '<p>test-injection implied value 0.7203 $/lb</p>'
        ifv_gate_text += ' test-injection implied value 0.7203 $/lb'
    body_parts.append(_section_title('IFV Leaderboard') + ifv_html)
    kit_md.append("IFV LEADERBOARD\n\n[IMAGE: ifv_leaderboard]")
    if ifv_png:
        png_jobs.append(('table', ifv_png, 'IFV Leaderboard — Implied Feedstock Value',
                         kit_dir / 'ifv_leaderboard.png', ''))
        kit_images.append(('ifv_leaderboard', 'ifv_leaderboard.png'))

    # in focus (+ one chart max). The chart is embedded as a data URI so
    # issue_{n}.html stays a genuinely single file (spec requirement).
    infocus = sections.get('in_focus', {})
    chart_html = ''
    with get_connection() as conn:
        with conn.cursor() as cur:
            chart = _brand_chart_history(cur, close)
    if chart:
        import base64
        import io as _io
        buf = _io.BytesIO()
        _write_chart_png(chart, buf)
        b64 = base64.b64encode(buf.getvalue()).decode('ascii')
        png_jobs.append(('chart', chart, '', kit_dir / 'in_focus_chart.png', ''))
        png_jobs.append(('chart', chart, '', out_dir / f'issue_{issue_no}_teaser.png', ''))
        kit_images.append(('in_focus_chart', 'in_focus_chart.png'))
        chart_html = (f'<img src="data:image/png;base64,{b64}" alt="{chart["label"]}" '
                      f'style="max-width:100%;border:1px solid {GOLD};margin-top:10px;">')
    if infocus.get('prose') or chart_html:
        body_parts.append(_section_title('In Focus') + _md(infocus.get('prose') or '')
                          + chart_html)
        kit_md.append("IN FOCUS\n\n" + (infocus.get('prose') or '')
                      + ("\n\n[IMAGE: in_focus_chart]" if chart_html else ''))

    # news
    news_html, news_sources = _render_news(data['news'])
    rendered_sources += news_sources
    body_parts.append(_section_title('News &amp; Policy Watch') + news_html)
    kit_md.append("NEWS & POLICY WATCH\n\n" + "\n".join(
        f"{i}. {n['headline']}" + (f" [{n['source']}]" if n.get('source') else '')
        + (f"\n   {n['short_take']}" if n.get('short_take') else '')
        for i, n in enumerate(data['news'], 1)))

    # week ahead
    wa = sections.get('week_ahead', {})
    if wa.get('prose'):
        body_parts.append(_section_title('The Week Ahead') + _md(wa['prose']))
        kit_md.append("THE WEEK AHEAD\n\n" + wa['prose'])

    # footer
    body_parts.append(_render_footer(issue, coverage_note))

    html_doc = f'''<!-- issue {issue_no} -->
<div style="background:{PAPER};margin:0;padding:24px 12px;">
<div style="max-width:{MAX_CONTENT_WIDTH_PX}px;margin:0 auto;background:{PAPER};">
{''.join(body_parts)}
</div></div>'''
    html_doc = ('<!DOCTYPE html><html><head><meta charset="utf-8">'
                f'<meta name="viewport" content="width=device-width, initial-scale=1">'
                f'<title>The Feedstock Report — Issue {issue_no}</title></head>'
                f'<body style="margin:0;background:{PAPER};">{html_doc}</body></html>')

    kit_body = "\n\n---\n\n".join(kit_md)

    # --- IFVS-008 gates on final artifacts ---
    run_gates(
        free_mode=free_mode,
        ifv_section_text=ifv_gate_text,
        rendered_sources=rendered_sources,
        artifacts={'issue.html': html_doc, 'linkedin/body.md': kit_body},
        level_rows=[{'source': r.get('source'), 'renders_level': True,
                     'label': r.get('product')} for r in data['prices']
                    if r.get('weekly_avg') is not None],
    )

    # Gates passed — only now touch the filesystem.
    html_path = out_dir / f'issue_{issue_no}.html'
    html_path.write_text(html_doc, encoding='utf-8')
    (kit_dir / 'body.md').write_text(kit_body, encoding='utf-8')
    for kind, payload, title, path, png_note in png_jobs:
        if kind == 'table':
            _write_table_png(payload, title, path, note=png_note)
        else:
            _write_chart_png(payload, path)

    logger.info(f"Rendered {html_path} + kit {kit_dir} "
                f"({len(kit_images)} images, {len(rendered_sources)} sourced rows)")
    return {'html': str(html_path), 'kit': str(kit_dir)}
