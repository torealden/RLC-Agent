"""
Data Freshness Dashboard — Conference Presentation Visual
Shows real-time collection status for all RLC-Agent data sources.
Queries live data from core.latest_collections, falls back to snapshot if DB unavailable.
"""

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch
from datetime import datetime, timezone, timedelta
import os
import sys

# ── Attempt live DB query ─────────────────────────────────────────
def fetch_live_data():
    """Query core.latest_collections for real freshness data."""
    try:
        import psycopg2
        from dotenv import load_dotenv
        load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', '.env'))
        conn = psycopg2.connect(
            host=os.getenv('RLC_PG_HOST', 'localhost'),
            port=5432,
            dbname='rlc_commodities',
            user='postgres',
            password=os.getenv('RLC_PG_PASSWORD', os.getenv('DB_PASSWORD', ''))
        )
        cur = conn.cursor()
        cur.execute("""
            SELECT collector_name, run_finished_at, status, rows_collected
            FROM core.latest_collections
            ORDER BY run_finished_at DESC NULLS LAST
        """)
        rows = cur.fetchall()
        conn.close()
        return rows
    except Exception as e:
        print(f"[INFO] DB unavailable ({e}), using snapshot data")
        return None


# ── Presentation-friendly source name mapping ────────────────────
DISPLAY_NAMES = {
    'weather_daily_summary':     'Weather Observations',
    'drought_monitor':           'Drought Monitor',
    'yfinance_futures':          'CME Settlements (yfinance)',
    'cme_settlements':           'CME Settlements',
    'usda_ams_cash_prices':      'USDA AMS Cash Prices',
    'eia_petroleum':             'EIA Petroleum',
    'eia_ethanol':               'EIA Ethanol',
    'usda_nass_crop_progress':   'USDA NASS Crop Progress',
    'fgis_inspections':          'FGIS Export Inspections',
    'cftc_cot':                  'CFTC Commitments of Traders',
    'usda_fas_export_sales':     'USDA Export Sales',
    'canada_cgc':                'Canada CGC Grain Stats',
    'canada_statscan':           'Canada StatsCan',
    'argentina_indec':           'Argentina INDEC Trade',
    'epa_echo_oilseed':         'EPA ECHO Facilities',
    'brazil_ibge_sidra':         'Brazil IBGE Production',
    'census_trade':              'US Census Trade',
    'epa_rfs':                   'EPA EMTS / RFS RINs',
    'mpob':                      'MPOB Palm Oil',
    'usda_ers_oil_crops':        'USDA ERS Oil Crops',
    'usda_ers_feed_grains':      'USDA ERS Feed Grains',
    'usda_ers_wheat':            'USDA ERS Wheat',
    'usda_ams_ddgs':             'USDA AMS DDGS Prices',
    'usda_ams_feedstocks':       'USDA AMS Feedstock Prices',
    'yield_forecast':            'Yield Forecast Model',
}

# Expected update cadence (hours) — used for freshness classification
EXPECTED_CADENCE_HOURS = {
    'weather_daily_summary':     24,
    'drought_monitor':           168,     # weekly
    'yfinance_futures':          24,
    'cme_settlements':           24,
    'usda_ams_cash_prices':      24,
    'eia_petroleum':             168,     # weekly
    'eia_ethanol':               168,     # weekly
    'usda_nass_crop_progress':   168,     # weekly (seasonal)
    'fgis_inspections':          168,     # weekly
    'cftc_cot':                  168,     # weekly (Friday)
    'usda_fas_export_sales':     168,     # weekly (Thursday)
    'canada_cgc':                168,     # weekly
    'canada_statscan':           720,     # monthly
    'argentina_indec':           720,     # monthly
    'epa_echo_oilseed':         720,     # monthly
    'brazil_ibge_sidra':         720,     # monthly
    'census_trade':              720,     # monthly
    'epa_rfs':                   720,     # monthly
    'mpob':                      720,     # monthly
    'usda_ers_oil_crops':        720,     # monthly
    'usda_ers_feed_grains':      720,     # monthly
    'usda_ers_wheat':            720,     # monthly
    'usda_ams_ddgs':             168,     # weekly
    'usda_ams_feedstocks':       168,     # weekly
    'yield_forecast':            168,     # weekly
}

# ── Freshness classification ─────────────────────────────────────
# Color palette (presentation dark theme)
CLR_BG      = '#1a1917'
CLR_GOLD    = '#C8963E'
CLR_GREEN   = '#3D6B4F'
CLR_YELLOW  = '#C8963E'
CLR_RED     = '#8B3A3A'
CLR_WHITE   = '#FFFFFF'
CLR_GRAY    = '#888888'
CLR_DARK    = '#2a2825'

# Status colors for the DB status field
STATUS_FAILED_COLOR = '#8B3A3A'


def classify_freshness(collector_name, last_run_dt, status, now):
    """
    Return (color, label, hours_ago).
    Green  = within expected cadence
    Yellow = 1-2x expected cadence (aging)
    Red    = >2x expected cadence OR failed status with no recent success
    """
    if last_run_dt is None:
        return CLR_RED, 'NO DATA', None

    if last_run_dt.tzinfo is None:
        last_run_dt = last_run_dt.replace(tzinfo=timezone.utc)

    hours_ago = (now - last_run_dt).total_seconds() / 3600
    cadence = EXPECTED_CADENCE_HOURS.get(collector_name, 168)

    # If the latest run failed, mark as stale/red unless it ran very recently
    if status == 'failed':
        if hours_ago < cadence * 0.5:
            return CLR_YELLOW, f'{hours_ago:.0f}h ago (failed)', hours_ago
        return CLR_RED, f'{hours_ago:.0f}h ago (failed)', hours_ago

    if hours_ago <= cadence * 1.2:
        return CLR_GREEN, f'{hours_ago:.0f}h ago', hours_ago
    elif hours_ago <= cadence * 2.5:
        return CLR_YELLOW, f'{hours_ago:.0f}h ago', hours_ago
    else:
        return CLR_RED, f'{hours_ago:.0f}h ago', hours_ago


def format_time_label(hours_ago, status):
    """Human-readable time label."""
    if hours_ago is None:
        return 'No data'
    suffix = ''

    if hours_ago < 1:
        return f'< 1 hour ago{suffix}'
    elif hours_ago < 24:
        return f'{hours_ago:.0f} hours ago{suffix}'
    elif hours_ago < 48:
        return f'1 day ago{suffix}'
    else:
        days = hours_ago / 24
        return f'{days:.0f} days ago{suffix}'


# ── Snapshot data (from DB query on 2026-03-26) ──────────────────
SNAPSHOT_DATA = [
    ('weather_daily_summary',     '2026-03-26T10:30:00+00:00', 'success', 1),
    ('drought_monitor',           '2026-03-26T12:30:15+00:00', 'success', 792),
    ('yfinance_futures',          '2026-03-25T21:15:31+00:00', 'success', 275),
    ('cme_settlements',           '2026-03-25T21:00:09+00:00', 'success', 6),
    ('usda_ams_cash_prices',      '2026-03-25T21:00:26+00:00', 'success', 2312),
    ('eia_petroleum',             '2026-03-25T14:30:42+00:00', 'partial', 13949),
    ('eia_ethanol',               '2026-03-25T14:30:12+00:00', 'partial', 3916),
    ('usda_nass_crop_progress',   '2026-03-23T20:00:05+00:00', 'partial', 10),
    ('fgis_inspections',          '2026-03-23T15:01:50+00:00', 'success', 1712),
    ('cftc_cot',                  '2026-03-20T19:30:27+00:00', 'success', 306),
    ('canada_cgc',                '2026-03-26T17:43:15+00:00', 'partial', 15717),
    ('canada_statscan',           '2026-03-20T13:21:41+00:00', 'success', 56847),
    ('argentina_indec',           '2026-03-20T16:00:36+00:00', 'success', 542),
    ('epa_echo_oilseed',         '2026-03-19T10:16:22+00:00', 'success', 202),
    ('brazil_ibge_sidra',         '2026-03-15T15:00:20+00:00', 'success', 330),
    ('census_trade',              '2026-03-12T22:35:26+00:00', 'partial', 45506),
    ('usda_fas_export_sales',     '2026-03-26T13:00:49+00:00', 'failed', 0),
    ('epa_rfs',                   '2026-03-06T12:11:40+00:00', 'failed', 0),
    ('mpob',                      '2026-03-06T20:20:44+00:00', 'failed', 0),
    ('usda_ers_oil_crops',        '2026-03-20T14:00:25+00:00', 'failed', 0),
    ('usda_ers_feed_grains',      '2026-03-20T13:30:07+00:00', 'failed', 0),
    ('usda_ers_wheat',            '2026-03-20T14:30:07+00:00', 'failed', 0),
    ('usda_ams_ddgs',             '2026-03-20T18:30:06+00:00', 'failed', 0),
    ('usda_ams_feedstocks',       '2026-03-20T18:30:06+00:00', 'failed', 0),
]


def parse_snapshot():
    """Convert snapshot strings into the same format as DB rows."""
    result = []
    for name, ts_str, status, rows in SNAPSHOT_DATA:
        dt = datetime.fromisoformat(ts_str)
        result.append((name, dt, status, rows))
    return result


# ── Build the chart ──────────────────────────────────────────────
def build_chart(output_path):
    now = datetime.now(timezone.utc)

    # Try live data first, fall back to snapshot
    raw = fetch_live_data()
    if raw is None:
        raw = parse_snapshot()
        data_source_label = 'snapshot'
    else:
        data_source_label = 'live'

    print(f"[INFO] Using {data_source_label} data ({len(raw)} collectors)")

    # Build per-collector records
    records = []
    for row in raw:
        name, finished_at, status, rows_collected = row[0], row[1], row[2], row[3]
        if name not in DISPLAY_NAMES:
            continue
        display = DISPLAY_NAMES[name]
        color, label, hours_ago = classify_freshness(name, finished_at, status, now)
        time_label = format_time_label(hours_ago, status)
        rows_label = f'{rows_collected:,}' if rows_collected and rows_collected > 0 else ''
        records.append({
            'name': name,
            'display': display,
            'color': color,
            'time_label': time_label,
            'hours_ago': hours_ago if hours_ago is not None else 99999,
            'status': status,
            'rows_label': rows_label,
        })

    # Sort: green first (by hours_ago), then yellow, then red
    color_order = {CLR_GREEN: 0, CLR_YELLOW: 1, CLR_RED: 2}
    records.sort(key=lambda r: (color_order.get(r['color'], 3), r['hours_ago']))

    n = len(records)

    # ── Figure setup ──────────────────────────────────────────────
    fig, ax = plt.subplots(figsize=(14, max(8, n * 0.42 + 2.5)), dpi=150)
    fig.patch.set_facecolor(CLR_BG)
    ax.set_facecolor(CLR_BG)

    # Remove axes
    ax.set_xlim(0, 10)
    ax.set_ylim(-0.5, n + 0.5)
    ax.axis('off')

    # ── Title block ───────────────────────────────────────────────
    fig.text(0.50, 0.96, 'Data Collection Status \u2014 All Systems',
             fontsize=22, fontweight='bold', color=CLR_WHITE,
             ha='center', va='top', fontfamily='sans-serif')
    fig.text(0.50, 0.935, 'Automated monitoring  |  Updated every 5 minutes',
             fontsize=11, color=CLR_GRAY, ha='center', va='top',
             fontfamily='sans-serif')

    # ── Column headers ────────────────────────────────────────────
    header_y = n - 0.15
    ax.text(0.3, n + 0.15, 'DATA SOURCE', fontsize=9, fontweight='bold',
            color=CLR_GOLD, va='center', fontfamily='sans-serif')
    ax.text(6.0, n + 0.15, 'STATUS', fontsize=9, fontweight='bold',
            color=CLR_GOLD, va='center', ha='center', fontfamily='sans-serif')
    ax.text(8.2, n + 0.15, 'LAST UPDATE', fontsize=9, fontweight='bold',
            color=CLR_GOLD, va='center', ha='center', fontfamily='sans-serif')
    ax.text(9.7, n + 0.15, 'ROWS', fontsize=9, fontweight='bold',
            color=CLR_GOLD, va='center', ha='right', fontfamily='sans-serif')

    # Thin separator line under headers
    ax.plot([0.15, 9.85], [n - 0.3, n - 0.3], color=CLR_GOLD, linewidth=0.5, alpha=0.5)

    # ── Draw each row ─────────────────────────────────────────────
    for i, rec in enumerate(records):
        y = n - 1 - i  # top-to-bottom

        # Alternating row background for readability
        if i % 2 == 0:
            row_bg = FancyBboxPatch(
                (0.1, y - 0.35), 9.8, 0.7,
                boxstyle="round,pad=0.05",
                facecolor='#222220', edgecolor='none', alpha=0.5
            )
            ax.add_patch(row_bg)

        # Status indicator dot
        dot_color = rec['color']
        ax.plot(0.25, y, 'o', color=dot_color, markersize=10, markeredgecolor='none')

        # Inner glow for green dots
        if dot_color == CLR_GREEN:
            ax.plot(0.25, y, 'o', color=dot_color, markersize=14,
                    markeredgecolor='none', alpha=0.25)

        # Source name
        ax.text(0.55, y, rec['display'], fontsize=10.5, color=CLR_WHITE,
                va='center', fontfamily='sans-serif', fontweight='medium')

        # Status badge
        status_text = rec['status'].upper()
        if status_text == 'SUCCESS':
            badge_color = CLR_GREEN
        elif status_text == 'PARTIAL':
            badge_color = '#5A6F3C'
        elif status_text == 'FAILED':
            badge_color = CLR_RED
        else:
            badge_color = CLR_GRAY

        badge = FancyBboxPatch(
            (5.35, y - 0.18), 1.3, 0.36,
            boxstyle="round,pad=0.08",
            facecolor=badge_color, edgecolor='none', alpha=0.7
        )
        ax.add_patch(badge)
        ax.text(6.0, y, status_text, fontsize=7.5, color=CLR_WHITE,
                va='center', ha='center', fontweight='bold', fontfamily='sans-serif')

        # Time label
        time_color = CLR_WHITE if rec['color'] != CLR_RED else '#E8A0A0'
        ax.text(8.2, y, rec['time_label'], fontsize=9.5, color=time_color,
                va='center', ha='center', fontfamily='sans-serif')

        # Row count
        ax.text(9.7, y, rec['rows_label'], fontsize=9, color=CLR_GRAY,
                va='center', ha='right', fontfamily='sans-serif')

    # ── Summary stats bar ─────────────────────────────────────────
    n_green = sum(1 for r in records if r['color'] == CLR_GREEN)
    n_yellow = sum(1 for r in records if r['color'] == CLR_YELLOW)
    n_red = sum(1 for r in records if r['color'] == CLR_RED)

    summary_y = -0.05
    fig.text(0.12, summary_y, f'{n} sources tracked', fontsize=10,
             color=CLR_GRAY, ha='left', va='center', fontfamily='sans-serif',
             transform=fig.transFigure)

    # Legend dots
    fig.text(0.52, summary_y, '\u25CF', fontsize=14, color=CLR_GREEN,
             ha='right', va='center', transform=fig.transFigure)
    fig.text(0.53, summary_y, f' {n_green} Fresh', fontsize=10,
             color=CLR_WHITE, ha='left', va='center', fontfamily='sans-serif',
             transform=fig.transFigure)

    fig.text(0.62, summary_y, '\u25CF', fontsize=14, color=CLR_YELLOW,
             ha='right', va='center', transform=fig.transFigure)
    fig.text(0.63, summary_y, f' {n_yellow} Aging', fontsize=10,
             color=CLR_WHITE, ha='left', va='center', fontfamily='sans-serif',
             transform=fig.transFigure)

    fig.text(0.72, summary_y, '\u25CF', fontsize=14, color=CLR_RED,
             ha='right', va='center', transform=fig.transFigure)
    fig.text(0.73, summary_y, f' {n_red} Stale/Failed', fontsize=10,
             color=CLR_WHITE, ha='left', va='center', fontfamily='sans-serif',
             transform=fig.transFigure)

    # ── Footer ────────────────────────────────────────────────────
    ts = datetime.now().strftime('%Y-%m-%d %H:%M')
    footer = f'Auto-generated from RLC database ({data_source_label})  |  {ts}  |  RLC Analytics'
    fig.text(0.50, 0.01, footer, fontsize=8, color='#555555',
             ha='center', va='bottom', fontfamily='sans-serif')

    # ── Save ──────────────────────────────────────────────────────
    plt.tight_layout(rect=[0.0, 0.03, 1.0, 0.93])
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    fig.savefig(output_path, facecolor=CLR_BG, edgecolor='none',
                bbox_inches='tight', dpi=150)
    plt.close(fig)
    print(f"[OK] Saved: {output_path}")
    print(f"     {n_green} fresh / {n_yellow} aging / {n_red} stale-or-failed out of {n} sources")


# ── Main ──────────────────────────────────────────────────────────
if __name__ == '__main__':
    output = os.path.join(
        os.path.dirname(__file__), '..', '..',
        'data', 'generated_graphics', 'charts',
        'data_freshness_dashboard_presentation.png'
    )
    output = os.path.normpath(output)
    build_chart(output)
