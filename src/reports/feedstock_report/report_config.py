"""The Feedstock Report — Issue 0+ configuration.

Everything the IFVS-008 gates need lives HERE as config, not hardcoded in the
renderer (per handoff spec: "Keep the whitelist as config, not hardcode").

Brand per handoff spec 2026-08-06 (supersedes the forest-green brand.py, which
remains for legacy DOCX work): INK / GOLD / PAPER, Georgia headers, Calibri body.
"""

from __future__ import annotations

# =============================================================
# Brand (handoff spec — client-facing INK/GOLD/PAPER system)
# =============================================================
INK = '#1B2A4A'
GOLD = '#C8A951'
PAPER = '#F7F3EB'

FONT_HEADER = "Georgia, 'Times New Roman', serif"
FONT_BODY = "Calibri, 'Segoe UI', Arial, sans-serif"
MAX_CONTENT_WIDTH_PX = 720
PNG_EXPORT_WIDTH_PX = 1400          # 2x for 700px LinkedIn display

# =============================================================
# Canonical vocabulary (11 codes)
# =============================================================
FEEDSTOCK_CODES = ['SBO', 'CAN', 'DCO', 'BFT', 'CWG', 'YG', 'PLT', 'UCO', 'CAM', 'CAR', 'OTH']

FEEDSTOCK_LABELS = {
    'SBO': 'Soybean Oil',
    'CAN': 'Canola Oil',
    'DCO': 'Distillers Corn Oil',
    'BFT': 'Bleachable Fancy Tallow',
    'CWG': 'Choice White Grease',
    'YG':  'Yellow Grease',
    'PLT': 'Poultry Fat',
    'UCO': 'Used Cooking Oil',
    'CAM': 'Camelina Oil',
    'CAR': 'Carinata Oil',
    'OTH': 'Other',
}

# =============================================================
# IFVS-008 compliance gates (renderer-enforced)
# =============================================================
# Public citation whitelist (ruled 2026-08-06: USDA added).
CITATION_WHITELIST = {'CARB', 'NREL', 'EIA', 'IEA', 'Argus', 'OPIS', 'USDA'}

# Exchange names — board data renders as levels with attribution.
EXCHANGE_WHITELIST = {'CME Group', 'CBOT', 'NYMEX', 'ICE', 'Bursa Malaysia'}

# Sources whose LEVELS are embargoed pending license review (render w/w or
# base-100 index only while licensed_levels_ok is False).
LICENSED_LEVEL_SOURCES = {'Argus', 'OPIS'}
LICENSED_LEVELS_OK = False

# Grep-level hard-error strings. "HOBO" and internal series identifiers per
# IFVS-008; "Fastmarkets"/"Jacobsen" per UCO sourcing ruling 2026-08-06.
BANNED_OUTPUT_STRINGS = [
    'HOBO',
    'Fastmarkets',
    'fastmarkets',
    'Jacobsen',
    # internal series / schema identifiers
    'DCO_IA', 'DCO_KS', 'DCO_WI', 'DCO_MO', 'DCO_NE', 'DCO_SD', 'DCO_MN', 'DCO_ECB',
    'BRSBO_FOB_PARITY',
    'price_mark', 'feedstock_prices_consolidated', 'specialty_price',
    'silver.', 'bronze.', 'gold.', 'kg_callable', 'slug_id',
]

# Staleness (ruled 2026-08-06): rows whose last actual print is older than
# this many days at the coverage-window close are EXCLUDED from the dashboard
# and moved to the "coverage expanding" line.
STALE_EXCLUDE_DAYS = 21

# Coverage window (ruled 2026-08-07, supersedes the 2026-08-06 Monday close):
# the window is the AMS reporting week, Monday through Friday inclusive, so
# coverage_start = week_ending - COVERAGE_WINDOW_DAYS.
#
# Both AMS families that back the dashboard cover Mon-Fri but differ in stamp
# and publication, and a Mon-Fri window is the only one that holds both:
#   3510/3511 (SBO, CWG, YG): stamped the week's MONDAY, published that FRIDAY
#                             ~13:30 ET.
#   3618      (DCO):          stamped the week's FRIDAY, published the FOLLOWING
#                             MONDAY ~09:00 ET.
# Hence the snapshot must run the Monday AFTER the coverage week — see the
# cadence note in snapshot.py. A Friday-evening run would miss DCO entirely.
COVERAGE_WINDOW_DAYS = 4

# =============================================================
# Section registry (render order)
# =============================================================
SECTION_REGISTRY = [
    # (order, code, title, kind)  kind: auto | written | mixed
    (1, 'masthead',        '',                          'auto'),
    (2, 'signal',          'The Signal',                'written'),
    (3, 'credit_stack',    'Credit Stack Monitor',      'mixed'),
    (4, 'dashboard',       'Feedstock Price Dashboard', 'auto'),
    (5, 'ifv_leaderboard', 'IFV Leaderboard',           'mixed'),
    (6, 'in_focus',        'In Focus',                  'written'),
    (7, 'news',            'News & Policy Watch',       'mixed'),
    (8, 'week_ahead',      'The Week Ahead',            'written'),
    (9, 'footer',          'Methodology & Disclosures', 'auto'),
]

# =============================================================
# Dashboard series registry — which live series backs each row.
# One row per (feedstock_code, series). kind:
#   'consolidated' -> silver.feedstock_prices_consolidated (code+region+source filter)
#   'price_mark'   -> silver.price_mark (series_key)
#   'gap'          -> coverage-gap line (no live whitelisted series)
# public_source is the string RENDERED (must pass the citation whitelist);
# it is deliberately not the internal source tag.
# Membership per Task 0 freshness audit 2026-08-06
# (docs/specs/feedstock_report_issue0_freshness_audit.md).
# =============================================================
DASHBOARD_SERIES = [
    {'code': 'SBO', 'kind': 'consolidated', 'region': 'illinois', 'source_like': 'USDA AMS 3511',
     'location_label': 'Central Illinois', 'public_source': 'USDA', 'unit': '¢/lb', 'scale': 100.0},
    {'code': 'DCO', 'kind': 'price_mark', 'series_key': 'DCO_IA',
     'location_label': 'Iowa FOB plant', 'public_source': 'USDA', 'unit': '¢/lb', 'scale': 1.0},
    {'code': 'BFT', 'kind': 'consolidated', 'region': 'chicago', 'source_like': 'USDA AMS 2837',
     'location_label': 'Chicago', 'public_source': 'USDA', 'unit': '¢/lb', 'scale': 100.0},
    {'code': 'CWG', 'kind': 'consolidated', 'region': 'minnesota', 'source_like': 'USDA AMS 3510',
     'location_label': 'Minnesota', 'public_source': 'USDA', 'unit': '¢/lb', 'scale': 100.0},
    {'code': 'YG',  'kind': 'consolidated', 'region': 'minnesota', 'source_like': 'USDA AMS 3510',
     'location_label': 'Minnesota', 'public_source': 'USDA', 'unit': '¢/lb', 'scale': 100.0},
    # Coverage-gap codes (no live whitelisted series as of the Task 0 audit).
    {'code': 'CAN', 'kind': 'gap'},
    {'code': 'PLT', 'kind': 'gap'},
    {'code': 'UCO', 'kind': 'gap'},   # CME UCO futures via manual CSV or Barchart (Task 1)
    {'code': 'CAM', 'kind': 'gap'},
    {'code': 'CAR', 'kind': 'gap'},
]

# Credit stack instruments (per-instrument row grain, mig 176).
CREDIT_INSTRUMENTS = [
    {'instrument': 'D4_RIN',  'label': 'D4 RIN (biomass-based diesel)', 'unit': '¢/RIN'},
    {'instrument': 'D6_RIN',  'label': 'D6 RIN (conventional)',         'unit': '¢/RIN'},
    {'instrument': 'LCFS_CA', 'label': 'California LCFS credit',        'unit': '$/MT'},
    {'instrument': 'CFP_OR',  'label': 'Oregon CFP credit',             'unit': '$/MT'},
    {'instrument': 'CFS_WA',  'label': 'Washington CFS credit',         'unit': '$/MT'},
]

# IFV leaderboard membership: canonical code -> IFV-callable feedstock_code.
# YG shares the waste-oil CI class with UCO in the callable's taxonomy.
IFV_LEADERBOARD_CODES = {
    'BFT': 'tallow',
    'CWG': 'choice_white_grease',
    'PLT': 'poultry_fat',
    'UCO': 'used_cooking_oil',
    'YG':  'uco',
    'DCO': 'distillers_corn_oil',
    'SBO': 'soybean_oil',
    'CAN': 'canola_oil',
}
