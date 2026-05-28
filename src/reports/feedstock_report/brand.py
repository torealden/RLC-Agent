"""The Feedstock Report — brand system.

Color palette anchored on RLC's balance-sheet forest green (#3C7D22).
Matplotlib + DOCX share this module so all chart and document output
is visually consistent.

FONT NOTE: Tore wants Google Sans. That family is restricted to Google
properties; not licensed for our redistribution. The closest open
substitute is INTER (https://rsms.me/inter/), which is nearly
visually identical and free for commercial use. Use INTER by default;
falls back to Calibri on Windows machines without Inter installed.
If Tore confirms he has Google Sans installed locally and only needs
his own copies to render with it, the macros / chart calls can be
flipped to "Google Sans" — the renderer just needs the font available
at render time.
"""

# ============================================================
# COLOR PALETTE — anchored on #3C7D22 (RLC forest green)
# ============================================================

# Primary anchor
FOREST       = '#3C7D22'  # primary brand color, headers, key data lines
DEEP_FOREST  = '#1F4012'  # titles, axis labels, strong emphasis
SAGE         = '#A8C99A'  # secondary fills, range bars, faint context
PALE_GREEN   = '#E8F0E2'  # subtle backgrounds, hover, alt-row stripe

# Accents — chosen to coexist with the green anchor (not compete)
BURNT_ORANGE = '#C97B2C'  # warning/alert/anomaly markers — sparingly
DEEP_ORANGE  = '#8C4D17'  # downstroke text where contrast needed
STEEL_BLUE   = '#4A6B8A'  # complementary cool — for "other" series
SLATE        = '#6B6F73'  # neutral series color, grid emphasis

# Neutrals — paper-and-ink
INK          = '#2A2A2A'  # body text
CHARCOAL     = '#4A4A4A'  # secondary text
SOFT_GRAY    = '#9B9B9B'  # tertiary text, axis ticks
PAPER        = '#F8F8F5'  # warm off-white background (institutional)
RULE         = '#D4D4CE'  # rule lines, table borders, axis lines

# Semantic colors for tables and dashboards
POSITIVE     = FOREST       # green for "up" / "good"
NEGATIVE     = '#A83232'    # restrained red — not Bloomberg neon
NEUTRAL      = SLATE        # flat / no-change

# 5-series rotation for multi-series charts (when more than the
# primary forest line is needed). Order matters — first series gets
# the brand color, additional series step through.
SERIES_ROTATION = [
    FOREST,
    STEEL_BLUE,
    BURNT_ORANGE,
    SAGE,
    DEEP_FOREST,
]

# Range-bar coloring (for 52-week range visualizations)
RANGE_TRACK_BG = SAGE        # the full 52w range bar (subtle)
RANGE_MARKER   = FOREST      # the current-value diamond
RANGE_PRIOR    = SOFT_GRAY   # prior-week marker if shown

# ============================================================
# FONT STACK
# ============================================================
# Inter is the open substitute for Google Sans (visually near-identical).
# Calibri is the Windows DOCX fallback. Both render cleanly.
FONT_PRIMARY    = 'Inter'           # for charts + DOCX body
FONT_FALLBACKS  = ['Calibri', 'Segoe UI', 'Arial', 'sans-serif']
FONT_MONO       = 'Consolas'        # for table numerics if monospace needed

# Heading sizes (pt) — DOCX
H1_SIZE = 22
H2_SIZE = 16
H3_SIZE = 12
BODY_SIZE = 10
CAPTION_SIZE = 8

# ============================================================
# MATPLOTLIB RCPARAMS
# ============================================================
def apply_matplotlib_style():
    """Apply the brand style to matplotlib's global rcParams.
    Call this once at the start of any chart-generation session."""
    import matplotlib.pyplot as plt
    from matplotlib import rcParams

    rcParams.update({
        # Font
        'font.family':       FONT_PRIMARY,
        'font.sans-serif':   [FONT_PRIMARY] + FONT_FALLBACKS,
        'font.size':         10,

        # Figure
        'figure.facecolor':  PAPER,
        'figure.edgecolor':  PAPER,
        'figure.dpi':        110,
        'savefig.dpi':       180,
        'savefig.facecolor': PAPER,
        'savefig.bbox':      'tight',

        # Axes
        'axes.facecolor':    PAPER,
        'axes.edgecolor':    RULE,
        'axes.labelcolor':   INK,
        'axes.titlecolor':   DEEP_FOREST,
        'axes.titlesize':    14,
        'axes.titleweight':  'bold',
        'axes.titlepad':     12,
        'axes.labelsize':    10,
        'axes.spines.top':   False,
        'axes.spines.right': False,
        'axes.grid':         True,
        'axes.axisbelow':    True,

        # Grid
        'grid.color':        RULE,
        'grid.linestyle':    '-',
        'grid.linewidth':    0.4,
        'grid.alpha':        0.6,

        # Lines
        'lines.linewidth':   1.8,
        'lines.markersize':  5,

        # Ticks
        'xtick.color':       CHARCOAL,
        'ytick.color':       CHARCOAL,
        'xtick.labelsize':   9,
        'ytick.labelsize':   9,

        # Legend
        'legend.frameon':    False,
        'legend.fontsize':   9,
        'legend.labelcolor': INK,

        # Color cycle
        'axes.prop_cycle':   plt.cycler(color=SERIES_ROTATION),
    })


# ============================================================
# DOCX HELPERS
# ============================================================
def docx_default_font_run(run):
    """Apply the brand font to a python-docx Run."""
    run.font.name = FONT_PRIMARY
    # Force the East-Asian + Latin font assignment so it sticks in Word
    try:
        from docx.oxml.ns import qn
        rPr = run._element.get_or_add_rPr()
        rFonts = rPr.find(qn('w:rFonts'))
        if rFonts is None:
            from docx.oxml import OxmlElement
            rFonts = OxmlElement('w:rFonts')
            rPr.append(rFonts)
        rFonts.set(qn('w:ascii'), FONT_PRIMARY)
        rFonts.set(qn('w:hAnsi'), FONT_PRIMARY)
        rFonts.set(qn('w:cs'),    FONT_PRIMARY)
    except Exception:
        pass


# ============================================================
# DEMO — generate a swatch image when run directly
# ============================================================
if __name__ == '__main__':
    import matplotlib.pyplot as plt
    apply_matplotlib_style()

    swatches = [
        ('FOREST',       FOREST),
        ('DEEP_FOREST',  DEEP_FOREST),
        ('SAGE',         SAGE),
        ('PALE_GREEN',   PALE_GREEN),
        ('BURNT_ORANGE', BURNT_ORANGE),
        ('STEEL_BLUE',   STEEL_BLUE),
        ('SLATE',        SLATE),
        ('INK',          INK),
        ('PAPER',        PAPER),
        ('NEGATIVE',     NEGATIVE),
    ]
    fig, ax = plt.subplots(figsize=(8, 6))
    for i, (name, color) in enumerate(swatches):
        ax.barh(i, 1, color=color, edgecolor=RULE, linewidth=0.5)
        # text contrast — light text on dark swatches
        text_color = PAPER if name in ('FOREST', 'DEEP_FOREST', 'INK', 'BURNT_ORANGE',
                                       'STEEL_BLUE', 'SLATE', 'NEGATIVE') else INK
        ax.text(0.02, i, f'  {name}  {color}', va='center',
                color=text_color, fontweight='bold', fontsize=10)
    ax.set_yticks([])
    ax.set_xticks([])
    ax.set_xlim(0, 1)
    ax.set_title('The Feedstock Report — Brand Palette', fontsize=16, pad=18)
    ax.invert_yaxis()
    for spine in ax.spines.values():
        spine.set_visible(False)
    out = 'output/visualizations/feedstock_report_brand_swatch.png'
    plt.savefig(out)
    print(f'Wrote {out}')
