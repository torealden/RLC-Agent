"""Weekly Feedstock Price Dashboard — v3 with 52-week range bars."""
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch
import numpy as np

BG = '#0f1318'
PANEL = '#181d24'
GOLD = '#C8963E'
WHITE = '#FFFFFF'
LIGHT = '#D4CFC5'
GRAY = '#6B6560'
GREEN = '#548235'
RED = '#c00000'
BLUE = '#2e75b6'
RANGE_BG = '#252a32'
RANGE_BAR = '#3a4050'

def cc(s):
    try:
        v = float(str(s).replace('+','').replace('$','').replace('C','').replace('N/A','0').replace('flat','0'))
        if v > 0: return GREEN
        if v < 0: return RED
    except: pass
    return GRAY


def zscore_color(wow_val, avg_weekly_move=0.8):
    """
    Color the bar based on z-score of weekly change.
    avg_weekly_move is typical absolute weekly change for the commodity.
    """
    try:
        v = float(str(wow_val).replace('+',''))
        z = abs(v) / avg_weekly_move if avg_weekly_move > 0 else 0
        if z > 2.0:
            return '#ff4444' if v > 0 else '#ff4444'  # Bright red = abnormal move
        elif z > 1.5:
            return GOLD  # Gold = notable move
        elif z > 1.0:
            return BLUE  # Blue = above average
        else:
            return '#3a4050'  # Muted = normal
    except:
        return '#3a4050'


def draw_range_bar(ax, y, x_start, bar_width, low, high, current,
                   wow, mom, yoy, label, avg_move=0.8,
                   unit='', show_range_labels=True):
    """
    Draw a 52-week range bar with current price marker and WoW/MoM/YoY.
    Bar color based on z-score of weekly change.
    """
    bar_height = 0.006

    # Range bar background
    ax.add_patch(FancyBboxPatch(
        (x_start, y - bar_height/2), bar_width, bar_height,
        boxstyle="round,pad=0.001", facecolor=RANGE_BG, edgecolor='none',
        transform=ax.transAxes, zorder=1))

    # Bar color from z-score of weekly change
    bar_color = zscore_color(wow, avg_move)

    # Filled portion (low to current)
    if high > low:
        pct = (current - low) / (high - low)
        pct = max(0, min(1, pct))
        fill_width = bar_width * pct

        ax.add_patch(FancyBboxPatch(
            (x_start, y - bar_height/2), fill_width, bar_height,
            boxstyle="round,pad=0.001", facecolor=bar_color, edgecolor='none',
            transform=ax.transAxes, alpha=0.8, zorder=2))

        # Current price marker (diamond)
        marker_x = x_start + fill_width
        ax.plot(marker_x, y, marker='D', markersize=5, color=WHITE,
                transform=ax.transAxes, zorder=3, markeredgecolor=bar_color,
                markeredgewidth=1.5)

    # Label (left of bar)
    ax.text(x_start - 0.02, y, label, fontsize=7.5, color=LIGHT,
            transform=ax.transAxes, ha='right', va='center')

    # Current price (right of bar)
    price_x = x_start + bar_width + 0.015
    ax.text(price_x, y, f'{current:.1f}{unit}',
            fontsize=8, color=WHITE, fontweight='bold',
            transform=ax.transAxes, ha='left', va='center')

    # WoW / MoM / YoY changes
    chg_x = price_x + 0.065
    ax.text(chg_x, y, wow, fontsize=6.5, color=cc(wow),
            transform=ax.transAxes, ha='right', va='center')
    ax.text(chg_x + 0.055, y, mom, fontsize=6.5, color=cc(mom),
            transform=ax.transAxes, ha='right', va='center')
    ax.text(chg_x + 0.11, y, yoy, fontsize=6.5, color=cc(yoy),
            transform=ax.transAxes, ha='right', va='center')

    # Range labels (low / high)
    if show_range_labels:
        ax.text(x_start, y - bar_height - 0.004, f'{low:.0f}',
                fontsize=5, color=GRAY, transform=ax.transAxes, ha='left', va='top')
        ax.text(x_start + bar_width, y - bar_height - 0.004, f'{high:.0f}',
                fontsize=5, color=GRAY, transform=ax.transAxes, ha='right', va='top')


fig = plt.figure(figsize=(11, 17))
fig.patch.set_facecolor(BG)

# Title
fig.text(0.04, 0.99, 'WEEKLY FEEDSTOCK PRICE DASHBOARD', fontsize=18,
         fontweight='bold', color=WHITE, va='top')
fig.text(0.04, 0.979, 'Week Ending March 28, 2026', fontsize=9, color=GRAY, va='top')
fig.text(0.96, 0.99, 'Round Lakes Companies', fontsize=9, fontweight='bold',
         color=GOLD, va='top', ha='right')

# Gold accent line
fig.add_axes([0.03, 0.974, 0.94, 0.001]).set_facecolor(GOLD)
fig.axes[-1].axis('off')

# ── KEY BENCHMARKS ─────────────────────────────────────────────────
ax0 = fig.add_axes([0.03, 0.945, 0.94, 0.025])
ax0.set_facecolor(PANEL)
ax0.axis('off')

for i, (nm, pr, ch) in enumerate([
    ('SBO (ZL)', '64.48c/lb', '+1.23'),
    ('ULSD', '$2.41/gal', '-0.08'),
    ('Brent', '$99.94/bbl', '-11.20'),
    ('D4 RIN', '$1.52', '+0.04'),
    ('D6 RIN', '$0.78', '-0.02'),
    ('LCFS', '$52/MT', '-3.00'),
    ('45Z Credit', '$0.50/gal', 'flat'),
]):
    x = 0.01 + i * 0.143
    ax0.text(x, 0.82, nm, fontsize=7, color=GRAY, fontweight='bold',
             transform=ax0.transAxes, va='top')
    ax0.text(x, 0.25, pr, fontsize=10, color=WHITE, fontweight='bold',
             transform=ax0.transAxes, va='center')
    ax0.text(x + 0.085, 0.25, ch, fontsize=7, color=cc(ch),
             transform=ax0.transAxes, va='center')

# ═══════════════════════════════════════════════════════════════════
# FEEDSTOCK PRICES WITH RANGE BARS (full width)
# ═══════════════════════════════════════════════════════════════════
ax1 = fig.add_axes([0.03, 0.60, 0.94, 0.335])
ax1.set_facecolor(PANEL)
ax1.axis('off')

ax1.text(0.01, 0.99, 'FEEDSTOCK PRICES', fontsize=13, fontweight='bold',
         color=GOLD, transform=ax1.transAxes, va='top')
ax1.text(0.99, 0.99, '52-Week Range  |  All prices cents/lb unless noted  |  Current price shown as diamond',
         fontsize=7, color=GOLD, transform=ax1.transAxes, va='top', ha='right')

# Column headers
ax1.text(0.28, 0.955, '52-Week Range', fontsize=6, color=GRAY,
         transform=ax1.transAxes, ha='left')
ax1.text(0.72, 0.955, 'Price', fontsize=6, color=GRAY,
         transform=ax1.transAxes, ha='center')
ax1.text(0.795, 0.955, 'WoW', fontsize=6, color=GRAY,
         transform=ax1.transAxes, ha='center')
ax1.text(0.85, 0.955, 'MoM', fontsize=6, color=GRAY,
         transform=ax1.transAxes, ha='center')
ax1.text(0.91, 0.955, 'YoY', fontsize=6, color=GRAY,
         transform=ax1.transAxes, ha='center')

# Vegetable Oils header
ax1.text(0.01, 0.935, 'VEGETABLE OILS', fontsize=8, color=BLUE,
         fontweight='bold', transform=ax1.transAxes)

# (label, low_52wk, high_52wk, current, wow, mom, yoy, avg_weekly_move)
veg_oils = [
    ('Soybean Oil (CBOT)',   48.5, 72.3, 64.48, '+1.23', '+3.40', '+8.72', 1.0),
    ('Canola Oil',           52.0, 75.0, 67.20, '+0.85', '+2.10', '+5.30', 0.9),
    ('Palm Oil (CIF NOLA)',  42.0, 58.5, 52.10, '-0.40', '+1.80', '-3.20', 0.8),
    ('Sunflower Oil',        41.0, 65.0, 58.30, '+2.10', '+4.50', '+12.40', 0.7),
    ('Corn Oil (DCO)',       38.0, 55.0, 48.50, '+0.30', '+1.20', '+6.80', 0.6),
]

y = 0.90
for label, low, high, current, wow, mom, yoy, avg in veg_oils:
    draw_range_bar(ax1, y, 0.28, 0.35, low, high, current,
                   wow, mom, yoy, label, avg_move=avg)
    y -= 0.05

# Animal Fats header
y -= 0.015
ax1.text(0.01, y + 0.01, 'ANIMAL FATS & GREASES', fontsize=8, color=BLUE,
         fontweight='bold', transform=ax1.transAxes)
y -= 0.03

fats = [
    ('Inedible Tallow',      32.0, 52.0, 44.25, '+1.80', '+3.50', '+9.20', 0.8),
    ('Edible Tallow',        36.0, 55.0, 48.50, '+1.50', '+2.80', '+7.10', 0.7),
    ('Yellow Grease',        28.0, 46.0, 38.75, '+0.90', '+2.20', '+5.40', 0.7),
    ('Choice White Grease',  30.0, 48.0, 41.20, '+1.10', '+2.60', '+6.30', 0.7),
    ('Poultry Fat',          25.0, 44.0, 36.50, '+0.60', '+1.50', '+4.20', 0.5),
    ('Lard',                 32.0, 50.0, 43.80, '+0.70', '+1.90', '+3.80', 0.6),
    ('UCO',                  22.0, 42.0, 35.00, '+2.50', '+5.80', '+14.50', 0.8),
]

for label, low, high, current, wow, mom, yoy, avg in fats:
    draw_range_bar(ax1, y, 0.28, 0.35, low, high, current,
                   wow, mom, yoy, label, avg_move=avg)
    y -= 0.05

# Fuels & Credits header
y -= 0.015
ax1.text(0.01, y + 0.01, 'FUELS & CREDITS', fontsize=8, color=BLUE,
         fontweight='bold', transform=ax1.transAxes)
y -= 0.03

fuels = [
    ('ULSD ($/gal)',    1.85, 3.20, 2.41,  '-0.08', '-0.15', '+0.42', 0.06),
    ('Brent ($/bbl)',   65.0, 120.0, 99.94, '-11.20', '+18.50', '+28.00', 3.0),
    ('Ethanol ($/gal)', 1.45, 2.10, 1.72,  '+0.04', '+0.08', '+0.15', 0.04),
    ('D4 RIN ($)',      0.90, 1.85, 1.52,  '+0.04', '+0.12', '+0.35', 0.03),
    ('D6 RIN ($)',      0.55, 1.10, 0.78,  '-0.02', '-0.05', '+0.08', 0.02),
    ('LCFS ($/MT)',     38.0, 72.0, 52.0,  '-3.00', '-5.00', '-12.00', 2.0),
]

for label, low, high, current, wow, mom, yoy, avg in fuels:
    draw_range_bar(ax1, y, 0.28, 0.35, low, high, current,
                   wow, mom, yoy, label, avg_move=avg)
    y -= 0.05

# Legend for z-score bar colors
ax1.text(0.01, 0.01, 'Bar color = weekly move z-score:', fontsize=6, color=GRAY,
         transform=ax1.transAxes)
for clr, lbl, xp in [('#3a4050', 'Normal (<1sd)', 0.19),
                       (BLUE, 'Above avg (1-1.5sd)', 0.31),
                       (GOLD, 'Notable (1.5-2sd)', 0.48),
                       ('#ff4444', 'Abnormal (>2sd)', 0.62)]:
    ax1.plot(xp, 0.013, 's', markersize=5, color=clr,
             transform=ax1.transAxes, zorder=3)
    ax1.text(xp + 0.012, 0.01, lbl, fontsize=5.5, color=clr,
             transform=ax1.transAxes)

# ═══════════════════════════════════════════════════════════════════
# MARGINS & SPREADS (bottom half)
# ═══════════════════════════════════════════════════════════════════
ax2 = fig.add_axes([0.03, 0.27, 0.45, 0.31])
ax2.set_facecolor(PANEL)
ax2.axis('off')

ax2.text(0.03, 0.97, 'CRUSH & BIOFUEL MARGINS', fontsize=11, fontweight='bold',
         color=GOLD, transform=ax2.transAxes, va='top')

# Crush margins with range bars
ax2.text(0.03, 0.90, 'OILSEED CRUSH', fontsize=8, color=BLUE,
         fontweight='bold', transform=ax2.transAxes)

crush = [
    ('Soybean ($/bu)', 0.40, 3.50, 2.45, '+0.18', '+0.45', '+0.80', 0.15),
    ('Canola (C$/MT)', -10.0, 65.0, 48.0, '-2.50', '+5.00', '+12.0', 3.0),
    ('Palm ($/MT)', 40.0, 120.0, 85.0, '+5.20', '+10.0', '+18.0', 4.0),
]

y = 0.84
for label, low, high, current, wow, mom, yoy, avg in crush:
    draw_range_bar(ax2, y, 0.38, 0.30, low, high, current,
                   wow, mom, yoy, label, avg_move=avg, show_range_labels=False)
    y -= 0.065

ax2.text(0.03, y + 0.01, 'BIOFUEL PRODUCTION', fontsize=8, color=BLUE,
         fontweight='bold', transform=ax2.transAxes)
y -= 0.04

biofuel = [
    ('RD from SBO', -0.20, 1.20, 0.42, '-0.08', '-0.15', '+0.10', 0.06),
    ('RD from Tallow', 0.10, 1.50, 0.85, '+0.05', '+0.12', '+0.25', 0.05),
    ('RD UCO+LCFS', 0.30, 1.80, 1.12, '-0.15', '-0.20', '+0.30', 0.08),
    ('BD from SBO', -0.15, 0.80, 0.28, '-0.12', '-0.18', '-0.05', 0.05),
    ('Ethanol (IA)', 0.05, 0.65, 0.35, '+0.02', '+0.05', '+0.08', 0.03),
]

for label, low, high, current, wow, mom, yoy, avg in biofuel:
    draw_range_bar(ax2, y, 0.38, 0.30, low, high, current,
                   wow, mom, yoy, label, avg_move=avg, show_range_labels=False)
    y -= 0.065

# ── KEY SPREADS (bottom right) ────────────────────────────────────
ax3 = fig.add_axes([0.52, 0.27, 0.45, 0.31])
ax3.set_facecolor(PANEL)
ax3.axis('off')

ax3.text(0.03, 0.97, 'KEY SPREADS & RATIOS', fontsize=11, fontweight='bold',
         color=GOLD, transform=ax3.transAxes, va='top')

ax3.text(0.03, 0.90, 'FEEDSTOCK SPREADS (c/lb)', fontsize=8, color=BLUE,
         fontweight='bold', transform=ax3.transAxes)

spreads = [
    ('SBO - Palm', 5.0, 18.0, 12.38, '+1.63', '+2.80', '+4.50', 1.0),
    ('SBO - Tallow', 12.0, 28.0, 20.23, '-0.57', '+0.60', '+1.50', 0.8),
    ('Tallow - UCO', 4.0, 15.0, 9.25, '-0.70', '-2.30', '-5.30', 0.6),
    ('Tallow - YG', 3.0, 10.0, 5.50, '+0.90', '+1.30', '+3.80', 0.5),
]

y = 0.84
for label, low, high, current, wow, mom, yoy, avg in spreads:
    draw_range_bar(ax3, y, 0.35, 0.30, low, high, current,
                   wow, mom, yoy, label, avg_move=avg, show_range_labels=False)
    y -= 0.065

ax3.text(0.03, y + 0.01, 'POLICY & CREDIT SPREADS', fontsize=8, color=BLUE,
         fontweight='bold', transform=ax3.transAxes)
y -= 0.04

policy = [
    ('D4-D6 RIN (c)', 30.0, 95.0, 74.0, '+6.00', '+8.00', '+15.0', 3.0),
    ('RD-ULSD (c/g)', -10.0, 40.0, 15.0, '-3.00', '+5.00', '+10.0', 2.0),
    ('SBO/SBM Ratio', 1.60, 2.80, 2.12, '+0.04', '+0.08', '+0.20', 0.03),
]

for label, low, high, current, wow, mom, yoy, avg in policy:
    draw_range_bar(ax3, y, 0.35, 0.30, low, high, current,
                   wow, mom, yoy, label, avg_move=avg, show_range_labels=False)
    y -= 0.065

ax3.text(0.03, y + 0.01, 'CRUSH ECONOMICS', fontsize=8, color=BLUE,
         fontweight='bold', transform=ax3.transAxes)
y -= 0.04

econ = [
    ('Board Crush $/bu', 0.40, 3.50, 2.45, '+0.18', '+0.45', '+0.80', 0.15),
    ('Crush Margin %', 5.0, 25.0, 21.0, '+1.2', '+2.5', '+5.0', 1.0),
    ('Oil Share GPV %', 35.0, 55.0, 48.5, '+0.8', '+1.5', '+3.0', 0.6),
]

for label, low, high, current, wow, mom, yoy, avg in econ:
    draw_range_bar(ax3, y, 0.35, 0.30, low, high, current,
                   wow, mom, yoy, label, avg_move=avg, show_range_labels=False)
    y -= 0.065

# ── IRAN BANNER ───────────────────────────────────────────────────
ax4 = fig.add_axes([0.03, 0.09, 0.94, 0.15])
ax4.set_facecolor('#1a1210')
ax4.axis('off')

ax4.text(0.02, 0.92, 'IRAN CRISIS MARKET IMPACT', fontsize=11,
         fontweight='bold', color=RED, transform=ax4.transAxes, va='top')

ax4.text(0.02, 0.70,
    'Strait of Hormuz disruption ongoing  |  Brent peaked at $120/bbl, now $99.94 (-11% WoW on diplomatic signals)',
    fontsize=8, color=LIGHT, transform=ax4.transAxes, va='top')
ax4.text(0.02, 0.50,
    'Urea +50% since Feb 28 ($450 > $700/MT)  |  20-30% of global fertilizer supply transits Hormuz  |  LNG spot +140% in Asia',
    fontsize=8, color=LIGHT, transform=ax4.transAxes, va='top')
ax4.text(0.02, 0.25,
    'Feedstock implications: Rising corn input costs > potential corn-to-soy acreage shift > bullish SBO/meal long-term',
    fontsize=8, color=GOLD, style='italic', transform=ax4.transAxes, va='top')
ax4.text(0.02, 0.08,
    'Energy spike supports RIN values near-term but compresses RD net margins vs ULSD  |  Watch for SPR release signals',
    fontsize=8, color=GOLD, style='italic', transform=ax4.transAxes, va='top')

# ── Footer ─────────────────────────────────────────────────────────
fig.text(0.04, 0.06, 'Sources: CME, ICE, USDA AMS, EIA, EPA, CARB  |  Prices are indicative',
         fontsize=6, color=GRAY)
fig.text(0.96, 0.06, 'Round Lakes Companies  |  roundlakescommodities.com',
         fontsize=6, color=GRAY, ha='right')

plt.savefig('data/generated_graphics/charts/weekly_price_dashboard_v3.png',
            dpi=150, facecolor=fig.get_facecolor())
plt.close()
print('Saved weekly_price_dashboard_v3.png')
