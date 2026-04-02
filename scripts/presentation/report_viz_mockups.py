"""
Report Visualization Mockups — Generate all four chart types for review.

1. Balance Sheet Waterfall
2. Trade Flow Sankey
3. Margin Stack Decomposition
4. Heatmap Calendar

All use placeholder data — will connect to real DB data for production.
"""

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch
import numpy as np
from pathlib import Path

OUT = Path("data/generated_graphics/report_mockups")
OUT.mkdir(parents=True, exist_ok=True)

# ── Colors ─────────────────────────────────────────────────────────
BG = '#0f1318'
PANEL = '#181d24'
GOLD = '#C8963E'
WHITE = '#FFFFFF'
LIGHT = '#D4CFC5'
GRAY = '#6B6560'
GREEN = '#548235'
RED = '#c00000'
BLUE = '#2e75b6'
NAVY = '#1f4e79'
TEAL = '#2a9d8f'
ORANGE = '#e76f51'
PURPLE = '#7030a0'

MONTH_NAMES = ['Sep','Oct','Nov','Dec','Jan','Feb','Mar','Apr','May','Jun','Jul','Aug']
MONTH_NAMES_CAL = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']


def save(fig, name):
    fig.savefig(OUT / name, dpi=150, facecolor=fig.get_facecolor())
    plt.close(fig)
    print(f"  Saved {name}")


# ═══════════════════════════════════════════════════════════════════
# 1. BALANCE SHEET WATERFALL
# ═══════════════════════════════════════════════════════════════════
def build_waterfall():
    """S&D as a waterfall chart — supply builds up, demand subtracts down."""
    fig, ax = plt.subplots(figsize=(14, 8))
    fig.patch.set_facecolor(BG)
    ax.set_facecolor(BG)

    # US Soybeans MY 2025/26 (1,000 MT)
    items = [
        ('Beg.\nStocks', 9517, 'supply', 0),
        ('Prod.', 115989, 'supply', None),
        ('Imports', 800, 'supply', None),
        ('Total\nSupply', None, 'total', None),
        ('Crush', -63503, 'demand', None),
        ('Exports', -42864, 'demand', None),
        ('Seed', -2800, 'demand', None),
        ('Residual', -1623, 'demand', None),
        ('End.\nStocks', None, 'result', None),
    ]

    # Calculate running total
    running = 0
    bars = []
    for i, (label, value, bar_type, _) in enumerate(items):
        if bar_type == 'total':
            bars.append((label, 0, running, bar_type, running))
        elif bar_type == 'result':
            bars.append((label, 0, running, bar_type, running))
        else:
            bottom = running
            bars.append((label, bottom, value, bar_type, running + value))
            running += value

    # YoY changes (fake for mockup)
    yoy_changes = [+200, -3000, +50, None, -1500, +2000, -100, +300, None]

    x = np.arange(len(bars))
    width = 0.6

    for i, (label, bottom, value, bar_type, end_val) in enumerate(bars):
        if bar_type == 'total':
            ax.bar(i, value, width, color=GOLD, alpha=0.9, edgecolor='none')
            ax.text(i, value + 1000, f'{value:,.0f}', ha='center', va='bottom',
                    fontsize=9, fontweight='bold', color=GOLD)
        elif bar_type == 'result':
            color = GREEN if value > 10000 else RED if value < 5000 else BLUE
            ax.bar(i, value, width, color=color, alpha=0.9, edgecolor='none')
            ax.text(i, value + 1000, f'{value:,.0f}', ha='center', va='bottom',
                    fontsize=10, fontweight='bold', color=color)
        elif bar_type == 'supply':
            ax.bar(i, value, width, bottom=bottom, color=BLUE, alpha=0.7, edgecolor='none')
            ax.text(i, bottom + value/2, f'{value:,.0f}', ha='center', va='center',
                    fontsize=8, color=WHITE, fontweight='bold')
        elif bar_type == 'demand':
            ax.bar(i, value, width, bottom=bottom + value, color=RED, alpha=0.5, edgecolor='none')
            ax.text(i, bottom + value/2, f'{abs(value):,.0f}', ha='center', va='center',
                    fontsize=8, color=WHITE, fontweight='bold')

        # YoY change indicator
        if yoy_changes[i] is not None:
            yoy = yoy_changes[i]
            color = GREEN if yoy > 0 else RED
            sign = '+' if yoy > 0 else ''
            ax.text(i, -6000, f'{sign}{yoy:,.0f}', ha='center', va='top',
                    fontsize=7, color=color, fontweight='bold')

    ax.set_xticks(x)
    ax.set_xticklabels([b[0] for b in bars], fontsize=9, color=LIGHT)
    ax.set_ylabel('1,000 MT', fontsize=10, color=GRAY)
    ax.tick_params(axis='y', colors=GRAY)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['bottom'].set_color(GRAY)
    ax.spines['left'].set_color(GRAY)
    ax.set_ylim(-8000, 135000)

    # Title
    fig.text(0.04, 0.97, 'US SOYBEANS — Supply & Demand Waterfall', fontsize=16,
             fontweight='bold', color=WHITE, va='top')
    fig.text(0.04, 0.935, 'MY 2025/26  |  1,000 MT  |  YoY change shown below each bar',
             fontsize=9, color=GRAY, va='top')
    fig.text(0.96, 0.97, 'Round Lakes Companies', fontsize=9, color=GOLD, va='top', ha='right')

    # Legend
    ax.text(0.02, 0.02, 'YoY change:', fontsize=7, color=GRAY, transform=ax.transAxes)
    ax.text(0.10, 0.02, '+200', fontsize=7, color=GREEN, transform=ax.transAxes)
    ax.text(0.15, 0.02, '= increase vs prior MY', fontsize=7, color=GRAY, transform=ax.transAxes)
    ax.text(0.35, 0.02, '-3,000', fontsize=7, color=RED, transform=ax.transAxes)
    ax.text(0.42, 0.02, '= decrease', fontsize=7, color=GRAY, transform=ax.transAxes)

    plt.tight_layout(rect=[0, 0.02, 1, 0.92])
    save(fig, 'viz_01_waterfall.png')


# ═══════════════════════════════════════════════════════════════════
# 2. TRADE FLOW VISUALIZATION (simplified Sankey-style)
# ═══════════════════════════════════════════════════════════════════
def build_trade_flow():
    """Soybean export flows from origins to destinations."""
    fig, ax = plt.subplots(figsize=(14, 8))
    fig.patch.set_facecolor(BG)
    ax.set_facecolor(BG)
    ax.axis('off')

    # Exporters (left) and Importers (right)
    exporters = [
        ('Brazil', 114000, GREEN, [('China', 78000), ('EU', 12000), ('Other', 24000)]),
        ('US', 42864, BLUE, [('China', 25000), ('EU', 5000), ('Mexico', 5000), ('Other', 7864)]),
        ('Argentina', 8250, TEAL, [('China', 3000), ('EU', 2500), ('Other', 2750)]),
        ('Paraguay', 7700, PURPLE, [('China', 2000), ('Argentina', 3000), ('Other', 2700)]),
    ]

    importers_total = {}
    for exp_name, total, color, dests in exporters:
        for dest, vol in dests:
            importers_total[dest] = importers_total.get(dest, 0) + vol

    # Layout
    left_x = 0.08
    right_x = 0.82
    flow_x1 = 0.22
    flow_x2 = 0.72

    # Draw exporters
    total_exports = sum(e[1] for e in exporters)
    y = 0.88
    exp_positions = {}

    for name, vol, color, dests in exporters:
        bar_h = (vol / total_exports) * 0.65
        ax.add_patch(FancyBboxPatch(
            (left_x, y - bar_h), 0.12, bar_h,
            boxstyle="round,pad=0.005", facecolor=color, alpha=0.8,
            edgecolor='none', transform=ax.transAxes, zorder=2))
        ax.text(left_x + 0.06, y - bar_h/2, f'{name}\n{vol/1000:.0f} MMT',
                ha='center', va='center', fontsize=9, color=WHITE,
                fontweight='bold', transform=ax.transAxes, zorder=3)
        exp_positions[name] = (y - bar_h/2, color, bar_h)
        y -= bar_h + 0.02

    # Draw importers
    imp_list = sorted(importers_total.items(), key=lambda x: -x[1])
    y = 0.88
    imp_positions = {}

    for name, vol in imp_list:
        bar_h = (vol / total_exports) * 0.65
        color = GOLD if name == 'China' else BLUE if name == 'EU' else ORANGE if name == 'Mexico' else GRAY
        ax.add_patch(FancyBboxPatch(
            (right_x, y - bar_h), 0.12, bar_h,
            boxstyle="round,pad=0.005", facecolor=color, alpha=0.8,
            edgecolor='none', transform=ax.transAxes, zorder=2))
        ax.text(right_x + 0.06, y - bar_h/2, f'{name}\n{vol/1000:.0f} MMT',
                ha='center', va='center', fontsize=9, color=WHITE,
                fontweight='bold', transform=ax.transAxes, zorder=3)
        imp_positions[name] = y - bar_h/2
        y -= bar_h + 0.02

    # Draw flows (simplified curved lines)
    for exp_name, total, color, dests in exporters:
        exp_y = exp_positions[exp_name][0]
        for dest, vol in dests:
            if dest in imp_positions:
                imp_y = imp_positions[dest]
                alpha = max(0.15, min(0.6, vol / 80000))
                lw = max(1, vol / 8000)
                mid_x = (flow_x1 + flow_x2) / 2
                ax.annotate('', xy=(flow_x2, imp_y), xytext=(flow_x1, exp_y),
                           arrowprops=dict(arrowstyle='-', color=color,
                                          alpha=alpha, lw=lw,
                                          connectionstyle=f'arc3,rad=0.1'),
                           transform=ax.transAxes, zorder=1)

    # Labels
    ax.text(left_x + 0.06, 0.95, 'EXPORTERS', ha='center', fontsize=11,
            fontweight='bold', color=GOLD, transform=ax.transAxes)
    ax.text(right_x + 0.06, 0.95, 'IMPORTERS', ha='center', fontsize=11,
            fontweight='bold', color=GOLD, transform=ax.transAxes)

    fig.text(0.04, 0.97, 'WORLD SOYBEAN TRADE FLOWS', fontsize=16,
             fontweight='bold', color=WHITE, va='top')
    fig.text(0.04, 0.935, 'MY 2025/26  |  Line width proportional to volume  |  1,000 MT',
             fontsize=9, color=GRAY, va='top')
    fig.text(0.96, 0.97, 'Round Lakes Companies', fontsize=9, color=GOLD, va='top', ha='right')

    save(fig, 'viz_02_trade_flows.png')


# ═══════════════════════════════════════════════════════════════════
# 3. MARGIN STACK DECOMPOSITION
# ═══════════════════════════════════════════════════════════════════
def build_margin_stack():
    """RD margin decomposition — revenue stack vs cost stack."""
    fig, ax = plt.subplots(figsize=(14, 8))
    fig.patch.set_facecolor(BG)
    ax.set_facecolor(BG)

    # Facilities with different feedstocks
    facilities = [
        'RD\nSBO\n(IL)',
        'RD\nTallow\n(TX)',
        'RD\nUCO\n(CA)',
        'BD\nSBO\n(IA)',
        'RD\nDCO\n(LA)',
        'RD\nCWG\n(MN)',
    ]

    # Revenue components ($/gal)
    fuel_price =  [2.50, 2.50, 2.50, 3.73, 2.50, 2.50]  # ULSD or B100
    rin_value =   [1.59, 1.59, 1.59, 1.06, 1.59, 1.59]  # D4 or D6
    lcfs_credit = [0.00, 0.00, 0.85, 0.00, 0.00, 0.00]  # Only CA
    tax_credit =  [0.50, 0.50, 0.50, 0.50, 0.50, 0.50]  # 45Z

    # Cost components ($/gal)
    feedstock =   [3.65, 2.85, 2.50, 3.65, 3.20, 2.95]
    processing =  [0.45, 0.45, 0.45, 0.40, 0.45, 0.45]
    logistics =   [0.08, 0.12, 0.15, 0.06, 0.10, 0.09]

    x = np.arange(len(facilities))
    width = 0.35

    # Revenue bars (left of center)
    rev_colors = [BLUE, TEAL, GREEN, GOLD]
    rev_labels = ['Fuel Price', 'RIN Value', 'LCFS Credit', '45Z Credit']
    rev_data = [fuel_price, rin_value, lcfs_credit, tax_credit]

    bottom = np.zeros(len(facilities))
    for i, (data, color, label) in enumerate(zip(rev_data, rev_colors, rev_labels)):
        bars = ax.bar(x - width/2 - 0.02, data, width, bottom=bottom,
                      color=color, alpha=0.8, label=label if i < 4 else None)
        # Label values in bars if significant
        for j, v in enumerate(data):
            if v > 0.20:
                ax.text(x[j] - width/2 - 0.02, bottom[j] + v/2, f'${v:.2f}',
                        ha='center', va='center', fontsize=7, color=WHITE, fontweight='bold')
        bottom += np.array(data)

    # Total revenue labels
    for j in range(len(facilities)):
        total_rev = fuel_price[j] + rin_value[j] + lcfs_credit[j] + tax_credit[j]
        ax.text(x[j] - width/2 - 0.02, total_rev + 0.05, f'${total_rev:.2f}',
                ha='center', va='bottom', fontsize=8, fontweight='bold', color=GOLD)

    # Cost bars (right of center)
    cost_colors = [RED, ORANGE, GRAY]
    cost_labels = ['Feedstock', 'Processing', 'Logistics']
    cost_data = [feedstock, processing, logistics]

    bottom = np.zeros(len(facilities))
    for i, (data, color, label) in enumerate(zip(cost_data, cost_colors, cost_labels)):
        ax.bar(x + width/2 + 0.02, data, width, bottom=bottom,
               color=color, alpha=0.7, label=label)
        for j, v in enumerate(data):
            if v > 0.20:
                ax.text(x[j] + width/2 + 0.02, bottom[j] + v/2, f'${v:.2f}',
                        ha='center', va='center', fontsize=7, color=WHITE, fontweight='bold')
        bottom += np.array(data)

    # Net margin line
    margins = []
    for j in range(len(facilities)):
        rev = fuel_price[j] + rin_value[j] + lcfs_credit[j] + tax_credit[j]
        cost = feedstock[j] + processing[j] + logistics[j]
        margins.append(rev - cost)

    # Draw margin markers
    for j, m in enumerate(margins):
        color = GREEN if m > 0 else RED
        ax.plot(x[j], max(bottom[j], fuel_price[j]+rin_value[j]+lcfs_credit[j]+tax_credit[j]) + 0.3,
                marker='D', markersize=8, color=color, zorder=5)
        ax.text(x[j], max(bottom[j], fuel_price[j]+rin_value[j]+lcfs_credit[j]+tax_credit[j]) + 0.45,
                f'${m:+.2f}', ha='center', va='bottom', fontsize=9,
                fontweight='bold', color=color)

    ax.set_xticks(x)
    ax.set_xticklabels(facilities, fontsize=9, color=LIGHT)
    ax.set_ylabel('$/gallon', fontsize=10, color=GRAY)
    ax.tick_params(axis='y', colors=GRAY)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['bottom'].set_color(GRAY)
    ax.spines['left'].set_color(GRAY)

    ax.legend(loc='upper right', fontsize=8, facecolor=PANEL, edgecolor=GRAY,
              labelcolor=LIGHT, ncol=2)

    fig.text(0.04, 0.97, 'BIOFUEL MARGIN STACK DECOMPOSITION', fontsize=16,
             fontweight='bold', color=WHITE, va='top')
    fig.text(0.04, 0.935,
             'Revenue (left) vs Cost (right) by facility type  |  Diamond = net margin  |  $/gallon',
             fontsize=9, color=GRAY, va='top')
    fig.text(0.96, 0.97, 'Round Lakes Companies', fontsize=9, color=GOLD, va='top', ha='right')

    plt.tight_layout(rect=[0, 0, 1, 0.92])
    save(fig, 'viz_03_margin_stack.png')


# ═══════════════════════════════════════════════════════════════════
# 4. HEATMAP CALENDAR
# ═══════════════════════════════════════════════════════════════════
def build_heatmap():
    """Monthly data by MY, colored by deviation from 5-year average."""
    fig, axes = plt.subplots(1, 3, figsize=(16, 7))
    fig.patch.set_facecolor(BG)

    datasets = [
        ('US Soybean Crush', 'mil bu', MONTH_NAMES,
         # MYs as rows, months as columns (Sep-Aug)
         {
             '21/22': [185, 190, 195, 192, 180, 182, 186, 175, 170, 165, 172, 188],
             '22/23': [192, 198, 200, 195, 188, 185, 190, 178, 175, 170, 178, 195],
             '23/24': [198, 205, 210, 202, 195, 190, 196, 185, 180, 178, 185, 200],
             '24/25': [205, 212, 218, 210, 200, 195, 202, 190, 185, 182, 192, 208],
             '25/26': [210, 220, 225, 215, 205, 198, None, None, None, None, None, None],
         },
         [185, 195, 200, 195, 188, 185, 190, 180, 175, 172, 180, 195],  # 5yr avg
        ),
        ('US Soybean Exports', 'mil bu', MONTH_NAMES,
         {
             '21/22': [120, 250, 280, 220, 180, 150, 130, 100, 80, 60, 50, 40],
             '22/23': [130, 260, 290, 230, 185, 155, 135, 105, 85, 65, 55, 45],
             '23/24': [125, 270, 300, 240, 190, 160, 140, 110, 90, 70, 60, 50],
             '24/25': [135, 280, 310, 250, 200, 165, 145, 115, 95, 75, 65, 55],
             '25/26': [140, 290, 320, 260, 210, 170, None, None, None, None, None, None],
         },
         [125, 265, 295, 235, 190, 155, 135, 108, 88, 68, 58, 48],
        ),
        ('Crop Condition G/E %', '%', MONTH_NAMES_CAL[4:11],  # May-Nov
         {
             '2021': [None, None, None, None, None, 72, 68, 65, 60, 58, 57],
             '2022': [None, None, None, None, None, 70, 62, 55, 52, 50, 50],
             '2023': [None, None, None, None, None, 65, 60, 57, 54, 52, 51],
             '2024': [None, None, None, None, None, 72, 70, 68, 66, 64, 63],
             '2025': [None, None, None, None, None, 74, 72, 70, 67, 65, 64],
         },
         [None, None, None, None, None, 70, 66, 63, 60, 58, 57],
        ),
    ]

    for ax_idx, (title, unit, months, my_data, avg) in enumerate(datasets):
        ax = axes[ax_idx]
        ax.set_facecolor(PANEL)

        my_labels = list(my_data.keys())
        data = list(my_data.values())
        n_months = len(months)
        n_mys = len(my_labels)

        # Build deviation matrix
        dev_matrix = np.full((n_mys, n_months), np.nan)
        for i, row in enumerate(data):
            for j in range(min(len(row), n_months)):
                if row[j] is not None and avg[j] is not None and avg[j] != 0:
                    dev_matrix[i, j] = (row[j] - avg[j]) / avg[j] * 100

        # Custom colormap: red (below avg) → white (at avg) → green (above avg)
        from matplotlib.colors import LinearSegmentedColormap
        cmap = LinearSegmentedColormap.from_list('rg', [RED, '#333333', GREEN])

        im = ax.imshow(dev_matrix, cmap=cmap, aspect='auto', vmin=-15, vmax=15,
                       interpolation='nearest')

        # Cell labels (actual values)
        for i, row in enumerate(data):
            for j in range(min(len(row), n_months)):
                if row[j] is not None:
                    ax.text(j, i, f'{row[j]:.0f}', ha='center', va='center',
                            fontsize=7, color=WHITE, fontweight='bold')

        ax.set_xticks(range(n_months))
        ax.set_xticklabels(months[:n_months], fontsize=7, color=LIGHT, rotation=45)
        ax.set_yticks(range(n_mys))
        ax.set_yticklabels(my_labels, fontsize=8, color=LIGHT)
        ax.set_title(f'{title}\n({unit})', fontsize=10, fontweight='bold', color=GOLD, pad=10)

        # Grid
        ax.set_xticks(np.arange(-0.5, n_months, 1), minor=True)
        ax.set_yticks(np.arange(-0.5, n_mys, 1), minor=True)
        ax.grid(which='minor', color=BG, linewidth=1)

    fig.text(0.04, 0.98, 'MONTHLY DATA HEATMAP — Deviation from 5-Year Average',
             fontsize=14, fontweight='bold', color=WHITE, va='top')
    fig.text(0.04, 0.955, 'Green = above average  |  Red = below average  |  Values shown in cells',
             fontsize=9, color=GRAY, va='top')
    fig.text(0.96, 0.98, 'Round Lakes Companies', fontsize=9, color=GOLD, va='top', ha='right')

    plt.tight_layout(rect=[0, 0, 1, 0.93])
    save(fig, 'viz_04_heatmap.png')


# ═══════════════════════════════════════════════════════════════════
# RUN ALL
# ═══════════════════════════════════════════════════════════════════
if __name__ == '__main__':
    print("Generating report visualization mockups...")
    build_waterfall()
    build_trade_flow()
    build_margin_stack()
    build_heatmap()
    print(f"\nAll mockups saved to {OUT}/")
