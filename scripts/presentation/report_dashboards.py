"""
Report-Specific Dashboard Mockups
Each dashboard summarizes what a client needs from a specific USDA/EIA report.

The goal: clients come to OUR dashboard instead of reading the raw report.
"""

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch
import numpy as np
from pathlib import Path

OUT = Path("data/generated_graphics/report_mockups")
OUT.mkdir(parents=True, exist_ok=True)

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

def save(fig, name):
    fig.savefig(OUT / name, dpi=150, facecolor=fig.get_facecolor())
    plt.close(fig)
    print(f"  Saved {name}")


# ═══════════════════════════════════════════════════════════════════
# WASDE DASHBOARD
# ═══════════════════════════════════════════════════════════════════
def build_wasde_dashboard():
    """WASDE report dashboard — changes from prior month highlighted."""
    fig = plt.figure(figsize=(14, 10))
    fig.patch.set_facecolor(BG)

    fig.text(0.04, 0.98, 'WASDE REPORT DASHBOARD', fontsize=18,
             fontweight='bold', color=WHITE, va='top')
    fig.text(0.04, 0.965, 'April 9, 2026  |  Changes from March highlighted',
             fontsize=9, color=GRAY, va='top')
    fig.text(0.96, 0.98, 'Round Lakes Companies', fontsize=9, color=GOLD,
             va='top', ha='right')

    # Gold line
    fig.add_axes([0.03, 0.955, 0.94, 0.001]).set_facecolor(GOLD)
    fig.axes[-1].axis('off')

    # ── Key Changes Panel ──────────────────────────────────────────
    ax1 = fig.add_axes([0.03, 0.78, 0.94, 0.165])
    ax1.set_facecolor(PANEL)
    ax1.axis('off')

    ax1.text(0.02, 0.92, 'KEY CHANGES THIS MONTH', fontsize=12,
             fontweight='bold', color=GOLD, transform=ax1.transAxes, va='top')

    changes = [
        ('US Soybean Ending Stocks', '9,517 → 9,200', '-317', 'mil bu', 'Raised exports on China buying pace'),
        ('US Corn Production', '432,342 → 431,000', '-1,342', '1000 MT', 'Revised harvested area down'),
        ('Brazil Soybean Prod', '178,000 → 180,000', '+2,000', '1000 MT', 'CONAB aligned, safrinha strong'),
        ('World Wheat Stocks', '261,000 → 258,500', '-2,500', '1000 MT', 'Russia export pace above forecast'),
        ('US SBO Dom. Use', '13,581 → 14,000', '+419', '1000 MT', 'RD capacity additions, EMTS data'),
    ]

    y = 0.78
    for item, values, change, unit, reason in changes:
        chg_val = float(change.replace(',', '').replace('+', ''))
        color = GREEN if chg_val > 0 else RED

        ax1.add_patch(FancyBboxPatch(
            (0.02, y - 0.02), 0.96, 0.14,
            boxstyle="round,pad=0.005", facecolor='#1a1f27',
            edgecolor=color, linewidth=1, alpha=0.8,
            transform=ax1.transAxes))

        ax1.text(0.04, y + 0.04, item, fontsize=10, fontweight='bold',
                 color=WHITE, transform=ax1.transAxes)
        ax1.text(0.40, y + 0.04, values, fontsize=9, color=LIGHT,
                 transform=ax1.transAxes)
        ax1.text(0.60, y + 0.04, f'{change} {unit}', fontsize=10,
                 fontweight='bold', color=color, transform=ax1.transAxes)
        ax1.text(0.78, y + 0.04, reason, fontsize=8, color=GRAY,
                 style='italic', transform=ax1.transAxes)
        y -= 0.165

    # ── US S&D Snapshot (soybeans) ─────────────────────────────────
    ax2 = fig.add_axes([0.03, 0.42, 0.45, 0.34])
    ax2.set_facecolor(PANEL)
    ax2.axis('off')

    ax2.text(0.03, 0.97, 'US SOYBEAN S&D SNAPSHOT', fontsize=11,
             fontweight='bold', color=GOLD, transform=ax2.transAxes, va='top')

    rows = [
        ('', 'MY 24/25', 'MY 25/26', 'Change'),
        ('Area Harvested (mil ac)', '87.1', '86.3', '-0.8'),
        ('Yield (bu/ac)', '52.1', '51.7', '-0.4'),
        ('Production (mil bu)', '4,540', '4,461', '-79'),
        ('Beg. Stocks', '342', '350', '+8'),
        ('Imports', '25', '30', '+5'),
        ('Total Supply', '4,907', '4,841', '-66'),
        ('Crush', '2,380', '2,410', '+30'),
        ('Exports', '1,800', '1,770', '-30'),
        ('Total Use', '4,557', '4,560', '+3'),
        ('Ending Stocks', '350', '281', '-69'),
        ('STU %', '7.7%', '6.2%', '-1.5%'),
    ]

    y = 0.90
    for i, (item, col1, col2, chg) in enumerate(rows):
        if i == 0:
            ax2.text(0.03, y, item, fontsize=7, color=GRAY, fontweight='bold',
                     transform=ax2.transAxes)
            ax2.text(0.55, y, col1, fontsize=7, color=GRAY, fontweight='bold',
                     transform=ax2.transAxes, ha='right')
            ax2.text(0.75, y, col2, fontsize=7, color=GRAY, fontweight='bold',
                     transform=ax2.transAxes, ha='right')
            ax2.text(0.95, y, chg, fontsize=7, color=GRAY, fontweight='bold',
                     transform=ax2.transAxes, ha='right')
        else:
            is_total = item in ('Total Supply', 'Total Use', 'Ending Stocks', 'STU %')
            font_weight = 'bold' if is_total else 'normal'
            ax2.text(0.03, y, item, fontsize=8, color=WHITE if is_total else LIGHT,
                     fontweight=font_weight, transform=ax2.transAxes)
            ax2.text(0.55, y, col1, fontsize=8, color=LIGHT,
                     transform=ax2.transAxes, ha='right')
            ax2.text(0.75, y, col2, fontsize=8, color=WHITE, fontweight='bold',
                     transform=ax2.transAxes, ha='right')
            try:
                cv = float(chg.replace('%','').replace(',','').replace('+',''))
                color = GREEN if cv > 0 else RED if cv < 0 else GRAY
            except:
                color = GRAY
            ax2.text(0.95, y, chg, fontsize=8, color=color, fontweight='bold',
                     transform=ax2.transAxes, ha='right')
        y -= 0.07

    # ── Ending Stocks Trend ────────────────────────────────────────
    ax3 = fig.add_axes([0.52, 0.42, 0.45, 0.34])
    ax3.set_facecolor(PANEL)

    years = ['19/20', '20/21', '21/22', '22/23', '23/24', '24/25', '25/26E']
    soy_stocks = [525, 257, 274, 264, 342, 350, 281]
    stu = [13.2, 5.7, 6.3, 6.0, 7.7, 7.7, 6.2]

    ax3_twin = ax3.twinx()

    bars = ax3.bar(range(len(years)), soy_stocks, color=BLUE, alpha=0.7, width=0.6)
    bars[-1].set_color(GOLD)
    bars[-1].set_alpha(0.9)

    ax3_twin.plot(range(len(years)), stu, color=RED, linewidth=2, marker='o',
                  markersize=6, zorder=5)

    ax3.set_xticks(range(len(years)))
    ax3.set_xticklabels(years, fontsize=8, color=LIGHT)
    ax3.set_ylabel('Ending Stocks (mil bu)', fontsize=8, color=BLUE)
    ax3_twin.set_ylabel('STU %', fontsize=8, color=RED)
    ax3.tick_params(axis='y', colors=BLUE)
    ax3_twin.tick_params(axis='y', colors=RED)
    ax3.spines['top'].set_visible(False)
    ax3_twin.spines['top'].set_visible(False)
    ax3.spines['bottom'].set_color(GRAY)
    ax3.spines['left'].set_color(BLUE)
    ax3_twin.spines['right'].set_color(RED)
    ax3.set_title('US Soybean Ending Stocks & STU', fontsize=10,
                  fontweight='bold', color=GOLD, pad=10)

    # ── Global Balance Quick View ──────────────────────────────────
    ax4 = fig.add_axes([0.03, 0.05, 0.94, 0.34])
    ax4.set_facecolor(PANEL)
    ax4.axis('off')

    ax4.text(0.02, 0.97, 'GLOBAL COMMODITY SNAPSHOT — Key Changes', fontsize=11,
             fontweight='bold', color=GOLD, transform=ax4.transAxes, va='top')
    ax4.text(0.02, 0.91, 'Ending stocks change from prior WASDE  |  Arrow = directional bias for next month',
             fontsize=8, color=GRAY, transform=ax4.transAxes, va='top')

    commodities = [
        ('US Corn', 1.9, 'bil bu', GREEN, '↑'),
        ('US Soybeans', -69, 'mil bu', RED, '↓'),
        ('US Wheat', -25, 'mil bu', RED, '→'),
        ('World Corn', +2.1, 'MMT', GREEN, '→'),
        ('World Soybeans', -1.5, 'MMT', RED, '↓'),
        ('World Wheat', -2.5, 'MMT', RED, '↓'),
        ('US SBO Dom', +419, '1000 MT', GREEN, '↑'),
        ('Brazil Soy Prod', +2000, '1000 MT', GREEN, '↑'),
    ]

    y = 0.82
    for i, (name, change, unit, color, arrow) in enumerate(commodities):
        col = i % 4
        row = i // 4
        x = 0.02 + col * 0.245
        y_pos = 0.82 - row * 0.42

        ax4.add_patch(FancyBboxPatch(
            (x, y_pos - 0.05), 0.23, 0.35,
            boxstyle="round,pad=0.008", facecolor='#1a1f27',
            edgecolor=color, linewidth=1.5, alpha=0.8,
            transform=ax4.transAxes))

        ax4.text(x + 0.115, y_pos + 0.22, name, fontsize=9, fontweight='bold',
                 color=WHITE, ha='center', transform=ax4.transAxes)

        sign = '+' if change > 0 else ''
        ax4.text(x + 0.115, y_pos + 0.10, f'{sign}{change:,.0f}', fontsize=18,
                 fontweight='bold', color=color, ha='center', transform=ax4.transAxes)

        ax4.text(x + 0.115, y_pos + 0.01, unit, fontsize=8, color=GRAY,
                 ha='center', transform=ax4.transAxes)

        ax4.text(x + 0.20, y_pos + 0.26, arrow, fontsize=14, color=color,
                 ha='center', transform=ax4.transAxes)

    save(fig, 'rpt_01_wasde.png')


# ═══════════════════════════════════════════════════════════════════
# EIA WEEKLY PETROLEUM / ETHANOL
# ═══════════════════════════════════════════════════════════════════
def build_eia_dashboard():
    """EIA Weekly Petroleum dashboard — ethanol focus."""
    fig = plt.figure(figsize=(14, 10))
    fig.patch.set_facecolor(BG)

    fig.text(0.04, 0.98, 'EIA WEEKLY PETROLEUM STATUS — Ethanol & Biofuels Focus',
             fontsize=16, fontweight='bold', color=WHITE, va='top')
    fig.text(0.04, 0.965, 'Week Ending March 28, 2026  |  WoW changes highlighted',
             fontsize=9, color=GRAY, va='top')
    fig.text(0.96, 0.98, 'Round Lakes Companies', fontsize=9, color=GOLD,
             va='top', ha='right')

    fig.add_axes([0.03, 0.955, 0.94, 0.001]).set_facecolor(GOLD)
    fig.axes[-1].axis('off')

    # ── Key Metrics Strip ──────────────────────────────────────────
    ax0 = fig.add_axes([0.03, 0.91, 0.94, 0.04])
    ax0.set_facecolor(PANEL)
    ax0.axis('off')

    metrics = [
        ('Ethanol Prod', '1,068 kbd', '+12', GREEN),
        ('Ethanol Stocks', '26.4 mil bbl', '-0.8', RED),
        ('Gasoline Demand', '8.92 mbd', '+0.15', GREEN),
        ('Distillate Stocks', '118 mil bbl', '-2.1', RED),
        ('Crude Imports', '6.2 mbd', '-0.3', RED),
        ('Refinery Util', '88.5%', '+0.8', GREEN),
    ]

    for i, (name, value, change, color) in enumerate(metrics):
        x = 0.01 + i * 0.165
        ax0.text(x, 0.85, name, fontsize=7, color=GRAY, fontweight='bold',
                 transform=ax0.transAxes, va='top')
        ax0.text(x, 0.35, value, fontsize=10, color=WHITE, fontweight='bold',
                 transform=ax0.transAxes, va='center')
        ax0.text(x + 0.10, 0.35, change, fontsize=8, color=color,
                 transform=ax0.transAxes, va='center')

    # ── Ethanol Production Chart ───────────────────────────────────
    ax1 = fig.add_axes([0.03, 0.52, 0.45, 0.37])
    ax1.set_facecolor(PANEL)

    weeks = list(range(52))
    # Simulated ethanol production data
    np.random.seed(42)
    prod_2025 = 1020 + np.cumsum(np.random.randn(52) * 8)
    prod_2026 = 1040 + np.cumsum(np.random.randn(52) * 8)
    prod_5yr_avg = 1010 + np.sin(np.linspace(0, 2*np.pi, 52)) * 30

    ax1.fill_between(weeks, prod_5yr_avg - 40, prod_5yr_avg + 40,
                     alpha=0.15, color=BLUE, label='5-yr range')
    ax1.plot(weeks, prod_5yr_avg, color=BLUE, alpha=0.5, linewidth=1,
             linestyle='--', label='5-yr avg')
    ax1.plot(weeks[:40], prod_2025[:40], color=GRAY, linewidth=1.5,
             label='2024/25')
    ax1.plot(weeks[:26], prod_2026[:26], color=GOLD, linewidth=2.5,
             label='2025/26')

    ax1.set_title('US Ethanol Production (kbd)', fontsize=10,
                  fontweight='bold', color=GOLD, pad=10)
    ax1.set_xlabel('Week', fontsize=8, color=GRAY)
    ax1.tick_params(colors=GRAY)
    ax1.spines['top'].set_visible(False)
    ax1.spines['right'].set_visible(False)
    ax1.spines['bottom'].set_color(GRAY)
    ax1.spines['left'].set_color(GRAY)
    ax1.legend(fontsize=7, facecolor=PANEL, edgecolor=GRAY, labelcolor=LIGHT)

    # ── Ethanol Stocks Chart ───────────────────────────────────────
    ax2 = fig.add_axes([0.52, 0.52, 0.45, 0.37])
    ax2.set_facecolor(PANEL)

    stocks_2025 = 24 + np.cumsum(np.random.randn(52) * 0.3)
    stocks_2026 = 25 + np.cumsum(np.random.randn(52) * 0.3)
    stocks_5yr = 24.5 + np.sin(np.linspace(0, 2*np.pi, 52)) * 1.5

    ax2.fill_between(weeks, stocks_5yr - 2, stocks_5yr + 2,
                     alpha=0.15, color=BLUE)
    ax2.plot(weeks, stocks_5yr, color=BLUE, alpha=0.5, linewidth=1, linestyle='--')
    ax2.plot(weeks[:40], stocks_2025[:40], color=GRAY, linewidth=1.5)
    ax2.plot(weeks[:26], stocks_2026[:26], color=GOLD, linewidth=2.5)

    ax2.set_title('US Ethanol Stocks (mil bbl)', fontsize=10,
                  fontweight='bold', color=GOLD, pad=10)
    ax2.set_xlabel('Week', fontsize=8, color=GRAY)
    ax2.tick_params(colors=GRAY)
    ax2.spines['top'].set_visible(False)
    ax2.spines['right'].set_visible(False)
    ax2.spines['bottom'].set_color(GRAY)
    ax2.spines['left'].set_color(GRAY)

    # ── Implied Ethanol Margins ────────────────────────────────────
    ax3 = fig.add_axes([0.03, 0.08, 0.45, 0.40])
    ax3.set_facecolor(PANEL)

    margin_weeks = list(range(26))
    corn_price = 4.50 + np.cumsum(np.random.randn(26) * 0.03)
    ethanol_price = 1.70 + np.cumsum(np.random.randn(26) * 0.02)
    margin = ethanol_price * 2.8 - corn_price - 0.55  # Simplified crush margin

    ax3.fill_between(margin_weeks, 0, margin, where=margin > 0,
                     alpha=0.4, color=GREEN)
    ax3.fill_between(margin_weeks, 0, margin, where=margin <= 0,
                     alpha=0.4, color=RED)
    ax3.plot(margin_weeks, margin, color=GOLD, linewidth=2)
    ax3.axhline(y=0, color=GRAY, linewidth=0.5, linestyle='--')

    ax3.set_title('Ethanol Crush Margin ($/bu corn equivalent)', fontsize=10,
                  fontweight='bold', color=GOLD, pad=10)
    ax3.tick_params(colors=GRAY)
    ax3.spines['top'].set_visible(False)
    ax3.spines['right'].set_visible(False)
    ax3.spines['bottom'].set_color(GRAY)
    ax3.spines['left'].set_color(GRAY)

    # ── Biofuel Feedstock Demand ───────────────────────────────────
    ax4 = fig.add_axes([0.52, 0.08, 0.45, 0.40])
    ax4.set_facecolor(PANEL)

    months = ['Oct','Nov','Dec','Jan','Feb','Mar']
    sbo_use = [850, 880, 900, 920, 910, 950]
    tallow_use = [320, 340, 350, 360, 355, 370]
    uco_use = [180, 190, 200, 210, 205, 220]
    other_use = [250, 260, 270, 280, 275, 290]

    x = np.arange(len(months))
    width = 0.6

    ax4.bar(x, sbo_use, width, label='SBO', color=BLUE, alpha=0.8)
    ax4.bar(x, tallow_use, width, bottom=sbo_use, label='Tallow', color=ORANGE, alpha=0.8)
    ax4.bar(x, uco_use, width, bottom=np.array(sbo_use)+np.array(tallow_use),
            label='UCO', color=TEAL, alpha=0.8)
    ax4.bar(x, other_use, width,
            bottom=np.array(sbo_use)+np.array(tallow_use)+np.array(uco_use),
            label='Other', color=GRAY, alpha=0.6)

    ax4.set_xticks(x)
    ax4.set_xticklabels(months, fontsize=8, color=LIGHT)
    ax4.set_title('BBD Feedstock Use by Type (mil lbs)', fontsize=10,
                  fontweight='bold', color=GOLD, pad=10)
    ax4.tick_params(colors=GRAY)
    ax4.spines['top'].set_visible(False)
    ax4.spines['right'].set_visible(False)
    ax4.spines['bottom'].set_color(GRAY)
    ax4.spines['left'].set_color(GRAY)
    ax4.legend(fontsize=7, facecolor=PANEL, edgecolor=GRAY, labelcolor=LIGHT,
               loc='upper left')

    save(fig, 'rpt_02_eia_weekly.png')


# ═══════════════════════════════════════════════════════════════════
# CROP PROGRESS DASHBOARD
# ═══════════════════════════════════════════════════════════════════
def build_crop_progress_dashboard():
    """NASS Crop Progress — conditions and planting pace."""
    fig = plt.figure(figsize=(14, 10))
    fig.patch.set_facecolor(BG)

    fig.text(0.04, 0.98, 'CROP PROGRESS & CONDITIONS DASHBOARD', fontsize=18,
             fontweight='bold', color=WHITE, va='top')
    fig.text(0.04, 0.965, 'Week Ending March 30, 2026  |  Season: Pre-planting',
             fontsize=9, color=GRAY, va='top')
    fig.text(0.96, 0.98, 'Round Lakes Companies', fontsize=9, color=GOLD,
             va='top', ha='right')

    fig.add_axes([0.03, 0.955, 0.94, 0.001]).set_facecolor(GOLD)
    fig.axes[-1].axis('off')

    # ── Corn Planting Progress ─────────────────────────────────────
    ax1 = fig.add_axes([0.03, 0.52, 0.45, 0.42])
    ax1.set_facecolor(PANEL)

    weeks = list(range(14, 26))
    week_labels = [f'Wk {w}' for w in weeks]

    # Planting progress (% complete)
    avg_5yr = [0, 2, 6, 14, 33, 56, 72, 83, 90, 94, 96, 97]
    yr_2025 = [0, 1, 4, 10, 28, 50, 68, 80, 88, 93, 95, 97]
    yr_2026 = [0, 3, 8, None, None, None, None, None, None, None, None, None]

    ax1.fill_between(range(len(avg_5yr)),
                     [max(0, a-8) for a in avg_5yr],
                     [min(100, a+8) for a in avg_5yr],
                     alpha=0.15, color=BLUE, label='5-yr range')
    ax1.plot(range(len(avg_5yr)), avg_5yr, color=BLUE, linewidth=1.5,
             linestyle='--', label='5-yr avg', alpha=0.7)
    ax1.plot(range(len(yr_2025)), yr_2025, color=GRAY, linewidth=1.5,
             label='2025')

    actual_2026 = [v for v in yr_2026 if v is not None]
    ax1.plot(range(len(actual_2026)), actual_2026, color=GOLD, linewidth=3,
             marker='o', markersize=6, label='2026')

    ax1.set_xticks(range(len(week_labels)))
    ax1.set_xticklabels(week_labels, fontsize=7, color=LIGHT, rotation=45)
    ax1.set_ylabel('% Planted', fontsize=9, color=GRAY)
    ax1.set_ylim(0, 105)
    ax1.set_title('US Corn Planting Progress', fontsize=11,
                  fontweight='bold', color=GOLD, pad=10)
    ax1.tick_params(colors=GRAY)
    ax1.spines['top'].set_visible(False)
    ax1.spines['right'].set_visible(False)
    ax1.spines['bottom'].set_color(GRAY)
    ax1.spines['left'].set_color(GRAY)
    ax1.legend(fontsize=8, facecolor=PANEL, edgecolor=GRAY, labelcolor=LIGHT)

    # ── Corn Condition Ratings ─────────────────────────────────────
    ax2 = fig.add_axes([0.52, 0.52, 0.45, 0.42])
    ax2.set_facecolor(PANEL)

    # Weekly G/E percentage
    cond_weeks = list(range(22, 40))
    ge_2024 = [74, 72, 70, 68, 67, 67, 66, 65, 65, 64, 64, 63, 63, 63, 63, 63, 63, 63]
    ge_2025 = [72, 70, 68, 65, 64, 64, 65, 66, 66, 65, 65, 64, 64, 64, 64, 64, None, None]
    ge_avg = [70, 68, 67, 66, 65, 64, 63, 62, 61, 61, 60, 60, 60, 59, 59, 59, 59, 59]

    ax2.fill_between(range(len(ge_avg)),
                     [max(0, a-6) for a in ge_avg],
                     [min(100, a+6) for a in ge_avg],
                     alpha=0.15, color=BLUE)
    ax2.plot(range(len(ge_avg)), ge_avg, color=BLUE, linewidth=1.5,
             linestyle='--', alpha=0.7, label='5-yr avg')
    ax2.plot(range(len(ge_2024)), ge_2024, color=GRAY, linewidth=1.5, label='2024')

    actual_ge = [v for v in ge_2025 if v is not None]
    ax2.plot(range(len(actual_ge)), actual_ge, color=GOLD, linewidth=3,
             marker='o', markersize=4, label='2025')

    ax2.set_xticks(range(0, len(cond_weeks), 2))
    ax2.set_xticklabels([f'Wk {w}' for w in cond_weeks[::2]], fontsize=7,
                        color=LIGHT, rotation=45)
    ax2.set_ylabel('Good/Excellent %', fontsize=9, color=GRAY)
    ax2.set_ylim(45, 80)
    ax2.set_title('US Corn Condition — Good/Excellent %', fontsize=11,
                  fontweight='bold', color=GOLD, pad=10)
    ax2.tick_params(colors=GRAY)
    ax2.spines['top'].set_visible(False)
    ax2.spines['right'].set_visible(False)
    ax2.spines['bottom'].set_color(GRAY)
    ax2.spines['left'].set_color(GRAY)
    ax2.legend(fontsize=8, facecolor=PANEL, edgecolor=GRAY, labelcolor=LIGHT)

    # ── State-Level Conditions Map (text-based for mockup) ─────────
    ax3 = fig.add_axes([0.03, 0.05, 0.94, 0.43])
    ax3.set_facecolor(PANEL)
    ax3.axis('off')

    ax3.text(0.02, 0.97, 'STATE-LEVEL CONDITIONS & YIELD IMPLICATIONS',
             fontsize=11, fontweight='bold', color=GOLD,
             transform=ax3.transAxes, va='top')

    states = [
        ('Iowa', 75, +3, 198, +2.0),
        ('Illinois', 72, +1, 195, +0.5),
        ('Indiana', 68, -2, 188, -1.5),
        ('Nebraska', 70, +2, 192, +1.0),
        ('Minnesota', 74, +4, 196, +2.5),
        ('Ohio', 65, -4, 182, -3.0),
        ('South Dakota', 71, +3, 190, +1.5),
        ('Wisconsin', 69, 0, 185, 0.0),
        ('Missouri', 64, -3, 178, -2.0),
        ('Kansas', 62, -5, 170, -4.0),
    ]

    # Headers
    headers = ['State', 'G/E %', 'WoW Chg', 'Yield Est', 'vs Trend']
    for j, h in enumerate(headers):
        x = 0.02 + j * 0.19
        ax3.text(x, 0.90, h, fontsize=8, color=GRAY, fontweight='bold',
                 transform=ax3.transAxes)

    y = 0.84
    for state, ge, ge_chg, yield_est, yield_vs_trend in states:
        ge_color = GREEN if ge >= 70 else GOLD if ge >= 65 else RED
        chg_color = GREEN if ge_chg > 0 else RED if ge_chg < 0 else GRAY
        yld_color = GREEN if yield_vs_trend > 0 else RED if yield_vs_trend < 0 else GRAY

        ax3.text(0.02, y, state, fontsize=9, color=WHITE, transform=ax3.transAxes)
        ax3.text(0.21, y, f'{ge}%', fontsize=9, color=ge_color, fontweight='bold',
                 transform=ax3.transAxes)
        ax3.text(0.40, y, f'{ge_chg:+d}', fontsize=9, color=chg_color,
                 fontweight='bold', transform=ax3.transAxes)
        ax3.text(0.59, y, f'{yield_est} bu/ac', fontsize=9, color=WHITE,
                 transform=ax3.transAxes)
        ax3.text(0.78, y, f'{yield_vs_trend:+.1f}', fontsize=9, color=yld_color,
                 fontweight='bold', transform=ax3.transAxes)

        # Mini progress bar for G/E
        bar_x = 0.12
        bar_w = 0.07 * (ge / 100)
        ax3.add_patch(FancyBboxPatch(
            (bar_x, y - 0.005), 0.07, 0.022,
            boxstyle="round,pad=0.002", facecolor='#252a32',
            edgecolor='none', transform=ax3.transAxes, zorder=1))
        ax3.add_patch(FancyBboxPatch(
            (bar_x, y - 0.005), bar_w, 0.022,
            boxstyle="round,pad=0.002", facecolor=ge_color,
            edgecolor='none', alpha=0.6, transform=ax3.transAxes, zorder=2))

        y -= 0.065

    save(fig, 'rpt_03_crop_progress.png')


# ═══════════════════════════════════════════════════════════════════
# NASS FATS & OILS DASHBOARD
# ═══════════════════════════════════════════════════════════════════
def build_fats_oils_dashboard():
    """NASS Fats & Oils report — crush, oil production, stocks."""
    fig = plt.figure(figsize=(14, 10))
    fig.patch.set_facecolor(BG)

    fig.text(0.04, 0.98, 'NASS FATS & OILS REPORT DASHBOARD', fontsize=18,
             fontweight='bold', color=WHITE, va='top')
    fig.text(0.04, 0.965, 'February 2026 Data  |  Released March 2026',
             fontsize=9, color=GRAY, va='top')
    fig.text(0.96, 0.98, 'Round Lakes Companies', fontsize=9, color=GOLD,
             va='top', ha='right')

    fig.add_axes([0.03, 0.955, 0.94, 0.001]).set_facecolor(GOLD)
    fig.axes[-1].axis('off')

    # ── Key Metrics ────────────────────────────────────────────────
    ax0 = fig.add_axes([0.03, 0.88, 0.94, 0.065])
    ax0.set_facecolor(PANEL)
    ax0.axis('off')

    for i, (name, value, change, color) in enumerate([
        ('Soy Crush', '205.2 mil bu', '+3.8 MoM', GREEN),
        ('Soy Oil Prod', '2,287 mil lbs', '+42 MoM', GREEN),
        ('Oil Yield', '11.15 lbs/bu', '+0.02', GREEN),
        ('SBO Stocks', '1,842 mil lbs', '-120 MoM', RED),
        ('Canola Crush', '298 thou MT', '-12 MoM', RED),
        ('Cottonseed Crush', '180 thou ST', '+8 MoM', GREEN),
    ]):
        x = 0.01 + i * 0.165
        ax0.text(x, 0.85, name, fontsize=7.5, color=GRAY, fontweight='bold',
                 transform=ax0.transAxes, va='top')
        ax0.text(x, 0.40, value, fontsize=9, color=WHITE, fontweight='bold',
                 transform=ax0.transAxes, va='center')
        ax0.text(x, 0.05, change, fontsize=8, color=color,
                 transform=ax0.transAxes, va='bottom')

    # ── Soybean Crush Pace ─────────────────────────────────────────
    ax1 = fig.add_axes([0.03, 0.48, 0.45, 0.38])
    ax1.set_facecolor(PANEL)

    months = ['Sep','Oct','Nov','Dec','Jan','Feb','Mar','Apr','May','Jun','Jul','Aug']
    crush_2425 = [175, 195, 205, 210, 198, 205, 200, 195, 190, 185, 180, 190]
    crush_2526 = [180, 200, 212, 218, 202, 205, None, None, None, None, None, None]
    usda_proj = [2420]  # Annual total

    actual = [v for v in crush_2526 if v is not None]
    cumul_actual = np.cumsum(actual)
    cumul_prior = np.cumsum(crush_2425[:len(actual)])

    ax1.bar(range(len(actual)), actual, color=GOLD, alpha=0.8, width=0.6, label='MY 25/26')
    ax1.bar(range(len(crush_2425)), crush_2425, color=BLUE, alpha=0.3,
            width=0.6, label='MY 24/25')

    ax1.set_xticks(range(len(months)))
    ax1.set_xticklabels(months, fontsize=7, color=LIGHT)
    ax1.set_ylabel('mil bu', fontsize=8, color=GRAY)
    ax1.set_title('Monthly Soybean Crush', fontsize=10,
                  fontweight='bold', color=GOLD, pad=10)
    ax1.tick_params(colors=GRAY)
    ax1.spines['top'].set_visible(False)
    ax1.spines['right'].set_visible(False)
    ax1.spines['bottom'].set_color(GRAY)
    ax1.spines['left'].set_color(GRAY)
    ax1.legend(fontsize=7, facecolor=PANEL, edgecolor=GRAY, labelcolor=LIGHT)

    # Pace annotation
    pace_pct = cumul_actual[-1] / usda_proj[0] * 100
    ax1.text(0.95, 0.95, f'Pace: {pace_pct:.1f}% of\nUSDA proj through {months[len(actual)-1]}',
             fontsize=8, color=GOLD, fontweight='bold',
             transform=ax1.transAxes, ha='right', va='top')

    # ── Oil Production & Yield ─────────────────────────────────────
    ax2 = fig.add_axes([0.52, 0.48, 0.45, 0.38])
    ax2.set_facecolor(PANEL)

    oil_prod = [1950, 2150, 2280, 2350, 2200, 2287, None, None, None, None, None, None]
    oil_yield = [11.14, 11.05, 11.12, 11.18, 11.11, 11.15, None, None, None, None, None, None]

    actual_prod = [v for v in oil_prod if v is not None]
    actual_yield = [v for v in oil_yield if v is not None]

    ax2.bar(range(len(actual_prod)), actual_prod, color=BLUE, alpha=0.7, width=0.6)

    ax2_twin = ax2.twinx()
    ax2_twin.plot(range(len(actual_yield)), actual_yield, color=GOLD,
                  linewidth=2.5, marker='D', markersize=6)

    ax2.set_xticks(range(len(months)))
    ax2.set_xticklabels(months, fontsize=7, color=LIGHT)
    ax2.set_ylabel('mil lbs', fontsize=8, color=BLUE)
    ax2_twin.set_ylabel('lbs/bu', fontsize=8, color=GOLD)
    ax2.set_title('SBO Production & Extraction Rate', fontsize=10,
                  fontweight='bold', color=GOLD, pad=10)
    ax2.tick_params(axis='y', colors=BLUE)
    ax2_twin.tick_params(axis='y', colors=GOLD)
    ax2.spines['top'].set_visible(False)
    ax2_twin.spines['top'].set_visible(False)
    ax2.spines['bottom'].set_color(GRAY)
    ax2.spines['left'].set_color(BLUE)
    ax2_twin.spines['right'].set_color(GOLD)

    # ── Multi-Oilseed Crush Comparison ─────────────────────────────
    ax3 = fig.add_axes([0.03, 0.05, 0.45, 0.40])
    ax3.set_facecolor(PANEL)
    ax3.axis('off')

    ax3.text(0.03, 0.97, 'OILSEED CRUSH COMPARISON', fontsize=11,
             fontweight='bold', color=GOLD, transform=ax3.transAxes, va='top')

    oilseeds = [
        ('Soybeans', '205.2 mil bu', '+3.8', '+1.9%', '50.3%', GREEN),
        ('Canola', '298 thou MT', '-12', '-3.9%', '48.8%', RED),
        ('Cottonseed', '180 thou ST', '+8', '+4.7%', '45.2%', GREEN),
        ('Sunflower', '82 thou ST', '+2', '+2.5%', '42.1%', GREEN),
        ('Corn (for eth)', '478 mil bu', '+5', '+1.1%', '89.2%', GREEN),
    ]

    headers = ['Oilseed', 'Crush', 'MoM Chg', 'MoM %', 'vs USDA Pace']
    y = 0.88
    for j, h in enumerate(headers):
        x = 0.03 + j * 0.195
        ax3.text(x, y, h, fontsize=7, color=GRAY, fontweight='bold',
                 transform=ax3.transAxes)

    y = 0.80
    for name, crush, chg, pct, pace, color in oilseeds:
        ax3.text(0.03, y, name, fontsize=9, color=WHITE, transform=ax3.transAxes)
        ax3.text(0.22, y, crush, fontsize=8, color=LIGHT, transform=ax3.transAxes)
        ax3.text(0.46, y, chg, fontsize=8, color=color, fontweight='bold',
                 transform=ax3.transAxes)
        ax3.text(0.61, y, pct, fontsize=8, color=color, transform=ax3.transAxes)
        ax3.text(0.80, y, pace, fontsize=8, color=WHITE, fontweight='bold',
                 transform=ax3.transAxes)
        y -= 0.12

    # ── SBO Stocks & Days Coverage ─────────────────────────────────
    ax4 = fig.add_axes([0.52, 0.05, 0.45, 0.40])
    ax4.set_facecolor(PANEL)

    stock_months = ['Sep','Oct','Nov','Dec','Jan','Feb']
    stocks = [2100, 2050, 1980, 1920, 1962, 1842]
    days_coverage = [28, 27, 26, 25, 26, 24]

    ax4.bar(range(len(stocks)), stocks, color=BLUE, alpha=0.7, width=0.6)

    ax4_twin = ax4.twinx()
    ax4_twin.plot(range(len(days_coverage)), days_coverage, color=RED,
                  linewidth=2.5, marker='o', markersize=6)

    ax4.set_xticks(range(len(stock_months)))
    ax4.set_xticklabels(stock_months, fontsize=8, color=LIGHT)
    ax4.set_ylabel('mil lbs', fontsize=8, color=BLUE)
    ax4_twin.set_ylabel('Days Coverage', fontsize=8, color=RED)
    ax4.set_title('SBO Ending Stocks & Days of Coverage', fontsize=10,
                  fontweight='bold', color=GOLD, pad=10)
    ax4.tick_params(axis='y', colors=BLUE)
    ax4_twin.tick_params(axis='y', colors=RED)
    ax4.spines['top'].set_visible(False)
    ax4_twin.spines['top'].set_visible(False)
    ax4.spines['bottom'].set_color(GRAY)
    ax4.spines['left'].set_color(BLUE)
    ax4_twin.spines['right'].set_color(RED)

    save(fig, 'rpt_04_fats_oils.png')


# ═══════════════════════════════════════════════════════════════════
# CFTC POSITIONING DASHBOARD
# ═══════════════════════════════════════════════════════════════════
def build_cftc_dashboard():
    """CFTC COT report — managed money positioning."""
    fig = plt.figure(figsize=(14, 10))
    fig.patch.set_facecolor(BG)

    fig.text(0.04, 0.98, 'CFTC COMMITMENT OF TRADERS — Managed Money Positioning',
             fontsize=16, fontweight='bold', color=WHITE, va='top')
    fig.text(0.04, 0.965, 'Report Date: March 28, 2026  |  Percentile vs 3-year history',
             fontsize=9, color=GRAY, va='top')
    fig.text(0.96, 0.98, 'Round Lakes Companies', fontsize=9, color=GOLD,
             va='top', ha='right')

    fig.add_axes([0.03, 0.955, 0.94, 0.001]).set_facecolor(GOLD)
    fig.axes[-1].axis('off')

    # ── Positioning Summary with Percentile Gauges ─────────────────
    ax1 = fig.add_axes([0.03, 0.05, 0.94, 0.89])
    ax1.set_facecolor(PANEL)
    ax1.axis('off')

    commodities = [
        ('Corn', +258000, 78, +32000, 'Approaching crowded long — liquidation risk above 90th'),
        ('Soybeans', +45000, 42, -12000, 'Neutral positioning — room to build either direction'),
        ('Soybean Oil', +82000, 85, +15000, 'Elevated long — RD demand narrative driving spec buying'),
        ('Soybean Meal', -15000, 25, -8000, 'Modest short — crush margins supportive but demand concerns'),
        ('Wheat (CBOT)', -42000, 18, +5000, 'Deep short — covering rally risk if Russia export pace slows'),
        ('Crude Oil', +180000, 65, -45000, 'Long but unwinding — Iran diplomatic signals reducing premium'),
        ('Corn (mini)', +18000, 55, +3000, 'Neutral — following full-size positioning'),
        ('Cotton', +28000, 48, -5000, 'Flat — waiting for planted acreage clarity'),
    ]

    y = 0.95
    ax1.text(0.02, y, 'Commodity', fontsize=8, color=GRAY, fontweight='bold',
             transform=ax1.transAxes)
    ax1.text(0.18, y, 'MM Net', fontsize=8, color=GRAY, fontweight='bold',
             transform=ax1.transAxes, ha='center')
    ax1.text(0.30, y, 'Percentile', fontsize=8, color=GRAY, fontweight='bold',
             transform=ax1.transAxes, ha='center')
    ax1.text(0.50, y, 'WoW Change', fontsize=8, color=GRAY, fontweight='bold',
             transform=ax1.transAxes, ha='center')
    ax1.text(0.72, y, 'Assessment', fontsize=8, color=GRAY, fontweight='bold',
             transform=ax1.transAxes)

    y = 0.88
    for name, net, pctl, wow, assessment in commodities:
        # Percentile color
        if pctl > 80: pctl_color = RED
        elif pctl < 20: pctl_color = GREEN
        else: pctl_color = BLUE

        wow_color = GREEN if wow > 0 else RED

        ax1.text(0.02, y, name, fontsize=10, color=WHITE, fontweight='bold',
                 transform=ax1.transAxes)

        # Net position
        ax1.text(0.18, y, f'{net:+,.0f}', fontsize=10, color=WHITE,
                 transform=ax1.transAxes, ha='center')

        # Percentile gauge bar
        gauge_x = 0.24
        gauge_w = 0.12
        ax1.add_patch(FancyBboxPatch(
            (gauge_x, y - 0.008), gauge_w, 0.018,
            boxstyle="round,pad=0.002", facecolor='#252a32',
            edgecolor='none', transform=ax1.transAxes, zorder=1))

        fill_w = gauge_w * (pctl / 100)
        ax1.add_patch(FancyBboxPatch(
            (gauge_x, y - 0.008), fill_w, 0.018,
            boxstyle="round,pad=0.002", facecolor=pctl_color,
            edgecolor='none', alpha=0.7, transform=ax1.transAxes, zorder=2))

        ax1.text(gauge_x + gauge_w + 0.01, y, f'{pctl}th',
                 fontsize=8, color=pctl_color, fontweight='bold',
                 transform=ax1.transAxes, va='center')

        # WoW change
        sign = '+' if wow > 0 else ''
        ax1.text(0.50, y, f'{sign}{wow:,.0f}', fontsize=9, color=wow_color,
                 fontweight='bold', transform=ax1.transAxes, ha='center')

        # Assessment
        ax1.text(0.58, y, assessment, fontsize=7.5, color=LIGHT, style='italic',
                 transform=ax1.transAxes)

        y -= 0.095

    # Legend
    ax1.text(0.02, 0.04, 'Percentile color:', fontsize=7, color=GRAY,
             transform=ax1.transAxes)
    for clr, lbl, xp in [(GREEN, '<20th (deep short/contrarian)', 0.12),
                          (BLUE, '20-80th (neutral)', 0.38),
                          (RED, '>80th (crowded/liquidation risk)', 0.55)]:
        ax1.plot(xp, 0.043, 's', markersize=5, color=clr, transform=ax1.transAxes)
        ax1.text(xp + 0.012, 0.04, lbl, fontsize=6.5, color=clr,
                 transform=ax1.transAxes)

    save(fig, 'rpt_05_cftc.png')


# ═══════════════════════════════════════════════════════════════════
# RUN ALL
# ═══════════════════════════════════════════════════════════════════
if __name__ == '__main__':
    print("Generating report dashboard mockups...")
    build_wasde_dashboard()
    build_eia_dashboard()
    build_crop_progress_dashboard()
    build_fats_oils_dashboard()
    build_cftc_dashboard()
    print(f"\nAll report dashboards saved to {OUT}/")
