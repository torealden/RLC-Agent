"""
Before/After Workflow Comparison — Presentation Visual
Contrasts a commodity analyst's manual Monday vs. AI-powered Monday.
"""

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch
from datetime import datetime

# ── RLC Color Palette ──────────────────────────────────────────────
COLORS = {
    'primary':    '#1f4e79',
    'secondary':  '#2e75b6',
    'accent':     '#70ad47',
    'warning':    '#ed7d31',
    'negative':   '#c00000',
    'neutral':    '#7f7f7f',
    'light_gray': '#e8e8e8',
    'bg_before':  '#fdf0e2',
    'bg_after':   '#e6f2e6',
    # Task type colors
    'download':   '#c55a11',
    'entry':      '#c00000',
    'format':     '#bf8f00',
    'reading':    '#7f6000',
    'analysis':   '#2e75b6',
    'insight':    '#1f4e79',
    'automated':  '#70ad47',
    'ai_assist':  '#548235',
    'overnight':  '#b4d69c',
}

# ── Style Setup ────────────────────────────────────────────────────
plt.style.use('seaborn-v0_8-whitegrid')
plt.rcParams.update({
    'font.size': 10,
    'axes.titlesize': 14,
    'axes.labelsize': 12,
    'font.family': 'sans-serif',
    'font.sans-serif': ['Segoe UI', 'Arial', 'Helvetica'],
})


def draw_task_block(ax, x, y, width, height, color, label, alpha=0.92,
                    fontsize=8.5, bold=False):
    """Draw a rounded task block with centered label."""
    box = FancyBboxPatch(
        (x, y), width, height,
        boxstyle="round,pad=0.08",
        facecolor=color, edgecolor='white', linewidth=1.5, alpha=alpha,
        zorder=3
    )
    ax.add_patch(box)
    weight = 'bold' if bold else 'normal'
    ax.text(
        x + width / 2, y + height / 2, label,
        ha='center', va='center', fontsize=fontsize,
        color='white', weight=weight, zorder=4,
        wrap=True
    )


def draw_time_label(ax, x, y, label, fontsize=8, color='#555555'):
    """Draw a time duration label below a block."""
    ax.text(x, y, label, ha='center', va='top', fontsize=fontsize,
            color=color, style='italic', zorder=4)


def create_before_after():
    fig, (ax_before, ax_after) = plt.subplots(
        2, 1, figsize=(16, 9), height_ratios=[1, 1]
    )
    fig.patch.set_facecolor('#fafafa')

    # ── Title ──────────────────────────────────────────────────────
    fig.suptitle(
        '"Monday Morning" — Before & After',
        fontsize=22, fontweight='bold', color=COLORS['primary'],
        y=0.97
    )
    fig.text(
        0.5, 0.935,
        'How a commodity analyst spends the first hours of the week',
        ha='center', fontsize=13, color=COLORS['neutral'], style='italic'
    )

    # ================================================================
    #  BEFORE — Manual Workflow
    # ================================================================
    ax = ax_before
    ax.set_facecolor(COLORS['bg_before'])
    ax.set_xlim(0, 20)
    ax.set_ylim(0, 3.5)
    ax.axis('off')

    # Section header
    ax.text(0.3, 3.05, 'BEFORE', fontsize=18, fontweight='bold',
            color=COLORS['negative'], va='center')
    ax.text(2.6, 3.05, '—  Manual Data Collection & Entry',
            fontsize=13, color=COLORS['neutral'], va='center')

    # Time axis
    y_bar = 1.6
    bar_h = 1.1
    gap = 0.12

    # Tasks (x_start, width, color, label, time_label)
    before_tasks = [
        (0.3,  2.4, COLORS['download'], 'Download\nWASDE PDF',       '~25 min'),
        (2.82, 2.4, COLORS['entry'],    'Key In\nS&D Numbers',       '~30 min'),
        (5.34, 2.0, COLORS['download'], 'Pull CFTC\nfrom Website',   '~15 min'),
        (7.46, 1.8, COLORS['entry'],    'Update COT\nSpreadsheet',   '~20 min'),
        (9.38, 1.8, COLORS['download'], 'EIA Ethanol\nDownload',     '~15 min'),
        (11.3, 1.5, COLORS['format'],   'Format &\nCopy Data',       '~15 min'),
        (12.92,2.0, COLORS['download'], 'Export Sales\nReport',       '~20 min'),
        (15.04,1.5, COLORS['reading'],  'Read Weather\nEmails',       '~15 min'),
        (16.66,1.2, COLORS['download'], 'Crop\nProgress',            '~10 min'),
        (17.98,1.7, COLORS['analysis'], 'Finally:\nAnalysis',         '~40 min'),
    ]

    for x, w, c, label, t in before_tasks:
        draw_task_block(ax, x, y_bar, w, bar_h, c, label, fontsize=8)
        draw_time_label(ax, x + w / 2, y_bar - 0.08, t, fontsize=7.5)

    # Divider line before "Analysis"
    ax.plot([17.78, 17.78], [y_bar - 0.2, y_bar + bar_h + 0.15],
            color=COLORS['primary'], linewidth=2, linestyle='--', alpha=0.5,
            zorder=5)

    # Time summaries
    ax.text(9.0, 0.55, '~3 hours of mechanical data work',
            ha='center', va='center', fontsize=12, fontweight='bold',
            color=COLORS['negative'])
    ax.text(9.0, 0.15, 'before any real analysis begins',
            ha='center', va='center', fontsize=10.5,
            color=COLORS['neutral'])

    # Arrow + label for analysis portion
    ax.annotate(
        '~20% of morning\nspent on actual analysis',
        xy=(18.8, y_bar + bar_h + 0.08), xytext=(15.5, y_bar + bar_h + 0.55),
        fontsize=8.5, color=COLORS['analysis'], fontweight='bold',
        ha='center',
        arrowprops=dict(arrowstyle='->', color=COLORS['analysis'], lw=1.5),
        zorder=6
    )

    # Legend
    legend_items = [
        (COLORS['download'], 'Download / Navigate'),
        (COLORS['entry'],    'Manual Data Entry'),
        (COLORS['format'],   'Format / Transform'),
        (COLORS['reading'],  'Reading / Review'),
        (COLORS['analysis'], 'Analysis'),
    ]
    for i, (c, lab) in enumerate(legend_items):
        ax.add_patch(FancyBboxPatch(
            (0.3 + i * 3.3, 3.25), 0.35, 0.18,
            boxstyle="round,pad=0.03", facecolor=c, edgecolor='none',
            alpha=0.85, zorder=3, clip_on=False
        ))
        ax.text(0.8 + i * 3.3, 3.34, lab, fontsize=7.5, va='center',
                color='#444444', clip_on=False)

    # ================================================================
    #  AFTER — AI-Powered Workflow
    # ================================================================
    ax = ax_after
    ax.set_facecolor(COLORS['bg_after'])
    ax.set_xlim(0, 20)
    ax.set_ylim(0, 3.5)
    ax.axis('off')

    # Section header
    ax.text(0.3, 3.05, 'AFTER', fontsize=18, fontweight='bold',
            color=COLORS['accent'], va='center')
    ax.text(2.65, 3.05, '—  AI-Powered Automated Pipeline',
            fontsize=13, color=COLORS['neutral'], va='center')

    y_bar = 1.6
    bar_h = 1.1

    # Overnight automated block
    draw_task_block(ax, 0.3, y_bar, 4.5, bar_h, COLORS['overnight'],
                    '', alpha=0.5, fontsize=9)
    ax.text(2.55, y_bar + bar_h / 2 + 0.18, 'Ran Overnight', ha='center',
            va='center', fontsize=11, color='#2d6b2d', fontweight='bold',
            zorder=4)
    ax.text(2.55, y_bar + bar_h / 2 - 0.18,
            '30+ collectors already finished', ha='center',
            va='center', fontsize=8.5, color='#3d7b3d', zorder=4)

    # Morning: review briefing
    draw_task_block(ax, 5.0, y_bar, 2.2, bar_h, COLORS['automated'],
                    'Review\nBriefing', fontsize=9, bold=True)
    draw_time_label(ax, 6.1, y_bar - 0.08, '~5 min', fontsize=7.5,
                    color='#2d6b2d')

    # KG context
    draw_task_block(ax, 7.4, y_bar, 2.0, bar_h, COLORS['ai_assist'],
                    'KG Context\nLoaded', fontsize=9, bold=True)
    draw_time_label(ax, 8.4, y_bar - 0.08, 'instant', fontsize=7.5,
                    color='#2d6b2d')

    # Big analysis block
    draw_task_block(ax, 9.6, y_bar, 10.1, bar_h, COLORS['analysis'],
                    '', fontsize=9, bold=True)
    ax.text(14.65, y_bar + bar_h / 2 + 0.18,
            'Analysis, Insight & Client Communication',
            ha='center', va='center', fontsize=13, color='white',
            fontweight='bold', zorder=4)
    ax.text(14.65, y_bar + bar_h / 2 - 0.22,
            'Compare forecasts  |  Generate reports  |  Write market commentary  |  Identify opportunities',
            ha='center', va='center', fontsize=8.5, color='#c8dcf0',
            zorder=4)

    # Divider line after KG
    ax.plot([9.4, 9.4], [y_bar - 0.2, y_bar + bar_h + 0.15],
            color=COLORS['primary'], linewidth=2, linestyle='--', alpha=0.5,
            zorder=5)

    # Time summaries
    ax.text(9.0, 0.55, '~80% of morning spent on analysis & insight',
            ha='center', va='center', fontsize=12, fontweight='bold',
            color=COLORS['accent'])
    ax.text(9.0, 0.15, 'data collection happened while you slept',
            ha='center', va='center', fontsize=10.5,
            color=COLORS['neutral'])

    # Arrow + label for analysis portion
    ax.annotate(
        'The part that\nactually matters',
        xy=(14.0, y_bar + bar_h + 0.08), xytext=(6.0, y_bar + bar_h + 0.55),
        fontsize=9, color=COLORS['primary'], fontweight='bold',
        ha='center',
        arrowprops=dict(arrowstyle='->', color=COLORS['primary'], lw=1.5),
        zorder=6
    )

    # Legend
    after_legend = [
        (COLORS['overnight'], 'Automated Overnight'),
        (COLORS['automated'], 'AI Briefing'),
        (COLORS['ai_assist'], 'Knowledge Graph'),
        (COLORS['analysis'],  'Analyst Work'),
    ]
    for i, (c, lab) in enumerate(after_legend):
        ax.add_patch(FancyBboxPatch(
            (0.3 + i * 3.8, 3.25), 0.35, 0.18,
            boxstyle="round,pad=0.03", facecolor=c, edgecolor='none',
            alpha=0.85, zorder=3, clip_on=False
        ))
        ax.text(0.8 + i * 3.8, 3.34, lab, fontsize=7.5, va='center',
                color='#444444', clip_on=False)

    # ── Footer ─────────────────────────────────────────────────────
    fig.text(
        0.98, 0.01,
        f'Generated: {datetime.now().strftime("%Y-%m-%d")}  |  RLC Analytics',
        ha='right', fontsize=8, color=COLORS['neutral'], style='italic'
    )

    plt.subplots_adjust(top=0.90, bottom=0.04, hspace=0.25,
                        left=0.02, right=0.98)

    out_path = 'data/generated_graphics/charts/before_after_workflow.png'
    fig.savefig(out_path, dpi=150, facecolor=fig.get_facecolor(),
                bbox_inches='tight')
    plt.close(fig)
    print(f'Saved to {out_path}')


if __name__ == '__main__':
    create_before_after()
