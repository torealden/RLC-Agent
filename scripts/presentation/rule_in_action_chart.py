"""
Generate presentation-quality timeline infographic:
"One Rule in Action" — KG alert -> analyst action -> edge captured.

Output: data/generated_graphics/charts/rule_in_action_timeline.png
"""

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch
import numpy as np
from pathlib import Path

# ── Palette ──────────────────────────────────────────────────────────────
BG       = "#1a1917"
GOLD     = "#C8963E"
WHITE    = "#FFFFFF"
GRAY     = "#6B6560"
GREEN    = "#3D6B4F"
RED      = "#8B3A3A"
CARD_BG  = "#2a2825"  # slightly lighter than background for cards

# ── Timeline data ────────────────────────────────────────────────────────
events = [
    {
        "time":  "TUESDAY PM",
        "label": "System flags RINs / LCFS\ncredit-stack divergence\nfrom feedstock margin",
        "color": RED,
        "icon":  "\u26a0",          # warning sign  (U+26A0 in DejaVu)
        "tag":   "ALERT",
    },
    {
        "time":  "WEDNESDAY 6 AM",
        "label": "Analyst reviews alert,\nidentifies soybean oil\nmispricing",
        "color": GOLD,
        "icon":  "\u2605",          # black star (U+2605 in DejaVu)
        "tag":   "ANALYSIS",
    },
    {
        "time":  "WEDNESDAY OPEN",
        "label": "On the phone with\na buyer before the\nmarket opens",
        "color": GOLD,
        "icon":  "\u260e",          # black telephone (U+260E in DejaVu)
        "tag":   "ACTION",
    },
    {
        "time":  "WEDNESDAY CLOSE",
        "label": "Market corrects\n--- edge captured",
        "color": GREEN,
        "icon":  "\u2713",          # checkmark
        "tag":   "RESULT",
    },
]

# ── Figure setup ─────────────────────────────────────────────────────────
fig, ax = plt.subplots(figsize=(14, 5), dpi=150)
fig.patch.set_facecolor(BG)
ax.set_facecolor(BG)
ax.set_xlim(-0.5, 4.5)
ax.set_ylim(-1.8, 2.6)
ax.set_aspect("auto")
ax.axis("off")

# ── Title ────────────────────────────────────────────────────────────────
ax.text(
    2.0, 2.35, "One Rule in Action",
    fontsize=26, fontweight="bold", color=WHITE,
    ha="center", va="top",
    fontfamily="sans-serif",
)

# Subtitle
ax.text(
    2.0, 1.95,
    "Knowledge Graph flags anomaly  \u2192  Analyst acts before the market  \u2192  Edge captured",
    fontsize=11, color=GRAY, ha="center", va="top",
    fontfamily="sans-serif", style="italic",
)

# ── Horizontal spine (timeline line) ────────────────────────────────────
spine_y = 0.55
x_positions = [0.5, 1.5, 2.5, 3.5]

# Draw the connecting line
ax.plot(
    [x_positions[0] - 0.25, x_positions[-1] + 0.25],
    [spine_y, spine_y],
    color=GRAY, linewidth=1.5, solid_capstyle="round", alpha=0.5, zorder=1,
)

# ── Draw arrow segments between nodes (gradient feel) ────────────────────
for i in range(len(x_positions) - 1):
    ax.annotate(
        "",
        xy=(x_positions[i + 1] - 0.22, spine_y),
        xytext=(x_positions[i] + 0.22, spine_y),
        arrowprops=dict(
            arrowstyle="->,head_width=0.18,head_length=0.1",
            color=GOLD, lw=1.8, alpha=0.7,
        ),
        zorder=2,
    )

# ── Draw each event ─────────────────────────────────────────────────────
card_width  = 0.82
card_height = 0.95

for i, ev in enumerate(events):
    x = x_positions[i]

    # ── Node circle on the timeline ──
    circle_radius = 0.12
    circle = plt.Circle(
        (x, spine_y), circle_radius,
        facecolor=ev["color"], edgecolor=WHITE, linewidth=1.5, zorder=5,
    )
    ax.add_patch(circle)

    # Icon inside circle
    ax.text(
        x, spine_y, ev["icon"],
        fontsize=11, ha="center", va="center", color=WHITE, zorder=6,
    )

    # ── Card below the timeline ──
    card_x = x - card_width / 2
    card_y = spine_y - 0.35 - card_height

    card = FancyBboxPatch(
        (card_x, card_y), card_width, card_height,
        boxstyle="round,pad=0.06",
        facecolor=CARD_BG, edgecolor=ev["color"],
        linewidth=1.8, zorder=3,
    )
    ax.add_patch(card)

    # Thin accent bar at top of card
    accent_bar = FancyBboxPatch(
        (card_x + 0.04, card_y + card_height - 0.06), card_width - 0.08, 0.035,
        boxstyle="round,pad=0.01",
        facecolor=ev["color"], edgecolor="none", alpha=0.9, zorder=4,
    )
    ax.add_patch(accent_bar)

    # Connector line from circle to card
    ax.plot(
        [x, x],
        [spine_y - circle_radius, card_y + card_height],
        color=ev["color"], linewidth=1.2, alpha=0.6, zorder=2,
    )

    # ── Tag label (small pill above time) ──
    ax.text(
        x, card_y + card_height - 0.14, ev["tag"],
        fontsize=7, fontweight="bold", color=ev["color"],
        ha="center", va="top",
        fontfamily="sans-serif", zorder=5,
    )

    # ── Time label ──
    ax.text(
        x, card_y + card_height - 0.26, ev["time"],
        fontsize=9, fontweight="bold", color=WHITE,
        ha="center", va="top",
        fontfamily="sans-serif", zorder=5,
    )

    # ── Description text ──
    ax.text(
        x, card_y + card_height - 0.42, ev["label"],
        fontsize=8, color=GRAY, ha="center", va="top",
        fontfamily="sans-serif", linespacing=1.35, zorder=5,
    )

# ── Elapsed-time annotations above the spine ────────────────────────────
elapsed = [
    (0, 1, "~16 hrs"),
    (1, 2, "~2.5 hrs"),
    (2, 3, "~6.5 hrs"),
]
for s, e, lbl in elapsed:
    mid_x = (x_positions[s] + x_positions[e]) / 2
    y_top = spine_y + 0.32
    ax.annotate(
        "",
        xy=(x_positions[e] - 0.15, y_top),
        xytext=(x_positions[s] + 0.15, y_top),
        arrowprops=dict(
            arrowstyle="<->", color=GOLD, lw=0.9, alpha=0.5,
        ),
        zorder=2,
    )
    ax.text(
        mid_x, y_top + 0.07, lbl,
        fontsize=7, color=GOLD, ha="center", va="bottom",
        fontfamily="sans-serif", alpha=0.8,
    )

# ── Total elapsed callout ───────────────────────────────────────────────
ax.text(
    2.0, spine_y + 0.62,
    "< 25 hours from detection to capture",
    fontsize=10, fontweight="bold", color=GOLD, ha="center", va="bottom",
    fontfamily="sans-serif",
    bbox=dict(
        boxstyle="round,pad=0.35",
        facecolor=BG, edgecolor=GOLD, linewidth=1.2, alpha=0.9,
    ),
    zorder=6,
)

# ── Footer ───────────────────────────────────────────────────────────────
ax.text(
    4.45, -1.65,
    "Generated: 2026-03-26  |  RLC Analytics",
    fontsize=7, color=GRAY, ha="right", va="bottom",
    fontfamily="sans-serif", style="italic",
)

# ── Save ─────────────────────────────────────────────────────────────────
out_path = Path(__file__).resolve().parents[2] / "data" / "generated_graphics" / "charts" / "rule_in_action_timeline.png"
out_path.parent.mkdir(parents=True, exist_ok=True)

fig.savefig(
    str(out_path),
    facecolor=fig.get_facecolor(),
    edgecolor="none",
    bbox_inches="tight",
    pad_inches=0.3,
)
plt.close(fig)
print(f"Saved: {out_path}")
