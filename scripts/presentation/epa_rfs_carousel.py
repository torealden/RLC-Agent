"""
EPA RFS Announcement — Social Media Carousel Graphics
Creates individual slides for LinkedIn/X carousel post.

Each slide: 1080x1080px, bold number + icon + context line.
RLC brand colors, clean and shareable.
"""

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch
from pathlib import Path
from datetime import datetime

OUT_DIR = Path("data/generated_graphics/social")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# RLC Brand
NAVY = '#1f4e79'
GOLD = '#C8963E'
WHITE = '#FFFFFF'
DARK_BG = '#0d1117'
LIGHT_TEXT = '#D4CFC5'
GREEN = '#548235'
ACCENT_BLUE = '#2e75b6'


def create_slide(filename, big_number, subtitle, context_line, icon_text,
                 accent_color=GOLD, bg_color=DARK_BG):
    """Create one carousel slide."""
    fig, ax = plt.subplots(figsize=(10.8, 10.8), dpi=100)  # 1080x1080
    fig.patch.set_facecolor(bg_color)
    ax.set_xlim(0, 10)
    ax.set_ylim(0, 10)
    ax.axis('off')

    # Top accent bar
    bar = FancyBboxPatch((0, 9.7), 10, 0.3, boxstyle="square",
                         facecolor=accent_color, edgecolor='none', zorder=2)
    ax.add_patch(bar)

    # Icon/emoji area (large circle with icon text)
    circle = plt.Circle((5, 6.8), 1.2, facecolor=accent_color, alpha=0.15,
                         edgecolor=accent_color, linewidth=3, zorder=2)
    ax.add_patch(circle)
    ax.text(5, 6.8, icon_text, ha='center', va='center', fontsize=60,
            color=accent_color, fontweight='bold', zorder=3)

    # Big number
    ax.text(5, 4.8, big_number, ha='center', va='center', fontsize=72,
            color=WHITE, fontweight='bold', zorder=3,
            fontfamily='sans-serif')

    # Subtitle
    ax.text(5, 3.6, subtitle, ha='center', va='center', fontsize=24,
            color=accent_color, fontweight='bold', zorder=3)

    # Context line
    ax.text(5, 2.6, context_line, ha='center', va='center', fontsize=16,
            color=LIGHT_TEXT, style='italic', zorder=3, wrap=True)

    # Bottom branding
    ax.text(5, 0.8, "EPA Renewable Fuel Standard", ha='center', va='center',
            fontsize=14, color=LIGHT_TEXT, alpha=0.6, zorder=3)
    ax.text(5, 0.4, "Round Lakes Companies  |  roundlakescommodities.com",
            ha='center', va='center', fontsize=11, color=LIGHT_TEXT, alpha=0.4, zorder=3)

    # Thin line separator above branding
    ax.plot([2, 8], [1.3, 1.3], color=accent_color, alpha=0.3, linewidth=1, zorder=2)

    path = OUT_DIR / filename
    fig.savefig(path, dpi=100, facecolor=fig.get_facecolor(), bbox_inches='tight',
                pad_inches=0.1)
    plt.close(fig)
    print(f"  Saved {path}")


def create_title_slide():
    """Create the opening hook slide."""
    fig, ax = plt.subplots(figsize=(10.8, 10.8), dpi=100)
    fig.patch.set_facecolor(DARK_BG)
    ax.set_xlim(0, 10)
    ax.set_ylim(0, 10)
    ax.axis('off')

    # Gold accent bar
    bar = FancyBboxPatch((0, 9.7), 10, 0.3, boxstyle="square",
                         facecolor=GOLD, edgecolor='none', zorder=2)
    ax.add_patch(bar)

    # Title text
    ax.text(5, 7.0, "EPA's RFS", ha='center', va='center', fontsize=56,
            color=WHITE, fontweight='bold', zorder=3)
    ax.text(5, 5.8, "by the Numbers", ha='center', va='center', fontsize=48,
            color=GOLD, fontweight='bold', style='italic', zorder=3)

    # Subtitle
    ax.text(5, 4.2, "The economic impact of the\nRenewable Fuel Standard",
            ha='center', va='center', fontsize=22, color=LIGHT_TEXT, zorder=3,
            linespacing=1.5)

    # Swipe indicator
    ax.text(5, 2.0, "Swipe to see the numbers  >>>",
            ha='center', va='center', fontsize=18, color=GOLD, alpha=0.7, zorder=3)

    # Bottom branding
    ax.text(5, 0.8, "Round Lakes Companies", ha='center', va='center',
            fontsize=14, color=LIGHT_TEXT, alpha=0.6, zorder=3)
    ax.text(5, 0.4, "roundlakescommodities.com",
            ha='center', va='center', fontsize=11, color=LIGHT_TEXT, alpha=0.4, zorder=3)

    path = OUT_DIR / "01_rfs_title.png"
    fig.savefig(path, dpi=100, facecolor=fig.get_facecolor(), bbox_inches='tight',
                pad_inches=0.1)
    plt.close(fig)
    print(f"  Saved {path}")


def create_closing_slide():
    """Create the CTA closing slide."""
    fig, ax = plt.subplots(figsize=(10.8, 10.8), dpi=100)
    fig.patch.set_facecolor(DARK_BG)
    ax.set_xlim(0, 10)
    ax.set_ylim(0, 10)
    ax.axis('off')

    bar = FancyBboxPatch((0, 9.7), 10, 0.3, boxstyle="square",
                         facecolor=GOLD, edgecolor='none', zorder=2)
    ax.add_patch(bar)

    ax.text(5, 7.0, "The RFS isn't just", ha='center', va='center', fontsize=36,
            color=LIGHT_TEXT, zorder=3)
    ax.text(5, 6.0, "energy policy.", ha='center', va='center', fontsize=36,
            color=LIGHT_TEXT, zorder=3)
    ax.text(5, 4.5, "It's farm policy.", ha='center', va='center', fontsize=44,
            color=GOLD, fontweight='bold', zorder=3)

    ax.text(5, 3.0, "Follow for more commodity\nmarket intelligence",
            ha='center', va='center', fontsize=20, color=LIGHT_TEXT, alpha=0.7,
            zorder=3, linespacing=1.5)

    # Branding
    ax.plot([2, 8], [1.8, 1.8], color=GOLD, alpha=0.3, linewidth=1, zorder=2)
    ax.text(5, 1.2, "Tore Alden  |  Round Lakes Companies",
            ha='center', va='center', fontsize=16, color=LIGHT_TEXT, alpha=0.6, zorder=3)
    ax.text(5, 0.6, "Fats, Fuels & Oils  |  YouTube Webinar Series",
            ha='center', va='center', fontsize=13, color=GOLD, alpha=0.5, zorder=3)

    path = OUT_DIR / "06_rfs_closing.png"
    fig.savefig(path, dpi=100, facecolor=fig.get_facecolor(), bbox_inches='tight',
                pad_inches=0.1)
    plt.close(fig)
    print(f"  Saved {path}")


if __name__ == '__main__':
    print("Generating EPA RFS carousel slides...")

    create_title_slide()

    create_slide(
        "02_rfs_farm_income.png",
        "$3\u20134B",
        "Farm Income Lift",
        "Direct annual boost to US agricultural producers",
        "$",
        accent_color=GREEN,
    )

    create_slide(
        "03_rfs_corn_soy_value.png",
        "$31B",
        "Corn & Soy Oil Value",
        "Annual value supported by renewable fuel demand",
        "SBO",
        accent_color=GOLD,
    )

    create_slide(
        "04_rfs_rural_economies.png",
        "$10B",
        "Rural Economic Impact",
        "Investment flowing into rural communities nationwide",
        "USA",
        accent_color=ACCENT_BLUE,
    )

    create_slide(
        "05_rfs_jobs.png",
        "100K+",
        "American Jobs",
        "Supported across biofuel production & agriculture",
        "JOBS",
        accent_color=GREEN,
    )

    create_closing_slide()

    print(f"\nAll 6 slides saved to {OUT_DIR}/")
    print("\nCarousel posting instructions:")
    print("  LinkedIn: Create post > Add document > Upload as PDF (combine slides)")
    print("  X/Twitter: Create post > Add images > Select all 6 PNGs")
    print("  Instagram: New post > Select multiple > Choose all 6")
