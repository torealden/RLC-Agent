"""
Crop individual supply-chain icons from the Cargill infographic
screenshot.

⚠ IP NOTE: These are Cargill's published icons. The source image is a
screenshot of Cargill's "Connecting the global supply chain" page.
Cropping these icons for personal reference is fine; using them in
public-facing RLC presentations carries IP/trademark risk because
they're recognizably Cargill's brand assets. Recommended path:
    1. Use these crops to define what style/subject you want.
    2. Either commission equivalent icons (~$10-30 each on Fiverr or
       Noun Project Pro), or source CC0 equivalents from Noun Project
       (https://thenounproject.com/) or Flaticon, or Streamline.
    3. Replace the cropped icons with originals before any external
       presentation.

Bounding boxes below are eyeballed from the source PNG (1030x673);
they may need a small nudge per-icon to look clean.
"""
from pathlib import Path
from PIL import Image

SRC = Path(__file__).parent / "source" / "cargill_supply_chain_infographic.png"
OUT_DIR = Path(__file__).parent
OUT_DIR.mkdir(parents=True, exist_ok=True)

# (left, top, right, bottom) — pixels into the 1030x673 source.
# Revisit after first crop pass; tighten any that include neighbor
# bleed or whitespace.
ICONS = {
    # Source-and-trade column
    "corn":          ( 35, 240,  77, 305),
    "wheat":         ( 75, 245, 110, 305),
    "cow":           (108, 260, 162, 308),
    "chicken":       (163, 263, 207, 308),
    "soybean":       ( 55, 375, 112, 437),
    "grain_silo":    (110, 372, 152, 437),  # the green elevator/silo

    # Make-and-transport column
    "truck":         (286, 240, 442, 345),
    "ship_with_crane": (444, 220, 658, 345),
    "steak":         (332, 400, 386, 458),
    "sugar_salt_pile": (392, 415, 452, 458),
    "chocolate_bars": (450, 415, 514, 458),
    "pig_feed":      (548, 405, 585, 462),
    "chicken_feed":  (586, 405, 622, 462),
    "fish_feed":     (623, 405, 658, 462),

    # Deliver-for-customers column
    "factory":       (732, 222, 822, 332),
    "farm_barn":     (820, 232, 920, 340),
    "home":          (724, 372, 790, 440),
    "store":         (788, 352, 870, 440),
    "shopping_cart": (872, 376, 942, 440),
}

# Icons explicitly excluded per user request (in screenshot but skip):
#   - shield with magnifying glass ("Provide global insights and risk management")
#   - paint roller + plant leaf ("Create nature-derived, bio-based")


def main():
    img = Image.open(SRC)
    print(f"Source: {SRC.name} ({img.size})")
    saved = 0
    for name, box in ICONS.items():
        crop = img.crop(box)
        out = OUT_DIR / f"{name}.png"
        crop.save(out)
        print(f"  {name:20} {box}  -> {out.name} ({crop.size})")
        saved += 1
    print(f"\nSaved {saved} icons to {OUT_DIR}")


if __name__ == "__main__":
    main()
