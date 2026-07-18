"""Build the RLC design-system icon library from the canonical path data.

Single source of truth for the icon set: the ICONS list below holds each mark's inner SVG
(24px grid, 1.75 stroke, round joins, currentColor so it inherits any brand token). This writes:

  design_system/icons/svg/<name>.svg   — standalone, recolorable SVGs (use inline in reports/web)
  design_system/icons/manifest.json    — name / group / label index
  design_system/icons/index.html       — gallery preview (design-system card, /design-sync)

Locked seed set of 15 (Tore, 2026-07-18). Livestock render as their CUT (steak/bacon/drumstick),
not the animal — more legible + on-brand. Alternates kept on the revisit shelf (see manifest):
canola-gold, cow (face), pig (head), chicken (head), tomahawk/T-bone steak, 3-strip bacon.

Run:  python scripts/build_design_system_icons.py
"""
import json
from pathlib import Path

ROOT = Path(r"C:/dev/RLC-Agent")
OUT = ROOT / "design_system" / "icons"

# (name, group, label, inner_svg). Stroke defaults come from the <svg> wrapper; the drumstick
# overrides stroke-width per-element (a heavier bone).
ICONS = [
    ("soybean", "Commodities", "Soybean",
     '<path d="M12 4.4c3.6 0 6.4 3.3 6.4 7.6S15.6 19.6 12 19.6 5.6 16.3 5.6 12 8.4 4.4 12 4.4Z"/>'
     '<path d="M8.6 10.5c-.9.9-.9 2.1 0 3"/>'),
    ("corn", "Commodities", "Corn",
     '<path d="M12 3.5c3 0 4.6 3.2 4.6 8.6S14.6 20.5 12 20.5 7.4 17.5 7.4 12.1 9 3.5 12 3.5Z"/>'
     '<path d="M12 4.6v14.8"/><path d="M9 8.5h6M9 12h6M9 15.5h6"/><path d="M9.4 5.4C7.9 5 6.4 6 6.5 8"/>'),
    ("wheat", "Commodities", "Wheat",
     '<path d="M12 21.5V6.5"/><path d="M12 10.8 8.6 8.2M12 10.8 15.4 8.2"/>'
     '<path d="M12 14.4 8.6 11.8M12 14.4 15.4 11.8"/><path d="M12 18 8.6 15.4M12 18 15.4 15.4"/>'
     '<path d="M12 6.8 10.2 5M12 6.8 13.8 5"/>'),
    ("canola", "Commodities", "Canola",
     '<ellipse cx="12" cy="7.4" rx="2.2" ry="3"/><ellipse cx="16.6" cy="12" rx="3" ry="2.2"/>'
     '<ellipse cx="12" cy="16.6" rx="2.2" ry="3"/><ellipse cx="7.4" cy="12" rx="3" ry="2.2"/>'
     '<circle cx="12" cy="12" r="1.4"/>'),
    ("oil-droplet", "Oils & Fats", "Oil droplet",
     '<path d="M12 3.2c0 0-6.3 6.7-6.3 11.3a6.3 6.3 0 0 0 12.6 0C18.3 9.9 12 3.2 12 3.2Z"/>'
     '<path d="M9.2 15.2a3 3 0 0 0 2.1 2.9"/>'),
    ("renewable", "Energy & Biofuel", "Renewable",
     '<path d="M4.5 19.5C3.5 12 8 4.8 19.5 4c1 10.5-4.2 15.9-13 15.5Z"/><path d="M5 19C7 15 9.8 12 13.5 10"/>'),
    ("barrel", "Energy & Biofuel", "Barrel",
     '<ellipse cx="12" cy="6" rx="5.8" ry="2"/>'
     '<path d="M6.2 6v11.8c0 1.1 2.6 1.9 5.8 1.9s5.8-.8 5.8-1.9V6"/>'
     '<path d="M6.2 10.6c0 1.1 2.6 1.9 5.8 1.9s5.8-.8 5.8-1.9M6.2 14.2c0 1.1 2.6 1.9 5.8 1.9s5.8-.8 5.8-1.9"/>'),
    ("refinery", "Facilities", "Refinery (flare)",
     '<path d="M10.8 20V8.2M13.2 20V8.2"/><path d="M9.8 20h4.4"/><path d="M10.5 12.5h3"/>'
     '<path d="M12 8.2c2-1.5 1.8-3.8 0-5 .4 1.4-1.1 1.9-1.1.2 0-1.3-.8-2-1.5-2.3.2 1.7-1.3 2.5-1.3 4.3 0 1.6 1.2 2.8 3.9 2.8Z"/>'),
    ("grain-silo", "Facilities", "Grain silo",
     '<path d="M4 21V11a2.75 2.75 0 0 1 5.5 0v10"/><path d="M11.6 21V8a3.2 3.2 0 0 1 6.4 0v13"/>'
     '<path d="M2.8 21h18.4"/><path d="M4 15.2h5.5M11.6 12.4h6.4"/>'),
    ("tanker-vessel", "Logistics", "Tanker vessel",
     '<path d="M3 14.6h18l-2 4.7a2 2 0 0 1-1.85 1.2H6.85A2 2 0 0 1 5 19.3Z"/>'
     '<path d="M7 14.6v-3.4h5v3.4"/><path d="M13.5 14.6V8.6H17v6"/>'),
    ("tanker-truck", "Logistics", "Tanker truck",
     '<circle cx="7.6" cy="17.4" r="2.2"/><circle cx="16.4" cy="17.4" r="2.2"/>'
     '<rect x="2.6" y="8.2" width="11.6" height="7.4" rx="3.7"/><path d="M14.6 15.6V9.6h2.6l2.2 3v3"/>'
     '<path d="M9.8 17.4h4.4M2.6 15.6v1.8M20 15.6v1.8h-1.4"/>'),
    ("rail-tanker", "Logistics", "Rail tanker car",
     '<rect x="3" y="8.6" width="18" height="5.6" rx="2.8"/><path d="M11 8.6V7.1h2v1.5"/>'
     '<path d="M5.5 14.2v1.3h13v-1.3"/><circle cx="6.6" cy="17" r="1.3"/><circle cx="9.1" cy="17" r="1.3"/>'
     '<circle cx="14.9" cy="17" r="1.3"/><circle cx="17.4" cy="17" r="1.3"/><path d="M2.5 19.4h19"/>'),
    ("steak", "Livestock", "Steak (beef)",
     '<path d="M4.5 11.6c-.5-2 .5-3.8 2.6-4.6 1.5-.6 2.7-.2 4.4-.7 1.8-.5 2.7-1.7 4.6-1.6 2 .1 3.9 1.1 4.4 2.9.3 1.3-.3 2.4-1.2 3.2.6.8.8 1.8.4 2.8-.7 1.7-2.8 2.2-4.9 2.2-1.5 0-2.6-.4-4.2-.2-1.9.2-3.9.6-5.5-.4-1.3-.8-1.9-2-1.6-3.5Z"/>'
     '<path d="M5 12.2c1.7 1.1 4 1.6 6.4 1.7 2.6.1 5-.4 6.9-1.5"/>'
     '<path d="M8.7 8.2 7.4 10M12 7.7 10.7 9.5M15.2 8 13.9 9.8"/>'),
    ("bacon", "Livestock", "Bacon (pork)",
     '<path d="M3 8.2c1.6-1.8 3.1-1.8 4.7 0s3.1 1.8 4.7 0 3.1-1.8 4.7 0 2.9-1.5 2.9-1.5v2.4s-1.3 1.4-2.9 1.4-3.1-1.8-4.7 0-3.1 1.8-4.7 0-3.1-1.8-4.7 0V8.2Z"/>'
     '<path d="M3 14c1.6-1.8 3.1-1.8 4.7 0s3.1 1.8 4.7 0 3.1-1.8 4.7 0 2.9-1.5 2.9-1.5v2.4s-1.3 1.4-2.9 1.4-3.1-1.8-4.7 0-3.1 1.8-4.7 0-3.1-1.8-4.7 0V14Z"/>'
     '<path d="M6.5 8.6v1.8M12 8.9v1.8M9 14.4v1.8M14.5 14.7v1.8"/>'),
    ("drumstick", "Livestock", "Drumstick (poultry)",
     '<path d="M17.6 6.4c1.9 1.9 1.4 5.4-1 7.9s-6 2.9-7.9 1-1.4-5.4 1-7.9 6-2.9 7.9-1Z"/>'
     '<path stroke-width="2.6" d="M8.7 15.3 6.2 17.8"/><circle stroke-width="2.6" cx="5.1" cy="18.9" r="1.5"/>'),
]

# ── Vegetable oils = the analytical Erlenmeyer flask + a mini of the seed inside; plus two new
#    standalones (sunflower with a seedy dot-center + pointed petals; cotton as a boll). Locked
#    with Tore 2026-07-18. ──
def _petal(cx, cy, R, L, W):
    t, b, m = round(cy - R - L, 2), round(cy - R, 2), round(cy - R - L * 0.55, 2)
    return (f"M{cx} {t}C{round(cx-W,2)} {m} {round(cx-W,2)} {b} {cx} {b}"
            f"C{round(cx+W,2)} {b} {round(cx+W,2)} {m} {cx} {t}Z")


def _flower(cx, cy, R, L, W, n, rc, dots):
    petals = "".join(f'<path d="{_petal(cx,cy,R,L,W)}" transform="rotate({round(360/n*i,1)} {cx} {cy})"/>'
                     for i in range(n))
    d = "".join(f'<path d="M{x} {y}h.01"/>' for x, y in dots)
    return petals + f'<circle cx="{cx}" cy="{cy}" r="{rc}"/>' + d


_SUN_SEED = _flower(12, 10, 3.4, 2.4, 1.05, 14, 3.4,
                    [(12, 10), (10.5, 9.1), (13.5, 9.1), (10.5, 10.9), (13.5, 10.9),
                     (12, 8.3), (12, 11.7), (11, 10), (13, 10)])
_SUN_MINI = _flower(12, 16, 2.4, 1.6, 0.75, 12, 2.4,
                    [(12, 16), (11, 15.2), (13, 15.2), (11, 16.8), (13, 16.8), (12, 15)])
_COTTON = ('<path d="M7.6 12.9a2.2 2.2 0 0 1 .2-3.9 2.5 2.5 0 0 1 4-1.8 2.5 2.5 0 0 1 4 1.8 2.2 2.2 0 0 1 .2 3.9 '
           '2.3 2.3 0 0 1-2.9 1.3 2.3 2.3 0 0 1-2.6 0 2.3 2.3 0 0 1-2.9-1.3Z"/>'
           '<path d="M12 6.9v6.4M9.6 9.6l1.6 1.8M14.4 9.6l-1.6 1.8"/>'
           '<path d="M9.7 13.4 8.3 16.8M12 13.8v3.6M14.3 13.4 15.7 16.8"/>')
_BEAKER = ('<path d="M10 3V8.6L5.1 19.3c-.5 1.1.5 2.4 1.7 2.4H17.2c1.2 0 2.2-1.3 1.7-2.4L14 8.6V3"/>'
           '<path d="M9.2 3h5.6"/><path d="M14.8 3 15.9 2.3"/><path d="M10.1 6.9c1.3-.7 2.6-.7 3.9 0"/>')
_MINI = {
    "soybean": '<path d="M12 12.3c2.3 0 4.1 2.1 4.1 4.6S14.3 21.5 12 21.5 7.9 19.4 7.9 16.9 9.7 12.3 12 12.3Z"/><path d="M9.9 15.4c-.6.6-.6 1.6 0 2.2"/>',
    "canola": '<ellipse cx="12" cy="13.4" rx="1.5" ry="2.1"/><ellipse cx="15" cy="16.2" rx="2.1" ry="1.5"/><ellipse cx="12" cy="19" rx="1.5" ry="2.1"/><ellipse cx="9" cy="16.2" rx="2.1" ry="1.5"/><circle cx="12" cy="16.2" r=".95"/>',
    "sunflower": _SUN_MINI,
    "corn": '<ellipse cx="12" cy="16" rx="2.2" ry="3.3"/><path d="M12 13.2v5.6"/><path d="M10.9 14.8h.01M13.1 14.8h.01M10.9 16.6h.01M13.1 16.6h.01M12 15.7h.01M12 17.6h.01"/>',
    "cottonseed": '<path d="M9.6 16.6a1.5 1.5 0 0 1 .1-2.7 1.7 1.7 0 0 1 2.8-1.2 1.7 1.7 0 0 1 2.8 1.2 1.5 1.5 0 0 1 .1 2.7 1.6 1.6 0 0 1-2 .8 1.6 1.6 0 0 1-1.8 0 1.6 1.6 0 0 1-2-.8Z"/><path d="M12 13.4v3.6"/><path d="M10.5 17.2 9.7 19.4M12 17.4v2.1M13.5 17.2 14.3 19.4"/>',
}
ICONS += [
    ("sunflower", "Commodities", "Sunflower", _SUN_SEED),
    ("cotton", "Commodities", "Cotton", _COTTON),
    ("soybean-oil", "Oils & Fats", "Soybean oil", _BEAKER + _MINI["soybean"]),
    ("canola-oil", "Oils & Fats", "Canola oil", _BEAKER + _MINI["canola"]),
    ("sunflower-oil", "Oils & Fats", "Sunflower oil", _BEAKER + _MINI["sunflower"]),
    ("corn-oil", "Oils & Fats", "Corn oil", _BEAKER + _MINI["corn"]),
    ("cottonseed-oil", "Oils & Fats", "Cottonseed oil", _BEAKER + _MINI["cottonseed"]),
]

WRAP = ('<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" width="24" height="24" '
        'fill="none" stroke="currentColor" stroke-width="1.75" stroke-linecap="round" '
        'stroke-linejoin="round">{inner}</svg>\n')


def main():
    svgdir = OUT / "svg"
    svgdir.mkdir(parents=True, exist_ok=True)
    manifest = []
    for name, group, label, inner in ICONS:
        (svgdir / f"{name}.svg").write_text(WRAP.format(inner=inner), encoding="utf-8")
        manifest.append({"name": name, "group": group, "label": label})
    (OUT / "manifest.json").write_text(json.dumps({
        "set": "RLC iconography v1",
        "locked": "2026-07-18",
        "style": {"grid": 24, "stroke": 1.75, "joins": "round", "color": "currentColor"},
        "icons": manifest,
        "alternates_on_shelf": ["canola-gold", "cattle-face", "pig-head", "chicken-head",
                                 "steak-tomahawk", "steak-tbone", "bacon-3strip"],
    }, indent=2), encoding="utf-8")

    # gallery page (design-system card for /design-sync)
    cells = "\n".join(
        f'    <figure><span class="ic">{WRAP.format(inner=inner).strip()}</span>'
        f'<figcaption>{label}<em>{group}</em></figcaption></figure>'
        for name, group, label, inner in ICONS)
    html = f'''<!-- @dsCard group="Iconography" -->
<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>RLC Iconography</title>
<style>
  :root{{--lake:#1B2A4A;--field:#3C7D22;--wheat:#C8A951;--paper:#F7F3EB;--hair:#E4DDCE;
    --ink:#1B2A4A;--muted:#6E7178;--surface:#fff;
    --serif:Georgia,serif;--sans:Calibri,"Segoe UI",system-ui,sans-serif;
    --mono:"Cascadia Mono",Consolas,ui-monospace,monospace;}}
  @media (prefers-color-scheme:dark){{:root{{--paper:#131C31;--surface:#1B2A4A;--ink:#F1ECE0;--muted:#9AA0AA;--hair:#2C3B5B;}}}}
  *{{box-sizing:border-box;}}
  body{{margin:0;background:var(--paper);color:var(--ink);font-family:var(--sans);}}
  .wrap{{max-width:960px;margin:0 auto;padding:clamp(1.25rem,4vw,3rem);}}
  .eyebrow{{font-size:.72rem;font-weight:700;letter-spacing:.22em;text-transform:uppercase;color:var(--field);margin:0 0 .6rem;}}
  h1{{font-family:var(--serif);font-size:clamp(2rem,5vw,2.8rem);margin:0 0 .4rem;}}
  .horizon{{border:0;margin:1.2rem 0 0;}}
  .horizon::before{{content:"";display:block;height:1px;background:var(--wheat);}}
  .horizon::after{{content:"";display:block;height:4px;margin-top:2px;background:var(--field);}}
  .meta{{font-family:var(--mono);font-size:.76rem;color:var(--muted);margin:1.2rem 0 2rem;text-transform:uppercase;letter-spacing:.07em;}}
  .grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(150px,1fr));gap:1px;background:var(--hair);border:1px solid var(--hair);}}
  figure{{background:var(--surface);margin:0;padding:1.4rem 1rem;display:flex;flex-direction:column;align-items:center;gap:.9rem;}}
  .ic svg{{width:38px;height:38px;color:var(--ink);}}
  figcaption{{font-family:var(--mono);font-size:.72rem;color:var(--ink);text-align:center;line-height:1.5;}}
  figcaption em{{display:block;font-style:normal;color:var(--muted);font-size:.64rem;text-transform:uppercase;letter-spacing:.06em;}}
</style></head><body><div class="wrap">
  <p class="eyebrow">Round Lakes Commodities &middot; Design System</p>
  <h1>Iconography</h1>
  <hr class="horizon">
  <p class="meta">24px grid &middot; 1.75 stroke &middot; round joins &middot; currentColor &middot; {len(ICONS)} marks</p>
  <div class="grid">
{cells}
  </div>
</div></body></html>
'''
    (OUT / "index.html").write_text(html, encoding="utf-8")
    print(f"Wrote {len(ICONS)} SVGs to {svgdir}")
    print(f"Wrote {OUT / 'manifest.json'} and {OUT / 'index.html'}")


if __name__ == "__main__":
    main()
