"""Generate the live RLC model coverage matrix (HTML) from the canonical coverage universe.

Single source of truth = scripts/build_pepsi_coverage_tracker.py (COMPLEXES / tiers / sheet sets).
This script does NOT redefine scope — it imports it, probes the live models/ tree for what is
actually built, and renders the Lake, Field & Grain coverage dashboard with real counts.

Status is derived, not hand-typed:
  empty   — the country folder holds no workbook matching the complex
  staged  — flat file present, no balance-sheet workbook yet (data ready)
  partial — balance-sheet workbook present but not on either curated list
  annual  — on ANNUAL_CLOSED (ledger fact: annual balance sheet closed + Excel-recalc-verified,
            monthly block not yet built)
  done    — on VERIFIED_CLOSED (ledger fact: monthly block closed + tied out)

The two curated lists (ANNUAL_CLOSED, VERIFIED_CLOSED) cannot be inferred from file presence, so
they are the only hand-curated inputs here. As Tore builds, a new workbook flips a cell
empty/staged->partial automatically; adding the (complex, country) pair to ANNUAL_CLOSED after the
recalc/tie-out passes flips it partial->annual, and later to VERIFIED_CLOSED flips it annual->done.

Run:  python scripts/build_coverage_matrix_html.py
Output: docs/specs/rlc_model_coverage_matrix.html
"""
from pathlib import Path
from datetime import date
import importlib.util

ROOT = Path(r"C:/dev/RLC-Agent")
OILSEEDS = ROOT / "models" / "Oilseeds"
OUT = ROOT / "docs" / "specs" / "rlc_model_coverage_matrix.html"

# ---- import the canonical coverage universe (no re-definition) -------------------------------
_spec = importlib.util.spec_from_file_location(
    "pepsi_tracker", ROOT / "scripts" / "build_pepsi_coverage_tracker.py")
T = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(T)

# ---- the one curated input: ledger-verified closed sheets (see docs/SESSION_LEDGER.md) -------
VERIFIED_CLOSED = {
    ("Soybean", "United States"),      # US soy oil supply closed forward, tied out (ledger 6d/6e)
    ("Corn Oil", "United States"),     # US DCO via the feedstock/IFV layer
}

# annual-closed + Excel-recalc-verified (TIE=0, non-triviality>0, 0 errors) but monthly block NOT
# yet built. A real milestone beyond "partial" (a bare workbook), short of "done" (monthly-closed,
# green). Ledger fact — see docs/SESSION_LEDGER.md 2026-07-27. Promote to VERIFIED_CLOSED once the
# monthly block closes.
ANNUAL_CLOSED = {
    ("Soybean", "Brazil"),
    ("Soybean", "Argentina"),
    ("Rapeseed / Canola", "Europe"),
    ("Rapeseed / Canola", "Canada"),
    ("Rapeseed / Canola", "Australia"),
    ("Rapeseed / Canola", "Russia"),
    ("Sunflower", "Ukraine"),
    ("Sunflower", "Russia"),
    ("Sunflower", "Argentina"),
    ("Palm", "Malaysia"),
    ("Palm", "Indonesia"),
}

# complex -> substrings that identify its workbooks in a country folder
COMPLEX_FILE_PATTERNS = {
    "Soybean":            ["soy"],
    "Rapeseed / Canola":  ["canola", "rapeseed", "rape"],
    "Sunflower":          ["sunflower", "sunflow", "sunseed"],
    "Palm":               ["palm"],
    "Corn Oil":           ["corn_oil", "cornoil", "dco"],
}

# display + editorial copy (scope stays data-driven; these are just labels)
DISPLAY = {"Europe": "European Union"}
SHEET_DESC = {
    "Palm":              "largest build · CPO + PKO, two oils",
    "Corn Oil":          "derived · DCO / wet-mill",
}
OILSEED_DESC = "Seed · Crush · Oil · Meal · Trade"
CX_ORDER = ["Soybean", "Rapeseed / Canola", "Sunflower", "Palm", "Corn Oil"]
CX_DISPLAY = {"Soybean": "Soybean oil", "Rapeseed / Canola": "Rapeseed / Canola",
              "Sunflower": "Sunflower oil", "Palm": "Palm", "Corn Oil": "Corn oil"}
A_SMALL = {  # optional annotation on specific exporter chips
    ("Palm", "Indonesia"): "+ B50 draw",
}
IMPORTER_ROLE = {
    "China":  "Largest veg-oil buyer; crush-driven SBO",
    "India":  "The marginal buyer — sets palm/sun substitution",
    "Europe": "Importer <em>and</em> Tier-A rapeseed exporter; biodiesel draw",
    "Turkey": "Sunseed crusher / re-exporter",
}
IMPORTER_SHORT = {"Palm": "Palm", "Sunflower": "Sun", "Rapeseed / Canola": "Rape", "Soybean": "Soy"}


def disp(country):
    return DISPLAY.get(country, country)


def cell_status(cx, country):
    """empty | staged | partial | done — derived from the live models/Oilseeds/<country>/ folder.
    staged = flat file (*_supply_demand) present, no balance sheet yet (data ready for Desktop);
    partial = a balance-sheet workbook exists; done = ledger-verified closed."""
    if (cx, country) in VERIFIED_CLOSED:
        return "done"
    if (cx, country) in ANNUAL_CLOSED:
        return "annual"
    folder = OILSEEDS / country
    if not folder.exists():
        return "empty"
    pats = COMPLEX_FILE_PATTERNS.get(cx, [])
    bal = flat = False
    for f in folder.glob("*.xls*"):
        if f.name.startswith("~$"):
            continue
        low = f.name.lower()
        if not any(p in low for p in pats):
            continue
        if low.endswith("_flat.xlsx"):
            flat = True                        # generated PSD-annual flat file (data staged)
        elif low.endswith("_supply_demand.xlsx"):
            flat = True                        # curated multi-source flat file (e.g. US reference)
        else:
            bal = True                         # balance-sheet model workbook
    if bal:
        return "partial"
    if flat:
        return "staged"
    return "empty"


CHIP_CLS = {"done": "chip done", "annual": "chip annual", "partial": "chip part",
            "staged": "chip staged", "empty": "chip", "auto": "chip auto"}
CHIP_SMALL = {"done": "closed", "annual": "annual ✓", "partial": "started", "staged": "data ready"}


def chip(name, status, small=None):
    lbl = small if small is not None else CHIP_SMALL.get(status)
    s = f' <small>{lbl}</small>' if lbl else ""
    return f'<span class="{CHIP_CLS[status]}"><span class="dot"></span>{name}{s}</span>'


def build_sections():
    # ---- exporter matrix (Tier A) + counts ----
    a_done = a_annual = a_part = a_staged = a_empty = 0
    rows = []
    for cx in CX_ORDER:
        cfg = T.COMPLEXES[cx]
        n_sheets = len(cfg["sheets"])
        desc = SHEET_DESC.get(cx, OILSEED_DESC)
        cells = []
        for country in cfg.get("A", []):
            st = cell_status(cx, country)
            a_done += st == "done"; a_annual += st == "annual"; a_part += st == "partial"
            a_staged += st == "staged"; a_empty += st == "empty"
            small = A_SMALL.get((cx, country))
            if st == "done":
                small = "DCO closed" if cx == "Corn Oil" else "closed"
            cells.append(chip(disp(country), st, small))
        rows.append(f"""
      <div class="mrow">
        <div class="lab"><span class="cx">{CX_DISPLAY[cx]}</span>
          <span class="meta"><span class="sheetpill">{n_sheets} sheets</span> {desc}</span></div>
        <div class="cells">
          {"".join(cells)}
        </div>
      </div>""")
    matrix = "\n".join(rows)

    # ---- importer cards (Tier B, union across complexes) ----
    importers = []
    for cx in CX_ORDER:
        for c in T.COMPLEXES[cx].get("B", []):
            if c not in importers:
                importers.append(c)
    # order by number of complexes served, desc
    served = {c: [cx for cx in CX_ORDER if c in T.COMPLEXES[cx].get("B", [])] for c in importers}
    importers.sort(key=lambda c: -len(served[c]))
    imp_cards = []
    for c in importers:
        tags = []
        for cx in ["Palm", "Sunflower", "Rapeseed / Canola", "Soybean"]:
            on = "on" if cx in served[c] else ""
            tags.append(f'<span class="tag {on}">{IMPORTER_SHORT[cx]}</span>')
        st = cell_status_importer(c)
        dot = "dot-empty" if st == "empty" else "dot-auto"
        imp_cards.append(f"""
      <div class="card">
        <div class="ct"><b>{disp(c)}</b><span class="mini"><span class="dot {dot}"></span></span></div>
        <div class="role">{IMPORTER_ROLE.get(c, "Swing importer")}</div>
        <div class="serves">{"".join(tags)}</div>
      </div>""")
    importer_cards = "\n".join(imp_cards)

    # ---- world rollups (Tier C) + scenario stubs (Tier D) ----
    world_built = (OILSEEDS / "World").exists() and any(
        f for f in (OILSEEDS / "World").glob("*.xls*") if not f.name.startswith("~$"))
    roll_status = "auto" if not world_built else "done"
    rollups = "".join(chip(CX_DISPLAY[cx].replace(" oil", ""), roll_status, small=None if world_built else "")
                      for cx in CX_ORDER)

    stub_pairs = [(cx, c) for cx in CX_ORDER for c in T.COMPLEXES[cx].get("D", [])]
    stub_by_cx = {}
    for cx, c in stub_pairs:
        stub_by_cx.setdefault(cx, []).append(c)
    stubs = "".join(chip(f'{CX_DISPLAY[cx].replace(" oil","")} ×{len(v)}', "empty", small="")
                    for cx, v in stub_by_cx.items())
    n_stubs = len(stub_pairs)
    n_rollups = len(CX_ORDER)
    n_importers = len(importers)

    counts = dict(a_total=a_done + a_annual + a_part + a_staged + a_empty, a_done=a_done,
                  a_annual=a_annual, a_part=a_part,
                  a_staged=a_staged, a_empty=a_empty,
                  n_importers=n_importers, n_rollups=n_rollups, n_stubs=n_stubs,
                  roll_built=world_built)
    return matrix, importer_cards, rollups, stubs, counts


def cell_status_importer(country):
    """Importer folders: partial if any oil workbook present, else empty."""
    folder = OILSEEDS / country
    if not folder.exists():
        return "empty"
    for f in folder.glob("*.xls*"):
        if not f.name.startswith("~$"):
            return "partial"
    return "empty"


# ---- static CSS (Lake, Field & Grain) -------------------------------------------------------
CSS = r"""
  :root {
    --paper:#F7F3EB; --paper-2:#EFE9DA; --card:#FFFFFF; --ink:#1B2A4A; --ink-soft:#3B486180;
    --lake:#1B2A4A; --field:#3C7D22; --wheat:#C8A951; --sage:#B7CCA4; --clay:#96492A; --slate:#8A8F98;
    --line:#DED6C4; --line-strong:#C9BEA6;
    --done:#3C7D22; --done-fill:#E8F0DE; --annual:#2E7D74; --annual-fill:#DCECE9; --partial:#B98F1F; --partial-fill:#F7EFD4;
    --empty:#9AA0A8; --empty-fill:#EFECE3; --auto:#28406B; --auto-fill:#E4E9F2;
    --text:#26314A; --muted:#6C7385; --heading:#152139;
    --shadow:0 1px 2px rgba(20,31,51,.06),0 8px 24px rgba(20,31,51,.05);
    --fs-serif:Georgia,"Iowan Old Style","Times New Roman",serif;
    --fs-sans:Calibri,"Segoe UI",system-ui,-apple-system,"Helvetica Neue",sans-serif;
  }
  @media (prefers-color-scheme:dark){
    :root{
      --paper:#141F33; --paper-2:#101A2B; --card:#1B2942; --ink:#EAF0F8; --ink-soft:#c7d2e480;
      --line:#2C3B57; --line-strong:#3A4A68; --text:#D4DEEE; --muted:#94A2BC; --heading:#EAF0F8;
      --done:#8FD16A; --done-fill:#20361A; --annual:#5FC9BC; --annual-fill:#163230; --partial:#E6C25E; --partial-fill:#3A3115;
      --empty:#7C879C; --empty-fill:#222E45; --auto:#9DB6E6; --auto-fill:#1C2C48;
      --shadow:0 1px 2px rgba(0,0,0,.3),0 10px 30px rgba(0,0,0,.28);
    }
  }
  :root[data-theme="light"]{
      --paper:#F7F3EB; --paper-2:#EFE9DA; --card:#FFFFFF; --ink:#1B2A4A;
      --line:#DED6C4; --line-strong:#C9BEA6; --text:#26314A; --muted:#6C7385; --heading:#152139;
      --done:#3C7D22; --done-fill:#E8F0DE; --annual:#2E7D74; --annual-fill:#DCECE9; --partial:#B98F1F; --partial-fill:#F7EFD4;
      --empty:#9AA0A8; --empty-fill:#EFECE3; --auto:#28406B; --auto-fill:#E4E9F2;
      --shadow:0 1px 2px rgba(20,31,51,.06),0 8px 24px rgba(20,31,51,.05);
  }
  :root[data-theme="dark"]{
      --paper:#141F33; --paper-2:#101A2B; --card:#1B2942; --ink:#EAF0F8;
      --line:#2C3B57; --line-strong:#3A4A68; --text:#D4DEEE; --muted:#94A2BC; --heading:#EAF0F8;
      --done:#8FD16A; --done-fill:#20361A; --annual:#5FC9BC; --annual-fill:#163230; --partial:#E6C25E; --partial-fill:#3A3115;
      --empty:#7C879C; --empty-fill:#222E45; --auto:#9DB6E6; --auto-fill:#1C2C48;
      --shadow:0 1px 2px rgba(0,0,0,.3),0 10px 30px rgba(0,0,0,.28);
  }
  *{box-sizing:border-box}
  body{margin:0}
  .wrap{
    font-family:var(--fs-sans); color:var(--text); background:var(--paper);
    background-image:radial-gradient(120% 80% at 50% -10%, color-mix(in srgb,var(--sage) 14%, transparent), transparent 60%);
    min-height:100vh; padding:clamp(20px,4vw,56px) clamp(16px,4vw,48px) 72px;
    -webkit-font-smoothing:antialiased; text-rendering:optimizeLegibility;
  }
  .page{max-width:1120px; margin:0 auto}
  .mast{display:flex; flex-wrap:wrap; align-items:flex-end; justify-content:space-between; gap:18px}
  .brand{display:flex; align-items:center; gap:14px}
  .seal{width:44px;height:44px;border-radius:50%;flex:none;
    background:radial-gradient(circle at 50% 34%,var(--wheat) 0 22%,transparent 23%),
               conic-gradient(from 0deg,var(--field),#2f6a1b,var(--field));
    box-shadow:inset 0 0 0 2px var(--paper),0 0 0 2px var(--lake); position:relative}
  .brand-txt small{display:block;font-family:var(--fs-sans);letter-spacing:.22em;text-transform:uppercase;
    font-size:11px;color:var(--muted);font-weight:600}
  .brand-txt b{font-family:var(--fs-serif);font-size:20px;color:var(--heading);font-weight:700;letter-spacing:.01em}
  h1{font-family:var(--fs-serif); font-weight:700; color:var(--heading);
    font-size:clamp(26px,4.4vw,42px); line-height:1.06; margin:22px 0 8px; text-wrap:balance; letter-spacing:-.01em}
  .dek{font-size:clamp(15px,1.6vw,17px); color:var(--muted); max-width:60ch; line-height:1.5; margin:0}
  .asof{font-size:12px;color:var(--muted);letter-spacing:.04em;text-align:right}
  .asof b{color:var(--text);font-variant-numeric:tabular-nums}
  .horizon{height:10px;margin:26px 0 30px;border-radius:2px;position:relative;overflow:hidden;
    background:linear-gradient(180deg,var(--field) 0 62%, #2c5c18 100%)}
  .horizon::before{content:"";position:absolute;left:0;right:0;top:0;height:2px;
    background:linear-gradient(90deg,transparent,var(--wheat) 22%,#f0dd9b 50%,var(--wheat) 78%,transparent);
    box-shadow:0 0 10px 1px color-mix(in srgb,var(--wheat) 70%,transparent)}
  .tiles{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-bottom:12px}
  @media(max-width:720px){.tiles{grid-template-columns:repeat(2,1fr)}}
  .tile{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:16px 16px 14px;
    box-shadow:var(--shadow);position:relative;overflow:hidden}
  .tile .cap{font-size:11px;letter-spacing:.13em;text-transform:uppercase;color:var(--muted);font-weight:600}
  .tile .num{font-family:var(--fs-serif);font-size:34px;line-height:1;margin:8px 0 3px;color:var(--heading);
    font-variant-numeric:tabular-nums}
  .tile .num small{font-size:16px;color:var(--muted)}
  .tile .sub{font-size:12.5px;color:var(--muted)}
  .tile::after{content:"";position:absolute;left:0;top:0;bottom:0;width:4px}
  .tile.t-done::after{background:var(--done)} .tile.t-part::after{background:var(--partial)}
  .tile.t-empty::after{background:var(--empty)} .tile.t-price::after{background:var(--clay)}
  .tile.t-staged::after{background:var(--auto)} .tile.t-staged .num{color:var(--auto)}
  .tile.t-price .num{color:var(--clay)}
  .sec{margin-top:38px}
  .sec-head{display:flex;align-items:baseline;gap:12px;margin-bottom:14px;flex-wrap:wrap}
  .sec-head h2{font-family:var(--fs-serif);font-size:21px;color:var(--heading);margin:0;font-weight:700}
  .sec-head .tier-tag{font-size:11px;font-weight:700;letter-spacing:.08em;text-transform:uppercase;
    color:var(--lake);background:color-mix(in srgb,var(--sage) 40%,var(--paper));border:1px solid var(--line-strong);
    padding:3px 9px;border-radius:20px}
  :root[data-theme="dark"] .sec-head .tier-tag{color:var(--ink)}
  @media(prefers-color-scheme:dark){.sec-head .tier-tag{color:var(--ink)}}
  .sec-head p{margin:0;font-size:13.5px;color:var(--muted)}
  .matrix{background:var(--card);border:1px solid var(--line);border-radius:12px;box-shadow:var(--shadow);overflow:hidden}
  .mrow{display:grid;grid-template-columns:minmax(190px,240px) 1fr;gap:0;border-top:1px solid var(--line)}
  .mrow:first-child{border-top:0}
  .mrow .lab{padding:16px 18px;border-right:1px solid var(--line);display:flex;flex-direction:column;gap:6px;
    background:color-mix(in srgb,var(--paper-2) 55%,var(--card))}
  .mrow .lab .cx{font-family:var(--fs-serif);font-size:17px;color:var(--heading);font-weight:700;line-height:1.1}
  .mrow .lab .meta{font-size:11.5px;color:var(--muted);display:flex;align-items:center;gap:7px;flex-wrap:wrap}
  .sheetpill{font-size:10.5px;font-weight:700;letter-spacing:.03em;color:var(--lake);
    background:var(--auto-fill);border:1px solid color-mix(in srgb,var(--auto) 25%,transparent);border-radius:4px;padding:1px 6px}
  :root[data-theme="dark"] .sheetpill{color:var(--auto)}
  .cells{padding:14px 16px;display:flex;flex-wrap:wrap;gap:9px;align-content:center}
  .chip{display:inline-flex;align-items:center;gap:8px;padding:7px 12px 7px 10px;border-radius:7px;
    font-size:13.5px;font-weight:600;border:1px solid var(--line-strong);background:var(--empty-fill);color:var(--text);
    transition:transform .12s ease,box-shadow .12s ease;cursor:default}
  .chip:hover{transform:translateY(-1px);box-shadow:0 4px 12px rgba(20,31,51,.1)}
  .chip .dot{width:9px;height:9px;border-radius:50%;flex:none;background:var(--empty);box-shadow:0 0 0 3px color-mix(in srgb,var(--empty) 22%,transparent)}
  .chip small{font-weight:600;font-size:11px;color:var(--muted);letter-spacing:.02em}
  .chip.done{background:var(--done-fill);border-color:color-mix(in srgb,var(--done) 45%,transparent);color:color-mix(in srgb,var(--done) 55%,var(--text))}
  .chip.done .dot{background:var(--done);box-shadow:0 0 0 3px color-mix(in srgb,var(--done) 25%,transparent)}
  .chip.annual{background:var(--annual-fill);border-color:color-mix(in srgb,var(--annual) 50%,transparent);color:color-mix(in srgb,var(--annual) 58%,var(--text))}
  .chip.annual .dot{background:var(--annual);box-shadow:0 0 0 3px color-mix(in srgb,var(--annual) 25%,transparent)}
  .chip.part{background:var(--partial-fill);border-color:color-mix(in srgb,var(--partial) 45%,transparent);color:color-mix(in srgb,var(--partial) 62%,var(--text))}
  .chip.part .dot{background:var(--partial);box-shadow:0 0 0 3px color-mix(in srgb,var(--partial) 25%,transparent)}
  .chip.auto{background:var(--auto-fill);border-color:color-mix(in srgb,var(--auto) 38%,transparent);color:var(--auto)}
  .chip.auto .dot{background:var(--auto);box-shadow:0 0 0 3px color-mix(in srgb,var(--auto) 22%,transparent)}
  .chip.staged{background:color-mix(in srgb,var(--sage) 20%,var(--card));border:1px dashed color-mix(in srgb,var(--auto) 55%,transparent);color:var(--auto)}
  .chip.staged .dot{background:transparent;border:2px solid var(--auto);box-shadow:none}
  .cards{display:grid;grid-template-columns:repeat(4,1fr);gap:14px}
  @media(max-width:860px){.cards{grid-template-columns:repeat(2,1fr)}}
  @media(max-width:460px){.cards{grid-template-columns:1fr}}
  .card{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:15px 16px;box-shadow:var(--shadow);
    display:flex;flex-direction:column;gap:9px}
  .card .ct{display:flex;align-items:center;justify-content:space-between;gap:8px}
  .card .ct b{font-family:var(--fs-serif);font-size:16px;color:var(--heading)}
  .card .role{font-size:11px;color:var(--muted);letter-spacing:.02em;line-height:1.35}
  .serves{display:flex;flex-wrap:wrap;gap:5px;margin-top:2px}
  .tag{font-size:10.5px;font-weight:700;letter-spacing:.03em;text-transform:uppercase;
    padding:2px 7px;border-radius:20px;background:var(--paper-2);color:var(--muted);border:1px solid var(--line)}
  .tag.on{background:color-mix(in srgb,var(--sage) 42%,var(--paper));color:var(--lake);border-color:var(--line-strong)}
  :root[data-theme="dark"] .tag.on{color:var(--ink);background:color-mix(in srgb,var(--field) 26%,transparent)}
  .mini{display:flex;align-items:center;gap:8px;font-size:12.5px;color:var(--muted)}
  .mini .dot{width:8px;height:8px;border-radius:50%}
  .dot-auto{background:var(--auto)} .dot-empty{background:var(--empty)}
  .rollups,.stubs{display:flex;flex-wrap:wrap;gap:9px}
  .track{display:grid;grid-template-columns:repeat(9,1fr);gap:8px;margin-top:6px}
  @media(max-width:900px){.track{grid-template-columns:repeat(3,1fr)}}
  @media(max-width:520px){.track{grid-template-columns:repeat(2,1fr)}}
  .ph{background:var(--card);border:1px solid var(--line);border-radius:9px;padding:11px 12px 12px;position:relative;
    box-shadow:var(--shadow);display:flex;flex-direction:column;gap:5px;min-height:104px}
  .ph .pk{font-size:11px;font-weight:800;letter-spacing:.06em;color:var(--lake);font-variant-numeric:tabular-nums}
  :root[data-theme="dark"] .ph .pk{color:var(--auto)}
  .ph .pt{font-size:12.5px;color:var(--text);line-height:1.28;font-weight:600}
  .ph .pn{font-size:11px;color:var(--muted);margin-top:auto}
  .ph.now{border-color:var(--field);box-shadow:0 0 0 1px var(--field),var(--shadow)}
  .ph.now::before{content:"NEXT";position:absolute;top:-9px;left:11px;font-size:9px;font-weight:800;letter-spacing:.1em;
    background:var(--field);color:#fff;padding:2px 6px;border-radius:4px}
  .ph.price{border-color:var(--clay)}
  .ph.price .pk{color:var(--clay)}
  .ph.price::before{content:"LAST";position:absolute;top:-9px;left:11px;font-size:9px;font-weight:800;letter-spacing:.1em;
    background:var(--clay);color:#fff;padding:2px 6px;border-radius:4px}
  .foot{display:flex;flex-wrap:wrap;gap:26px;justify-content:space-between;align-items:flex-start;margin-top:34px;
    padding-top:20px;border-top:1px solid var(--line)}
  .legend{display:flex;flex-wrap:wrap;gap:16px}
  .lg{display:flex;align-items:center;gap:8px;font-size:12.5px;color:var(--muted)}
  .lg .dot{width:10px;height:10px;border-radius:50%}
  .note{max-width:44ch;font-size:12.5px;color:var(--muted);line-height:1.5}
  .note b{color:var(--clay)}
  .src{margin-top:22px;font-size:11.5px;color:var(--muted);letter-spacing:.02em}
  @media (prefers-reduced-motion:reduce){*{transition:none!important}}
"""


def render():
    matrix, importer_cards, rollups, stubs, c = build_sections()
    pending = c["n_importers"] + c["n_stubs"]
    roll_note = "5 · pending" if not c["roll_built"] else "5 · live"
    today = date.today().strftime("%d %b %Y")
    return f"""<title>RLC Model Coverage Matrix — Helios / Pepsi</title>
<style>{CSS}</style>

<div class="wrap">
<div class="page">

  <header class="mast">
    <div class="brand">
      <div class="seal" aria-hidden="true"></div>
      <div class="brand-txt"><small>Round Lakes Commodities</small><b>Lake, Field &amp; Grain</b></div>
    </div>
    <div class="asof">Coverage as of <b>{today}</b><br>Live from the <code>models/</code> tree · prices deferred</div>
  </header>

  <h1>Model Coverage Matrix</h1>
  <p class="dek">The balance-sheet build behind the Helios&nbsp;/&nbsp;Pepsi vegetable-oils report. Fundamentals for
  every country&times;complex are built first; the guidance-price layer runs <em>last</em>, as a single pass across
  everything closed here.</p>

  <div class="horizon" role="presentation"></div>

  <div class="tiles">
    <div class="tile t-done">
      <div class="cap">Built &amp; closed</div>
      <div class="num">{c['a_done']}<small> / {c['a_total']}</small></div>
      <div class="sub">price-setting exporter builds (Tier&nbsp;A)</div>
    </div>
    <div class="tile t-staged">
      <div class="cap">Data staged</div>
      <div class="num">{c['a_staged']}</div>
      <div class="sub">PSD flat files ready &mdash; awaiting Desktop wiring</div>
    </div>
    <div class="tile t-part">
      <div class="cap">Not started</div>
      <div class="num">{c['a_empty']}<small> +{pending}</small></div>
      <div class="sub">{c['a_part']} building · +{c['n_importers']} importers / {c['n_stubs']} stubs</div>
    </div>
    <div class="tile t-price">
      <div class="cap">Price pass</div>
      <div class="num">0<small> / 5</small></div>
      <div class="sub">reference series — deferred to the final phase</div>
    </div>
  </div>

  <section class="sec">
    <div class="sec-head">
      <h2>Price-setting exporters</h2>
      <span class="tier-tag">Tier A · full sheet set</span>
      <p>These origins set the quoted series. No shortcuts — the complete crush-complex build each.</p>
    </div>
    <div class="matrix">
{matrix}
    </div>
  </section>

  <section class="sec">
    <div class="sec-head">
      <h2>Swing importers</h2>
      <span class="tier-tag">Tier B · shared workbook</span>
      <p>One workbook per country, a tab per oil, plus a shared allocation tab — this is where cross-oil substitution lives.</p>
    </div>
    <div class="cards">
{importer_cards}
    </div>
  </section>

  <section class="sec">
    <div class="sec-head">
      <h2>World rollups &amp; scenario stubs</h2>
      <span class="tier-tag">Tier C · automated &nbsp;/&nbsp; Tier D · stub</span>
      <p>Rollups refresh straight from <code>bronze.fas_psd</code> — no manual build. Stubs carry only a shock coefficient.</p>
    </div>
    <div class="cards" style="grid-template-columns:1.4fr 1fr">
      <div class="card">
        <div class="ct"><b>World rollups</b><span class="mini"><span class="dot dot-auto"></span>{roll_note}</span></div>
        <div class="role">Production · trade · ending stocks · stocks-to-use — the first-order price answer, refreshed automatically.</div>
        <div class="rollups">{rollups}</div>
      </div>
      <div class="card">
        <div class="ct"><b>Scenario stubs</b><span class="mini"><span class="dot dot-empty"></span>{c['n_stubs']} origins</span></div>
        <div class="role">Colombia · Guatemala · Mexico · Russia · Turkey · Brazil — single-page, what-if only.</div>
        <div class="stubs">{stubs}</div>
      </div>
    </div>
  </section>

  <section class="sec">
    <div class="sec-head">
      <h2>Build sequence</h2>
      <p>Each phase runs the country-build SOP once per cell. Prices are the final phase, across everything closed.</p>
    </div>
    <div class="track">
      <div class="ph"><span class="pk">P0</span><span class="pt">Finish US</span><span class="pn">template country</span></div>
      <div class="ph now"><span class="pk">P0.5</span><span class="pt">World rollups</span><span class="pn">5 · automated</span></div>
      <div class="ph"><span class="pk">P1</span><span class="pt">Helios oils</span><span class="pn">sun → palm → rape → soy</span></div>
      <div class="ph"><span class="pk">P2</span><span class="pt">Oil franchise</span><span class="pn">minor oils</span></div>
      <div class="ph"><span class="pk">P3</span><span class="pt">Grains</span><span class="pn">SOW No.2</span></div>
      <div class="ph"><span class="pk">P4</span><span class="pt">Fats &amp; biofuels</span><span class="pn">BBD moat</span></div>
      <div class="ph"><span class="pk">P5</span><span class="pt">Fuels</span><span class="pn">energy / macro</span></div>
      <div class="ph"><span class="pk">P6</span><span class="pt">Other majors</span><span class="pn">client-pulled</span></div>
      <div class="ph price"><span class="pk">P·FIN</span><span class="pt">Price pass</span><span class="pn">all combos</span></div>
    </div>
  </section>

  <div class="foot">
    <div class="legend">
      <span class="lg"><span class="dot" style="background:var(--done)"></span>Built &amp; closed <small>(monthly)</small></span>
      <span class="lg"><span class="dot" style="background:var(--annual)"></span>Annual closed &amp; tied out</span>
      <span class="lg"><span class="dot" style="border:2px solid var(--auto);background:transparent"></span>Data staged</span>
      <span class="lg"><span class="dot" style="background:var(--partial)"></span>Building</span>
      <span class="lg"><span class="dot" style="background:var(--empty)"></span>Not started</span>
      <span class="lg"><span class="dot" style="background:var(--auto)"></span>Automated from DB</span>
    </div>
    <p class="note"><b>Prices last.</b> Everything here is fundamental supply &amp; demand. The guidance-price
    layer — S/U&nbsp;&rarr;&nbsp;price, basis to the quoted series, forward-window shift — is one shared engine,
    designed and back-tested once against the full set of closed sheets.</p>
  </div>

  <p class="src">Generated by <code>scripts/build_coverage_matrix_html.py</code> from the live <code>models/</code> tree
  and the canonical universe in <code>build_pepsi_coverage_tracker.py</code>. Status is derived, not hand-typed.
  Regular Helios report scope — bespoke Pepsi levers tracked separately.</p>

</div>
</div>
"""


def main():
    html = render()
    OUT.write_text(html, encoding="utf-8")
    _, _, _, _, c = build_sections()
    print(f"Wrote {OUT}")
    print(f"  Tier A: {c['a_done']} done / {c['a_annual']} annual / {c['a_staged']} staged / "
          f"{c['a_part']} building / {c['a_empty']} empty (of {c['a_total']})")
    print(f"  importers={c['n_importers']}  rollups={c['n_rollups']}  stubs={c['n_stubs']}  "
          f"world_rollups_built={c['roll_built']}")


if __name__ == "__main__":
    main()
