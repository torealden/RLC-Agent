# Desktop Build Brief — Veg-Oil Balance-Sheet Templates (36-hour sprint)

> **For Claude-Desktop (Claude-Content).** Claude-Code owns data + flat files; you own the balance-
> sheet workbooks and the SUMIFS wiring. Read `flat_file_contract_v1.md` FIRST — it is the frozen
> interface; every formula you write binds to it. Coordination substrate = Notion (source of truth);
> physical files = `C:\dev\RLC-Agent\models\` (commit to git as you go).

**Goal:** a closed *annual* fundamental S&D workbook for every Tier-A veg-oil cell, wired to the flat
files Claude-Code publishes. **Fundamentals only. Do NOT build any price layer** — prices are the
final pass, separately (Tore's ruling). Annual grain (PSD) now; monthly rows upgrade the same sheets
later with no rework (the vintage ladder does it).

---

## The reusable unit is a TEMPLATE, not a workbook. Build 3, clone 14.

Do **not** hand-build 14 workbooks. Build **three complex templates**, verify each once, then clone
per country by swapping the flat-file the SUMIFS point at (and the `commodity`/country filters).

| Template | Tabs (annual S&D each) | Clones to |
|---|---|---|
| **Oilseed-5** | Seed · Crush · Oil · Meal · Trade | Soy (BR, AR) · Rape (EU, CA, AU, RU) · Sun (UA, RU, AR) |
| **Palm-8** | Plantation · Kernel(seed) · Crush · CPO Oil · PKO Oil · PKC Meal · Trade · Stocks | Palm (MY, ID) |
| **Corn-oil-2** | Oil supply · Trade | Corn oil (BR) |

US soy oil + US DCO are already done — do not rebuild; use `us_soybean_oil_supply_demand.xlsx` as the
**reference implementation** for how the wiring looks.

---

## Step-by-step per template

1. **Lay out the annual S&D rows** for each tab using the identity:
   `Beginning Stocks + Production + Imports = Total Supply`;
   `Domestic Use + Exports = Total Distribution`; `Ending = Supply − Distribution`.
   Marketing years across columns; the line items are exactly the `series` in contract §4.
2. **Wire each data row** to the flat file's long tab with the MAXIFS→SUMIFS idiom in contract §6.
   Bind by column letter, whole-column ranges. No Excel Tables, no defined names.
3. **Derived rows** (Total Supply, Total Distribution, Ending) are Excel arithmetic of the wired rows
   — never wired themselves.
4. **Two tie-out cells per tab**, visible, must read 0:
   `=TotalSupply-(Beg+Prod+Imp)` and `=Ending-(Supply-Distribution)`. If non-zero, the wiring is wrong
   — fix before cloning.
5. **Crush linkage** (oilseed + palm): Oil production and Meal production tie to Crush ×
   oil-yield / meal-yield. Enter the yield as one cell per complex; carry it.
6. **Verify the template once** against US soy oil's numbers where they overlap, then clone.

## Cloning a template to a country

- Copy the template workbook to `models/Oilseeds/<Country>/<country>_<complex>_balance_sheet.xlsx`.
- Repoint every SUMIFS `tab!` reference to that country's flat file
  (`<country>_<complex>_supply_demand.xlsx`, published by Claude-Code) and set the `commodity` filter.
- Re-check the two tie-out cells read 0. Done.

---

## Definition of done (per country workbook)

- [ ] Every `series` in contract §4 for the complex is wired (missing-data rows resolve to 0, not #REF!).
- [ ] Both tie-out cells read 0 across all marketing years present.
- [ ] No `#REF!` / `#VALUE!` / `#DIV/0!` anywhere.
- [ ] Binds to the flat file by column letter only; no hard-coded values in wired rows.
- [ ] Saved to the canonical country folder; committed to git; the workbook was **not open** in Excel
      when Claude-Code last wrote its flat file.

## Do NOT

- Do **not** build any guidance-price / basis / forward-window layer. Fundamentals only.
- Do **not** hand-edit a flat file — Claude-Code regenerates them; your edits vanish.
- Do **not** invent new `series` names to fit missing data — flag the gap; the contract is frozen.
- Do **not** start a country whose flat file Claude-Code has not yet published (check the tracker).

---

## Coordination loop (both Claudes)

1. Claude-Code freezes the contract (done: `flat_file_contract_v1.md`) and publishes flat files
   country-by-country, announcing each in Notion as it lands.
2. Desktop builds the 3 templates immediately (they need only the contract, not the data), then clones
   against each flat file as it appears.
3. As each country workbook ties out, note it — Claude-Code adds `(complex, country)` to
   `VERIFIED_CLOSED` and re-runs `build_coverage_matrix_html.py`; the cell flips to green.
4. Any schema tension → stop, agree in Notion, Claude-Code republishes, then resume. Never wire around
   a broken contract.

**Honest note carried from Claude-Code:** this sprint produces the S&D backbone, not model prices. The
Tuesday report, if it happens, is fundamentals + a manual price view. That is the ceiling, and it is
still a real deliverable.
