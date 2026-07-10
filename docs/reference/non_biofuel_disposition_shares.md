# Non-Biofuel Disposition Shares — Reference Table

**Purpose:** the fixed weights for splitting each feedstock's **non-biofuel** demand into
end-use industries, for building the demand side of the fats/oils balance sheets. Shared
reference so Claude Code (DB/flat files) and Claude Desktop (balance-sheet workbooks) use
the **same** weights.

**Provenance:** USDA/Census "Fats & Oils: consumption by end-use" blocks, mirrored in the
`Census Crush` tab of `models/Oilseeds/us_oilseed_crush.xlsm`. NASS discontinued that
end-use survey after **2011**, so shares are computed over the **2006–2011** window and
**held forward** ("the portioning survives over time"), per the Q4 ruling
([[project-feedstock-forecast-method]], `project_feedstock_forecast_method.md`).

**Convention:** "Methyl esters" = biodiesel and is **excluded** — it's the biofuel slice,
handled by the allocator. Every share below is a share of the **non-biofuel remainder**
(total disposition − methyl esters).

**How to apply (Q4 ruling):**
1. Non-biofuel available = commodity disposition − biofuel use (allocator/EIA).
2. Where USDA reports **"removed for processing"** for the feedstock, apply these shares to
   that volume. Otherwise assume the split survives and scale ∝ available supply.
3. Only three commodities have a **measured** split; the rest are **analogs/assumptions** —
   flag them as assumptions in the balance sheet, do **not** present as measured.

---

## A. Data-backed splits (measured, 2006–2011 Census end-use)

### Soybean oil  — ~95% edible / ~5% industrial
| Non-biofuel industry | Share |
|---|---:|
| Salad or cooking oil | 64.2% |
| Baking or frying fats | 30.7% |
| Margarine | 2.7% |
| Other inedible products | 1.5% |
| Resins & plastics | 0.4% |
| Other edible products | 0.3% |
| Paint & varnish | 0.2% |
| *(Fatty acids, Soap ≈ 0)* | — |

### Inedible tallow & grease  — feed-dominated
Covers the rendered inedible fats pool collectively: **IBFT, yellow grease, and white
grease**. Apply the same split to each of those feedstocks' non-biofuel demand.
| Non-biofuel industry | Share |
|---|---:|
| Feed (animal) | 73.4% |
| Fatty acids | 17.4% |
| Other inedible products | 9.3% |
| *(Lubricants, Soap ≈ 0 in modern era)* | — |

### Cottonseed oil  — essentially all edible
| Non-biofuel industry | Share |
|---|---:|
| Salad or cooking oil | ~81% |
| Baking or frying fats | ~17% |
| Other edible products | ~1% |
| *(Inedible / methyl esters negligible)* | — |

---

## B. Analog / assumption splits (NO measured Census end-use — flag as assumption)

| Commodity | Non-biofuel treatment | Basis |
|---|---|---|
| **Canola oil** | Use the **Soybean oil edible** shares (cooking oil / baking-frying / margarine) | Edible-dominated veg oil; no canola-specific end-use series |
| **DCO (distillers corn oil)** | ~all biofuel; small residual = **animal feed** (no edible split) | DCO is a biofuel/feed grade, not a food oil |
| **Poultry fat** | Use the **tallow & grease** split (feed-dominated) | Same rendered-fats pool |
| **Yellow grease** (standalone) | Use the **tallow & grease** split | Same pool (already covered in the block) |
| **White grease / CWG** | Use the **tallow & grease** split | Same pool |
| **UCO** | ~all biofuel; small residual = **oleochemical / feed** | Post-consumer; minimal non-fuel use |
| **Palm oil** | Edible + oleochemical; treat as assumption until a source is found | No US end-use series |

---

## Source columns (Census Crush tab, `us_oilseed_crush.xlsm`)

| Block | Columns |
|---|---|
| Soybean oil consumption | HF–HT (methyl esters = HM–HO, excluded) |
| Inedible tallow & grease consumption | HV–IL (methyl esters = IH–IK, excluded) |
| Cottonseed oil consumption | IP–IX (methyl esters = IW, excluded) |

**Series end 2011-07.** Refresh source: none current (survey discontinued). If a modern
end-use source appears (e.g. a Census oleochemical series), revisit these weights and record
the change here.
