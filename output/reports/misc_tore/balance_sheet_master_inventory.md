# Balance Sheet Master Inventory
## Country/Commodity Combinations Needed

Generated 2026-03-28 from D: drive scan of historical spreadsheet files.
Organized into 5 market groups per user framework.

---

## GROUP 1: OILSEEDS

### 1A. Soybean Complex (seed + oil + meal)
| Country | Tier | Bal Sheet | Trade | Source File |
|---------|------|-----------|-------|-------------|
| World | 1 | Y | - | sbwldbal |
| United States | 1 | Y | Y | sbusbal + ussoytrade |
| Brazil | 1 | Y | Y | sbwldbal + wldsoytrade |
| Argentina | 1 | Y | Y | sbwldbal + wldsoytrade |
| China | 1 | Y | Y | sbwldbal + wldsoytrade + chinaoilseedbal |
| EU | 1 | Y | Y | sbwldbal + wldsoytrade |
| India | 1 | Y | Y | sbwldbal + wldsoytrade |
| Paraguay | 2 | Y | Y | sbwldbal + wldsoytrade |
| Uruguay | 2 | Y | Y | sbwldbal + wldsoytrade |
| Canada | 2 | Y | Y | sbwldbal + wldsoytrade |
| Japan | 2 | Y | Y | sbwldbal + wldsoytrade |
| Mexico | 2 | Y | Y | sbwldbal + wldsoytrade |
| Ukraine | 2 | Y | Y | sbwldbal + wldsoytrade |
| Russia | 2 | Y | Y | sbwldbal + wldsoytrade |

### 1B. Rapeseed/Canola Complex (seed + oil + meal)
| Country | Tier | Bal Sheet | Trade | Source File |
|---------|------|-----------|-------|-------------|
| World | 1 | Y | - | rawldbal |
| Canada | 1 | Y | Y | rawldbal + wldraptrade |
| EU | 1 | Y | Y | rawldbal + wldraptrade |
| China | 1 | Y | Y | rawldbal + wldraptrade |
| Australia | 2 | Y | Y | rawldbal + wldraptrade |
| India | 2 | Y | Y | rawldbal + wldraptrade |
| Ukraine | 2 | Y | Y | rawldbal + wldraptrade |
| Russia | 2 | Y | Y | rawldbal + wldraptrade |
| Japan | 2 | Y | Y | rawldbal + wldraptrade |
| United States | 2 | Y | Y | rawldbal + usoilseedtrade |
| Mexico | 3 | Y | Y | rawldbal + wldraptrade |

### 1C. Sunflower Complex (seed + oil + meal)
| Country | Tier | Bal Sheet | Trade | Source File |
|---------|------|-----------|-------|-------------|
| World | 1 | Y | - | suwldbal |
| Ukraine | 1 | Y | Y | suwldbal |
| Russia | 1 | Y | Y | suwldbal |
| Argentina | 1 | Y | Y | suwldbal |
| EU | 1 | Y | Y | suwldbal |
| Turkey | 2 | Y | Y | suwldbal |
| China | 2 | Y | Y | suwldbal |
| India | 2 | Y | Y | suwldbal |
| United States | 2 | Y | Y | sbusbal + usoilseedtrade |
| Canada | 3 | Y | Y | suwldbal |
| Mexico | 3 | Y | Y | suwldbal |

### 1D. Palm / Lauric Complex (palm oil + PKO + coconut oil + copra)
| Country | Tier | Bal Sheet | Trade | Source File |
|---------|------|-----------|-------|-------------|
| World | 1 | Y | - | WLDlauricbal |
| Indonesia | 1 | Y | Y | WLDlauricbal + wldlautrade |
| Malaysia | 1 | Y | Y | WLDlauricbal + wldlautrade |
| Philippines | 2 | Y | Y | WLDlauricbal + wldlautrade |
| United States | 2 | imports | Y | sbusbal (Palm Oil, PKO, Coconut Oil) |

### 1E. Cottonseed Complex (seed + oil + meal)
| Country | Tier | Bal Sheet | Trade | Source File |
|---------|------|-----------|-------|-------------|
| United States | 1 | Y | Y | sbusbal + usoilseedtrade |
| China | 2 | Y | Y | chinaoilseedbal |

### 1F. Peanut Complex (seed + oil + meal)
| Country | Tier | Bal Sheet | Trade | Source File |
|---------|------|-----------|-------|-------------|
| Argentina | 1 | Y | Y | pnwldbal + wldpeatrade |
| China | 1 | Y | Y | pnwldbal + wldpeatrade |
| India | 1 | Y | Y | pnwldbal |
| United States | 2 | Y | Y | sbusbal + usoilseedtrade |

### 1G. Minor Oilseeds (US only)
| Commodity | Bal Sheet | Trade | Source File |
|-----------|-----------|-------|-------------|
| Flaxseed | Y | Y | sbusbal + usoilseedtrade |
| Safflower | Y | - | sbusbal |
| Corn Oil | Y | - | sbusbal |

### 1H. US Aggregate Oilseed Balances
| Balance Sheet | Source File |
|---------------|-------------|
| Total Vegetable Oil | sbusbal |
| Total Protein Meal | sbusbal |
| PCAU (per capita apparent use) | sbusbal |
| Edible Fats & Oil Balance | sbusbal |
| Vegoil Domestic Use | sbusbal |
| Lauric Domestic Use | sbusbal |
| Fat Domestic Use | sbusbal |

---

## GROUP 2: FATS & GREASES (Biofuel Feedstock Markets)

### 2A. Tallow
| Country | Tier | Bal Sheet | Trade | Source File |
|---------|------|-----------|-------|-------------|
| United States (edible) | 1 | Y | Y | sbusbal + usoilseedtrade |
| United States (inedible) | 1 | Y | Y | sbusbal + usoilseedtrade |
| Canada | 1 | Y | Y | wldtalbal + wldtaltrade |
| Australia | 1 | Y | Y | wldtalbal + wldtaltrade |
| Brazil | 2 | Y | Y | wldtalbal + wldtaltrade |
| New Zealand | 2 | Y | Y | wldtalbal + wldtaltrade |
| EU-28 | 2 | Y | - | wldtalbal |
| Paraguay | 3 | Y | Y | wldtalbal + wldtaltrade |
| Uruguay | 3 | Y | Y | wldtalbal + wldtaltrade |
| Mexico | 3 | Y | - | wldtalbal |
| China | 3 | Y | - | wldtalbal |

### 2B. Used Cooking Oil (UCO)
| Country | Tier | Bal Sheet | Trade | Source File |
|---------|------|-----------|-------|-------------|
| China | 1 | Y | Y | wlducobal + wlducotrade |
| EU-28 | 1 | Y | Y | wlducobal + wlducotrade |
| Indonesia | 2 | Y | Y | wlducobal + wlducotrade |
| India | 2 | Y | Y | wlducobal + wlducotrade |
| Japan | 2 | Y | Y | wlducobal + wlducotrade |
| Canada | 2 | Y | Y | wlducobal + wlducotrade |
| Mexico | 3 | Y | Y | wlducobal + wlducotrade |
| New Zealand | 3 | Y | Y | wlducobal + wlducotrade |
| Australia | 3 | - | Y | wlducotrade |
| Malaysia | 3 | - | Y | wlducotrade |

### 2C. Other US Fats & Greases
| Commodity | Bal Sheet | Trade | Source File |
|-----------|-----------|-------|-------------|
| Yellow Grease | Y | Y | sbusbal + usoilseedtrade |
| Choice White Grease (CWG) | Y | Y | uscalyearbal + usoilseedtrade |
| Other Grease | Y | Y | sbusbal + usoilseedtrade |
| Lard | Y | Y | sbusbal + usoilseedtrade |
| Poultry Fat | Y | Y | sbusbal + usoilseedtrade |
| Distillers Corn Oil (DCO) | Y | Y | uscalyearbal + usoilseedtrade |
| Meat & Bone Meal | Y | Y | sbusbal + usoilseedtrade |
| Feather Meal | Y | Y | sbusbal + usoilseedtrade |
| Poultry Byproduct Meal | Y | Y | sbusbal + usoilseedtrade |

### 2D. US Feedstock Aggregates (Calendar Year)
| Balance Sheet | Source File |
|---------------|-------------|
| Total Feedstocks | uscalyearbal |
| Vegetable Oils (aggregate) | uscalyearbal |
| Fats and Grease (aggregate) | uscalyearbal |

---

## GROUP 3: FEED GRAINS

### 3A. Corn
| Country | Tier | Bal Sheet | Trade | Source File |
|---------|------|-----------|-------|-------------|
| World | 1 | Y | - | cnwldbal |
| United States | 1 | Y | Y | fgusbal + usfgtrade |
| China | 1 | Y | Y | cnwldbal |
| Brazil | 1 | Y | Y | cnwldbal + wldfgbal |
| EU | 1 | Y | Y | cnwldbal |
| Argentina | 1 | Y | Y | cnwldbal |
| Ukraine | 1 | Y | Y | cnwldbal |
| India | 2 | Y | Y | cnwldbal |
| Mexico | 2 | Y | Y | cnwldbal |
| South Africa | 2 | Y | Y | cnwldbal |
| Russia | 2 | Y | Y | cnwldbal |
| Canada | 2 | Y | Y | cnwldbal |
| Indonesia | 2 | Y | Y | cnwldbal |
| Thailand | 3 | Y | Y | cnwldbal |
| Japan | 3 | imports | Y | cnwldbal |
| South Korea | 3 | imports | Y | cnwldbal |
| Taiwan | 3 | imports | Y | cnwldbal |
| Malaysia | 3 | imports | Y | cnwldbal |
| Philippines | 3 | imports | Y | cnwldbal |
| Egypt | 3 | imports | Y | cnwldbal |
| Paraguay | 3 | Y | Y | cnwldbal |
| Serbia | 3 | Y | Y | cnwldbal |
| Croatia | 3 | Y | Y | cnwldbal |

### 3B. Sorghum
| Country | Tier | Bal Sheet | Trade | Source File |
|---------|------|-----------|-------|-------------|
| World | 1 | Y | - | gswldbal |
| United States | 1 | Y | - | fgusbal + gswldbal |
| China | 1 | imports | Y | gswldbal |
| Argentina | 2 | Y | Y | gswldbal |
| Australia | 2 | Y | Y | gswldbal |
| India | 2 | Y | Y | gswldbal |
| Mexico | 2 | Y | Y | gswldbal |
| Brazil | 3 | Y | Y | gswldbal |
| EU | 3 | Y | Y | gswldbal |
| Japan | 3 | imports | Y | gswldbal |
| Taiwan | 3 | imports | Y | gswldbal |

### 3C. Barley
| Country | Tier | Bal Sheet | Trade | Source File |
|---------|------|-----------|-------|-------------|
| World | 1 | Y | - | bawldbal |
| EU | 1 | Y | Y | bawldbal |
| Russia | 1 | Y | Y | bawldbal |
| Australia | 1 | Y | Y | bawldbal |
| Canada | 1 | Y | Y | bawldbal |
| Ukraine | 2 | Y | Y | bawldbal |
| United States | 2 | Y | - | fgusbal + bawldbal |
| Kazakhstan | 2 | Y | Y | bawldbal |
| Argentina | 2 | Y | Y | bawldbal |
| Saudi Arabia | 2 | imports | Y | bawldbal |
| China | 3 | Y | Y | bawldbal |
| Japan | 3 | imports | Y | bawldbal |

### 3D. Oats
| Country | Tier | Bal Sheet | Trade | Source File |
|---------|------|-----------|-------|-------------|
| World | 1 | Y | - | oawldbal |
| EU | 1 | Y | Y | oawldbal |
| Canada | 1 | Y | Y | oawldbal |
| Australia | 2 | Y | Y | oawldbal |
| United States | 2 | Y | - | fgusbal + oawldbal |
| Russia | 2 | Y | - | oawldbal |
| Ukraine | 3 | Y | - | oawldbal |
| China | 3 | Y | - | oawldbal |

### 3E. US Feed Grain Aggregates
| Balance Sheet | Source File |
|---------------|-------------|
| US Feed Grain Composite | fgusbal (FGComp) |
| GCAU (grain consuming animal units) | fgusbal |
| Feed & Residual Model | fgusbal |

---

## GROUP 4: FOOD GRAINS

### 4A. Wheat (by class for US)
| Country | Tier | Bal Sheet | Trade | Source File |
|---------|------|-----------|-------|-------------|
| World | 1 | Y | - | world wheat files |
| United States (all wheat) | 1 | Y | Y | whusbal |
| United States (HRW) | 1 | Y | - | USwinter wheat |
| United States (HRS) | 1 | Y | - | USspring wheat |
| United States (SRW) | 1 | Y | - | whusbal |
| United States (durum) | 2 | Y | - | durum wheat |
| Russia | 1 | Y | Y | TBD |
| EU | 1 | Y | Y | TBD |
| Canada | 1 | Y | Y | TBD |
| Australia | 1 | Y | Y | TBD |
| Argentina | 1 | Y | Y | HB Balance Sheets |
| Ukraine | 1 | Y | Y | TBD |
| China | 1 | Y | Y | HB Chinese Balance Sheets |
| India | 1 | Y | - | TBD |
| Kazakhstan | 2 | Y | Y | TBD |
| Egypt | 2 | imports | - | TBD |
| Brazil | 2 | imports | - | TBD |
| Mexico | 3 | imports | - | TBD |
| Pakistan | 3 | Y | - | TBD |

### 4B. Rice
| Country | Tier | Bal Sheet | Trade | Source File |
|---------|------|-----------|-------|-------------|
| World | 1 | TBD | - | TBD |
| India | 1 | TBD | - | TBD |
| China | 1 | TBD | - | TBD |
| United States | 2 | Y | - | rice balance |
| Thailand | 1 | TBD | - | TBD |
| Vietnam | 1 | TBD | - | TBD |
| Indonesia | 2 | TBD | - | TBD |
| Pakistan | 2 | TBD | - | TBD |
| Brazil | 3 | TBD | - | TBD |

### 4C. Sugar
| Country | Tier | Bal Sheet | Trade | Source File |
|---------|------|-----------|-------|-------------|
| World | 1 | TBD | - | TBD |
| Brazil | 1 | TBD | - | TBD (linked to ethanol) |
| India | 1 | TBD | - | TBD |
| United States | 2 | TBD | - | TBD |
| EU | 2 | TBD | - | TBD |
| Thailand | 2 | TBD | - | TBD |

---

## GROUP 5: ENERGY / BIOFUELS

### 5A. Ethanol
| Market | Bal Sheet | Source File |
|--------|-----------|-------------|
| US Ethanol | Y | usethanolbal |
| World Ethanol | Y | fgethanol (EIA World Ethanol) |
| Brazil Ethanol | TBD | (linked to sugar) |

### 5B. Ethanol Co-Products
| Product | Bal Sheet | Source File |
|---------|-----------|-------------|
| DDG/DDGS | Y | usethanolbal |
| Corn Gluten Meal | Y | usethanolbal |
| Corn Gluten Feed | Y | fgethanol |
| Co-Products Production (aggregate) | Y | usethanolbal |

### 5C. Biodiesel / Renewable Diesel
| Market | Bal Sheet | Trade | Source File |
|--------|-----------|-------|-------------|
| World Biodiesel | Y | Y | wldbiodiesel + wldbiodieseltrade |
| US Biodiesel | Y | Y | wldbiodiesel |
| Brazil Biodiesel | TBD | - | brazilbiodiesel |
| EU Biodiesel | TBD | - | TBD |

### 5D. Global Oilseed Crush Margins
| Market | Source File |
|--------|-------------|
| US crush margins | Global oilseeds margins model |
| Brazil crush margins | Global oilseeds margins model |
| China crush margins (dom + import) | Global oilseeds margins model |
| Malaysia palm margins | Global oilseeds margins model |

---

## SUMMARY COUNTS

| Group | Unique Sheets Needed | Countries | Commodities |
|-------|---------------------|-----------|-------------|
| Oilseeds | ~85 | 14+ | 7 complexes + aggregates |
| Fats & Greases | ~35 | 11 | 12 products |
| Feed Grains | ~55 | 23 | 4 grains + aggregates |
| Food Grains | ~35 | 19 | 3 commodities |
| Energy/Biofuels | ~15 | 3 | 5 products |
| **TOTAL** | **~225** | **~30** | **~30+** |
