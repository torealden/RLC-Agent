# Peanut food use balance sheet — design

**Status:** schema TBD. Trade-flow inputs landing now (mig 123). NASS
domestic-side already wired via the existing crush/usage refs.

## Goal

Monthly view of "peanuts used in US food" — built from the 3-bucket trade
split + NASS domestic edible/in-shell food usage. Companion to the master
peanut balance sheet (`silver.peanut_balance_sheet_master`, annual ERS).

## Identity

All quantities in shelled-equivalent (000 lb).

```
US peanuts used in food, monthly =
    NASS domestic edible shelled usage          [PEANUTS, SHELLED, EDIBLE - USAGE, RAW BASIS]
      (= candy + snacks + peanut butter + other)
  + NASS domestic in-shell food usage           [PEANUTS, IN SHELL - USAGE, RAW BASIS]
  + net imports of finished food products
      + peanut butter (2008.11.05)              [imports - exports]
      + other prep & preserved (2008.11.15)
  + net imports of food-stream raw materials
      + in-shell raw (1202.41)                  [imports = 0, exports leave US]
      + shelled raw (1202.42)                   [imports = 0, exports leave US]
```

For US-only consumption we subtract exports (they leave the US food chain).
For imports we have to convert prepared-product weight to peanut-equivalent:
peanut butter is ~93% peanut by weight → divide imported peanut butter by
0.93 to get peanut-equivalent.

## Inputs

### Domestic (NASS)
Already in `silver.crush_attribute_reference` rows 4-8, 10:
- Edible candy, snacks, peanut butter, other, total edible (NASS pg 7)
- In-shell peanuts food usage (NASS pg 7)

### Trade (Census, mig 123)
Five HS codes × 2 flows:
- 1202.41 in-shell, not for sowing
- 1202.42 shelled, not for sowing
- 1202.30 seed peanuts
- 2008.11.05 peanut butter
- 2008.11.15 other prepared peanuts

## Output table

`silver.peanut_food_use_monthly`:

| column                            | unit              | source                      |
|-----------------------------------|-------------------|-----------------------------|
| year, month                       | -                 | -                           |
| edible_candy                      | 000 lb shelled    | NASS                        |
| edible_snacks                     | 000 lb shelled    | NASS                        |
| edible_peanut_butter              | 000 lb shelled    | NASS                        |
| edible_other                      | 000 lb shelled    | NASS                        |
| edible_total                      | 000 lb shelled    | NASS                        |
| in_shell_food_usage               | 000 lb shelled    | NASS                        |
| in_shell_imports                  | 000 lb shelled    | Census                      |
| in_shell_exports                  | 000 lb shelled    | Census                      |
| in_shell_net_trade                | 000 lb shelled    | derived                     |
| shelled_raw_imports               | 000 lb shelled    | Census                      |
| shelled_raw_exports               | 000 lb shelled    | Census                      |
| peanut_butter_imports             | 000 lb peanut-eq  | Census * 1/0.93 conv        |
| peanut_butter_exports             | 000 lb peanut-eq  | Census * 1/0.93 conv        |
| other_prep_imports                | 000 lb shelled    | Census                      |
| other_prep_exports                | 000 lb shelled    | Census                      |
| **total_food_use_shelled_basis**  | 000 lb shelled    | derived sum                 |

## Open questions

1. Confirm peanut butter → peanut conversion factor. 0.93 is the conventional
   "peanut content" of peanut butter; some product specs use 0.90-0.95.
2. Net trade vs gross trade — show both, default to net.
3. Annual rollup → reconcile against ERS `silver.peanut_balance_sheet_master`
   food_use_mil_lbs. Expect close but not identical match because ERS uses
   annual averages and includes adjustments not visible in monthly data.

## Next step

After mig 123 trade backfill completes:
1. Validate trade quantities (especially the Census shelled-export quantity
   suppression — check if value/price gives us implied quantity).
2. Build `silver.peanut_food_use_monthly` migration.
3. Reconcile monthly totals against ERS annual.
