# Power BI DAX Templates for Common Calculations

These DAX formulas can be used in Power BI to create calculated columns or measures for common agricultural data analysis patterns.

---

## Year-over-Year (YoY) Change

### YoY Percent Change (Calculated Column)
Use when you have a date/year column and want to compare each row to the same period last year.

```dax
YoY_Pct =
VAR CurrentValue = [Production]
VAR CurrentYear = [Crop_Year]
VAR PriorYear =
    CALCULATE(
        MAX([Production]),
        FILTER(
            ALL('Table'),
            [Crop_Year] = EARLIER([Crop_Year]) - 1
            && [State] = EARLIER([State])
            && [Commodity] = EARLIER([Commodity])
        )
    )
RETURN
IF(
    PriorYear <> 0 && NOT(ISBLANK(PriorYear)),
    ROUND((CurrentValue - PriorYear) / PriorYear * 100, 1),
    BLANK()
)
```

### YoY Change Measure (for aggregations)
```dax
Production YoY % =
VAR CurrentYear = MAX([Marketing_Year])
VAR CurrentVal = CALCULATE(SUM([Production]), [Marketing_Year] = CurrentYear)
VAR PriorVal = CALCULATE(SUM([Production]), [Marketing_Year] = CurrentYear - 1)
RETURN
IF(PriorVal <> 0, DIVIDE(CurrentVal - PriorVal, PriorVal) * 100, BLANK())
```

---

## 5-Year Average and Departure

### 5-Year Trailing Average (Calculated Column)
```dax
Avg_5Yr_Production =
VAR CurrentYear = [Crop_Year]
VAR AvgValue =
    CALCULATE(
        AVERAGE([Production]),
        FILTER(
            ALL('Table'),
            [Crop_Year] < CurrentYear
            && [Crop_Year] >= CurrentYear - 5
            && [State] = EARLIER([State])
            && [Commodity] = EARLIER([Commodity])
        )
    )
RETURN
IF(NOT(ISBLANK(AvgValue)), AvgValue, BLANK())
```

### Departure from 5-Year Average (%)
```dax
Vs_5Yr_Avg_Pct =
VAR CurrentValue = [Production]
VAR Avg5Yr = [Avg_5Yr_Production]
RETURN
IF(
    Avg5Yr <> 0 && NOT(ISBLANK(Avg5Yr)),
    ROUND((CurrentValue - Avg5Yr) / Avg5Yr * 100, 1),
    BLANK()
)
```

---

## Stocks-to-Use Ratio

### Stocks/Use Ratio (Calculated Column)
```dax
Stocks_Use_Ratio =
IF(
    [Total_Use] <> 0 && NOT(ISBLANK([Total_Use])),
    ROUND([Ending_Stocks] / [Total_Use] * 100, 1),
    BLANK()
)
```

### Stocks/Use Measure
```dax
Stocks/Use % =
VAR Stocks = SUM([Ending_Stocks])
VAR Use = SUM([Total_Use])
RETURN
IF(Use <> 0, DIVIDE(Stocks, Use) * 100, BLANK())
```

---

## Share/Percentage Calculations

### Export Share of Total Use
```dax
Export_Share_Pct =
IF(
    [Total_Use] <> 0 && NOT(ISBLANK([Total_Use])),
    ROUND([Exports] / [Total_Use] * 100, 1),
    BLANK()
)
```

### Crush Share of Domestic Use
```dax
Crush_Share_Pct =
IF(
    [Domestic_Use] <> 0 && NOT(ISBLANK([Domestic_Use])),
    ROUND([Crush] / [Domestic_Use] * 100, 1),
    BLANK()
)
```

---

## Week-over-Week Change (for weekly data)

### WoW Change %
```dax
WoW_Pct =
VAR CurrentValue = [Value]
VAR PriorWeek =
    CALCULATE(
        MAX([Value]),
        FILTER(
            ALL('Table'),
            [Week_Ending] = EARLIER([Week_Ending]) - 7
            && [Commodity] = EARLIER([Commodity])
        )
    )
RETURN
IF(
    PriorWeek <> 0 && NOT(ISBLANK(PriorWeek)),
    ROUND((CurrentValue - PriorWeek) / PriorWeek * 100, 2),
    BLANK()
)
```

---

## Crop Condition Comparison to Prior Year

### Prior Year Same Week (for crop conditions)
```dax
Prior_Year_GE_Pct =
CALCULATE(
    MAX([Good_Excellent_Pct]),
    FILTER(
        ALL('Table'),
        YEAR([Week_Ending]) = YEAR(EARLIER([Week_Ending])) - 1
        && WEEKNUM([Week_Ending]) = WEEKNUM(EARLIER([Week_Ending]))
        && [Commodity] = EARLIER([Commodity])
        && [State] = EARLIER([State])
    )
)
```

### Change from Prior Year
```dax
GE_Change_vs_PY = [Good_Excellent_Pct] - [Prior_Year_GE_Pct]
```

---

## Rolling Averages

### 4-Week Rolling Average (Calculated Column)
```dax
Rolling_4Wk_Avg =
CALCULATE(
    AVERAGE([Value]),
    FILTER(
        ALL('Table'),
        [Week_Ending] <= EARLIER([Week_Ending])
        && [Week_Ending] > EARLIER([Week_Ending]) - 28
        && [Commodity] = EARLIER([Commodity])
    )
)
```

### 52-Week Rolling Sum (for cumulative exports)
```dax
Rolling_52Wk_Total =
CALCULATE(
    SUM([Weekly_Exports]),
    FILTER(
        ALL('Table'),
        [Week_Ending] <= EARLIER([Week_Ending])
        && [Week_Ending] > EARLIER([Week_Ending]) - 364
        && [Commodity] = EARLIER([Commodity])
    )
)
```

---

## Percentile/Ranking

### Percentile within Historical Range
```dax
Percentile_1Yr =
VAR CurrentValue = [MM_Net_Position]
VAR MinVal = CALCULATE(MIN([MM_Net_Position]), FILTER(ALL('Table'), [Report_Date] >= TODAY() - 365))
VAR MaxVal = CALCULATE(MAX([MM_Net_Position]), FILTER(ALL('Table'), [Report_Date] >= TODAY() - 365))
RETURN
IF(MaxVal <> MinVal, ROUND((CurrentValue - MinVal) / (MaxVal - MinVal) * 100, 0), 50)
```

---

## Tips for Using These Templates

1. **Replace field names** - Adjust `[Production]`, `[Crop_Year]`, etc. to match your actual column names
2. **Adjust grouping fields** - Modify `[State]`, `[Commodity]` to match your data structure
3. **Handle marketing years** - For crop year strings like "2024/25", you may need to extract the year first:
   ```dax
   Marketing_Year_Start = VALUE(LEFT([Crop_Year], 4))
   ```
4. **Test with small datasets** - Complex DAX can be slow on large tables; consider calculated columns vs measures based on performance

---

## PostgreSQL Equivalents

For reference, the database now calculates these automatically. See:
- `database/migrations/populate_calculated_columns.sql`
- Run after each CONAB data load to update calculated columns
