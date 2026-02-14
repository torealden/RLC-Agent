// ============================================================================
// US SOYBEANS DASHBOARD - POWER QUERY M CONNECTIONS
// ============================================================================
// Copy each section into Power BI Desktop > Home > Transform Data > New Source > Blank Query
// Then paste the M code in the Advanced Editor
// ============================================================================

// ============================================================================
// CONNECTION PARAMETERS (Edit these for your environment)
// ============================================================================
// Create a new Query called "DB_Connection" with these parameters:

/*
let
    Server = "localhost",
    Database = "rlc_commodities",
    Source = PostgreSQL.Database(Server, Database)
in
    Source
*/

// ============================================================================
// 1. FAS_PSD - Annual Balance Sheet Data
// ============================================================================
// Query Name: fas_psd_soybeans

/*
let
    Source = PostgreSQL.Database("localhost", "rlc_commodities"),
    bronze_fas_psd = Source{[Schema="bronze",Item="fas_psd"]}[Data],
    FilteredRows = Table.SelectRows(bronze_fas_psd, each [commodity] = "soybeans"),
    SortedRows = Table.Sort(FilteredRows,{{"marketing_year", Order.Descending}, {"report_date", Order.Descending}})
in
    SortedRows
*/

// ============================================================================
// 2. MONTHLY_REALIZED - Crush, Oil Production, etc.
// ============================================================================
// Query Name: monthly_realized_soybeans

/*
let
    Source = PostgreSQL.Database("localhost", "rlc_commodities"),
    silver_monthly_realized = Source{[Schema="silver",Item="monthly_realized"]}[Data],
    FilteredRows = Table.SelectRows(silver_monthly_realized, each [commodity] = "soybeans"),
    SortedRows = Table.Sort(FilteredRows,{{"calendar_year", Order.Descending}, {"month", Order.Descending}})
in
    SortedRows
*/

// ============================================================================
// 3. CFTC_SENTIMENT - Speculative Positioning
// ============================================================================
// Query Name: cftc_sentiment

/*
let
    Source = PostgreSQL.Database("localhost", "rlc_commodities"),
    gold_cftc_sentiment = Source{[Schema="gold",Item="cftc_sentiment"]}[Data],
    FilteredRows = Table.SelectRows(gold_cftc_sentiment, each Text.Contains([commodity], "soy"))
in
    FilteredRows
*/

// ============================================================================
// 4. FAS_EXPORT_SALES - Weekly Export Data
// ============================================================================
// Query Name: fas_export_sales_soybeans

/*
let
    Source = PostgreSQL.Database("localhost", "rlc_commodities"),
    bronze_fas_export_sales = Source{[Schema="bronze",Item="fas_export_sales"]}[Data],
    FilteredRows = Table.SelectRows(bronze_fas_export_sales, each [commodity] = "soybeans"),
    SortedRows = Table.Sort(FilteredRows,{{"week_ending", Order.Descending}})
in
    SortedRows
*/

// ============================================================================
// 5. USER_SD_ESTIMATE - RLC Predictions
// ============================================================================
// Query Name: user_sd_estimate_soybeans

/*
let
    Source = PostgreSQL.Database("localhost", "rlc_commodities"),
    silver_user_sd_estimate = Source{[Schema="silver",Item="user_sd_estimate"]}[Data],
    FilteredRows = Table.SelectRows(silver_user_sd_estimate, each [commodity] = "soybeans" and [is_current] = true),
    SortedRows = Table.Sort(FilteredRows,{{"marketing_year", Order.Descending}})
in
    SortedRows
*/

// ============================================================================
// 6. NASS_CROP_PROGRESS - Planting/Harvest Progress
// ============================================================================
// Query Name: nass_progress_soybeans

/*
let
    Source = PostgreSQL.Database("localhost", "rlc_commodities"),
    silver_nass_progress = Source{[Schema="silver",Item="nass_crop_progress"]}[Data],
    FilteredRows = Table.SelectRows(silver_nass_progress, each [commodity] = "soybeans"),
    SortedRows = Table.Sort(FilteredRows,{{"year", Order.Descending}, {"week", Order.Descending}})
in
    SortedRows
*/

// ============================================================================
// 7. WASDE_HISTORY - USDA Report Archive (for revision tracking)
// ============================================================================
// Query Name: wasde_history_soybeans
// Note: This table may need to be created - see SQL views file

/*
let
    Source = PostgreSQL.Database("localhost", "rlc_commodities"),
    bronze_wasde = Source{[Schema="bronze",Item="wasde_history"]}[Data],
    FilteredRows = Table.SelectRows(bronze_wasde, each [commodity] = "soybeans" and [country] = "US"),
    SortedRows = Table.Sort(FilteredRows,{{"marketing_year", Order.Descending}, {"report_date", Order.Descending}})
in
    SortedRows
*/

// ============================================================================
// 8. FUTURES_PRICE - Daily Settlement Prices
// ============================================================================
// Query Name: futures_price_soybeans

/*
let
    Source = PostgreSQL.Database("localhost", "rlc_commodities"),
    silver_futures = Source{[Schema="silver",Item="futures_price"]}[Data],
    FilteredRows = Table.SelectRows(silver_futures, each [commodity] = "soybeans"),
    SortedRows = Table.Sort(FilteredRows,{{"date", Order.Descending}})
in
    SortedRows
*/

// ============================================================================
// 9. BALANCE SHEET STRUCTURE - Dimension Table
// ============================================================================
// Query Name: dim_balance_sheet_structure
// This creates the row ordering for the S&D matrix

/*
let
    Source = Table.FromRecords({
        [Category = "SUPPLY", SortOrder = 1, IsHeader = true, Level = 0],
        [Category = "Planted Area", SortOrder = 2, IsHeader = false, Level = 1],
        [Category = "Harvested Area", SortOrder = 3, IsHeader = false, Level = 1],
        [Category = "Yield", SortOrder = 4, IsHeader = false, Level = 1],
        [Category = "Beginning Stocks", SortOrder = 5, IsHeader = false, Level = 1],
        [Category = "Production", SortOrder = 6, IsHeader = false, Level = 1],
        [Category = "Imports", SortOrder = 7, IsHeader = false, Level = 1],
        [Category = "Total Supply", SortOrder = 8, IsHeader = false, Level = 0],
        [Category = "DEMAND", SortOrder = 9, IsHeader = true, Level = 0],
        [Category = "Crush", SortOrder = 10, IsHeader = false, Level = 1],
        [Category = "Exports", SortOrder = 11, IsHeader = false, Level = 1],
        [Category = "Seed", SortOrder = 12, IsHeader = false, Level = 1],
        [Category = "Residual", SortOrder = 13, IsHeader = false, Level = 1],
        [Category = "Total Use", SortOrder = 14, IsHeader = false, Level = 0],
        [Category = "Ending Stocks", SortOrder = 15, IsHeader = false, Level = 0],
        [Category = "Stocks/Use", SortOrder = 16, IsHeader = false, Level = 0],
        [Category = "Farm Price", SortOrder = 17, IsHeader = false, Level = 0]
    }),
    TypedTable = Table.TransformColumnTypes(Source,{
        {"Category", type text},
        {"SortOrder", Int64.Type},
        {"IsHeader", type logical},
        {"Level", Int64.Type}
    })
in
    TypedTable
*/

// ============================================================================
// 10. MARKETING YEAR CALENDAR - Dimension Table
// ============================================================================
// Query Name: dim_marketing_year

/*
let
    // Generate marketing years from 2015 to 2030
    YearList = {2015..2030},
    ToTable = Table.FromList(YearList, Splitter.SplitByNothing(), {"MarketingYear"}),
    AddLabel = Table.AddColumn(ToTable, "MY_Label", each
        Text.From([MarketingYear]-1) & "/" & Text.End(Text.From([MarketingYear]), 2)
    ),
    AddStartDate = Table.AddColumn(AddLabel, "StartDate", each #date([MarketingYear]-1, 9, 1)),
    AddEndDate = Table.AddColumn(AddStartDate, "EndDate", each #date([MarketingYear], 8, 31)),
    AddIsCurrent = Table.AddColumn(AddEndDate, "IsCurrent", each
        Date.From(DateTime.LocalNow()) >= [StartDate] and Date.From(DateTime.LocalNow()) <= [EndDate]
    ),
    TypedTable = Table.TransformColumnTypes(AddIsCurrent,{
        {"MarketingYear", Int64.Type},
        {"MY_Label", type text},
        {"StartDate", type date},
        {"EndDate", type date},
        {"IsCurrent", type logical}
    })
in
    TypedTable
*/

// ============================================================================
// 11. CALENDAR - Standard Date Dimension
// ============================================================================
// Query Name: dim_calendar

/*
let
    StartDate = #date(2015, 1, 1),
    EndDate = #date(2030, 12, 31),
    NumberOfDays = Duration.Days(EndDate - StartDate) + 1,
    DateList = List.Dates(StartDate, NumberOfDays, #duration(1, 0, 0, 0)),
    ToTable = Table.FromList(DateList, Splitter.SplitByNothing(), {"Date"}),
    ChangedType = Table.TransformColumnTypes(ToTable,{{"Date", type date}}),
    AddYear = Table.AddColumn(ChangedType, "Year", each Date.Year([Date]), Int64.Type),
    AddMonth = Table.AddColumn(AddYear, "Month", each Date.Month([Date]), Int64.Type),
    AddMonthName = Table.AddColumn(AddMonth, "MonthName", each Date.MonthName([Date]), type text),
    AddQuarter = Table.AddColumn(AddMonthName, "Quarter", each Date.QuarterOfYear([Date]), Int64.Type),
    AddWeek = Table.AddColumn(AddQuarter, "WeekOfYear", each Date.WeekOfYear([Date]), Int64.Type),
    // Marketing Year (Sep-Aug for soybeans)
    AddMY = Table.AddColumn(AddWeek, "MarketingYear", each
        if [Month] >= 9 then [Year] + 1 else [Year], Int64.Type
    ),
    AddMYLabel = Table.AddColumn(AddMY, "MY_Label", each
        Text.From([MarketingYear]-1) & "/" & Text.End(Text.From([MarketingYear]), 2), type text
    ),
    // Marketing Year Month (1 = Sep, 12 = Aug)
    AddMYMonth = Table.AddColumn(AddMYLabel, "MY_Month", each
        if [Month] >= 9 then [Month] - 8 else [Month] + 4, Int64.Type
    )
in
    AddMYMonth
*/

// ============================================================================
// 12. USDA COMP VIEW - Pre-formatted for WASDE Comparison Table
// ============================================================================
// Query Name: usda_comp_soybeans
// This query creates the exact format shown in your screenshot

/*
let
    Source = PostgreSQL.Database("localhost", "rlc_commodities"),

    // Get USDA data (latest report for each marketing year)
    USDA_Data = Source{[Schema="gold",Item="usda_comp_soybeans"]}[Data],

    // If this view doesn't exist, use this alternative:
    // USDA_Data = Source{[Schema="bronze",Item="fas_psd"]}[Data],
    // FilteredUSDA = Table.SelectRows(USDA_Data, each [commodity] = "soybeans" and [country_code] = "US"),

    SortedRows = Table.Sort(USDA_Data, {{"marketing_year", Order.Descending}})
in
    SortedRows
*/
