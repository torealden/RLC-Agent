' =============================================================================
' BiofuelDataUpdater - Pure VBA with PostgreSQL Connection
' =============================================================================
' Connects directly to PostgreSQL database to update biofuel holding sheet.
' No Python required. No closing/reopening workbook.
'
' Target: us_biofuel_holding_sheet.xlsm (10 sheets)
' Layout: Varies by sheet — see constants below
'         Column A = dates (1st of month, as date serial)
'         Remaining columns = data series
'
' Data source: gold.rin_generation, gold.rin_separation, gold.rin_retirement,
'              gold.eia_feedstock, gold.eia_feedstock_plant_type,
'              gold.eia_biofuel_production, gold.eia_biofuel_trade,
'              gold.eia_biofuel_capacity, gold.eia_blending_context
'
' Key similarity to CrushUpdaterSQL:
'   Both are rows = months, columns = data attributes
'
' Requirements:
' - PostgreSQL ODBC Driver installed (psqlODBC)
' - Reference to "Microsoft ActiveX Data Objects" (Tools > References)
'
' Installation:
' 1. Download psqlODBC from https://www.postgresql.org/ftp/odbc/versions/msi/
' 2. Install the 64-bit version (psqlodbc_x64.msi)
' 3. In VBA: Tools > References > Check "Microsoft ActiveX Data Objects 6.1 Library"
' =============================================================================

Option Explicit

' Database connection settings
Private Const DB_SERVER As String = "localhost"
Private Const DB_PORT As String = "5432"
Private Const DB_NAME As String = "rlc_commodities"
Private Const DB_USER As String = "postgres"
Private Const DB_PASSWORD As String = "SoupBoss1"

' Spreadsheet structure — common across most sheets
Private Const DATE_COLUMN As Integer = 1      ' Column A = dates

' Sheet-specific layout constants
' Sheets 1-3 (RIN): header row 3, units row 4, data starts row 5
Private Const RIN_HEADER_ROW As Integer = 3
Private Const RIN_DATA_START As Integer = 5

' Sheet 4 (Feedstock): has grouped two-tier header, data starts row 6
Private Const FEED_HEADER_ROW As Integer = 3
Private Const FEED_DATA_START As Integer = 6

' Sheets 5-7, 9: header row 3, data starts row 5
Private Const STD_HEADER_ROW As Integer = 3
Private Const STD_DATA_START As Integer = 5

' Sheet 8 (Capacity): plant-level table, header row 4, data starts row 5
Private Const CAP_HEADER_ROW As Integer = 4
Private Const CAP_DATA_START As Integer = 5

' =============================================================================
' MAIN UPDATE FUNCTIONS
' =============================================================================

Public Sub UpdateBiofuelData()
    ' Quick update - latest 6 months of available data
    ' Keyboard shortcut: Ctrl+B

    Dim result As VbMsgBoxResult
    result = MsgBox("Update all biofuel sheets with the latest 6 months of data?", _
                    vbYesNo + vbQuestion, "Biofuel Updater")

    If result = vbYes Then
        UpdateFromDatabase 6
    End If
End Sub

Public Sub UpdateBiofuelDataCustom()
    ' Custom update with user-specified months
    ' Keyboard shortcut: Ctrl+Shift+B

    Dim monthCount As String
    monthCount = InputBox("How many months of data to update?" & vbCrLf & vbCrLf & _
                          "Enter a number (e.g., 12 for last 12 months)" & vbCrLf & _
                          "Or enter 0 to update ALL available data", _
                          "Biofuel Updater", "6")

    If monthCount = "" Then Exit Sub

    If Not IsNumeric(monthCount) Then
        MsgBox "Please enter a valid number.", vbExclamation, "Biofuel Updater"
        Exit Sub
    End If

    UpdateFromDatabase CInt(monthCount)
End Sub

' =============================================================================
' DATABASE CONNECTION
' =============================================================================

Private Function GetConnection() As Object
    ' Create and return ADODB Connection to PostgreSQL

    Dim conn As Object
    Dim connString As String

    Set conn = CreateObject("ADODB.Connection")

    ' PostgreSQL ODBC connection string
    connString = "Driver={PostgreSQL UNICODE(x64)};" & _
                 "Server=" & DB_SERVER & ";" & _
                 "Port=" & DB_PORT & ";" & _
                 "Database=" & DB_NAME & ";" & _
                 "Uid=" & DB_USER & ";" & _
                 "Pwd=" & DB_PASSWORD & ";"

    On Error GoTo ConnError
    conn.Open connString
    Set GetConnection = conn
    Exit Function

ConnError:
    MsgBox "Database connection failed:" & vbCrLf & vbCrLf & Err.Description & vbCrLf & vbCrLf & _
           "Make sure PostgreSQL ODBC driver is installed.", vbCritical, "Connection Error"
    Set GetConnection = Nothing
End Function

' =============================================================================
' MAIN UPDATE LOGIC
' =============================================================================

Private Sub UpdateFromDatabase(monthCount As Integer)
    ' Fetch data from database and update all sheets

    Dim conn As Object
    Dim totalCells As Long
    Dim sheetCount As Integer

    ' Show status
    Application.StatusBar = "Connecting to database..."
    Application.Cursor = xlWait
    DoEvents

    ' Get connection
    Set conn = GetConnection()
    If conn Is Nothing Then
        Application.StatusBar = False
        Application.Cursor = xlDefault
        Exit Sub
    End If

    totalCells = 0
    sheetCount = 0

    On Error Resume Next

    Application.StatusBar = "Updating RIN Generation..."
    DoEvents
    totalCells = totalCells + UpdateRINGeneration(conn, monthCount)
    sheetCount = sheetCount + 1

    Application.StatusBar = "Updating RIN Separation & Available..."
    DoEvents
    totalCells = totalCells + UpdateRINSeparation(conn, monthCount)
    sheetCount = sheetCount + 1

    Application.StatusBar = "Updating RIN Retirement..."
    DoEvents
    totalCells = totalCells + UpdateRINRetirement(conn, monthCount)
    sheetCount = sheetCount + 1

    Application.StatusBar = "Updating Feedstock Consumption..."
    DoEvents
    totalCells = totalCells + UpdateFeedstockConsumption(conn, monthCount)
    sheetCount = sheetCount + 1

    Application.StatusBar = "Updating Feedstock by Plant Type..."
    DoEvents
    totalCells = totalCells + UpdateFeedstockByPlantType(conn, monthCount)
    sheetCount = sheetCount + 1

    Application.StatusBar = "Updating Biofuel Production..."
    DoEvents
    totalCells = totalCells + UpdateBiofuelProduction(conn, monthCount)
    sheetCount = sheetCount + 1

    Application.StatusBar = "Updating Biofuel Trade..."
    DoEvents
    totalCells = totalCells + UpdateBiofuelTrade(conn, monthCount)
    sheetCount = sheetCount + 1

    Application.StatusBar = "Updating Capacity..."
    DoEvents
    totalCells = totalCells + UpdateCapacity(conn)
    sheetCount = sheetCount + 1

    Application.StatusBar = "Updating Blending Context..."
    DoEvents
    totalCells = totalCells + UpdateBlendingContext(conn, monthCount)
    sheetCount = sheetCount + 1

    On Error GoTo 0

    conn.Close

    ' Reset status
    Application.StatusBar = False
    Application.Cursor = xlDefault

    ' Report results
    Dim monthLabel As String
    If monthCount = 0 Then
        monthLabel = "all available"
    Else
        monthLabel = "latest " & monthCount & " months"
    End If

    MsgBox "Update complete!" & vbCrLf & vbCrLf & _
           "Sheets updated: " & sheetCount & vbCrLf & _
           "Cells updated: " & totalCells & vbCrLf & _
           "Period: " & monthLabel, vbInformation, "Biofuel Updater"
End Sub

' =============================================================================
' HELPER FUNCTIONS
' =============================================================================

Private Function FindRowForDate(ws As Worksheet, yr As Integer, mo As Integer, _
                                 dataStartRow As Integer) As Integer
    ' Find row number for a given year/month by scanning the date column (A)
    ' Dates in column A are 1st-of-month date serial numbers

    Dim row As Integer
    Dim cellVal As Variant
    Dim cellDate As Date
    Dim lastRow As Integer

    ' Find last row with data in column A
    lastRow = ws.Cells(ws.Rows.Count, DATE_COLUMN).End(xlUp).row

    For row = dataStartRow To lastRow
        cellVal = ws.Cells(row, DATE_COLUMN).Value

        If IsDate(cellVal) Then
            cellDate = CDate(cellVal)
            If Year(cellDate) = yr And Month(cellDate) = mo Then
                FindRowForDate = row
                Exit Function
            End If
        ElseIf IsNumeric(cellVal) And cellVal > 0 Then
            ' Excel date serial number
            On Error Resume Next
            cellDate = CDate(cellVal)
            If Err.Number = 0 Then
                If Year(cellDate) = yr And Month(cellDate) = mo Then
                    FindRowForDate = row
                    Exit Function
                End If
            End If
            On Error GoTo 0
        End If
    Next row

    FindRowForDate = 0  ' Not found
End Function

Private Function BuildMonthLimitClause(monthCount As Integer, _
                                        tableName As String) As String
    ' Build the SQL clause to limit to the latest N months
    ' monthCount=0 means no limit (all data)

    If monthCount > 0 Then
        BuildMonthLimitClause = _
            "AND (year, month) IN ( " & _
            "    SELECT DISTINCT year, month " & _
            "    FROM " & tableName & " " & _
            "    ORDER BY year DESC, month DESC " & _
            "    LIMIT " & monthCount & _
            ") "
    Else
        BuildMonthLimitClause = ""
    End If
End Function

' =============================================================================
' SHEET UPDATERS
' Each returns the number of cells updated
' =============================================================================

'--- Sheet 1: RIN Generation ---
Private Function UpdateRINGeneration(conn As Object, monthCount As Integer) As Long
    Dim ws As Worksheet
    Dim rs As Object
    Dim sql As String
    Dim cellsUpdated As Long

    On Error GoTo SheetError
    Set ws = ThisWorkbook.Sheets("RIN Generation")

    sql = "SELECT year, month, " & _
          "    d3_rins, d4_rins, d5_rins, d6_rins, d7_rins, " & _
          "    d4_biodiesel_rins, d4_rd_rins, d4_other_rins, " & _
          "    physical_vol_total, physical_vol_biodiesel, physical_vol_rd " & _
          "FROM gold.rin_generation " & _
          "WHERE 1=1 " & _
          BuildMonthLimitClause(monthCount, "gold.rin_generation") & _
          "ORDER BY year, month"

    Set rs = CreateObject("ADODB.Recordset")
    rs.Open sql, conn

    If rs.EOF Then
        rs.Close
        UpdateRINGeneration = 0
        Exit Function
    End If

    cellsUpdated = 0

    Do While Not rs.EOF
        Dim targetRow As Integer
        targetRow = FindRowForDate(ws, CInt(rs("year")), CInt(rs("month")), RIN_DATA_START)

        If targetRow > 0 Then
            ' Col B=D3, C=D4, D=D5, E=D6, F=D7
            ' Col G = Total (formula, skip)
            ' Col H=D4 Biodiesel, I=D4 RD, J=D4 Other
            ' Col K=Physical Vol Total, L=Biodiesel, M=RD
            ' Col N=YoY Total (formula), O=YoY D4 (formula), P=YoY D6 (formula)
            If Not IsNull(rs("d3_rins")) Then ws.Cells(targetRow, 2).Value = CDbl(rs("d3_rins")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("d4_rins")) Then ws.Cells(targetRow, 3).Value = CDbl(rs("d4_rins")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("d5_rins")) Then ws.Cells(targetRow, 4).Value = CDbl(rs("d5_rins")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("d6_rins")) Then ws.Cells(targetRow, 5).Value = CDbl(rs("d6_rins")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("d7_rins")) Then ws.Cells(targetRow, 6).Value = CDbl(rs("d7_rins")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("d4_biodiesel_rins")) Then ws.Cells(targetRow, 8).Value = CDbl(rs("d4_biodiesel_rins")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("d4_rd_rins")) Then ws.Cells(targetRow, 9).Value = CDbl(rs("d4_rd_rins")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("d4_other_rins")) Then ws.Cells(targetRow, 10).Value = CDbl(rs("d4_other_rins")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("physical_vol_total")) Then ws.Cells(targetRow, 11).Value = CDbl(rs("physical_vol_total")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("physical_vol_biodiesel")) Then ws.Cells(targetRow, 12).Value = CDbl(rs("physical_vol_biodiesel")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("physical_vol_rd")) Then ws.Cells(targetRow, 13).Value = CDbl(rs("physical_vol_rd")): cellsUpdated = cellsUpdated + 1
        End If

        rs.MoveNext
    Loop

    rs.Close
    UpdateRINGeneration = cellsUpdated
    Exit Function

SheetError:
    Debug.Print "RIN Generation error: " & Err.Description
    UpdateRINGeneration = 0
End Function

'--- Sheet 2: RIN Separation & Available ---
Private Function UpdateRINSeparation(conn As Object, monthCount As Integer) As Long
    Dim ws As Worksheet
    Dim rs As Object
    Dim sql As String
    Dim cellsUpdated As Long

    On Error GoTo SheetError
    Set ws = ThisWorkbook.Sheets("RIN Sep & Available")

    sql = "SELECT year, month, " & _
          "    sep_d3, sep_d4, sep_d5, sep_d6, sep_d7, " & _
          "    avail_d3, avail_d4, avail_d5, avail_d6, avail_d7 " & _
          "FROM gold.rin_separation " & _
          "WHERE 1=1 " & _
          BuildMonthLimitClause(monthCount, "gold.rin_separation") & _
          "ORDER BY year, month"

    Set rs = CreateObject("ADODB.Recordset")
    rs.Open sql, conn

    If rs.EOF Then
        rs.Close
        UpdateRINSeparation = 0
        Exit Function
    End If

    cellsUpdated = 0

    Do While Not rs.EOF
        Dim targetRow As Integer
        targetRow = FindRowForDate(ws, CInt(rs("year")), CInt(rs("month")), RIN_DATA_START)

        If targetRow > 0 Then
            ' Col B-F = Separation D3-D7
            ' Col G = Sep Total (formula, skip)
            ' Col H-L = Available D3-D7
            ' Col M = Avail Total (formula, skip)
            ' Col N = Sep/Gen Ratio (formula, skip)
            If Not IsNull(rs("sep_d3")) Then ws.Cells(targetRow, 2).Value = CDbl(rs("sep_d3")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("sep_d4")) Then ws.Cells(targetRow, 3).Value = CDbl(rs("sep_d4")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("sep_d5")) Then ws.Cells(targetRow, 4).Value = CDbl(rs("sep_d5")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("sep_d6")) Then ws.Cells(targetRow, 5).Value = CDbl(rs("sep_d6")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("sep_d7")) Then ws.Cells(targetRow, 6).Value = CDbl(rs("sep_d7")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("avail_d3")) Then ws.Cells(targetRow, 8).Value = CDbl(rs("avail_d3")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("avail_d4")) Then ws.Cells(targetRow, 9).Value = CDbl(rs("avail_d4")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("avail_d5")) Then ws.Cells(targetRow, 10).Value = CDbl(rs("avail_d5")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("avail_d6")) Then ws.Cells(targetRow, 11).Value = CDbl(rs("avail_d6")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("avail_d7")) Then ws.Cells(targetRow, 12).Value = CDbl(rs("avail_d7")): cellsUpdated = cellsUpdated + 1
        End If

        rs.MoveNext
    Loop

    rs.Close
    UpdateRINSeparation = cellsUpdated
    Exit Function

SheetError:
    Debug.Print "RIN Separation error: " & Err.Description
    UpdateRINSeparation = 0
End Function

'--- Sheet 3: RIN Retirement ---
Private Function UpdateRINRetirement(conn As Object, monthCount As Integer) As Long
    Dim ws As Worksheet
    Dim rs As Object
    Dim sql As String
    Dim cellsUpdated As Long

    On Error GoTo SheetError
    Set ws = ThisWorkbook.Sheets("RIN Retirement")

    sql = "SELECT year, month, " & _
          "    ret_d3, ret_d4, ret_d5, ret_d6, ret_d7, " & _
          "    rvo_target " & _
          "FROM gold.rin_retirement " & _
          "WHERE 1=1 " & _
          BuildMonthLimitClause(monthCount, "gold.rin_retirement") & _
          "ORDER BY year, month"

    Set rs = CreateObject("ADODB.Recordset")
    rs.Open sql, conn

    If rs.EOF Then
        rs.Close
        UpdateRINRetirement = 0
        Exit Function
    End If

    cellsUpdated = 0

    Do While Not rs.EOF
        Dim targetRow As Integer
        targetRow = FindRowForDate(ws, CInt(rs("year")), CInt(rs("month")), RIN_DATA_START)

        If targetRow > 0 Then
            ' Col B-F = Retirement D3-D7
            ' Col G = Ret Total (formula, skip)
            ' Col H-L = Balance D3-D7 (formula — Available minus Retired)
            ' Col M = Bal Total (formula)
            ' Col N = Compliance Year (formula)
            ' Col O = RVO Target
            If Not IsNull(rs("ret_d3")) Then ws.Cells(targetRow, 2).Value = CDbl(rs("ret_d3")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("ret_d4")) Then ws.Cells(targetRow, 3).Value = CDbl(rs("ret_d4")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("ret_d5")) Then ws.Cells(targetRow, 4).Value = CDbl(rs("ret_d5")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("ret_d6")) Then ws.Cells(targetRow, 5).Value = CDbl(rs("ret_d6")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("ret_d7")) Then ws.Cells(targetRow, 6).Value = CDbl(rs("ret_d7")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("rvo_target")) Then ws.Cells(targetRow, 15).Value = CDbl(rs("rvo_target")): cellsUpdated = cellsUpdated + 1
        End If

        rs.MoveNext
    Loop

    rs.Close
    UpdateRINRetirement = cellsUpdated
    Exit Function

SheetError:
    Debug.Print "RIN Retirement error: " & Err.Description
    UpdateRINRetirement = 0
End Function

'--- Sheet 4: Feedstock Consumption ---
Private Function UpdateFeedstockConsumption(conn As Object, monthCount As Integer) As Long
    Dim ws As Worksheet
    Dim rs As Object
    Dim sql As String
    Dim cellsUpdated As Long

    On Error GoTo SheetError
    Set ws = ThisWorkbook.Sheets("Feedstock Consumption")

    sql = "SELECT year, month, " & _
          "    soybean_oil, corn_oil, canola_oil, palm_oil, other_veg_oil, " & _
          "    tallow, poultry_fat, white_grease, yellow_grease_uco, other_animal, " & _
          "    distillers_corn_oil, tall_oil, waste_oils_fats, other_biomass, " & _
          "    total_biodiesel_plants, total_rd_plants " & _
          "FROM gold.eia_feedstock " & _
          "WHERE 1=1 " & _
          BuildMonthLimitClause(monthCount, "gold.eia_feedstock") & _
          "ORDER BY year, month"

    Set rs = CreateObject("ADODB.Recordset")
    rs.Open sql, conn

    If rs.EOF Then
        rs.Close
        UpdateFeedstockConsumption = 0
        Exit Function
    End If

    cellsUpdated = 0

    Do While Not rs.EOF
        Dim targetRow As Integer
        targetRow = FindRowForDate(ws, CInt(rs("year")), CInt(rs("month")), FEED_DATA_START)

        If targetRow > 0 Then
            ' Vegetable Oils: B=SBO, C=Corn, D=Canola, E=Palm, F=Other Veg
            ' Animal Fats: G=Tallow, H=Poultry, I=White Grease, J=UCO/YG, K=Other Animal
            ' Other: L=DCO, M=Tall Oil, N=Waste Oils, O=Other Biomass
            ' Totals: P=Biodiesel Plants, Q=RD Plants
            ' R=All Plants Total (formula, skip), S=YoY (formula, skip)
            If Not IsNull(rs("soybean_oil")) Then ws.Cells(targetRow, 2).Value = CDbl(rs("soybean_oil")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("corn_oil")) Then ws.Cells(targetRow, 3).Value = CDbl(rs("corn_oil")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("canola_oil")) Then ws.Cells(targetRow, 4).Value = CDbl(rs("canola_oil")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("palm_oil")) Then ws.Cells(targetRow, 5).Value = CDbl(rs("palm_oil")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("other_veg_oil")) Then ws.Cells(targetRow, 6).Value = CDbl(rs("other_veg_oil")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("tallow")) Then ws.Cells(targetRow, 7).Value = CDbl(rs("tallow")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("poultry_fat")) Then ws.Cells(targetRow, 8).Value = CDbl(rs("poultry_fat")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("white_grease")) Then ws.Cells(targetRow, 9).Value = CDbl(rs("white_grease")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("yellow_grease_uco")) Then ws.Cells(targetRow, 10).Value = CDbl(rs("yellow_grease_uco")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("other_animal")) Then ws.Cells(targetRow, 11).Value = CDbl(rs("other_animal")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("distillers_corn_oil")) Then ws.Cells(targetRow, 12).Value = CDbl(rs("distillers_corn_oil")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("tall_oil")) Then ws.Cells(targetRow, 13).Value = CDbl(rs("tall_oil")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("waste_oils_fats")) Then ws.Cells(targetRow, 14).Value = CDbl(rs("waste_oils_fats")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("other_biomass")) Then ws.Cells(targetRow, 15).Value = CDbl(rs("other_biomass")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("total_biodiesel_plants")) Then ws.Cells(targetRow, 16).Value = CDbl(rs("total_biodiesel_plants")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("total_rd_plants")) Then ws.Cells(targetRow, 17).Value = CDbl(rs("total_rd_plants")): cellsUpdated = cellsUpdated + 1
        End If

        rs.MoveNext
    Loop

    rs.Close
    UpdateFeedstockConsumption = cellsUpdated
    Exit Function

SheetError:
    Debug.Print "Feedstock Consumption error: " & Err.Description
    UpdateFeedstockConsumption = 0
End Function

'--- Sheet 5: Feedstock by Plant Type ---
Private Function UpdateFeedstockByPlantType(conn As Object, monthCount As Integer) As Long
    Dim ws As Worksheet
    Dim rs As Object
    Dim sql As String
    Dim cellsUpdated As Long

    On Error GoTo SheetError
    Set ws = ThisWorkbook.Sheets("Feedstock by Plant Type")

    sql = "SELECT year, month, " & _
          "    sbo_biodiesel, sbo_rd, " & _
          "    corn_oil_biodiesel, corn_oil_rd, " & _
          "    tallow_biodiesel, tallow_rd, " & _
          "    uco_biodiesel, uco_rd, " & _
          "    all_feeds_biodiesel, all_feeds_rd " & _
          "FROM gold.eia_feedstock_plant_type " & _
          "WHERE 1=1 " & _
          BuildMonthLimitClause(monthCount, "gold.eia_feedstock_plant_type") & _
          "ORDER BY year, month"

    Set rs = CreateObject("ADODB.Recordset")
    rs.Open sql, conn

    If rs.EOF Then
        rs.Close
        UpdateFeedstockByPlantType = 0
        Exit Function
    End If

    cellsUpdated = 0

    Do While Not rs.EOF
        Dim targetRow As Integer
        targetRow = FindRowForDate(ws, CInt(rs("year")), CInt(rs("month")), STD_DATA_START)

        If targetRow > 0 Then
            ' SBO: B=Biodiesel, C=RD, D=Total (formula)
            ' Corn Oil: E=Biodiesel, F=RD, G=Total (formula)
            ' Tallow: H=Biodiesel, I=RD, J=Total (formula)
            ' UCO/YG: K=Biodiesel, L=RD, M=Total (formula)
            ' All Feeds: N=Biodiesel, O=RD, P=Total (formula)
            If Not IsNull(rs("sbo_biodiesel")) Then ws.Cells(targetRow, 2).Value = CDbl(rs("sbo_biodiesel")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("sbo_rd")) Then ws.Cells(targetRow, 3).Value = CDbl(rs("sbo_rd")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("corn_oil_biodiesel")) Then ws.Cells(targetRow, 5).Value = CDbl(rs("corn_oil_biodiesel")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("corn_oil_rd")) Then ws.Cells(targetRow, 6).Value = CDbl(rs("corn_oil_rd")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("tallow_biodiesel")) Then ws.Cells(targetRow, 8).Value = CDbl(rs("tallow_biodiesel")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("tallow_rd")) Then ws.Cells(targetRow, 9).Value = CDbl(rs("tallow_rd")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("uco_biodiesel")) Then ws.Cells(targetRow, 11).Value = CDbl(rs("uco_biodiesel")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("uco_rd")) Then ws.Cells(targetRow, 12).Value = CDbl(rs("uco_rd")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("all_feeds_biodiesel")) Then ws.Cells(targetRow, 14).Value = CDbl(rs("all_feeds_biodiesel")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("all_feeds_rd")) Then ws.Cells(targetRow, 15).Value = CDbl(rs("all_feeds_rd")): cellsUpdated = cellsUpdated + 1
        End If

        rs.MoveNext
    Loop

    rs.Close
    UpdateFeedstockByPlantType = cellsUpdated
    Exit Function

SheetError:
    Debug.Print "Feedstock Plant Type error: " & Err.Description
    UpdateFeedstockByPlantType = 0
End Function

'--- Sheet 6: Biofuel Production ---
Private Function UpdateBiofuelProduction(conn As Object, monthCount As Integer) As Long
    Dim ws As Worksheet
    Dim rs As Object
    Dim sql As String
    Dim cellsUpdated As Long

    On Error GoTo SheetError
    Set ws = ThisWorkbook.Sheets("Biofuel Production")

    sql = "SELECT year, month, " & _
          "    ethanol_prod_kbd, " & _
          "    biodiesel_prod_kbd, " & _
          "    rd_prod_kbd, " & _
          "    other_biofuel_prod_kbd, " & _
          "    ethanol_stocks_kbbl, " & _
          "    biodiesel_stocks_kbbl, " & _
          "    rd_stocks_kbbl " & _
          "FROM gold.eia_biofuel_production " & _
          "WHERE 1=1 " & _
          BuildMonthLimitClause(monthCount, "gold.eia_biofuel_production") & _
          "ORDER BY year, month"

    Set rs = CreateObject("ADODB.Recordset")
    rs.Open sql, conn

    If rs.EOF Then
        rs.Close
        UpdateBiofuelProduction = 0
        Exit Function
    End If

    cellsUpdated = 0

    Do While Not rs.EOF
        Dim targetRow As Integer
        targetRow = FindRowForDate(ws, CInt(rs("year")), CInt(rs("month")), STD_DATA_START)

        If targetRow > 0 Then
            ' B=Ethanol kbd, C=Ethanol mil gal (formula)
            ' D=Biodiesel kbd, E=BD mil gal (formula)
            ' F=RD kbd, G=RD mil gal (formula)
            ' H=Other kbd, I=Other mil gal (formula)
            ' J=Total Biofuel kbd (formula)
            ' K=Ethanol Stocks, L=BD Stocks, M=RD Stocks
            ' N=Ethanol YoY (formula), O=BBD YoY (formula)
            If Not IsNull(rs("ethanol_prod_kbd")) Then ws.Cells(targetRow, 2).Value = CDbl(rs("ethanol_prod_kbd")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("biodiesel_prod_kbd")) Then ws.Cells(targetRow, 4).Value = CDbl(rs("biodiesel_prod_kbd")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("rd_prod_kbd")) Then ws.Cells(targetRow, 6).Value = CDbl(rs("rd_prod_kbd")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("other_biofuel_prod_kbd")) Then ws.Cells(targetRow, 8).Value = CDbl(rs("other_biofuel_prod_kbd")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("ethanol_stocks_kbbl")) Then ws.Cells(targetRow, 11).Value = CDbl(rs("ethanol_stocks_kbbl")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("biodiesel_stocks_kbbl")) Then ws.Cells(targetRow, 12).Value = CDbl(rs("biodiesel_stocks_kbbl")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("rd_stocks_kbbl")) Then ws.Cells(targetRow, 13).Value = CDbl(rs("rd_stocks_kbbl")): cellsUpdated = cellsUpdated + 1
        End If

        rs.MoveNext
    Loop

    rs.Close
    UpdateBiofuelProduction = cellsUpdated
    Exit Function

SheetError:
    Debug.Print "Biofuel Production error: " & Err.Description
    UpdateBiofuelProduction = 0
End Function

'--- Sheet 7: Biofuel Trade ---
Private Function UpdateBiofuelTrade(conn As Object, monthCount As Integer) As Long
    Dim ws As Worksheet
    Dim rs As Object
    Dim sql As String
    Dim cellsUpdated As Long

    On Error GoTo SheetError
    Set ws = ThisWorkbook.Sheets("Biofuel Trade")

    sql = "SELECT year, month, " & _
          "    ethanol_imports, ethanol_exports, " & _
          "    biodiesel_imports, biodiesel_exports, " & _
          "    rd_imports, rd_exports " & _
          "FROM gold.eia_biofuel_trade " & _
          "WHERE 1=1 " & _
          BuildMonthLimitClause(monthCount, "gold.eia_biofuel_trade") & _
          "ORDER BY year, month"

    Set rs = CreateObject("ADODB.Recordset")
    rs.Open sql, conn

    If rs.EOF Then
        rs.Close
        UpdateBiofuelTrade = 0
        Exit Function
    End If

    cellsUpdated = 0

    Do While Not rs.EOF
        Dim targetRow As Integer
        targetRow = FindRowForDate(ws, CInt(rs("year")), CInt(rs("month")), STD_DATA_START)

        If targetRow > 0 Then
            ' B=Ethanol Imports, C=Ethanol Exports, D=Ethanol Net (formula)
            ' E=BD Imports, F=BD Exports, G=BD Net (formula)
            ' H=RD Imports, I=RD Exports, J=RD Net (formula)
            ' K=Total Net Imports (formula)
            If Not IsNull(rs("ethanol_imports")) Then ws.Cells(targetRow, 2).Value = CDbl(rs("ethanol_imports")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("ethanol_exports")) Then ws.Cells(targetRow, 3).Value = CDbl(rs("ethanol_exports")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("biodiesel_imports")) Then ws.Cells(targetRow, 5).Value = CDbl(rs("biodiesel_imports")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("biodiesel_exports")) Then ws.Cells(targetRow, 6).Value = CDbl(rs("biodiesel_exports")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("rd_imports")) Then ws.Cells(targetRow, 8).Value = CDbl(rs("rd_imports")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("rd_exports")) Then ws.Cells(targetRow, 9).Value = CDbl(rs("rd_exports")): cellsUpdated = cellsUpdated + 1
        End If

        rs.MoveNext
    Loop

    rs.Close
    UpdateBiofuelTrade = cellsUpdated
    Exit Function

SheetError:
    Debug.Print "Biofuel Trade error: " & Err.Description
    UpdateBiofuelTrade = 0
End Function

'--- Sheet 8: Capacity (plant-level, not time-series) ---
Private Function UpdateCapacity(conn As Object) As Long
    Dim ws As Worksheet
    Dim rs As Object
    Dim sql As String
    Dim cellsUpdated As Long

    On Error GoTo SheetError
    Set ws = ThisWorkbook.Sheets("Capacity")

    ' Clear existing data below header
    Dim lastRow As Integer
    lastRow = ws.Cells(ws.Rows.Count, 1).End(xlUp).row
    If lastRow >= CAP_DATA_START Then
        ws.Range(ws.Cells(CAP_DATA_START, 1), ws.Cells(lastRow, 10)).ClearContents
    End If

    sql = "SELECT " & _
          "    company, plant_location, state, padd, fuel_type, " & _
          "    nameplate_capacity_mmgy, operable_status, year_online, " & _
          "    feedstock_primary, notes " & _
          "FROM gold.eia_biofuel_capacity " & _
          "ORDER BY fuel_type, state, company"

    Set rs = CreateObject("ADODB.Recordset")
    rs.Open sql, conn

    If rs.EOF Then
        rs.Close
        UpdateCapacity = 0
        Exit Function
    End If

    cellsUpdated = 0
    Dim r As Integer
    r = CAP_DATA_START

    Do While Not rs.EOF
        Dim c As Integer
        For c = 0 To rs.Fields.Count - 1
            If Not IsNull(rs.Fields(c).Value) Then
                ws.Cells(r, c + 1).Value = rs.Fields(c).Value
                cellsUpdated = cellsUpdated + 1
            End If
        Next c
        r = r + 1
        rs.MoveNext
    Loop

    rs.Close

    ' Reapply autofilter
    If r > CAP_DATA_START Then
        On Error Resume Next
        ws.AutoFilterMode = False
        ws.Range("A" & CAP_HEADER_ROW & ":J" & r - 1).AutoFilter
        On Error GoTo 0
    End If

    UpdateCapacity = cellsUpdated
    Exit Function

SheetError:
    Debug.Print "Capacity error: " & Err.Description
    UpdateCapacity = 0
End Function

'--- Sheet 9: Blending Context ---
Private Function UpdateBlendingContext(conn As Object, monthCount As Integer) As Long
    Dim ws As Worksheet
    Dim rs As Object
    Dim sql As String
    Dim cellsUpdated As Long

    On Error GoTo SheetError
    Set ws = ThisWorkbook.Sheets("Blending Context")

    sql = "SELECT year, month, " & _
          "    gasoline_prod_kbd, gasoline_demand_kbd, gasoline_stocks_kbbl, " & _
          "    diesel_prod_kbd, diesel_demand_kbd, diesel_stocks_kbbl " & _
          "FROM gold.eia_blending_context " & _
          "WHERE 1=1 " & _
          BuildMonthLimitClause(monthCount, "gold.eia_blending_context") & _
          "ORDER BY year, month"

    Set rs = CreateObject("ADODB.Recordset")
    rs.Open sql, conn

    If rs.EOF Then
        rs.Close
        UpdateBlendingContext = 0
        Exit Function
    End If

    cellsUpdated = 0

    Do While Not rs.EOF
        Dim targetRow As Integer
        targetRow = FindRowForDate(ws, CInt(rs("year")), CInt(rs("month")), STD_DATA_START)

        If targetRow > 0 Then
            ' B=Gas Prod, C=Gas Demand, D=Gas Stocks
            ' E=Ethanol Blend Rate (formula, skip)
            ' F=Diesel Prod, G=Diesel Demand, H=Diesel Stocks
            ' I=BBD Blend Rate (formula, skip)
            If Not IsNull(rs("gasoline_prod_kbd")) Then ws.Cells(targetRow, 2).Value = CDbl(rs("gasoline_prod_kbd")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("gasoline_demand_kbd")) Then ws.Cells(targetRow, 3).Value = CDbl(rs("gasoline_demand_kbd")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("gasoline_stocks_kbbl")) Then ws.Cells(targetRow, 4).Value = CDbl(rs("gasoline_stocks_kbbl")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("diesel_prod_kbd")) Then ws.Cells(targetRow, 6).Value = CDbl(rs("diesel_prod_kbd")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("diesel_demand_kbd")) Then ws.Cells(targetRow, 7).Value = CDbl(rs("diesel_demand_kbd")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("diesel_stocks_kbbl")) Then ws.Cells(targetRow, 8).Value = CDbl(rs("diesel_stocks_kbbl")): cellsUpdated = cellsUpdated + 1
        End If

        rs.MoveNext
    Loop

    rs.Close
    UpdateBlendingContext = cellsUpdated
    Exit Function

SheetError:
    Debug.Print "Blending Context error: " & Err.Description
    UpdateBlendingContext = 0
End Function

' =============================================================================
' KEYBOARD SHORTCUTS
' =============================================================================

Public Sub AssignBiofuelShortcuts()
    ' Assign Ctrl+B and Ctrl+Shift+B shortcuts

    Application.OnKey "^b", "UpdateBiofuelData"
    Application.OnKey "^+b", "UpdateBiofuelDataCustom"

    MsgBox "Keyboard shortcuts assigned:" & vbCrLf & vbCrLf & _
           "Ctrl+B = Quick update (latest 6 months)" & vbCrLf & _
           "Ctrl+Shift+B = Custom month count", vbInformation, "Biofuel Updater"
End Sub

Public Sub RemoveBiofuelShortcuts()
    Application.OnKey "^b"
    Application.OnKey "^+b"
End Sub
