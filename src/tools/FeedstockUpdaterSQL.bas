' =============================================================================
' FeedstockUpdaterSQL - Pure VBA with PostgreSQL Connection
' =============================================================================
' Connects directly to PostgreSQL database to update EIA feedstock/energy data.
' No Python required. No closing/reopening workbook.
'
' Target: us_feedstock_data.xlsm (2 sheets now, expandable later)
' Sheets:
'   1. "Ethanol Weekly"    - Weekly ethanol production, stocks, blending
'   2. "Petroleum Weekly"  - Weekly petroleum context (crude, gasoline, diesel)
'
' Future sheets (when EIA feedstock-by-type data is collected):
'   3. "Feedstock Inputs"  - Monthly feedstock consumption by oil/fat type
'   4. "Biofuel Production"- Monthly biodiesel/RD production & stocks
'
' Layout: Row 3 = headers, Row 4 = units, Data starts Row 5
'         Column A = dates (week-ending date for weekly sheets)
'
' Data sources:
'   gold.eia_ethanol_weekly   - Weekly ethanol data (pre-existing)
'   gold.eia_petroleum_weekly - Weekly petroleum data (pre-existing)
'
' Key similarity to CrushUpdaterSQL:
'   Both are rows = time periods, columns = data attributes
'   This one uses week-ending dates instead of 1st-of-month dates
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

' Spreadsheet structure
Private Const DATE_COLUMN As Integer = 1      ' Column A = dates
Private Const HEADER_ROW As Integer = 3       ' Row with attribute headers
Private Const UNIT_ROW As Integer = 4         ' Row with units
Private Const DATA_START_ROW As Integer = 5   ' First data row

' =============================================================================
' MAIN UPDATE FUNCTIONS
' =============================================================================

Public Sub UpdateFeedstockData()
    ' Quick update - latest 26 weeks (~6 months)
    ' Keyboard shortcut: Ctrl+E

    Dim result As VbMsgBoxResult
    result = MsgBox("Update all EIA sheets with the latest 26 weeks of data?", _
                    vbYesNo + vbQuestion, "EIA Feedstock Updater")

    If result = vbYes Then
        UpdateFromDatabase 26
    End If
End Sub

Public Sub UpdateFeedstockDataCustom()
    ' Custom update with user-specified weeks
    ' Keyboard shortcut: Ctrl+Shift+E

    Dim weekCount As String
    weekCount = InputBox("How many weeks of data to update?" & vbCrLf & vbCrLf & _
                         "Enter a number (e.g., 52 for last year)" & vbCrLf & _
                         "Or enter 0 to update ALL available data", _
                         "EIA Feedstock Updater", "26")

    If weekCount = "" Then Exit Sub

    If Not IsNumeric(weekCount) Then
        MsgBox "Please enter a valid number.", vbExclamation, "EIA Feedstock Updater"
        Exit Sub
    End If

    UpdateFromDatabase CInt(weekCount)
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

Private Sub UpdateFromDatabase(weekCount As Integer)
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

    ' Sheet 1: Ethanol Weekly
    Application.StatusBar = "Updating Ethanol Weekly..."
    DoEvents
    totalCells = totalCells + UpdateEthanolWeekly(conn, weekCount)
    sheetCount = sheetCount + 1

    ' Sheet 2: Petroleum Weekly
    Application.StatusBar = "Updating Petroleum Weekly..."
    DoEvents
    totalCells = totalCells + UpdatePetroleumWeekly(conn, weekCount)
    sheetCount = sheetCount + 1

    On Error GoTo 0

    conn.Close

    ' Reset status
    Application.StatusBar = False
    Application.Cursor = xlDefault

    ' Report results
    Dim weekLabel As String
    If weekCount = 0 Then
        weekLabel = "all available"
    Else
        weekLabel = "latest " & weekCount & " weeks"
    End If

    MsgBox "Update complete!" & vbCrLf & vbCrLf & _
           "Sheets updated: " & sheetCount & vbCrLf & _
           "Cells updated: " & totalCells & vbCrLf & _
           "Period: " & weekLabel, vbInformation, "EIA Feedstock Updater"
End Sub

' =============================================================================
' HELPER FUNCTIONS
' =============================================================================

Private Function FindRowForExactDate(ws As Worksheet, targetDate As Date) As Integer
    ' Find row number for an exact date match by scanning column A
    ' Used for weekly data where dates must match exactly

    Dim row As Integer
    Dim cellVal As Variant
    Dim lastRow As Integer

    ' Find last row with data in column A
    lastRow = ws.Cells(ws.Rows.Count, DATE_COLUMN).End(xlUp).row

    For row = DATA_START_ROW To lastRow
        cellVal = ws.Cells(row, DATE_COLUMN).Value

        If IsDate(cellVal) Then
            If CDate(cellVal) = targetDate Then
                FindRowForExactDate = row
                Exit Function
            End If
        ElseIf IsNumeric(cellVal) And cellVal > 0 Then
            ' Excel date serial number
            On Error Resume Next
            If CDate(cellVal) = targetDate Then
                FindRowForExactDate = row
                On Error GoTo 0
                Exit Function
            End If
            On Error GoTo 0
        End If
    Next row

    FindRowForExactDate = 0  ' Not found
End Function

Private Function BuildWeekLimitClause(weekCount As Integer, _
                                       dateColumn As String) As String
    ' Build SQL WHERE clause to limit to the latest N weeks
    ' weekCount=0 means no limit (all data)

    If weekCount > 0 Then
        BuildWeekLimitClause = _
            "WHERE " & dateColumn & " >= ( " & _
            "    SELECT MAX(" & dateColumn & ") - INTERVAL '" & weekCount & " weeks' " & _
            "    FROM gold.eia_ethanol_weekly " & _
            ") "
    Else
        BuildWeekLimitClause = ""
    End If
End Function

' =============================================================================
' SHEET UPDATERS
' Each returns the number of cells updated
' =============================================================================

'--- Sheet 1: Ethanol Weekly ---
Private Function UpdateEthanolWeekly(conn As Object, weekCount As Integer) As Long
    ' Source: gold.eia_ethanol_weekly
    ' Layout: Col A=Week Ending Date
    '   B=Production (kbd), C=Stocks (kb), D=Blender Input (kbd), E=Balance (kbd)

    Dim ws As Worksheet
    Dim rs As Object
    Dim sql As String
    Dim cellsUpdated As Long

    On Error GoTo SheetError
    Set ws = ThisWorkbook.Sheets("Ethanol Weekly")

    ' Build query with optional week limit
    If weekCount > 0 Then
        sql = "SELECT week_ending, " & _
              "    ethanol_production_kbd, ethanol_stocks_kb, " & _
              "    ethanol_blender_input_kbd, ethanol_balance_kbd " & _
              "FROM gold.eia_ethanol_weekly " & _
              "WHERE week_ending >= ( " & _
              "    SELECT MAX(week_ending) - INTERVAL '" & weekCount & " weeks' " & _
              "    FROM gold.eia_ethanol_weekly " & _
              ") " & _
              "ORDER BY week_ending"
    Else
        sql = "SELECT week_ending, " & _
              "    ethanol_production_kbd, ethanol_stocks_kb, " & _
              "    ethanol_blender_input_kbd, ethanol_balance_kbd " & _
              "FROM gold.eia_ethanol_weekly " & _
              "ORDER BY week_ending"
    End If

    Set rs = CreateObject("ADODB.Recordset")
    rs.Open sql, conn

    If rs.EOF Then
        rs.Close
        UpdateEthanolWeekly = 0
        Exit Function
    End If

    cellsUpdated = 0

    Do While Not rs.EOF
        Dim targetDate As Date
        targetDate = CDate(rs("week_ending"))

        Dim targetRow As Integer
        targetRow = FindRowForExactDate(ws, targetDate)

        If targetRow > 0 Then
            ' B=Production kbd, C=Stocks kb, D=Blender Input kbd, E=Balance kbd
            If Not IsNull(rs("ethanol_production_kbd")) Then ws.Cells(targetRow, 2).Value = CDbl(rs("ethanol_production_kbd")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("ethanol_stocks_kb")) Then ws.Cells(targetRow, 3).Value = CDbl(rs("ethanol_stocks_kb")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("ethanol_blender_input_kbd")) Then ws.Cells(targetRow, 4).Value = CDbl(rs("ethanol_blender_input_kbd")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("ethanol_balance_kbd")) Then ws.Cells(targetRow, 5).Value = CDbl(rs("ethanol_balance_kbd")): cellsUpdated = cellsUpdated + 1
        End If

        rs.MoveNext
    Loop

    rs.Close
    UpdateEthanolWeekly = cellsUpdated
    Exit Function

SheetError:
    Debug.Print "Ethanol Weekly error: " & Err.Description
    UpdateEthanolWeekly = 0
End Function

'--- Sheet 2: Petroleum Weekly ---
Private Function UpdatePetroleumWeekly(conn As Object, weekCount As Integer) As Long
    ' Source: gold.eia_petroleum_weekly
    ' Layout: Col A=Week Ending Date
    '   B=Crude Stocks (kb), C=Crude Stocks ex-SPR (kb), D=SPR Stocks (kb)
    '   E=Gasoline Stocks (kb), F=Distillate Stocks (kb)
    '   G=Crude Production (kbd), H=Crude Imports (kbd)
    '   I=Refinery Inputs (kbd), J=Refinery Util (%)
    '   K=Gasoline Days Supply

    Dim ws As Worksheet
    Dim rs As Object
    Dim sql As String
    Dim cellsUpdated As Long

    On Error GoTo SheetError
    Set ws = ThisWorkbook.Sheets("Petroleum Weekly")

    ' Build query with optional week limit
    If weekCount > 0 Then
        sql = "SELECT week_ending, " & _
              "    crude_stocks_total_kb, crude_stocks_ex_spr_kb, spr_stocks_kb, " & _
              "    gasoline_stocks_kb, distillate_stocks_kb, " & _
              "    crude_production_kbd, crude_imports_kbd, " & _
              "    refinery_inputs_kbd, refinery_utilization_pct, " & _
              "    gasoline_days_supply " & _
              "FROM gold.eia_petroleum_weekly " & _
              "WHERE week_ending >= ( " & _
              "    SELECT MAX(week_ending) - INTERVAL '" & weekCount & " weeks' " & _
              "    FROM gold.eia_petroleum_weekly " & _
              ") " & _
              "ORDER BY week_ending"
    Else
        sql = "SELECT week_ending, " & _
              "    crude_stocks_total_kb, crude_stocks_ex_spr_kb, spr_stocks_kb, " & _
              "    gasoline_stocks_kb, distillate_stocks_kb, " & _
              "    crude_production_kbd, crude_imports_kbd, " & _
              "    refinery_inputs_kbd, refinery_utilization_pct, " & _
              "    gasoline_days_supply " & _
              "FROM gold.eia_petroleum_weekly " & _
              "ORDER BY week_ending"
    End If

    Set rs = CreateObject("ADODB.Recordset")
    rs.Open sql, conn

    If rs.EOF Then
        rs.Close
        UpdatePetroleumWeekly = 0
        Exit Function
    End If

    cellsUpdated = 0

    Do While Not rs.EOF
        Dim targetDate As Date
        targetDate = CDate(rs("week_ending"))

        Dim targetRow As Integer
        targetRow = FindRowForExactDate(ws, targetDate)

        If targetRow > 0 Then
            ' Stocks: B=Crude Total, C=Crude ex-SPR, D=SPR, E=Gasoline, F=Distillate
            If Not IsNull(rs("crude_stocks_total_kb")) Then ws.Cells(targetRow, 2).Value = CDbl(rs("crude_stocks_total_kb")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("crude_stocks_ex_spr_kb")) Then ws.Cells(targetRow, 3).Value = CDbl(rs("crude_stocks_ex_spr_kb")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("spr_stocks_kb")) Then ws.Cells(targetRow, 4).Value = CDbl(rs("spr_stocks_kb")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("gasoline_stocks_kb")) Then ws.Cells(targetRow, 5).Value = CDbl(rs("gasoline_stocks_kb")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("distillate_stocks_kb")) Then ws.Cells(targetRow, 6).Value = CDbl(rs("distillate_stocks_kb")): cellsUpdated = cellsUpdated + 1
            ' Production: G=Crude Production, H=Crude Imports
            If Not IsNull(rs("crude_production_kbd")) Then ws.Cells(targetRow, 7).Value = CDbl(rs("crude_production_kbd")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("crude_imports_kbd")) Then ws.Cells(targetRow, 8).Value = CDbl(rs("crude_imports_kbd")): cellsUpdated = cellsUpdated + 1
            ' Refinery: I=Inputs, J=Utilization
            If Not IsNull(rs("refinery_inputs_kbd")) Then ws.Cells(targetRow, 9).Value = CDbl(rs("refinery_inputs_kbd")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("refinery_utilization_pct")) Then ws.Cells(targetRow, 10).Value = CDbl(rs("refinery_utilization_pct")): cellsUpdated = cellsUpdated + 1
            ' Gasoline: K=Days Supply
            If Not IsNull(rs("gasoline_days_supply")) Then ws.Cells(targetRow, 11).Value = CDbl(rs("gasoline_days_supply")): cellsUpdated = cellsUpdated + 1
        End If

        rs.MoveNext
    Loop

    rs.Close
    UpdatePetroleumWeekly = cellsUpdated
    Exit Function

SheetError:
    Debug.Print "Petroleum Weekly error: " & Err.Description
    UpdatePetroleumWeekly = 0
End Function

' =============================================================================
' KEYBOARD SHORTCUTS
' =============================================================================

Public Sub AssignFeedstockShortcuts()
    ' Assign Ctrl+E and Ctrl+Shift+E shortcuts

    Application.OnKey "^e", "UpdateFeedstockData"
    Application.OnKey "^+e", "UpdateFeedstockDataCustom"

    MsgBox "Keyboard shortcuts assigned:" & vbCrLf & vbCrLf & _
           "Ctrl+E = Quick update (latest 26 weeks)" & vbCrLf & _
           "Ctrl+Shift+E = Custom week count", vbInformation, "EIA Feedstock Updater"
End Sub

Public Sub RemoveFeedstockShortcuts()
    Application.OnKey "^e"
    Application.OnKey "^+e"
End Sub
