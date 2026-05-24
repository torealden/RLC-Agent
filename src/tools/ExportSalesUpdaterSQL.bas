Attribute VB_Name = "ExportSalesUpdaterSQL"
' =============================================================================
' ExportSalesUpdaterSQL - FAS ESR Weekly Export Sales VBA Updater
' =============================================================================
' Connects directly to PostgreSQL and fills any of the four ESR tab types in
' us_soy_complex_trade.xlsm (and future sibling workbooks):
'
'   Sales       (net sales for the current marketing year)
'   Shipments   (weekly exports for the current marketing year)
'   Commits     (sales + shipments per cell — DB-computed via the view)
'   NMY Sales   (net sales filtered to NEXT marketing year)
'
' Sheet name parsing:
'   Commodity:  "Soybean" / "Soy " -> soybeans
'               "Meal"             -> soybean_meal
'               "SBO" / "Oil"      -> soybean_oil
'   Flow:       "Sales" (no NMY)   -> sales
'               "Shipments"        -> shipments
'               "Commits"          -> commitments
'               "NMY"              -> nmy_sales
'
' Keyboard shortcuts:
'   Ctrl+E       = Quick update (latest 12 weeks)
'   Ctrl+Shift+E = Custom week count
'
' Data source: gold.export_sales_matrix (see migration 107).
' =============================================================================

Option Explicit

Private Const DB_SERVER As String = "rlc-commodities.c16c6wm826t7.us-east-2.rds.amazonaws.com"
Private Const DB_PORT As String = "5432"
Private Const DB_NAME As String = "rlc_commodities"
Private Const DB_USER As String = "postgres"
Private Const DB_PASSWORD As String = "SoupBoss1"

' Spreadsheet structure — matches the Census-trade tab layout the ESR
' tabs were cloned from. Row 2 = week_ending date headers; data rows
' 4..218 (row 4 starts the country block, 217 = WORLD TOTAL,
' 218 = UNKNOWN — the FAS UNKNOWN-destination bucket that's needed for
' the world total to balance).
Private Const HEADER_ROW As Integer = 2
Private Const DATA_START_ROW As Integer = 4
Private Const DATA_END_ROW As Integer = 218
Private Const COUNTRY_COLUMN As Integer = 1

' Regional subtotal rows — same convention as TradeUpdaterSQL / InspectionsUpdaterSQL.
Private Const REGIONAL_ROWS As String = "4,33,47,61,108,165,216"

' =============================================================================
' MAIN UPDATE FUNCTIONS
' =============================================================================

Public Sub UpdateExportSalesData()
    ' Quick update — latest 12 weeks (~3 months)
    ' Keyboard shortcut: Ctrl+E
    Dim result As VbMsgBoxResult
    result = MsgBox("Update " & ActiveSheet.Name & " with the latest 12 weeks of ESR data?", _
                    vbYesNo + vbQuestion, "Export Sales Updater")
    If result = vbYes Then UpdateFromDatabase 12
End Sub

Public Sub UpdateExportSalesDataCustom()
    ' Custom update with user-specified week count
    ' Keyboard shortcut: Ctrl+Shift+E
    Dim weekStr As String
    weekStr = InputBox("How many weeks of ESR data to update?" & vbCrLf & vbCrLf & _
                       "Enter a number (e.g., 26 for half a marketing year)", _
                       "Export Sales Updater", "26")
    If weekStr = "" Or Not IsNumeric(weekStr) Then Exit Sub
    UpdateFromDatabase CInt(weekStr)
End Sub

' =============================================================================
' CORE UPDATE LOGIC
' =============================================================================

Private Sub UpdateFromDatabase(weekCount As Integer)
    Dim conn As Object, rs As Object, ws As Worksheet
    Dim commodity As String, flow As String
    Dim sql As String
    Dim cellsUpdated As Long, countriesNotFound As Long

    Application.StatusBar = "Connecting to database..."
    Application.Cursor = xlWait
    DoEvents

    Set conn = GetConnection()
    If conn Is Nothing Then
        Application.StatusBar = False
        Application.Cursor = xlDefault
        Exit Sub
    End If

    Set ws = ActiveSheet
    GetCommodityAndFlow ws.Name, commodity, flow

    If commodity = "UNKNOWN" Or flow = "UNKNOWN" Then
        MsgBox "Could not determine commodity/flow from sheet name: " & ws.Name & vbCrLf & vbCrLf & _
               "Expected names like 'Weekly Soybean Export Sales', 'Weekly Meal Export Shipments', " & _
               "'Weekly SBO Export Commits', 'Weekly Meal Export NMY Sales'.", _
               vbExclamation, "Export Sales Updater"
        conn.Close
        Application.StatusBar = False : Application.Cursor = xlDefault
        Exit Sub
    End If

    Application.StatusBar = "Fetching " & commodity & " " & flow & "..."
    DoEvents

    ' MY filter: 'sales' and 'shipments' and 'commitments' use the CURRENT marketing year,
    ' 'nmy_sales' uses NEXT marketing year. Current MY = max MY in matrix for commodity.
    Dim valueColumn As String, myFilter As String
    Select Case flow
        Case "sales":       valueColumn = "sales"
        Case "shipments":   valueColumn = "shipments"
        Case "commitments": valueColumn = "commitments"
        Case "nmy_sales":   valueColumn = "nmy_sales"
    End Select

    ' MY logic:
    '   Current MY = the marketing_year associated with the latest week_ending for
    '     this commodity. When a week has both current+NMY rows (forward sales
    '     booked), pick the smaller one.
    '   NMY = current_MY + 1.
    ' Bronze stores MY as starting calendar year (e.g., 2024 = MY 2024/25).
    Dim currentMyExpr As String
    currentMyExpr = "(SELECT MIN(marketing_year) FROM gold.export_sales_matrix " & _
                    "WHERE commodity = '" & commodity & "' AND week_ending = " & _
                    "(SELECT MAX(week_ending) FROM gold.export_sales_matrix WHERE commodity = '" & commodity & "'))"

    If flow = "nmy_sales" Then
        myFilter = "marketing_year = " & currentMyExpr & " + 1"
    Else
        myFilter = "marketing_year = " & currentMyExpr
    End If

    ' Query latest N weeks for this commodity x MY x column
    sql = "SELECT week_ending, country_name, spreadsheet_row, " & valueColumn & " AS qty " & _
          "FROM gold.export_sales_matrix " & _
          "WHERE commodity = '" & commodity & "' " & _
          "  AND " & myFilter & " " & _
          "  AND (is_regional_total = FALSE OR country_name = 'WORLD TOTAL') " & _
          "  AND week_ending IN ( " & _
          "      SELECT DISTINCT week_ending FROM gold.export_sales_matrix " & _
          "      WHERE commodity = '" & commodity & "' " & _
          "      ORDER BY week_ending DESC LIMIT " & weekCount & _
          "  ) " & _
          "ORDER BY week_ending, spreadsheet_row"

    On Error GoTo QueryError
    Set rs = CreateObject("ADODB.Recordset")
    rs.Open sql, conn

    ' First pass: identify columns to update
    Application.StatusBar = "Identifying columns..."
    DoEvents
    Dim columnsToUpdate As Object
    Set columnsToUpdate = CreateObject("Scripting.Dictionary")
    Dim colNum As Integer, weekEnd As Date

    Do While Not rs.EOF
        weekEnd = rs("week_ending")
        colNum = FindColumnForWeekDate(ws, weekEnd)
        If colNum > 0 And Not columnsToUpdate.Exists(colNum) Then
            columnsToUpdate.Add colNum, CStr(weekEnd)
        End If
        rs.MoveNext
    Loop

    ' Clear columns (preserve formula cells + regional rows)
    Application.StatusBar = "Clearing " & columnsToUpdate.Count & " columns..."
    DoEvents
    Application.ScreenUpdating = False
    Application.Calculation = xlCalculationManual
    Application.EnableEvents = False
    On Error GoTo CleanupExit  ' if anything errors past here, restore app state

    Dim colKey As Variant, columnsCleared As Integer
    columnsCleared = 0
    For Each colKey In columnsToUpdate.Keys
        ClearColumnForUpdate ws, CInt(colKey)
        columnsCleared = columnsCleared + 1
    Next colKey

    ' Second pass: populate
    Application.StatusBar = "Updating cells..."
    DoEvents
    rs.MoveFirst
    cellsUpdated = 0 : countriesNotFound = 0

    Do While Not rs.EOF
        Dim countryName As String, rowNum As Variant, qty As Variant, targetRow As Integer
        weekEnd = rs("week_ending")
        countryName = rs("country_name")
        rowNum = rs("spreadsheet_row")
        qty = rs("qty")

        colNum = FindColumnForWeekDate(ws, weekEnd)
        If colNum > 0 Then
            If IsNull(rowNum) Or rowNum = 0 Then
                targetRow = FindRowForCountry(ws, countryName)
            Else
                targetRow = CInt(rowNum)
            End If

            If targetRow > 0 Then
                If Not IsProtectedRow(targetRow) Then
                    If Not IsNull(qty) Then
                        ws.Cells(targetRow, colNum).Value = qty
                        cellsUpdated = cellsUpdated + 1
                    End If
                End If
            Else
                countriesNotFound = countriesNotFound + 1
            End If
        End If
        rs.MoveNext
    Loop

    rs.Close : conn.Close

CleanupExit:
    Application.Calculation = xlCalculationAutomatic
    Application.EnableEvents = True
    Application.ScreenUpdating = True
    Application.StatusBar = False
    Application.Cursor = xlDefault

    If Err.Number <> 0 Then
        MsgBox "Error during update: " & Err.Description, vbCritical, "Export Sales Updater"
        Exit Sub
    End If

    MsgBox "Export Sales update complete!" & vbCrLf & vbCrLf & _
           "Commodity: " & commodity & vbCrLf & _
           "Flow: " & flow & vbCrLf & _
           "Columns cleared: " & columnsCleared & vbCrLf & _
           "Cells updated: " & cellsUpdated & vbCrLf & _
           "Destinations not found: " & countriesNotFound, vbInformation, "Export Sales Updater"
    Exit Sub

QueryError:
    Application.Calculation = xlCalculationAutomatic
    Application.EnableEvents = True
    Application.ScreenUpdating = True
    Application.StatusBar = False
    Application.Cursor = xlDefault
    MsgBox "Query error:" & vbCrLf & vbCrLf & Err.Description, vbCritical, "Export Sales Updater"
    If Not conn Is Nothing Then conn.Close
End Sub

' =============================================================================
' HELPERS
' =============================================================================

Private Sub GetCommodityAndFlow(sheetName As String, ByRef commodity As String, ByRef flow As String)
    Dim s As String
    s = LCase(sheetName)
    commodity = "UNKNOWN"
    flow = "UNKNOWN"

    ' Order matters — check meal/oil before generic soybean
    If InStr(s, "meal") > 0 Then
        commodity = "soybean_meal"
    ElseIf InStr(s, "sbo") > 0 Or (InStr(s, "soy") > 0 And InStr(s, "oil") > 0) Then
        commodity = "soybean_oil"
    ElseIf InStr(s, "soy") > 0 Then
        commodity = "soybeans"
    End If

    ' Flow detection — check NMY first since "NMY Sales" contains "Sales"
    If InStr(s, "nmy") > 0 Then
        flow = "nmy_sales"
    ElseIf InStr(s, "commit") > 0 Then
        flow = "commitments"
    ElseIf InStr(s, "shipment") > 0 Then
        flow = "shipments"
    ElseIf InStr(s, "sales") > 0 Then
        flow = "sales"
    End If
End Sub

Private Function FindColumnForWeekDate(ws As Worksheet, weekEnd As Date) As Integer
    Dim col As Integer, cellVal As Variant
    FindColumnForWeekDate = 0
    For col = 2 To ws.Cells(HEADER_ROW, ws.Columns.Count).End(xlToLeft).Column
        cellVal = ws.Cells(HEADER_ROW, col).Value
        If IsDate(cellVal) Then
            If CDate(cellVal) = weekEnd Then
                FindColumnForWeekDate = col
                Exit Function
            End If
        End If
    Next col
End Function

Private Function FindRowForCountry(ws As Worksheet, countryName As String) As Integer
    Dim row As Integer, cellVal As String
    FindRowForCountry = 0
    Dim needle As String
    needle = UCase(Trim(countryName))
    For row = DATA_START_ROW To DATA_END_ROW
        cellVal = UCase(Trim(CStr(ws.Cells(row, COUNTRY_COLUMN).Value)))
        If cellVal = needle Then
            FindRowForCountry = row
            Exit Function
        End If
    Next row
End Function

Private Sub ClearColumnForUpdate(ws As Worksheet, colNum As Integer)
    Dim row As Integer
    For row = DATA_START_ROW To DATA_END_ROW
        If Not IsProtectedRow(row) Then
            If Not ws.Cells(row, colNum).HasFormula Then
                ws.Cells(row, colNum).ClearContents
            End If
        End If
    Next row
End Sub

Private Function IsProtectedRow(rowNum As Integer) As Boolean
    Dim parts() As String, i As Integer
    parts = Split(REGIONAL_ROWS, ",")
    For i = LBound(parts) To UBound(parts)
        If rowNum = CInt(parts(i)) Then
            IsProtectedRow = True
            Exit Function
        End If
    Next i
    IsProtectedRow = False
End Function

Private Function GetConnection() As Object
    Dim conn As Object
    Dim connStr As String
    On Error GoTo ConnError
    Set conn = CreateObject("ADODB.Connection")
    connStr = "Driver={PostgreSQL Unicode(x64)};" & _
              "Server=" & DB_SERVER & ";Port=" & DB_PORT & ";" & _
              "Database=" & DB_NAME & ";Uid=" & DB_USER & ";Pwd=" & DB_PASSWORD & ";" & _
              "sslmode=require;"
    conn.Open connStr
    Set GetConnection = conn
    Exit Function
ConnError:
    MsgBox "Could not connect to database:" & vbCrLf & vbCrLf & Err.Description, vbCritical, "Export Sales Updater"
    Set GetConnection = Nothing
End Function

' =============================================================================
' KEYBOARD SHORTCUT REGISTRATION
' =============================================================================

Public Sub AssignExportSalesShortcuts()
    Application.OnKey "^e", "ExportSalesUpdaterSQL.UpdateExportSalesData"
    Application.OnKey "^+e", "ExportSalesUpdaterSQL.UpdateExportSalesDataCustom"
End Sub

Public Sub RemoveExportSalesShortcuts()
    Application.OnKey "^e"
    Application.OnKey "^+e"
End Sub
