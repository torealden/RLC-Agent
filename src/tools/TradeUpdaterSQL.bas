' =============================================================================
' TradeUpdaterSQL - Pure VBA with PostgreSQL Connection
' =============================================================================
' Connects directly to PostgreSQL database to update trade data.
' No Python required. No closing/reopening workbook.
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
Private Const HEADER_ROW As Integer = 2
Private Const DATA_START_ROW As Integer = 4
Private Const DATA_END_ROW As Integer = 217
Private Const COUNTRY_COLUMN As Integer = 1

' Regional subtotal rows - these contain formulas, never clear or overwrite
' EU-27, Other Europe, FSU, Asia/Oceania, Africa, Western Hemisphere, Sum of Regional Totals
Private Const REGIONAL_ROWS As String = "4,33,47,61,108,165,216"

' =============================================================================
' MAIN UPDATE FUNCTIONS
' =============================================================================

Public Sub UpdateTradeData()
    ' Quick update - latest 3 months of available data
    ' Keyboard shortcut: Ctrl+I

    Dim result As VbMsgBoxResult
    result = MsgBox("Update " & ActiveSheet.Name & " with the latest 3 months of available data?", _
                    vbYesNo + vbQuestion, "Trade Updater")

    If result = vbYes Then
        UpdateFromDatabase 3
    End If
End Sub

Public Sub UpdateTradeDataCustom()
    ' Custom update with user-specified months
    ' Keyboard shortcut: Ctrl+Shift+I

    Dim monthCount As String
    monthCount = InputBox("How many months of data to update?" & vbCrLf & vbCrLf & _
                          "Enter a number (e.g., 6 for last 6 months)", _
                          "Trade Updater", "3")

    If monthCount = "" Then Exit Sub

    If Not IsNumeric(monthCount) Then
        MsgBox "Please enter a valid number.", vbExclamation, "Trade Updater"
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
    ' Fetch data from database and update the active sheet

    Dim conn As Object
    Dim rs As Object
    Dim ws As Worksheet
    Dim commodity As String
    Dim flow As String
    Dim sql As String
    Dim cellsUpdated As Long
    Dim countriesNotFound As Long

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

    Set ws = ActiveSheet

    ' Determine commodity and flow from sheet name
    GetCommodityAndFlow ws.Name, commodity, flow

    If commodity = "UNKNOWN" Then
        MsgBox "Could not determine commodity from sheet name: " & ws.Name, vbExclamation, "Trade Updater"
        conn.Close
        Application.StatusBar = False
        Application.Cursor = xlDefault
        Exit Sub
    End If

    Application.StatusBar = "Fetching " & commodity & " " & flow & " data..."
    DoEvents

    ' Query for latest months of data
    ' Exclude regional totals (sheet calculates those) except WORLD TOTAL
    ' Include zeros so columns are properly cleared when no trade occurs
    sql = "SELECT " & _
          "    year, month, country_name, spreadsheet_row, quantity " & _
          "FROM gold.trade_export_matrix " & _
          "WHERE commodity_group = '" & commodity & "' " & _
          "  AND flow = '" & flow & "' " & _
          "  AND (is_regional_total = FALSE OR country_name = 'WORLD TOTAL') " & _
          "  AND (year, month) IN ( " & _
          "      SELECT DISTINCT year, month " & _
          "      FROM bronze.census_trade " & _
          "      ORDER BY year DESC, month DESC " & _
          "      LIMIT " & monthCount & _
          "  ) " & _
          "ORDER BY year, month, spreadsheet_row"

    On Error GoTo QueryError
    Set rs = CreateObject("ADODB.Recordset")
    rs.Open sql, conn

    ' First pass: identify all columns that will be updated
    Application.StatusBar = "Identifying columns to update..."
    DoEvents

    Dim columnsToUpdate As Object
    Set columnsToUpdate = CreateObject("Scripting.Dictionary")
    Dim colNum As Integer

    Do While Not rs.EOF
        Dim yr As Integer, mo As Integer
        yr = rs("year")
        mo = rs("month")

        colNum = FindColumnForDate(ws, yr, mo)
        If colNum > 0 Then
            If Not columnsToUpdate.Exists(colNum) Then
                columnsToUpdate.Add colNum, yr & "-" & mo
            End If
        End If

        rs.MoveNext
    Loop

    ' Clear all columns that will be updated (except regional subtotal rows)
    Application.StatusBar = "Clearing " & columnsToUpdate.Count & " columns..."
    DoEvents

    Dim colKey As Variant
    Dim columnsCleared As Integer
    columnsCleared = 0

    For Each colKey In columnsToUpdate.Keys
        ClearColumnForUpdate ws, CInt(colKey)
        columnsCleared = columnsCleared + 1
    Next colKey

    ' Reset recordset to beginning for second pass
    rs.MoveFirst

    Application.StatusBar = "Updating cells..."
    DoEvents

    ' Second pass: populate the data
    cellsUpdated = 0
    countriesNotFound = 0

    Do While Not rs.EOF
        Dim countryName As String
        Dim rowNum As Variant
        Dim quantity As Variant

        yr = rs("year")
        mo = rs("month")
        countryName = rs("country_name")
        rowNum = rs("spreadsheet_row")
        quantity = rs("quantity")

        ' Find column for this month
        colNum = FindColumnForDate(ws, yr, mo)

        If colNum > 0 Then
            ' Use spreadsheet_row from database, or search by country name
            Dim targetRow As Integer
            If IsNull(rowNum) Or rowNum = 0 Then
                targetRow = FindRowForCountry(ws, countryName)
            Else
                targetRow = rowNum
            End If

            If targetRow > 0 Then
                ' Skip regional subtotal rows (they have formulas)
                If Not IsRegionalRow(targetRow) Then
                    If Not IsNull(quantity) Then
                        ' Only write zeros for WORLD TOTAL (row 217), skip zeros for other rows
                        If quantity > 0 Or targetRow = 217 Then
                            ws.Cells(targetRow, colNum).Value = quantity
                            cellsUpdated = cellsUpdated + 1
                        End If
                    End If
                End If
            Else
                countriesNotFound = countriesNotFound + 1
            End If
        End If

        rs.MoveNext
    Loop

    rs.Close
    conn.Close

    ' Reset status
    Application.StatusBar = False
    Application.Cursor = xlDefault

    ' Report results
    MsgBox "Update complete!" & vbCrLf & vbCrLf & _
           "Columns cleared: " & columnsCleared & vbCrLf & _
           "Cells updated: " & cellsUpdated & vbCrLf & _
           "Countries not found: " & countriesNotFound, vbInformation, "Trade Updater"

    Exit Sub

QueryError:
    Application.StatusBar = False
    Application.Cursor = xlDefault
    MsgBox "Query error:" & vbCrLf & vbCrLf & Err.Description, vbCritical, "Trade Updater"
    If Not conn Is Nothing Then conn.Close
End Sub

' =============================================================================
' HELPER FUNCTIONS
' =============================================================================

Private Sub ClearColumnForUpdate(ws As Worksheet, colNum As Integer)
    ' Clear a column before populating with new data
    ' Clears rows DATA_START_ROW to DATA_END_ROW, EXCEPT regional subtotal rows
    ' This prevents stale data from persisting when countries stop receiving shipments

    Dim row As Integer
    Dim protectedRows() As String
    Dim i As Integer
    Dim isProtected As Boolean

    ' Parse the protected regional rows
    protectedRows = Split(REGIONAL_ROWS, ",")

    ' Clear each row unless it's a protected regional subtotal
    For row = DATA_START_ROW To DATA_END_ROW
        isProtected = False

        ' Check if this row is a regional subtotal
        For i = LBound(protectedRows) To UBound(protectedRows)
            If row = CInt(protectedRows(i)) Then
                isProtected = True
                Exit For
            End If
        Next i

        ' Clear the cell if not protected
        If Not isProtected Then
            ws.Cells(row, colNum).ClearContents
        End If
    Next row
End Sub

Private Function IsRegionalRow(rowNum As Integer) As Boolean
    ' Check if a row is a regional subtotal (protected from updates)

    Dim protectedRows() As String
    Dim i As Integer

    protectedRows = Split(REGIONAL_ROWS, ",")

    For i = LBound(protectedRows) To UBound(protectedRows)
        If rowNum = CInt(protectedRows(i)) Then
            IsRegionalRow = True
            Exit Function
        End If
    Next i

    IsRegionalRow = False
End Function

Private Sub GetCommodityAndFlow(sheetName As String, ByRef commodity As String, ByRef flow As String)
    ' Determine commodity and flow type from sheet name

    Dim sheetLower As String
    sheetLower = LCase(sheetName)

    ' Determine flow type first (lowercase to match database)
    If InStr(sheetLower, "import") > 0 Then
        flow = "imports"
    Else
        flow = "exports"
    End If

    ' Determine commodity (some depend on flow)
    If InStr(sheetLower, "soybean") > 0 And InStr(sheetLower, "meal") > 0 Then
        commodity = "SOYBEAN_MEAL"
    ElseIf InStr(sheetLower, "soybean") > 0 And InStr(sheetLower, "oil") > 0 Then
        commodity = "SOYBEAN_OIL"
    ElseIf InStr(sheetLower, "soybean") > 0 Or InStr(sheetLower, "soy ") > 0 Then
        commodity = "SOYBEANS"
    ElseIf InStr(sheetLower, "corn") > 0 And InStr(sheetLower, "oil") > 0 Then
        commodity = "CORN_OIL"
    ElseIf InStr(sheetLower, "corn") > 0 Or InStr(sheetLower, "maize") > 0 Then
        commodity = "CORN"
    ElseIf InStr(sheetLower, "wheat") > 0 Then
        commodity = "WHEAT"
    ElseIf InStr(sheetLower, "ddgs") > 0 Then
        commodity = "DDGS"
    ElseIf InStr(sheetLower, "canola") > 0 And InStr(sheetLower, "meal") > 0 Then
        commodity = "CANOLA_MEAL"
    ElseIf InStr(sheetLower, "canola") > 0 And InStr(sheetLower, "oil") > 0 Then
        commodity = "CANOLA_OIL"
    ElseIf (InStr(sheetLower, "canola") > 0 Or InStr(sheetLower, "rapeseed") > 0) Then
        ' Canola seed - both exports and imports use CANOLA commodity group
        commodity = "CANOLA"
    ElseIf InStr(sheetLower, "sunflower") > 0 And InStr(sheetLower, "meal") > 0 Then
        commodity = "SUNFLOWER_MEAL"
    ElseIf InStr(sheetLower, "sunflower") > 0 And InStr(sheetLower, "oil") > 0 Then
        commodity = "SUNFLOWER_OIL"
    ElseIf InStr(sheetLower, "sunflower") > 0 Then
        commodity = "SUNFLOWER"
    ElseIf InStr(sheetLower, "palm") > 0 And InStr(sheetLower, "kernel") > 0 Then
        commodity = "PALM_KERNEL_OIL"
    ElseIf InStr(sheetLower, "palm") > 0 Then
        commodity = "PALM_OIL"
    ElseIf InStr(sheetLower, "cottonseed") > 0 And InStr(sheetLower, "meal") > 0 Then
        commodity = "COTTONSEED_MEAL"
    ElseIf InStr(sheetLower, "cottonseed") > 0 And InStr(sheetLower, "oil") > 0 Then
        commodity = "COTTONSEED_OIL"
    ElseIf InStr(sheetLower, "cottonseed") > 0 Then
        commodity = "COTTONSEED"
    Else
        commodity = "UNKNOWN"
    End If
End Sub

Private Function FindColumnForDate(ws As Worksheet, yr As Integer, mo As Integer) As Integer
    ' Find column number for a given year/month in the header row

    Dim col As Integer
    Dim cellVal As Variant
    Dim cellDate As Date

    For col = 2 To ws.UsedRange.Columns.Count + 2
        cellVal = ws.Cells(HEADER_ROW, col).Value

        If IsDate(cellVal) Then
            cellDate = CDate(cellVal)
            If Year(cellDate) = yr And Month(cellDate) = mo Then
                FindColumnForDate = col
                Exit Function
            End If
        ElseIf IsNumeric(cellVal) Then
            ' Excel date serial number
            On Error Resume Next
            cellDate = CDate(cellVal)
            If Err.Number = 0 Then
                If Year(cellDate) = yr And Month(cellDate) = mo Then
                    FindColumnForDate = col
                    Exit Function
                End If
            End If
            On Error GoTo 0
        End If
    Next col

    FindColumnForDate = 0  ' Not found
End Function

Private Function FindRowForCountry(ws As Worksheet, countryName As String) As Integer
    ' Find row number for a country name

    Dim row As Integer
    Dim cellVal As String
    Dim searchName As String

    searchName = UCase(Trim(countryName))

    For row = DATA_START_ROW To ws.UsedRange.Rows.Count + DATA_START_ROW
        cellVal = UCase(Trim(CStr(ws.Cells(row, COUNTRY_COLUMN).Value)))
        If cellVal = searchName Then
            FindRowForCountry = row
            Exit Function
        End If
    Next row

    FindRowForCountry = 0  ' Not found
End Function

' =============================================================================
' KEYBOARD SHORTCUTS
' =============================================================================

Public Sub AssignKeyboardShortcuts()
    ' Assign Ctrl+I and Ctrl+Shift+I shortcuts

    Application.OnKey "^i", "UpdateTradeData"
    Application.OnKey "^+i", "UpdateTradeDataCustom"

    MsgBox "Keyboard shortcuts assigned:" & vbCrLf & vbCrLf & _
           "Ctrl+I = Quick update (latest 3 months)" & vbCrLf & _
           "Ctrl+Shift+I = Custom month count", vbInformation, "Trade Updater"
End Sub

Public Sub RemoveKeyboardShortcuts()
    Application.OnKey "^i"
    Application.OnKey "^+i"
End Sub
