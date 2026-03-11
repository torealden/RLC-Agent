' =============================================================================
' InspectionsUpdaterSQL - FGIS Export Inspections VBA Updater
' =============================================================================
' Connects directly to PostgreSQL database to update FGIS inspection data.
' Mirrors TradeUpdaterSQL.bas pattern for Census trade data.
'
' Keyboard shortcuts:
'   Ctrl+G       = Quick update (latest 6 months)
'   Ctrl+Shift+G = Custom month/week count
'
' Requirements:
' - PostgreSQL ODBC Driver installed (psqlODBC)
' - Reference to "Microsoft ActiveX Data Objects" (Tools > References)
' =============================================================================

Option Explicit

' Database connection settings
Private Const DB_SERVER As String = "rlc-commodities.c16c6wm826t7.us-east-2.rds.amazonaws.com"
Private Const DB_PORT As String = "5432"
Private Const DB_NAME As String = "rlc_commodities"
Private Const DB_USER As String = "postgres"
Private Const DB_PASSWORD As String = "SoupBoss1"

' Spreadsheet structure
Private Const HEADER_ROW As Integer = 3       ' Row 3 has date headers (inspections tabs)
Private Const DATA_START_ROW As Integer = 4
Private Const DATA_END_ROW As Integer = 231   ' Extends to comparison section
Private Const COUNTRY_COLUMN As Integer = 1

' Regional subtotal rows - these contain formulas, never clear or overwrite
' EU-27, Other Europe, FSU, Asia/Oceania, Africa, Western Hemisphere, Sum of Regional Totals
Private Const REGIONAL_ROWS As String = "4,33,47,61,108,165,216"

' Comparison section rows that are labels or formula rows (do not clear)
' Row 217=WORLD TOTAL (we write to this), 218=ADJUSTED, 219=INSPECTIONS header
' Row 221=Mexico header, 222=Census tonnes, 223=Census kbu, 225=Insp/Census ratio
' Row 227=Canada header, 228=Census tonnes, 229=Census kbu, 231=Insp/Census ratio
Private Const COMPARISON_LABEL_ROWS As String = "218,219,221,222,223,225,227,228,229,231"

' =============================================================================
' MAIN UPDATE FUNCTIONS
' =============================================================================

Public Sub UpdateInspectionsData()
    ' Quick update - latest 6 months of available data
    ' Keyboard shortcut: Ctrl+G

    Dim result As VbMsgBoxResult
    result = MsgBox("Update " & ActiveSheet.Name & " with the latest 6 months of inspections data?", _
                    vbYesNo + vbQuestion, "Inspections Updater")

    If result = vbYes Then
        If IsWeeklySheet(ActiveSheet.Name) Then
            UpdateWeeklyFromDatabase 26  ' ~6 months of weeks
        Else
            UpdateMonthlyFromDatabase 6
        End If
    End If
End Sub

Public Sub UpdateInspectionsDataCustom()
    ' Custom update with user-specified count
    ' Keyboard shortcut: Ctrl+Shift+G

    Dim countStr As String
    Dim isWeekly As Boolean

    isWeekly = IsWeeklySheet(ActiveSheet.Name)

    If isWeekly Then
        countStr = InputBox("How many weeks of data to update?" & vbCrLf & vbCrLf & _
                            "Enter a number (e.g., 52 for last year)", _
                            "Inspections Updater", "26")
    Else
        countStr = InputBox("How many months of data to update?" & vbCrLf & vbCrLf & _
                            "Enter a number (e.g., 12 for last year, 999 for all)", _
                            "Inspections Updater", "6")
    End If

    If countStr = "" Then Exit Sub

    If Not IsNumeric(countStr) Then
        MsgBox "Please enter a valid number.", vbExclamation, "Inspections Updater"
        Exit Sub
    End If

    If isWeekly Then
        UpdateWeeklyFromDatabase CInt(countStr)
    Else
        UpdateMonthlyFromDatabase CInt(countStr)
    End If
End Sub

' =============================================================================
' DATABASE CONNECTION
' =============================================================================

Private Function GetConnection() As Object
    Dim conn As Object
    Dim connString As String

    Set conn = CreateObject("ADODB.Connection")

    connString = "Driver={PostgreSQL UNICODE(x64)};" & _
                 "Server=" & DB_SERVER & ";" & _
                 "Port=" & DB_PORT & ";" & _
                 "Database=" & DB_NAME & ";" & _
                 "Uid=" & DB_USER & ";" & _
                 "Pwd=" & DB_PASSWORD & ";" & _
                 "sslmode=require;"

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
' MONTHLY UPDATE LOGIC
' =============================================================================

Private Sub UpdateMonthlyFromDatabase(monthCount As Integer)
    Dim conn As Object
    Dim rs As Object
    Dim ws As Worksheet
    Dim grain As String
    Dim sql As String
    Dim cellsUpdated As Long
    Dim countriesNotFound As Long

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

    ' Determine grain from sheet name
    grain = GetGrainFromSheet(ws.Name)

    If grain = "UNKNOWN" Then
        MsgBox "Could not determine commodity from sheet name: " & ws.Name, vbExclamation, "Inspections Updater"
        conn.Close
        Application.StatusBar = False
        Application.Cursor = xlDefault
        Exit Sub
    End If

    Application.StatusBar = "Fetching " & grain & " monthly inspections..."
    DoEvents

    ' Query latest months from the matrix view
    sql = "SELECT year, month, country_name, spreadsheet_row, quantity " & _
          "FROM gold.fgis_inspections_monthly_matrix_kbu " & _
          "WHERE grain = '" & grain & "' " & _
          "  AND (year, month) IN ( " & _
          "      SELECT DISTINCT year, month " & _
          "      FROM gold.fgis_inspections_monthly_matrix_kbu " & _
          "      WHERE grain = '" & grain & "' " & _
          "      ORDER BY year DESC, month DESC " & _
          "      LIMIT " & monthCount & _
          "  ) " & _
          "ORDER BY year, month, spreadsheet_row"

    On Error GoTo QueryError
    Set rs = CreateObject("ADODB.Recordset")
    rs.Open sql, conn

    ' First pass: identify columns to update
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

    ' Clear columns (except protected rows)
    Application.StatusBar = "Clearing " & columnsToUpdate.Count & " columns..."
    DoEvents

    Dim colKey As Variant
    Dim columnsCleared As Integer
    columnsCleared = 0

    For Each colKey In columnsToUpdate.Keys
        ClearColumnForUpdate ws, CInt(colKey)
        columnsCleared = columnsCleared + 1
    Next colKey

    ' Second pass: populate data
    rs.MoveFirst

    Application.StatusBar = "Updating cells..."
    DoEvents

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

        colNum = FindColumnForDate(ws, yr, mo)

        If colNum > 0 Then
            Dim targetRow As Integer

            If IsNull(rowNum) Or rowNum = 0 Then
                targetRow = FindRowForCountry(ws, countryName)
            Else
                targetRow = CInt(rowNum)
            End If

            If targetRow > 0 Then
                If Not IsProtectedRow(targetRow) Then
                    ' Never overwrite formula cells (accumulator columns)
                    If Not ws.Cells(targetRow, colNum).HasFormula Then
                        If Not IsNull(quantity) Then
                            If quantity > 0 Or targetRow = 217 Then
                                ws.Cells(targetRow, colNum).Value = quantity
                                cellsUpdated = cellsUpdated + 1
                            End If
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

    Application.StatusBar = False
    Application.Cursor = xlDefault

    MsgBox "Monthly update complete!" & vbCrLf & vbCrLf & _
           "Grain: " & grain & vbCrLf & _
           "Columns cleared: " & columnsCleared & vbCrLf & _
           "Cells updated: " & cellsUpdated & vbCrLf & _
           "Destinations not found: " & countriesNotFound, vbInformation, "Inspections Updater"

    Exit Sub

QueryError:
    Application.StatusBar = False
    Application.Cursor = xlDefault
    MsgBox "Query error:" & vbCrLf & vbCrLf & Err.Description, vbCritical, "Inspections Updater"
    If Not conn Is Nothing Then conn.Close
End Sub

' =============================================================================
' WEEKLY UPDATE LOGIC
' =============================================================================

Private Sub UpdateWeeklyFromDatabase(weekCount As Integer)
    Dim conn As Object
    Dim rs As Object
    Dim ws As Worksheet
    Dim grain As String
    Dim sql As String
    Dim cellsUpdated As Long
    Dim countriesNotFound As Long

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

    grain = GetGrainFromSheet(ws.Name)

    If grain = "UNKNOWN" Then
        MsgBox "Could not determine commodity from sheet name: " & ws.Name, vbExclamation, "Inspections Updater"
        conn.Close
        Application.StatusBar = False
        Application.Cursor = xlDefault
        Exit Sub
    End If

    Application.StatusBar = "Fetching " & grain & " weekly inspections..."
    DoEvents

    ' Query latest weeks from the weekly matrix view
    sql = "SELECT week_ending, year, month, country_name, spreadsheet_row, quantity " & _
          "FROM gold.fgis_inspections_weekly_matrix_kbu " & _
          "WHERE grain = '" & grain & "' " & _
          "  AND week_ending IN ( " & _
          "      SELECT DISTINCT week_ending " & _
          "      FROM gold.fgis_inspections_weekly_matrix_kbu " & _
          "      WHERE grain = '" & grain & "' " & _
          "      ORDER BY week_ending DESC " & _
          "      LIMIT " & weekCount & _
          "  ) " & _
          "ORDER BY week_ending, spreadsheet_row"

    On Error GoTo QueryError
    Set rs = CreateObject("ADODB.Recordset")
    rs.Open sql, conn

    ' First pass: identify columns to update
    Application.StatusBar = "Identifying columns to update..."
    DoEvents

    Dim columnsToUpdate As Object
    Set columnsToUpdate = CreateObject("Scripting.Dictionary")
    Dim colNum As Integer

    Do While Not rs.EOF
        Dim weekEnd As Date
        weekEnd = rs("week_ending")

        colNum = FindColumnForWeekDate(ws, weekEnd)
        If colNum > 0 Then
            If Not columnsToUpdate.Exists(colNum) Then
                columnsToUpdate.Add colNum, CStr(weekEnd)
            End If
        End If

        rs.MoveNext
    Loop

    ' Clear columns
    Application.StatusBar = "Clearing " & columnsToUpdate.Count & " columns..."
    DoEvents

    Dim colKey As Variant
    Dim columnsCleared As Integer
    columnsCleared = 0

    For Each colKey In columnsToUpdate.Keys
        ClearColumnForUpdate ws, CInt(colKey)
        columnsCleared = columnsCleared + 1
    Next colKey

    ' Second pass: populate
    rs.MoveFirst

    Application.StatusBar = "Updating cells..."
    DoEvents

    cellsUpdated = 0
    countriesNotFound = 0

    Do While Not rs.EOF
        Dim countryName As String
        Dim rowNum As Variant
        Dim quantity As Variant

        weekEnd = rs("week_ending")
        countryName = rs("country_name")
        rowNum = rs("spreadsheet_row")
        quantity = rs("quantity")

        colNum = FindColumnForWeekDate(ws, weekEnd)

        If colNum > 0 Then
            Dim targetRow As Integer

            If IsNull(rowNum) Or rowNum = 0 Then
                targetRow = FindRowForCountry(ws, countryName)
            Else
                targetRow = CInt(rowNum)
            End If

            If targetRow > 0 Then
                If Not IsProtectedRow(targetRow) Then
                    If Not IsNull(quantity) Then
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

    Application.StatusBar = False
    Application.Cursor = xlDefault

    MsgBox "Weekly update complete!" & vbCrLf & vbCrLf & _
           "Grain: " & grain & vbCrLf & _
           "Columns cleared: " & columnsCleared & vbCrLf & _
           "Cells updated: " & cellsUpdated & vbCrLf & _
           "Destinations not found: " & countriesNotFound, vbInformation, "Inspections Updater"

    Exit Sub

QueryError:
    Application.StatusBar = False
    Application.Cursor = xlDefault
    MsgBox "Query error:" & vbCrLf & vbCrLf & Err.Description, vbCritical, "Inspections Updater"
    If Not conn Is Nothing Then conn.Close
End Sub

' =============================================================================
' HELPER FUNCTIONS
' =============================================================================

Private Sub ClearColumnForUpdate(ws As Worksheet, colNum As Integer)
    ' Clear a column before populating with new data.
    ' Skips regional subtotal rows, comparison label rows, AND formula cells
    ' (accumulator columns use SUM formulas that must not be cleared).

    Dim row As Integer

    For row = DATA_START_ROW To DATA_END_ROW
        If Not IsProtectedRow(row) Then
            ' Never clear formula cells — they belong to accumulator columns
            If Not ws.Cells(row, colNum).HasFormula Then
                ws.Cells(row, colNum).ClearContents
            End If
        End If
    Next row
End Sub

Private Function IsProtectedRow(rowNum As Integer) As Boolean
    ' Check if a row is protected (regional subtotal or comparison label)

    Dim protectedRows() As String
    Dim i As Integer

    ' Check regional rows
    protectedRows = Split(REGIONAL_ROWS, ",")
    For i = LBound(protectedRows) To UBound(protectedRows)
        If rowNum = CInt(protectedRows(i)) Then
            IsProtectedRow = True
            Exit Function
        End If
    Next i

    ' Check comparison label rows
    protectedRows = Split(COMPARISON_LABEL_ROWS, ",")
    For i = LBound(protectedRows) To UBound(protectedRows)
        If rowNum = CInt(protectedRows(i)) Then
            IsProtectedRow = True
            Exit Function
        End If
    Next i

    IsProtectedRow = False
End Function

Private Function GetGrainFromSheet(sheetName As String) As String
    ' Determine grain type from sheet name

    Dim sheetLower As String
    sheetLower = LCase(sheetName)

    If InStr(sheetLower, "soybean") > 0 Or InStr(sheetLower, "soy ") > 0 Then
        GetGrainFromSheet = "SOYBEANS"
    ElseIf InStr(sheetLower, "corn") > 0 Or InStr(sheetLower, "maize") > 0 Then
        GetGrainFromSheet = "CORN"
    ElseIf InStr(sheetLower, "wheat") > 0 Then
        GetGrainFromSheet = "WHEAT"
    ElseIf InStr(sheetLower, "sorghum") > 0 Then
        GetGrainFromSheet = "SORGHUM"
    ElseIf InStr(sheetLower, "barley") > 0 Then
        GetGrainFromSheet = "BARLEY"
    ElseIf InStr(sheetLower, "canola") > 0 Or InStr(sheetLower, "rapeseed") > 0 Then
        GetGrainFromSheet = "CANOLA"
    Else
        GetGrainFromSheet = "UNKNOWN"
    End If
End Function

Private Function IsWeeklySheet(sheetName As String) As Boolean
    IsWeeklySheet = InStr(LCase(sheetName), "weekly") > 0
End Function

Private Function FindColumnForDate(ws As Worksheet, yr As Integer, mo As Integer) As Integer
    ' Find column for a given year/month in the header row (row 3).
    '
    ' Monthly inspections sheets have accumulator columns (B through ~AI)
    ' with text headers like '01/02 (apostrophe-prefixed to prevent date
    ' conversion).  Real date columns start ~AJ with Feb 1993.
    '
    ' We use the .PrefixCharacter property to detect apostrophe-prefixed
    ' text cells, and VarType to reject any other text strings.
    ' This is the same core logic as TradeUpdaterSQL.FindColumnForDate
    ' with the accumulator text guard added.

    Dim col As Integer
    Dim lastCol As Integer
    Dim cellVal As Variant
    Dim cellDate As Date

    lastCol = ws.UsedRange.Columns.Count + 2

    For col = 2 To lastCol
        ' --- Skip apostrophe-prefixed text cells (accumulator headers) ---
        If ws.Cells(HEADER_ROW, col).PrefixCharacter = "'" Then GoTo NextCol

        ' --- Skip any other text strings ---
        If VarType(ws.Cells(HEADER_ROW, col).Value) = vbString Then GoTo NextCol

        cellVal = ws.Cells(HEADER_ROW, col).Value

        ' Skip empty cells
        If IsEmpty(cellVal) Then GoTo NextCol

        ' Standard date matching (identical to TradeUpdaterSQL)
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

NextCol:
    Next col

    FindColumnForDate = 0
End Function

Private Function FindColumnForWeekDate(ws As Worksheet, weekEnd As Date) As Integer
    ' Find column for a specific week-ending date (exact match within +/- 3 days)

    Dim col As Integer
    Dim cellVal As Variant
    Dim cellDate As Date

    For col = 2 To ws.UsedRange.Columns.Count + 2
        cellVal = ws.Cells(HEADER_ROW, col).Value

        If IsDate(cellVal) Then
            cellDate = CDate(cellVal)
            ' Match within 3 days to handle Thursday alignment differences
            If Abs(CLng(cellDate) - CLng(weekEnd)) <= 3 Then
                FindColumnForWeekDate = col
                Exit Function
            End If
        ElseIf IsNumeric(cellVal) And cellVal > 30000 Then
            On Error Resume Next
            cellDate = CDate(cellVal)
            If Err.Number = 0 Then
                If Abs(CLng(cellDate) - CLng(weekEnd)) <= 3 Then
                    FindColumnForWeekDate = col
                    Exit Function
                End If
            End If
            On Error GoTo 0
        End If
    Next col

    FindColumnForWeekDate = 0
End Function

Private Function FindRowForCountry(ws As Worksheet, countryName As String) As Integer
    ' Find row number for a country name

    Dim row As Integer
    Dim cellVal As String
    Dim searchName As String

    searchName = UCase(Trim(countryName))

    For row = DATA_START_ROW To DATA_END_ROW
        cellVal = UCase(Trim(CStr(ws.Cells(row, COUNTRY_COLUMN).Value)))
        If cellVal = searchName Then
            FindRowForCountry = row
            Exit Function
        End If
    Next row

    FindRowForCountry = 0
End Function

' =============================================================================
' KEYBOARD SHORTCUTS
' =============================================================================

Public Sub AssignInspectionsShortcuts()
    ' Assign Ctrl+G and Ctrl+Shift+G shortcuts

    Application.OnKey "^g", "UpdateInspectionsData"
    Application.OnKey "^+g", "UpdateInspectionsDataCustom"

    MsgBox "Inspections keyboard shortcuts assigned:" & vbCrLf & vbCrLf & _
           "Ctrl+G = Quick update (latest 6 months / 26 weeks)" & vbCrLf & _
           "Ctrl+Shift+G = Custom period count", vbInformation, "Inspections Updater"
End Sub

Public Sub RemoveInspectionsShortcuts()
    Application.OnKey "^g"
    Application.OnKey "^+g"
End Sub
