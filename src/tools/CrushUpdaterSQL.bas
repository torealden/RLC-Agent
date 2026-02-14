' =============================================================================
' CrushUpdaterSQL - Pure VBA with PostgreSQL Connection
' =============================================================================
' Connects directly to PostgreSQL database to update NASS crush data.
' No Python required. No closing/reopening workbook.
'
' Target: us_soy_crush.xlsx ("NASS Crush" sheet)
' Layout: Row 3 = headers, Row 4 = units
'         Column A = dates (1st of month, as date serial)
'         Columns C-AI = data series
'
' Data source: gold.nass_soy_crush_matrix view
' Mapping: silver.crush_attribute_reference table
'
' Key difference from TradeUpdaterSQL:
'   Trade: rows = countries, columns = months
'   Crush: rows = months, columns = data attributes
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
Private Const HEADER_ROW As Integer = 3       ' Row with attribute headers
Private Const UNIT_ROW As Integer = 4         ' Row with units
Private Const DATA_START_ROW As Integer = 5   ' First data row
Private Const DATE_COLUMN As Integer = 1      ' Column A = dates

' =============================================================================
' MAIN UPDATE FUNCTIONS
' =============================================================================

Public Sub UpdateCrushData()
    ' Quick update - latest 6 months of available data
    ' Keyboard shortcut: Ctrl+U

    Dim result As VbMsgBoxResult
    result = MsgBox("Update " & ActiveSheet.Name & " with the latest 6 months of crush data?", _
                    vbYesNo + vbQuestion, "Crush Updater")

    If result = vbYes Then
        UpdateFromDatabase 6
    End If
End Sub

Public Sub UpdateCrushDataCustom()
    ' Custom update with user-specified months
    ' Keyboard shortcut: Ctrl+Shift+U

    Dim monthCount As String
    monthCount = InputBox("How many months of data to update?" & vbCrLf & vbCrLf & _
                          "Enter a number (e.g., 12 for last 12 months)" & vbCrLf & _
                          "Or enter 0 to update ALL available data", _
                          "Crush Updater", "6")

    If monthCount = "" Then Exit Sub

    If Not IsNumeric(monthCount) Then
        MsgBox "Please enter a valid number.", vbExclamation, "Crush Updater"
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
    Dim sql As String
    Dim cellsUpdated As Long
    Dim rowsNotFound As Long

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

    Application.StatusBar = "Fetching soybean crush data..."
    DoEvents

    ' Build query
    ' monthCount=0 means fetch all available data
    If monthCount > 0 Then
        sql = "SELECT " & _
              "    year, month, attribute_code, spreadsheet_column, " & _
              "    display_value, display_unit " & _
              "FROM gold.nass_soy_crush_matrix " & _
              "WHERE (year, month) IN ( " & _
              "    SELECT DISTINCT year, month " & _
              "    FROM gold.nass_soy_crush_matrix " & _
              "    ORDER BY year DESC, month DESC " & _
              "    LIMIT " & monthCount & _
              ") " & _
              "ORDER BY year, month, spreadsheet_column"
    Else
        sql = "SELECT " & _
              "    year, month, attribute_code, spreadsheet_column, " & _
              "    display_value, display_unit " & _
              "FROM gold.nass_soy_crush_matrix " & _
              "ORDER BY year, month, spreadsheet_column"
    End If

    On Error GoTo QueryError
    Set rs = CreateObject("ADODB.Recordset")
    rs.Open sql, conn

    If rs.EOF Then
        MsgBox "No data returned from database.", vbInformation, "Crush Updater"
        rs.Close
        conn.Close
        Application.StatusBar = False
        Application.Cursor = xlDefault
        Exit Sub
    End If

    Application.StatusBar = "Updating cells..."
    DoEvents

    ' Update cells
    cellsUpdated = 0
    rowsNotFound = 0

    Do While Not rs.EOF
        Dim yr As Integer, mo As Integer
        Dim attrCode As String
        Dim spCol As Integer
        Dim displayValue As Variant

        yr = rs("year")
        mo = rs("month")
        attrCode = rs("attribute_code")
        spCol = rs("spreadsheet_column")
        displayValue = rs("display_value")

        ' Find the row for this (year, month)
        Dim targetRow As Integer
        targetRow = FindRowForDate(ws, yr, mo)

        If targetRow > 0 Then
            ' Write value or "D" for NASS-suppressed data
            If Not IsNull(displayValue) Then
                ws.Cells(targetRow, spCol).Value = CDbl(displayValue)
                cellsUpdated = cellsUpdated + 1
            Else
                ' NASS suppresses this value — mark with "D" like USDA reports
                ws.Cells(targetRow, spCol).Value = "D"
                cellsUpdated = cellsUpdated + 1
            End If
        Else
            rowsNotFound = rowsNotFound + 1
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
           "Cells updated: " & cellsUpdated & vbCrLf & _
           "Months not found in sheet: " & rowsNotFound, vbInformation, "Crush Updater"

    Exit Sub

QueryError:
    Application.StatusBar = False
    Application.Cursor = xlDefault
    MsgBox "Query error:" & vbCrLf & vbCrLf & Err.Description, vbCritical, "Crush Updater"
    If Not conn Is Nothing Then conn.Close
End Sub

' =============================================================================
' HELPER FUNCTIONS
' =============================================================================

Private Function FindRowForDate(ws As Worksheet, yr As Integer, mo As Integer) As Integer
    ' Find row number for a given year/month by scanning the date column (A)
    ' Dates in column A are 1st-of-month date serial numbers

    Dim row As Integer
    Dim cellVal As Variant
    Dim cellDate As Date
    Dim lastRow As Integer

    ' Find last row with data in column A
    lastRow = ws.Cells(ws.Rows.Count, DATE_COLUMN).End(xlUp).row

    For row = DATA_START_ROW To lastRow
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

' =============================================================================
' KEYBOARD SHORTCUTS
' =============================================================================

Public Sub AssignCrushShortcuts()
    ' Assign Ctrl+U and Ctrl+Shift+U shortcuts

    Application.OnKey "^u", "UpdateCrushData"
    Application.OnKey "^+u", "UpdateCrushDataCustom"

    MsgBox "Keyboard shortcuts assigned:" & vbCrLf & vbCrLf & _
           "Ctrl+U = Quick update (latest 6 months)" & vbCrLf & _
           "Ctrl+Shift+U = Custom month count", vbInformation, "Crush Updater"
End Sub

Public Sub RemoveCrushShortcuts()
    Application.OnKey "^u"
    Application.OnKey "^+u"
End Sub
