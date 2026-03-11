Attribute VB_Name = "EMTSDataUpdater"
' =============================================================================
' EMTSDataUpdater - VBA + ODBC for EMTS Data workbook
' =============================================================================
' Pulls EPA EMTS monthly RIN generation data directly from the PostgreSQL
' database and writes to all 10 monthly tabs (D3-D7 Gallon + RIN).
'
' Prerequisites:
' 1. EPA CSV loaded into database:
'    python src/tools/emts_csv_loader.py path/to/monthly_rin_generation.csv
' 2. PostgreSQL ODBC driver installed (psqlODBC x64)
' 3. Gold view gold.emts_monthly_matrix exists (023_epa_emts_monthly.sql)
'
' Installation:
' 1. Import this module into VBA (Alt+F11 > File > Import)
' 2. Paste EMTSWorkbookEvents code into ThisWorkbook module
'
' Keyboard shortcuts:
'   Ctrl+E       = Quick update (latest 6 months)
'   Ctrl+Shift+E = Custom month count / update all
' =============================================================================

Option Explicit

' Database connection constants
Private Const DB_SERVER As String = "localhost"
Private Const DB_PORT As String = "5432"
Private Const DB_NAME As String = "rlc_commodities"
Private Const DB_USER As String = "postgres"
Private Const DB_PASSWORD As String = "SoupBoss1"

' =============================================================================
' MAIN UPDATE FUNCTIONS
' =============================================================================

Public Sub UpdateEMTSData()
    ' Quick update - latest 6 months from database
    ' Keyboard shortcut: Ctrl+E

    Dim result As VbMsgBoxResult
    result = MsgBox("Update all EMTS sheets with the latest 6 months from database?" & vbCrLf & vbCrLf & _
                    "Make sure the EPA CSV has been loaded first." & vbCrLf & _
                    "(python emts_csv_loader.py path/to/csv)", _
                    vbYesNo + vbQuestion, "EMTS Updater")

    If result = vbYes Then UpdateFromDatabase 6
End Sub

Public Sub UpdateEMTSDataCustom()
    ' Custom update with user-specified months
    ' Keyboard shortcut: Ctrl+Shift+E

    Dim monthCount As String
    monthCount = InputBox("How many months to update?" & vbCrLf & vbCrLf & _
                          "Enter a number (e.g., 12 for last 12 months)" & vbCrLf & _
                          "Or enter 0 to update ALL available data", _
                          "EMTS Updater", "12")

    If monthCount = "" Then Exit Sub

    If Not IsNumeric(monthCount) Then
        MsgBox "Please enter a valid number.", vbExclamation, "EMTS Updater"
        Exit Sub
    End If

    UpdateFromDatabase CInt(monthCount)
End Sub

' =============================================================================
' DATABASE UPDATE
' =============================================================================

Private Sub UpdateFromDatabase(monthCount As Integer)
    ' Query gold.emts_monthly_matrix and write values to spreadsheet cells.
    ' Each row from the view specifies exactly which tab, row date, and column
    ' to write to. No closing/reopening - updates happen in-place.

    Dim conn As Object
    Dim rs As Object
    Dim sql As String
    Dim cellsUpdated As Long
    Dim tabsUpdated As Long
    Dim currentTab As String
    Dim ws As Worksheet
    Dim yr As Integer
    Dim mo As Integer
    Dim startRow As Integer
    Dim colNum As Integer
    Dim val As Variant
    Dim tabName As String
    Dim targetRow As Integer

    Application.StatusBar = "Connecting to database..."
    Application.Cursor = xlWait
    Application.ScreenUpdating = False
    DoEvents

    Set conn = GetConnection()
    If conn Is Nothing Then
        Application.StatusBar = False
        Application.Cursor = xlDefault
        Application.ScreenUpdating = True
        Exit Sub
    End If

    ' Build query
    sql = "SELECT year, month, tab_name, data_start_row, column_number, value " & _
          "FROM gold.emts_monthly_matrix "

    If monthCount > 0 Then
        sql = sql & "WHERE (year, month) IN (" & _
              "SELECT DISTINCT rin_year, month FROM bronze.epa_emts_monthly " & _
              "ORDER BY rin_year DESC, month DESC LIMIT " & monthCount & ") "
    End If

    sql = sql & "ORDER BY tab_name, year, month, column_number"

    On Error GoTo QueryError
    Set rs = CreateObject("ADODB.Recordset")
    rs.Open sql, conn

    If rs.EOF Then
        MsgBox "No data returned from database." & vbCrLf & vbCrLf & _
               "Make sure the EPA CSV has been loaded first:" & vbCrLf & _
               "  python emts_csv_loader.py path/to/csv", _
               vbInformation, "EMTS Updater"
        rs.Close
        conn.Close
        Application.StatusBar = False
        Application.Cursor = xlDefault
        Application.ScreenUpdating = True
        Exit Sub
    End If

    currentTab = ""
    cellsUpdated = 0
    tabsUpdated = 0

    Do While Not rs.EOF
        tabName = rs("tab_name") & ""

        ' Switch worksheet when tab changes
        If tabName <> currentTab Then
            Set ws = Nothing
            On Error Resume Next
            Set ws = ThisWorkbook.Sheets(tabName)
            On Error GoTo QueryError
            currentTab = tabName
            If Not ws Is Nothing Then
                tabsUpdated = tabsUpdated + 1
                Application.StatusBar = "Updating " & tabName & "..."
                DoEvents
            End If
        End If

        If Not ws Is Nothing Then
            yr = CInt(rs("year"))
            mo = CInt(rs("month"))
            startRow = CInt(rs("data_start_row"))
            colNum = CInt(rs("column_number"))
            val = rs("value")

            targetRow = FindRowForDate(ws, yr, mo, startRow)

            If targetRow > 0 And Not IsNull(val) Then
                ws.Cells(targetRow, colNum).Value = CDbl(val)
                cellsUpdated = cellsUpdated + 1
            End If
        End If

        rs.MoveNext
    Loop

    rs.Close
    conn.Close

    Application.StatusBar = False
    Application.Cursor = xlDefault
    Application.ScreenUpdating = True

    Dim monthLabel As String
    If monthCount = 0 Then
        monthLabel = "all available"
    Else
        monthLabel = "latest " & monthCount & " months"
    End If

    MsgBox "Update complete!" & vbCrLf & vbCrLf & _
           "Tabs updated: " & tabsUpdated & vbCrLf & _
           "Cells updated: " & cellsUpdated & vbCrLf & _
           "Period: " & monthLabel, vbInformation, "EMTS Updater"

    Exit Sub

QueryError:
    Application.StatusBar = False
    Application.Cursor = xlDefault
    Application.ScreenUpdating = True
    MsgBox "Query error:" & vbCrLf & vbCrLf & Err.Description, vbCritical, "EMTS Updater"
    If Not rs Is Nothing Then
        If rs.State = 1 Then rs.Close
    End If
    If Not conn Is Nothing Then
        If conn.State = 1 Then conn.Close
    End If
End Sub

' =============================================================================
' DATABASE CONNECTION
' =============================================================================

Private Function GetConnection() As Object
    ' Create ADODB Connection to PostgreSQL via ODBC

    Dim conn As Object
    Dim connString As String

    Set conn = CreateObject("ADODB.Connection")

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
    MsgBox "Database connection failed:" & vbCrLf & vbCrLf & _
           Err.Description & vbCrLf & vbCrLf & _
           "Make sure:" & vbCrLf & _
           "  1. PostgreSQL is running" & vbCrLf & _
           "  2. psqlODBC x64 driver is installed" & vbCrLf & _
           "  3. Database credentials are correct", _
           vbCritical, "EMTS Updater - Connection Error"
    Set GetConnection = Nothing
End Function

' =============================================================================
' HELPER FUNCTIONS
' =============================================================================

Private Function FindRowForDate(ws As Worksheet, yr As Integer, mo As Integer, startRow As Integer) As Integer
    ' Find row number for a given year/month by scanning column A for dates.
    ' Column A contains 1st-of-month dates as Excel date serial numbers.

    Dim row As Integer
    Dim cellVal As Variant
    Dim cellDate As Date
    Dim lastRow As Integer

    lastRow = ws.Cells(ws.Rows.Count, 1).End(xlUp).row

    For row = startRow To lastRow
        cellVal = ws.Cells(row, 1).Value

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

    FindRowForDate = 0
End Function

' =============================================================================
' KEYBOARD SHORTCUTS
' =============================================================================

Public Sub AssignEMTSShortcuts()
    ' Assign Ctrl+E and Ctrl+Shift+E shortcuts

    Application.OnKey "^e", "UpdateEMTSData"
    Application.OnKey "^+e", "UpdateEMTSDataCustom"

    MsgBox "EMTS keyboard shortcuts assigned:" & vbCrLf & vbCrLf & _
           "Ctrl+E = Quick update (latest 6 months)" & vbCrLf & _
           "Ctrl+Shift+E = Custom month count / update all" & vbCrLf & vbCrLf & _
           "Data is pulled directly from the database." & vbCrLf & _
           "(Make sure EPA CSV has been loaded first)", _
           vbInformation, "EMTS Updater"
End Sub

Public Sub RemoveEMTSShortcuts()
    ' Remove keyboard shortcuts (called on workbook close)

    Application.OnKey "^e"
    Application.OnKey "^+e"
End Sub
