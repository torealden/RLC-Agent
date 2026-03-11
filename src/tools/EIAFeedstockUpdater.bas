Attribute VB_Name = "EIAFeedstockUpdater"
' =============================================================================
' EIAFeedstockUpdater - VBA + ODBC for EIA Feedstock/Capacity workbook
' =============================================================================
' Pulls EIA feedstock consumption and capacity data directly from the
' PostgreSQL database and writes to spreadsheet tabs.
'
' Prerequisites:
' 1. EIA data loaded into database:
'    python src/tools/eia_biofuels_collector.py --download
' 2. PostgreSQL ODBC driver installed (psqlODBC x64)
' 3. Gold view gold.eia_biofuels_matrix exists (024_eia_biofuels_feedstock.sql)
'
' Installation:
' 1. Import this module into VBA (Alt+F11 > File > Import)
' 2. Paste EIAFeedstockWorkbookEvents code into ThisWorkbook module
'
' Keyboard shortcuts:
'   Ctrl+D       = Quick update (latest 6 months)
'   Ctrl+Shift+D = Custom month count / update all
' =============================================================================

Option Explicit

' Database connection constants
Private Const DB_SERVER As String = "rlc-commodities.c16c6wm826t7.us-east-2.rds.amazonaws.com"
Private Const DB_PORT As String = "5432"
Private Const DB_NAME As String = "rlc_commodities"
Private Const DB_USER As String = "postgres"
Private Const DB_PASSWORD As String = "SoupBoss1"

' =============================================================================
' MAIN UPDATE FUNCTIONS
' =============================================================================

Public Sub UpdateFeedstockData()
    ' Quick update - latest 6 months from database
    ' Keyboard shortcut: Ctrl+D

    Dim result As VbMsgBoxResult
    result = MsgBox("Update all feedstock/capacity sheets with the latest 6 months from database?" & vbCrLf & vbCrLf & _
                    "Make sure the EIA data has been loaded first." & vbCrLf & _
                    "(python eia_biofuels_collector.py --download)", _
                    vbYesNo + vbQuestion, "EIA Feedstock Updater")

    If result = vbYes Then UpdateFromDatabase 6
End Sub

Public Sub UpdateFeedstockDataCustom()
    ' Custom update with user-specified months
    ' Keyboard shortcut: Ctrl+Shift+D

    Dim monthCount As String
    monthCount = InputBox("How many months to update?" & vbCrLf & vbCrLf & _
                          "Enter a number (e.g., 12 for last 12 months)" & vbCrLf & _
                          "Or enter 0 to update ALL available data", _
                          "EIA Feedstock Updater", "12")

    If monthCount = "" Then Exit Sub

    If Not IsNumeric(monthCount) Then
        MsgBox "Please enter a valid number.", vbExclamation, "EIA Feedstock Updater"
        Exit Sub
    End If

    UpdateFromDatabase CInt(monthCount)
End Sub

' =============================================================================
' DATABASE UPDATE
' =============================================================================

Private Sub UpdateFromDatabase(monthCount As Integer)
    ' Query gold.eia_biofuels_matrix and write values to spreadsheet cells.

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
          "FROM gold.eia_biofuels_matrix "

    If monthCount > 0 Then
        sql = sql & "WHERE (year, month) IN (" & _
              "SELECT DISTINCT year, month FROM bronze.eia_feedstock_monthly " & _
              "ORDER BY year DESC, month DESC LIMIT " & monthCount & ") "
    End If

    sql = sql & "ORDER BY tab_name, year, month, column_number"

    On Error GoTo QueryError
    Set rs = CreateObject("ADODB.Recordset")
    rs.Open sql, conn

    If rs.EOF Then
        MsgBox "No data returned from database." & vbCrLf & vbCrLf & _
               "Make sure the EIA data has been loaded first:" & vbCrLf & _
               "  python eia_biofuels_collector.py --download", _
               vbInformation, "EIA Feedstock Updater"
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
           "Period: " & monthLabel, vbInformation, "EIA Feedstock Updater"

    Exit Sub

QueryError:
    Application.StatusBar = False
    Application.Cursor = xlDefault
    Application.ScreenUpdating = True
    MsgBox "Query error:" & vbCrLf & vbCrLf & Err.Description, vbCritical, "EIA Feedstock Updater"
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
                 "Pwd=" & DB_PASSWORD & ";" & _
                 "sslmode=require;"

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
           vbCritical, "EIA Feedstock Updater - Connection Error"
    Set GetConnection = Nothing
End Function

' =============================================================================
' HELPER FUNCTIONS
' =============================================================================

Private Function FindRowForDate(ws As Worksheet, yr As Integer, mo As Integer, startRow As Integer) As Integer
    ' Find row number for a given year/month by scanning column A for dates.

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

Public Sub AssignFeedstockShortcuts()
    ' Assign Ctrl+Shift+D and Ctrl+Shift+Alt+D shortcuts

    Application.OnKey "^d", "UpdateFeedstockData"
    Application.OnKey "^+d", "UpdateFeedstockDataCustom"
End Sub

Public Sub RemoveFeedstockShortcuts()
    ' Remove keyboard shortcuts (called on workbook close)

    Application.OnKey "^d"
    Application.OnKey "^+d"
End Sub
