Attribute VB_Name = "EthanolUpdater"
' =============================================================================
' EthanolUpdater - updates the ethanol tabs of us_grain_crush.xlsm
' =============================================================================
' Handles BOTH ethanol tabs based on the active sheet:
'   weekly_ethanol_production  -> gold.weekly_ethanol_matrix  (match by week-end)
'   monthly_ethanol_data       -> gold.monthly_ethanol_matrix (match by month)
'
' Values are raw EIA (no conversion) -- the tabs are built around native EIA
' sourcekeys; in-sheet formulas handle the derived/ratio columns. Date is in
' column A, data rows start at row 5. Only the gold view's target columns are
' written; computed/Census columns are left alone.
'
' Keyboard shortcut: Ctrl+L (quick, latest 52 weeks / 24 months),
'                    Ctrl+Shift+L (full history).
'
' Requires psqlODBC (x64) + Microsoft ActiveX Data Objects reference.
' =============================================================================

Option Explicit

Private Const DB_SERVER As String = "rlc-commodities.c16c6wm826t7.us-east-2.rds.amazonaws.com"
Private Const DB_PORT As String = "5432"
Private Const DB_NAME As String = "rlc_commodities"
Private Const DB_USER As String = "postgres"
Private Const DB_PASSWORD As String = "SoupBoss1"

Private Const DATE_COL As Integer = 1
Private Const DATA_START_ROW As Integer = 5

Public Sub UpdateEthanol()
    UpdateFromDatabase False   ' Ctrl+L  - recent
End Sub

Public Sub UpdateEthanolAll()
    UpdateFromDatabase True    ' Ctrl+Shift+L - full history
End Sub

Private Function GetConnection() As Object
    Dim conn As Object, connStr As String
    Set conn = CreateObject("ADODB.Connection")
    connStr = "Driver={PostgreSQL UNICODE(x64)};Server=" & DB_SERVER & ";Port=" & DB_PORT & _
              ";Database=" & DB_NAME & ";Uid=" & DB_USER & ";Pwd=" & DB_PASSWORD & ";sslmode=require;"
    On Error GoTo ConnError
    conn.Open connStr
    conn.CommandTimeout = 120
    Set GetConnection = conn
    Exit Function
ConnError:
    MsgBox "Database connection failed:" & vbCrLf & vbCrLf & Err.Description, vbCritical, "Ethanol Updater"
    Set GetConnection = Nothing
End Function

Private Sub UpdateFromDatabase(fullHistory As Boolean)
    Dim conn As Object, rs As Object, ws As Worksheet
    Dim sheetName As String, isWeekly As Boolean, viewName As String, dateCol As String
    Dim sql As String, cellsUpdated As Long, notFound As Long

    Set ws = ActiveSheet
    sheetName = LCase(ws.Name)
    If InStr(sheetName, "weekly_ethanol") > 0 Then
        isWeekly = True: viewName = "gold.weekly_ethanol_matrix": dateCol = "week_ending"
    ElseIf InStr(sheetName, "monthly_ethanol") > 0 Then
        isWeekly = False: viewName = "gold.monthly_ethanol_matrix": dateCol = "month_date"
    Else
        MsgBox "Run this on weekly_ethanol_production or monthly_ethanol_data.", vbExclamation, "Ethanol Updater"
        Exit Sub
    End If

    Dim savedCalc As Long, savedScreen As Boolean, savedEvents As Boolean
    savedCalc = Application.Calculation: savedScreen = Application.ScreenUpdating: savedEvents = Application.EnableEvents
    Application.Calculation = xlCalculationManual: Application.ScreenUpdating = False: Application.EnableEvents = False
    Application.Cursor = xlWait
    On Error GoTo CleanupExit

    Set conn = GetConnection()
    If conn Is Nothing Then GoTo CleanupExit

    ' Map sheet dates (col A, rows 5+) to row numbers.
    '   weekly  -> key = date serial (CLng)
    '   monthly -> key = year*100 + month
    Dim lastRow As Long: lastRow = ws.Cells(ws.Rows.Count, DATE_COL).End(xlUp).Row
    Dim rowByKey As Object: Set rowByKey = CreateObject("Scripting.Dictionary")
    Dim r As Long, v As Variant
    For r = DATA_START_ROW To lastRow
        v = ws.Cells(r, DATE_COL).Value
        If IsDate(v) Then
            If isWeekly Then
                rowByKey(CLng(CDate(v))) = r
            Else
                rowByKey(Year(v) * 100 + Month(v)) = r
            End If
        End If
    Next r

    sql = "SELECT " & dateCol & " AS d, target_col, value FROM " & viewName
    If Not fullHistory Then
        Dim lim As Integer: lim = IIf(isWeekly, 52, 24)
        sql = sql & " WHERE " & dateCol & " IN (SELECT DISTINCT " & dateCol & " FROM " & viewName & _
              " ORDER BY 1 DESC LIMIT " & lim & ")"
    End If

    Set rs = CreateObject("ADODB.Recordset")
    rs.Open sql, conn, 3, 1, 1
    Do While Not rs.EOF
        Dim dval As Date, tgt As String, val As Variant, key As Long, targetRow As Variant
        dval = rs("d"): tgt = rs("target_col"): val = rs("value")
        If isWeekly Then key = CLng(dval) Else key = Year(dval) * 100 + Month(dval)
        targetRow = FindRow(rowByKey, key, isWeekly)
        If targetRow > 0 Then
            If Not IsNull(val) Then ws.Range(tgt & targetRow).Value = val: cellsUpdated = cellsUpdated + 1
        Else
            notFound = notFound + 1
        End If
        rs.MoveNext
    Loop
    rs.Close: conn.Close
    Application.Calculation = xlCalculationAutomatic: Application.Calculate

    MsgBox "Ethanol update complete (" & ws.Name & ")." & vbCrLf & vbCrLf & _
           "Cells updated: " & cellsUpdated & vbCrLf & "Periods not on sheet: " & notFound, _
           vbInformation, "Ethanol Updater"

CleanupExit:
    Application.Calculation = savedCalc: Application.ScreenUpdating = savedScreen: Application.EnableEvents = savedEvents
    Application.Cursor = xlDefault: Application.StatusBar = False
    If Err.Number <> 0 Then MsgBox "Error: " & Err.Description, vbCritical, "Ethanol Updater"
    On Error Resume Next
    If Not rs Is Nothing Then If rs.State = 1 Then rs.Close
    If Not conn Is Nothing Then If conn.State = 1 Then conn.Close
End Sub

Private Function FindRow(rowByKey As Object, key As Long, isWeekly As Boolean) As Long
    ' Exact match; for weekly also try +/- 3 days (EIA week-ending vs sheet
    ' alignment can differ by a couple days).
    Dim k As Long
    If rowByKey.Exists(key) Then FindRow = rowByKey(key): Exit Function
    If isWeekly Then
        For k = key - 3 To key + 3
            If rowByKey.Exists(k) Then FindRow = rowByKey(k): Exit Function
        Next k
    End If
    FindRow = 0
End Function

Public Sub AssignEthanolShortcuts()
    Application.OnKey "^l", "EthanolUpdater.UpdateEthanol"
    Application.OnKey "^+l", "EthanolUpdater.UpdateEthanolAll"
    ShortcutsHelper.ShowShortcutBanner _
        "Ethanol Updater (EIA weekly + monthly)", _
        "Ctrl+L", "Quick update", _
        "Ctrl+Shift+L", "Full history"
End Sub

Public Sub RemoveEthanolShortcuts()
    Application.OnKey "^l"
    Application.OnKey "^+l"
End Sub
