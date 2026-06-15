Attribute VB_Name = "CornGrindUpdater"
' =============================================================================
' CornGrindUpdater - updates the corn_grind tab of us_grain_crush.xlsm
' =============================================================================
' Reads gold.corn_grind_monthly (NASS Grain Crushings PDF co-products/usage +
' Fats & Oils corn oil) and writes each month's values into the corn_grind tab.
'
' Layout: date in column A (first-of-month), data rows 3-146 (Jan-2015 ->
' Dec-2026). Each gold row carries a target_col letter (C-H, J-T, U-W, Y-AA)
' and a converted display_value. Computed columns (B total, S, X, AB, AC) are
' in-sheet formulas and are never written.
'
' Keyboard shortcuts:  Ctrl+K = quick (latest 12 months),  Ctrl+Shift+K = all.
'
' Requires psqlODBC (x64) + Microsoft ActiveX Data Objects reference.
' =============================================================================

Option Explicit

Private Const DB_SERVER As String = "rlc-commodities.c16c6wm826t7.us-east-2.rds.amazonaws.com"
Private Const DB_PORT As String = "5432"
Private Const DB_NAME As String = "rlc_commodities"
Private Const DB_USER As String = "postgres"
Private Const DB_PASSWORD As String = "SoupBoss1"

Private Const SHEET_NAME As String = "corn_grind"
Private Const DATE_COL As Integer = 1
Private Const DATA_START_ROW As Integer = 3
Private Const DATA_END_ROW As Integer = 146

Public Sub UpdateCornGrind()
    ' Ctrl+K - latest 12 months
    UpdateFromDatabase 12
End Sub

Public Sub UpdateCornGrindAll()
    ' Ctrl+Shift+K - full history
    UpdateFromDatabase 0
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
    MsgBox "Database connection failed:" & vbCrLf & vbCrLf & Err.Description, vbCritical, "Corn Grind Updater"
    Set GetConnection = Nothing
End Function

Private Sub UpdateFromDatabase(monthCount As Integer)
    Dim conn As Object, rs As Object, ws As Worksheet
    Dim sql As String, cellsUpdated As Long, rowsNotFound As Long

    Set ws = ThisWorkbook.Worksheets(SHEET_NAME)

    Dim savedCalc As Long, savedScreen As Boolean, savedEvents As Boolean
    savedCalc = Application.Calculation: savedScreen = Application.ScreenUpdating: savedEvents = Application.EnableEvents
    Application.Calculation = xlCalculationManual: Application.ScreenUpdating = False: Application.EnableEvents = False
    Application.Cursor = xlWait
    On Error GoTo CleanupExit

    Set conn = GetConnection()
    If conn Is Nothing Then GoTo CleanupExit

    ' Map each month present in column A to its sheet row (single scan).
    Dim rowByYM As Object: Set rowByYM = CreateObject("Scripting.Dictionary")
    Dim r As Integer, v As Variant
    For r = DATA_START_ROW To DATA_END_ROW
        v = ws.Cells(r, DATE_COL).Value
        If IsDate(v) Then rowByYM(Year(v) * 100 + Month(v)) = r
    Next r

    sql = "SELECT year, month, target_col, display_value FROM gold.corn_grind_monthly"
    If monthCount > 0 Then
        sql = sql & " WHERE (year*100+month) IN (SELECT DISTINCT year*100+month " & _
              "FROM gold.corn_grind_monthly ORDER BY 1 DESC LIMIT " & monthCount & ")"
    End If

    Set rs = CreateObject("ADODB.Recordset")
    rs.Open sql, conn, 3, 1, 1   ' adOpenStatic, adLockReadOnly, adCmdText

    Do While Not rs.EOF
        Dim ym As Long, tgt As String, val As Variant, targetRow As Variant
        ym = rs("year") * 100 + rs("month")
        tgt = rs("target_col")
        val = rs("display_value")
        If rowByYM.Exists(ym) Then
            targetRow = rowByYM(ym)
            If Not IsNull(val) Then
                ws.Range(tgt & targetRow).Value = val
                cellsUpdated = cellsUpdated + 1
            End If
        Else
            rowsNotFound = rowsNotFound + 1
        End If
        rs.MoveNext
    Loop
    rs.Close: conn.Close

    Application.Calculation = xlCalculationAutomatic: Application.Calculate

    MsgBox "corn_grind update complete." & vbCrLf & vbCrLf & _
           "Cells updated: " & cellsUpdated & vbCrLf & _
           "Months not on sheet: " & rowsNotFound, vbInformation, "Corn Grind Updater"

CleanupExit:
    Application.Calculation = savedCalc: Application.ScreenUpdating = savedScreen: Application.EnableEvents = savedEvents
    Application.Cursor = xlDefault: Application.StatusBar = False
    If Err.Number <> 0 Then MsgBox "Error: " & Err.Description, vbCritical, "Corn Grind Updater"
    On Error Resume Next
    If Not rs Is Nothing Then If rs.State = 1 Then rs.Close
    If Not conn Is Nothing Then If conn.State = 1 Then conn.Close
End Sub

Public Sub AssignCornGrindShortcuts()
    Application.OnKey "^k", "CornGrindUpdater.UpdateCornGrind"
    Application.OnKey "^+k", "CornGrindUpdater.UpdateCornGrindAll"
    ShortcutsHelper.ShowShortcutBanner _
        "Corn Grind Updater (NASS GCCP + Fats & Oils)", _
        "Ctrl+K", "Quick update (12 mo)", _
        "Ctrl+Shift+K", "Full history"
End Sub

Public Sub RemoveCornGrindShortcuts()
    Application.OnKey "^k"
    Application.OnKey "^+k"
End Sub
