Attribute VB_Name = "CornProductsUpdater"
' =============================================================================
' CornProductsUpdater - updates the corn_products tab of us_grain_crush.xlsm
' =============================================================================
' Reads gold.corn_products_wide (ERS corn-input + sweetener production,
' hybrid-monthlyized) and writes each month's values into the corn_products tab.
'
' Layout: date in column A (first-of-month), data rows 3-146 (Jan-2015 ->
' Dec-2026). Each gold row carries a target_col letter and a display_value.
' The view emits only data columns B,C,D,E,F,G,I,J,K -- it never emits the
' in-sheet formula columns (H, Q, R, S) or the placeholder stocks (N,O,P) or
' the yield-assumption block (U:V), so those are left untouched. L/M (flour/
' meal/grits, hominy) are not produced (no yields supplied) and stay as-is.
'
' Blanks, not zeros: months with no data are absent from the view, so their
' cells are left untouched (preserving blanks the formulas depend on).
'
' Keyboard shortcut: Ctrl+J (full refresh).  (corn_grind=Ctrl+K, ethanol=Ctrl+L)
'
' Requires psqlODBC (x64) + Microsoft ActiveX Data Objects reference.
' =============================================================================

Option Explicit

Private Const DB_SERVER As String = "rlc-commodities.c16c6wm826t7.us-east-2.rds.amazonaws.com"
Private Const DB_PORT As String = "5432"
Private Const DB_NAME As String = "rlc_commodities"
Private Const DB_USER As String = "postgres"
Private Const DB_PASSWORD As String = "SoupBoss1"

Private Const SHEET_NAME As String = "corn_products"
Private Const DATE_COL As Integer = 1
Private Const DATA_START_ROW As Integer = 3
Private Const DATA_END_ROW As Integer = 146

Public Sub UpdateCornProducts()
    UpdateFromDatabase
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
    MsgBox "Database connection failed:" & vbCrLf & vbCrLf & Err.Description, vbCritical, "Corn Products Updater"
    Set GetConnection = Nothing
End Function

Private Sub UpdateFromDatabase()
    Dim conn As Object, rs As Object, ws As Worksheet
    Dim cellsUpdated As Long, notFound As Long

    Set ws = ThisWorkbook.Worksheets(SHEET_NAME)

    Dim savedCalc As Long, savedScreen As Boolean, savedEvents As Boolean
    savedCalc = Application.Calculation: savedScreen = Application.ScreenUpdating: savedEvents = Application.EnableEvents
    Application.Calculation = xlCalculationManual: Application.ScreenUpdating = False: Application.EnableEvents = False
    Application.Cursor = xlWait
    On Error GoTo CleanupExit

    Set conn = GetConnection()
    If conn Is Nothing Then GoTo CleanupExit

    ' map year*100+month -> sheet row (single col-A scan)
    Dim rowByYM As Object: Set rowByYM = CreateObject("Scripting.Dictionary")
    Dim r As Integer, v As Variant
    For r = DATA_START_ROW To DATA_END_ROW
        v = ws.Cells(r, DATE_COL).Value
        If IsDate(v) Then rowByYM(Year(v) * 100 + Month(v)) = r
    Next r

    Set rs = CreateObject("ADODB.Recordset")
    rs.Open "SELECT year, month, target_col, display_value FROM gold.corn_products_wide", _
            conn, 3, 1, 1
    Do While Not rs.EOF
        Dim ym As Long, tgt As String, val As Variant, targetRow As Variant
        ym = rs("year") * 100 + rs("month")
        tgt = rs("target_col")
        val = rs("display_value")
        If rowByYM.Exists(ym) Then
            targetRow = rowByYM(ym)
            If Not IsNull(val) Then ws.Range(tgt & targetRow).Value = val: cellsUpdated = cellsUpdated + 1
        Else
            notFound = notFound + 1
        End If
        rs.MoveNext
    Loop
    rs.Close: conn.Close
    Application.Calculation = xlCalculationAutomatic: Application.Calculate

    MsgBox "corn_products update complete." & vbCrLf & vbCrLf & _
           "Cells updated: " & cellsUpdated & vbCrLf & _
           "Months not on sheet: " & notFound, vbInformation, "Corn Products Updater"

CleanupExit:
    Application.Calculation = savedCalc: Application.ScreenUpdating = savedScreen: Application.EnableEvents = savedEvents
    Application.Cursor = xlDefault: Application.StatusBar = False
    If Err.Number <> 0 Then MsgBox "Error: " & Err.Description, vbCritical, "Corn Products Updater"
    On Error Resume Next
    If Not rs Is Nothing Then If rs.State = 1 Then rs.Close
    If Not conn Is Nothing Then If conn.State = 1 Then conn.Close
End Sub

Public Sub AssignCornProductsShortcuts()
    Application.OnKey "^j", "CornProductsUpdater.UpdateCornProducts"
    ShortcutsHelper.ShowShortcutBanner _
        "Corn Products Updater (ERS sweeteners/starch)", _
        "Ctrl+J", "Full refresh", "", ""
End Sub

Public Sub RemoveCornProductsShortcuts()
    Application.OnKey "^j"
End Sub
