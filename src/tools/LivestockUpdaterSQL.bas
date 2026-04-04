' =============================================================================
' LivestockUpdaterSQL - US Livestock Slaughter Data Updater
' =============================================================================
' Updates us_livestock_slaughter.xlsx from gold.livestock_slaughter_flat view.
'
' Target: us_livestock_slaughter.xlsx (sheet: "US Livestock Slaughter")
' Layout: Row 1 = group headers, Row 2 = series headers, Row 3 = units
'         Row 4+ = data, Column A = dates (1st of month)
'
' Column mapping:
'   B = hog_slaughter_head       C = hog_avg_wt           D = hog_production_lbs
'   E = hog_fi_head              F = hog_fi_bg_head       G = hog_fi_sows_head
'   H = (spacer)
'   I = cattle_slaughter_head    J = cattle_avg_wt        K = cattle_production_lbs
'   L = (spacer)
'   M = calves_slaughter_head    N = calves_production_lbs
'   O = (spacer)
'   P = chickens_slaughter_head  Q = chickens_avg_wt      R = chickens_production_lbs
'   S = (spacer)
'   T = broilers_slaughter_head  U = broilers_avg_wt      V = broilers_production_lbs
'   W = (spacer)
'   X = turkeys_slaughter_head   Y = turkeys_avg_wt       Z = turkeys_production_lbs
'
' Keyboard shortcuts: Ctrl+L (quick, last 6 months), Ctrl+Shift+L (custom)
'
' Requirements:
' - PostgreSQL ODBC Driver installed (psqlODBC x64)
' - Reference to "Microsoft ActiveX Data Objects" (Tools > References)
' =============================================================================

Option Explicit

Private Const DB_SERVER As String = "rlc-commodities.c16c6wm826t7.us-east-2.rds.amazonaws.com"
Private Const DB_PORT As String = "5432"
Private Const DB_NAME As String = "rlc_commodities"
Private Const DB_USER As String = "postgres"
Private Const DB_PASSWORD As String = "SoupBoss1"

Private Const DATA_START_ROW As Integer = 4
Private Const DATE_COLUMN As Integer = 1

' Column mapping (must match flat file structure)
' Each entry: (db_field_name, excel_column_number)
Private Type ColumnMap
    dbField As String
    excelCol As Integer
End Type

' =============================================================================
' MAIN UPDATE FUNCTIONS
' =============================================================================

Public Sub UpdateLivestockData()
    ' Quick update - latest 6 months
    ' Keyboard shortcut: Ctrl+L
    UpdateFromDatabase 6
End Sub

Public Sub UpdateLivestockDataCustom()
    ' Custom update with user-specified months
    ' Keyboard shortcut: Ctrl+Shift+L

    Dim monthCount As String
    monthCount = InputBox("How many months of livestock data to update?" & vbCrLf & vbCrLf & _
                          "Enter a number (e.g., 12 for last 12 months)" & vbCrLf & _
                          "Or enter 0 to update ALL available data (from 2000)", _
                          "Livestock Data Updater", "12")

    If monthCount = "" Then Exit Sub
    If Not IsNumeric(monthCount) Then
        MsgBox "Please enter a valid number.", vbExclamation, "Livestock Updater"
        Exit Sub
    End If

    UpdateFromDatabase CInt(monthCount)
End Sub

' =============================================================================
' DATABASE UPDATE ENGINE
' =============================================================================

Private Sub UpdateFromDatabase(monthLimit As Integer)
    Dim conn As Object
    Dim rs As Object
    Dim sql As String
    Dim ws As Worksheet
    Dim updatedRows As Long

    Application.StatusBar = "Connecting to database..."
    Application.ScreenUpdating = False

    Set ws = ActiveSheet

    ' Build SQL
    sql = "SELECT price_date, " & _
          "hog_slaughter_head, hog_avg_wt, hog_production_lbs, " & _
          "hog_fi_head, hog_fi_bg_head, hog_fi_sows_head, " & _
          "cattle_slaughter_head, cattle_avg_wt, cattle_production_lbs, " & _
          "calves_slaughter_head, calves_production_lbs, " & _
          "chickens_slaughter_head, chickens_avg_wt, chickens_production_lbs, " & _
          "broilers_slaughter_head, broilers_avg_wt, broilers_production_lbs, " & _
          "turkeys_slaughter_head, turkeys_avg_wt, turkeys_production_lbs " & _
          "FROM gold.livestock_slaughter_flat " & _
          "WHERE year >= 2000 "

    If monthLimit > 0 Then
        sql = sql & "AND price_date >= (CURRENT_DATE - INTERVAL '" & monthLimit & " months') "
    End If

    sql = sql & "ORDER BY price_date"

    ' Connect
    Set conn = GetConnection()
    If conn Is Nothing Then GoTo Cleanup

    Application.StatusBar = "Querying livestock slaughter data..."

    Set rs = CreateObject("ADODB.Recordset")
    rs.Open sql, conn

    If rs.EOF Then
        MsgBox "No data returned from database.", vbInformation, "Livestock Updater"
        GoTo Cleanup
    End If

    ' Process records
    updatedRows = 0
    Do While Not rs.EOF
        Dim targetDate As Date
        targetDate = rs("price_date")

        ' Find or create row for this date
        Dim targetRow As Long
        targetRow = FindOrCreateDateRow(ws, targetDate)

        If targetRow > 0 Then
            ' Hogs (B-G)
            SafePlace ws, targetRow, 2, rs("hog_slaughter_head")
            SafePlace ws, targetRow, 3, rs("hog_avg_wt")
            SafePlace ws, targetRow, 4, rs("hog_production_lbs")
            SafePlace ws, targetRow, 5, rs("hog_fi_head")
            SafePlace ws, targetRow, 6, rs("hog_fi_bg_head")
            SafePlace ws, targetRow, 7, rs("hog_fi_sows_head")
            ' Cattle (I-K)
            SafePlace ws, targetRow, 9, rs("cattle_slaughter_head")
            SafePlace ws, targetRow, 10, rs("cattle_avg_wt")
            SafePlace ws, targetRow, 11, rs("cattle_production_lbs")
            ' Calves (M-N)
            SafePlace ws, targetRow, 13, rs("calves_slaughter_head")
            SafePlace ws, targetRow, 14, rs("calves_production_lbs")
            ' Chickens (P-R)
            SafePlace ws, targetRow, 16, rs("chickens_slaughter_head")
            SafePlace ws, targetRow, 17, rs("chickens_avg_wt")
            SafePlace ws, targetRow, 18, rs("chickens_production_lbs")
            ' Broilers (T-V)
            SafePlace ws, targetRow, 20, rs("broilers_slaughter_head")
            SafePlace ws, targetRow, 21, rs("broilers_avg_wt")
            SafePlace ws, targetRow, 22, rs("broilers_production_lbs")
            ' Turkeys (X-Z)
            SafePlace ws, targetRow, 24, rs("turkeys_slaughter_head")
            SafePlace ws, targetRow, 25, rs("turkeys_avg_wt")
            SafePlace ws, targetRow, 26, rs("turkeys_production_lbs")

            updatedRows = updatedRows + 1
        End If

        rs.MoveNext
    Loop

    MsgBox "Updated " & updatedRows & " months of livestock slaughter data.", _
           vbInformation, "Livestock Updater"

Cleanup:
    If Not rs Is Nothing Then
        If rs.State = 1 Then rs.Close
        Set rs = Nothing
    End If
    If Not conn Is Nothing Then
        If conn.State = 1 Then conn.Close
        Set conn = Nothing
    End If
    Application.StatusBar = False
    Application.ScreenUpdating = True
End Sub

' =============================================================================
' HELPER FUNCTIONS
' =============================================================================

Private Function FindOrCreateDateRow(ws As Worksheet, targetDate As Date) As Long
    ' Find existing row with this date, or insert a new one in chronological order
    Dim r As Long
    Dim cellDate As Variant

    For r = DATA_START_ROW To ws.Cells(ws.Rows.Count, DATE_COLUMN).End(xlUp).Row
        cellDate = ws.Cells(r, DATE_COLUMN).Value
        If IsDate(cellDate) Then
            If CDate(cellDate) = targetDate Then
                FindOrCreateDateRow = r
                Exit Function
            ElseIf CDate(cellDate) > targetDate Then
                ' Insert row before this one
                ws.Rows(r).Insert Shift:=xlDown
                ws.Cells(r, DATE_COLUMN).Value = targetDate
                ws.Cells(r, DATE_COLUMN).NumberFormat = "MMM-YY"
                FindOrCreateDateRow = r
                Exit Function
            End If
        End If
    Next r

    ' Append at end
    r = ws.Cells(ws.Rows.Count, DATE_COLUMN).End(xlUp).Row + 1
    ws.Cells(r, DATE_COLUMN).Value = targetDate
    ws.Cells(r, DATE_COLUMN).NumberFormat = "MMM-YY"
    FindOrCreateDateRow = r
End Function

Private Sub SafePlace(ws As Worksheet, row As Long, col As Integer, val As Variant)
    ' Place value if not null
    If Not IsNull(val) Then
        ws.Cells(row, col).Value = val
        ws.Cells(row, col).NumberFormat = "#,##0"
    End If
End Sub

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
' KEYBOARD SHORTCUT ASSIGNMENT
' =============================================================================

Public Sub AssignLivestockShortcuts()
    Application.OnKey "^l", "UpdateLivestockData"         ' Ctrl+L
    Application.OnKey "^+l", "UpdateLivestockDataCustom"   ' Ctrl+Shift+L
End Sub

Public Sub RemoveLivestockShortcuts()
    Application.OnKey "^l"
    Application.OnKey "^+l"
End Sub
