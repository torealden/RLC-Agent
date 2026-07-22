Attribute VB_Name = "WASDECompUpdater"
' =============================================================================
' WASDECompUpdater - Pure VBA with PostgreSQL Connection
' =============================================================================
' Updates the wasde_comp tab from the latest 2 WASDE vintages (current +
' prior month), for whichever workbook it's running in:
'
'   us_corn_balance_sheet.xlsx              -> corn block
'   us_wheat_balance_sheet.xlsx             -> wheat block
'   us_soybean_complex_bal_sheets.xlsm      -> soybeans + soybean_meal + soybean_oil blocks
'
' Same module, imported into all three workbooks -- one shared updater instead
' of three separate ones. Detects which workbook it's in by checking which
' balance-sheet tab exists.
'
' Only overwrites cells that are NOT formulas. Total Supply, Total Demand,
' Yield, Stocks-to-Use, and the RLC-linked columns (D/G) are all formulas in
' the sheet already and are left completely alone -- this updater only ever
' touches leaf USDA input cells (Beginning Stocks, Production, Imports, Crush,
' FSI, Feed & Residual, Exports, Ending Stocks). Planted Area and Average
' Farm/Meal/Oil Price are not published by the FAS PSD API for any of these
' commodities, so those rows are never touched either -- whatever is already
' there (manual entry or "n/a") is left as-is.
'
' Data source: gold.fas_us_wasde_comp (see database/views/09_wasde_comp_view.sql)
'
' Keyboard shortcut: Ctrl+Shift+W (bound via Workbook_Open -- see
' WASDECompWorkbookEvents.bas, paste into ThisWorkbook)
'
' DB CREDENTIALS: read at runtime from a local file OUTSIDE the network share
' (see CredentialFilePath below) -- never hardcoded here and never stored on
' the shared drive. See WASDECompUpdater_README.md for one-time setup.
'
' Requirements:
' - PostgreSQL ODBC Driver installed (psqlODBC)
' - Reference to "Microsoft ActiveX Data Objects" (Tools > References)
'
' Installation: see WASDECompUpdater_README.md
' =============================================================================

Option Explicit

' Unit conversion factors (validated against hand-typed WASDE numbers already
' in the sheets -- see conversation/commit notes for the cross-check)
Private Const BU_FACTOR_56LB As Double = 0.03936825    ' corn:            1000 MT -> million bu (56 lb bu)
Private Const BU_FACTOR_60LB As Double = 0.0367437     ' wheat/soybeans:  1000 MT -> million bu (60 lb bu)
Private Const HA_TO_MACRE As Double = 0.00247105       ' area:       1000 HA -> million acres
Private Const MT_TO_KST As Double = 1.102311           ' soybean meal: 1000 MT -> 1000 short tons
Private Const MT_TO_MMLB As Double = 2.204623          ' soybean oil:  1000 MT -> million lbs
Private Const YIELD_FACTOR_CORN As Double = 15.9316    ' corn:            MT/HA -> bu/acre (56 lb bu)
Private Const YIELD_FACTOR_60LB As Double = 14.8673    ' wheat/soybeans:  MT/HA -> bu/acre (60 lb bu)

' =============================================================================
' MAIN ENTRY POINT
' =============================================================================

Public Sub UpdateWASDEComp()
    ' Keyboard shortcut: Ctrl+Shift+W

    Dim confirmMsg As VbMsgBoxResult
    confirmMsg = MsgBox("Update wasde_comp with the latest WASDE data?" & vbCrLf & vbCrLf & _
                         "A timestamped backup of this file will be saved to the same folder first.", _
                         vbYesNo + vbQuestion, "WASDE Comp Updater")
    If confirmMsg <> vbYes Then Exit Sub

    Dim summary As String
    summary = DoUpdate()

    If summary <> "" Then
        MsgBox "wasde_comp updated." & vbCrLf & vbCrLf & summary & vbCrLf & _
               "Review the changes, then save the workbook.", vbInformation, "WASDE Comp Updater"
    End If
End Sub

' Test-only entry point: runs the same update with no confirmation/summary
' dialogs, for automated end-to-end testing via COM on workbook copies.
' Not bound to any shortcut -- never wired to Workbook_Open.
Public Function UpdateWASDEComp_Silent() As String
    UpdateWASDEComp_Silent = DoUpdate()
End Function

Private Function DoUpdate() As String
    Dim conn As Object

    On Error GoTo ErrHandler

    BackupWorkbookCopy

    Application.StatusBar = "Connecting to database..."
    Application.Cursor = xlWait
    DoEvents

    Set conn = GetConnection()
    If conn Is Nothing Then
        Application.StatusBar = False
        Application.Cursor = xlDefault
        DoUpdate = ""
        Exit Function
    End If

    Dim ws As Worksheet
    Set ws = ThisWorkbook.Sheets("wasde_comp")

    Application.StatusBar = "Updating wasde_comp..."
    DoEvents

    Dim summary As String
    summary = ""

    If SheetExists("wheat_balance_sheet") Then
        summary = summary & UpdateBlock(conn, ws, "wheat", 3, 5, 18, 20)
    ElseIf SheetExists("soy_balance_sheet") Then
        summary = summary & UpdateBlock(conn, ws, "soybeans", 3, 5, 19, 0)
        summary = summary & UpdateBlock(conn, ws, "soybean_meal", 22, 24, 32, 0)
        summary = summary & UpdateBlock(conn, ws, "soybean_oil", 35, 37, 47, 0)
    Else
        summary = summary & UpdateBlock(conn, ws, "corn", 3, 5, 18, 20)
    End If

    conn.Close

    Application.StatusBar = False
    Application.Cursor = xlDefault

    DoUpdate = summary
    Exit Function

ErrHandler:
    Application.StatusBar = False
    Application.Cursor = xlDefault
    MsgBox "Update failed:" & vbCrLf & vbCrLf & Err.Description, vbCritical, "WASDE Comp Updater"
    If Not conn Is Nothing Then
        On Error Resume Next
        conn.Close
    End If
    DoUpdate = ""
End Function

' =============================================================================
' DATABASE CONNECTION -- credentials from a local file, never hardcoded here
' =============================================================================

Private Function CredentialFilePath() As String
    ' Deliberately NOT on the network share -- lives only on this machine.
    CredentialFilePath = Environ("USERPROFILE") & "\.rlc_db_credentials"
End Function

Private Function LoadDBCredentials() As Object
    Dim path As String
    path = CredentialFilePath()

    If Dir(path) = "" Then
        Set LoadDBCredentials = Nothing
        Exit Function
    End If

    Dim dict As Object
    Set dict = CreateObject("Scripting.Dictionary")

    Dim fnum As Integer
    fnum = FreeFile
    Open path For Input As #fnum

    Dim ln As String, eqPos As Long, k As String, v As String
    Do While Not EOF(fnum)
        Line Input #fnum, ln
        ln = Trim(ln)
        If Len(ln) > 0 And Left(ln, 1) <> "#" Then
            eqPos = InStr(ln, "=")
            If eqPos > 0 Then
                k = Trim(Left(ln, eqPos - 1))
                v = Trim(Mid(ln, eqPos + 1))
                dict(k) = v
            End If
        End If
    Loop
    Close #fnum

    Set LoadDBCredentials = dict
End Function

Private Function GetConnection() As Object
    Dim creds As Object
    Set creds = LoadDBCredentials()

    If creds Is Nothing Then
        MsgBox "Database credentials not found." & vbCrLf & vbCrLf & _
               "Create this file (one-time setup, see WASDECompUpdater_README.md):" & vbCrLf & _
               CredentialFilePath() & vbCrLf & vbCrLf & _
               "With these lines:" & vbCrLf & _
               "DB_SERVER=..." & vbCrLf & "DB_PORT=5432" & vbCrLf & "DB_NAME=rlc_commodities" & vbCrLf & _
               "DB_USER=..." & vbCrLf & "DB_PASSWORD=...", vbCritical, "WASDE Comp Updater"
        Set GetConnection = Nothing
        Exit Function
    End If

    Dim required As Variant, rk As Variant
    required = Array("DB_SERVER", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD")
    For Each rk In required
        If Not creds.Exists(CStr(rk)) Then
            MsgBox "Credential file is missing " & rk & ":" & vbCrLf & CredentialFilePath(), _
                   vbCritical, "WASDE Comp Updater"
            Set GetConnection = Nothing
            Exit Function
        End If
    Next rk

    Dim conn As Object
    Dim connString As String
    Set conn = CreateObject("ADODB.Connection")

    connString = "Driver={PostgreSQL UNICODE(x64)};" & _
                 "Server=" & creds("DB_SERVER") & ";" & _
                 "Port=" & creds("DB_PORT") & ";" & _
                 "Database=" & creds("DB_NAME") & ";" & _
                 "Uid=" & creds("DB_USER") & ";" & _
                 "Pwd=" & creds("DB_PASSWORD") & ";" & _
                 "sslmode=require;"

    On Error GoTo ConnError
    conn.Open connString
    Set GetConnection = conn
    Exit Function

ConnError:
    MsgBox "Database connection failed:" & vbCrLf & vbCrLf & Err.Description & vbCrLf & vbCrLf & _
           "Make sure the PostgreSQL ODBC driver is installed and " & CredentialFilePath() & " has correct values.", _
           vbCritical, "Connection Error"
    Set GetConnection = Nothing
End Function

' =============================================================================
' BACKUP
' =============================================================================

Private Sub BackupWorkbookCopy()
    Dim baseName As String, ext As String, dotPos As Long
    dotPos = InStrRev(ThisWorkbook.Name, ".")
    baseName = Left(ThisWorkbook.Name, dotPos - 1)
    ext = Mid(ThisWorkbook.Name, dotPos)

    Dim backupPath As String
    backupPath = ThisWorkbook.path & "\" & baseName & "_backup_" & _
                 Format(Now, "yyyymmdd_hhnnss") & ext

    ThisWorkbook.SaveCopyAs backupPath
End Sub

' =============================================================================
' PER-COMMODITY BLOCK UPDATE
' =============================================================================

Private Function UpdateBlock(conn As Object, ws As Worksheet, commodityKey As String, _
                              headerRow As Long, dataStartRow As Long, dataEndRow As Long, _
                              Optional noteRow As Long = 0) As String

    Dim vintages As Object
    Set vintages = LoadVintages(conn, commodityKey)

    Dim mys As Variant
    mys = DistinctMYsAscending(vintages)

    If Not IsArray(mys) Then
        UpdateBlock = "  " & commodityKey & ": no WASDE data found, skipped." & vbCrLf
        Exit Function
    End If
    If (UBound(mys) - LBound(mys) + 1) < 2 Then
        UpdateBlock = "  " & commodityKey & ": fewer than 2 marketing years available, skipped." & vbCrLf
        Exit Function
    End If

    Dim my1 As Long, my2 As Long
    my1 = mys(LBound(mys))
    my2 = mys(LBound(mys) + 1)

    Dim cur1 As Object, prior1 As Object, cur2 As Object, prior2 As Object
    Set cur1 = GetVintage(vintages, my1, 1)
    Set prior1 = GetVintage(vintages, my1, 2)
    Set cur2 = GetVintage(vintages, my2, 1)
    Set prior2 = GetVintage(vintages, my2, 2)

    Dim cellsUpdated As Long
    cellsUpdated = 0

    Dim r As Long
    For r = dataStartRow To dataEndRow
        Dim label As String
        label = CStr(ws.Cells(r, 1).Value)
        If Len(Trim(label)) > 0 Then
            Dim field As String
            field = LabelToField(label)
            If field <> "" Then
                cellsUpdated = cellsUpdated + WriteIfLiteral(ws.Cells(r, 2), FieldValue(cur1, field), commodityKey, field)    ' B = current, MY1
                cellsUpdated = cellsUpdated + WriteIfLiteral(ws.Cells(r, 9), FieldValue(prior1, field), commodityKey, field)  ' I = prior, MY1
                cellsUpdated = cellsUpdated + WriteIfLiteral(ws.Cells(r, 5), FieldValue(cur2, field), commodityKey, field)    ' E = current, MY2
                cellsUpdated = cellsUpdated + WriteIfLiteral(ws.Cells(r, 10), FieldValue(prior2, field), commodityKey, field) ' J = prior, MY2
            End If
        End If
    Next r

    Dim priorMonthName As String, curMonthName As String, curYear As String
    priorMonthName = VintageMonthName(prior1)
    curMonthName = VintageMonthName(cur1)
    curYear = VintageYear(cur1)

    UpdateIfLiteral ws.Cells(headerRow, 9), priorMonthName   ' I{headerRow} = prior vintage month
    ' Build the delta character at runtime (ChrW(916) = "Î”") instead of using a literal
    ' in source -- VBA's Import reads .bas files as ANSI/cp1252, which corrupts a literal
    ' UTF-8 "Î”" into "ÃŽâ€�" on import. ChrW avoids the encoding entirely.
    UpdateIfLiteral ws.Cells(headerRow + 1, 3), ChrW(916) & " from " & priorMonthName ' C{headerRow+1} = "Î” from <Month>"

    If noteRow > 0 Then
        UpdateNoteMonth ws, noteRow, curMonthName & " " & curYear
    End If

    UpdateBlock = "  " & commodityKey & ": " & cellsUpdated & " cells (" & curMonthName & " vs " & priorMonthName & " WASDE)" & vbCrLf
End Function

' =============================================================================
' DATA LOADING
' =============================================================================

Private Function LoadVintages(conn As Object, commodityKey As String) As Object
    Dim dict As Object
    Set dict = CreateObject("Scripting.Dictionary")

    ' Restrict to the 2 most recent marketing years -- gold.fas_us_wasde_comp
    ' carries full history back to 1990, and we only ever want the current
    ' and next MY (the two the latest WASDE actually publishes).
    Dim sql As String
    sql = "SELECT marketing_year, report_date, vintage_rank, area_harvested, yield, beginning_stocks, " & _
          "production, imports, fsi_consumption, feed_dom_consumption, crush, exports, ending_stocks " & _
          "FROM gold.fas_us_wasde_comp WHERE commodity = '" & commodityKey & "' " & _
          "AND marketing_year >= (SELECT MAX(marketing_year) - 1 FROM gold.fas_us_wasde_comp WHERE commodity = '" & commodityKey & "') " & _
          "ORDER BY marketing_year, vintage_rank"

    Dim rs As Object
    Set rs = CreateObject("ADODB.Recordset")
    rs.Open sql, conn

    Dim fieldNames As Variant
    fieldNames = Array("area_harvested", "yield", "beginning_stocks", "production", "imports", _
                        "fsi_consumption", "feed_dom_consumption", "crush", "exports", "ending_stocks")

    Do While Not rs.EOF
        Dim rowDict As Object
        Set rowDict = CreateObject("Scripting.Dictionary")

        Dim fn As Variant
        For Each fn In fieldNames
            rowDict.Add CStr(fn), rs.Fields(CStr(fn)).Value
        Next fn
        rowDict.Add "report_date", rs.Fields("report_date").Value

        Dim key As String
        key = CStr(rs.Fields("marketing_year").Value) & "|" & CStr(rs.Fields("vintage_rank").Value)
        If Not dict.Exists(key) Then dict.Add key, rowDict

        rs.MoveNext
    Loop
    rs.Close

    Set LoadVintages = dict
End Function

Private Function GetVintage(vintages As Object, my As Long, vr As Integer) As Object
    Dim key As String
    key = CStr(my) & "|" & CStr(vr)
    If vintages.Exists(key) Then
        Set GetVintage = vintages(key)
    Else
        Set GetVintage = Nothing
    End If
End Function

Private Function FieldValue(rowDict As Object, field As String) As Variant
    If rowDict Is Nothing Then
        FieldValue = Null
        Exit Function
    End If
    If Not rowDict.Exists(field) Then
        FieldValue = Null
        Exit Function
    End If
    FieldValue = rowDict(field)
End Function

Private Function DistinctMYsAscending(vintages As Object) As Variant
    Dim seen As Object
    Set seen = CreateObject("Scripting.Dictionary")

    Dim key As Variant
    For Each key In vintages.Keys
        Dim myPart As String
        myPart = Split(CStr(key), "|")(0)
        If Not seen.Exists(myPart) Then seen.Add myPart, CLng(myPart)
    Next key

    If seen.Count = 0 Then
        DistinctMYsAscending = Empty
        Exit Function
    End If

    Dim n As Long
    n = seen.Count
    Dim arr() As Long
    ReDim arr(0 To n - 1)

    Dim idx As Long
    idx = 0
    Dim k As Variant
    For Each k In seen.Keys
        arr(idx) = seen(k)
        idx = idx + 1
    Next k

    Dim i As Long, j As Long, t As Long
    For i = 0 To n - 2
        For j = 0 To n - 2 - i
            If arr(j) > arr(j + 1) Then
                t = arr(j): arr(j) = arr(j + 1): arr(j + 1) = t
            End If
        Next j
    Next i

    DistinctMYsAscending = arr
End Function

' =============================================================================
' ROW LABEL -> PSD FIELD MAPPING
' =============================================================================

Private Function LabelToField(label As String) As String
    Dim L As String
    L = LCase(Trim(label))

    If InStr(L, "harvested area") > 0 Then
        LabelToField = "area_harvested"
    ElseIf InStr(L, "yield") > 0 Then
        LabelToField = "yield"   ' only ever lands on I/J -- B/E Yield cells are formulas (Production/Area) and get skipped
    ElseIf InStr(L, "beginning stocks") > 0 Then
        LabelToField = "beginning_stocks"
    ElseIf InStr(L, "production") > 0 Then
        LabelToField = "production"
    ElseIf InStr(L, "imports") > 0 Then
        LabelToField = "imports"
    ElseIf InStr(L, "crush") > 0 Then
        LabelToField = "crush"
    ElseIf InStr(L, "food") > 0 And InStr(L, "seed") > 0 Then
        LabelToField = "fsi_consumption"    ' corn "Food, Seed, & Industrial", wheat "Food + Seed (USDA combined FSI)"
    ElseIf InStr(L, "feed and residual") > 0 Then
        LabelToField = "feed_dom_consumption"
    ElseIf InStr(L, "exports") > 0 Then
        LabelToField = "exports"
    ElseIf InStr(L, "ending stocks") > 0 Then
        LabelToField = "ending_stocks"
    Else
        LabelToField = ""   ' Planted Area, Avg Yield, Total Supply, Seed, Residual, Domestic/Total Demand,
                             ' Stocks-to-Use, Avg Farm/Meal/Oil Price, Biofuel, Food/Feed/Other Industrial,
                             ' Domestic Disappearance -- not in PSD, or already formulas. Left untouched.
    End If
End Function

' =============================================================================
' CELL WRITE HELPERS (never overwrite a formula)
' =============================================================================

Private Function WriteIfLiteral(cell As Range, rawValue As Variant, commodityKey As String, field As String) As Long
    If cell.HasFormula Then
        WriteIfLiteral = 0
        Exit Function
    End If
    If IsNull(rawValue) Then
        WriteIfLiteral = 0
        Exit Function
    End If
    cell.Value = ConvertValue(commodityKey, field, rawValue)
    WriteIfLiteral = 1
End Function

Private Sub UpdateIfLiteral(cell As Range, newText As String)
    If cell.HasFormula Then Exit Sub
    If Len(newText) = 0 Then Exit Sub
    cell.Value = newText
End Sub

Private Function ConvertValue(commodityKey As String, field As String, raw As Variant) As Double
    Dim v As Double
    v = CDbl(raw)

    If field = "area_harvested" Then
        ConvertValue = Round(v * HA_TO_MACRE, 2)
        Exit Function
    End If

    If field = "yield" Then
        Select Case commodityKey
            Case "corn"
                ConvertValue = Round(v * YIELD_FACTOR_CORN, 2)
            Case "wheat", "soybeans"
                ConvertValue = Round(v * YIELD_FACTOR_60LB, 2)
            Case Else
                ConvertValue = v
        End Select
        Exit Function
    End If

    Select Case commodityKey
        Case "corn"
            ConvertValue = Round(v * BU_FACTOR_56LB, 2)
        Case "wheat", "soybeans"
            ConvertValue = Round(v * BU_FACTOR_60LB, 2)
        Case "soybean_meal"
            ConvertValue = Round(v * MT_TO_KST, 2)
        Case "soybean_oil"
            ConvertValue = Round(v * MT_TO_MMLB, 2)
        Case Else
            ConvertValue = v
    End Select
End Function

' =============================================================================
' VINTAGE LABEL HELPERS
' =============================================================================

Private Function VintageMonthName(rowDict As Object) As String
    If rowDict Is Nothing Then
        VintageMonthName = ""
        Exit Function
    End If
    Dim d As Variant
    d = rowDict("report_date")
    If IsNull(d) Then
        VintageMonthName = ""
        Exit Function
    End If
    VintageMonthName = MonthName(Month(CDate(d)))
End Function

Private Function VintageYear(rowDict As Object) As String
    If rowDict Is Nothing Then
        VintageYear = ""
        Exit Function
    End If
    Dim d As Variant
    d = rowDict("report_date")
    If IsNull(d) Then
        VintageYear = ""
        Exit Function
    End If
    VintageYear = CStr(Year(CDate(d)))
End Function

Private Sub UpdateNoteMonth(ws As Worksheet, noteRow As Long, monthYearText As String)
    On Error Resume Next

    Dim cell As Range
    Set cell = ws.Cells(noteRow, 1)
    If cell.HasFormula Then Exit Sub

    Dim txt As String
    txt = CStr(cell.Value)
    If Len(txt) = 0 Then Exit Sub

    Dim p1 As Long, p2 As Long
    p1 = InStr(txt, "WASDE (")
    If p1 > 0 Then
        p1 = p1 + Len("WASDE (")
        p2 = InStr(p1, txt, ")")
        If p2 > p1 Then
            txt = Left(txt, p1 - 1) & monthYearText & Mid(txt, p2)
        End If
    End If

    Dim stalePhrase As String
    stalePhrase = "currently 0 -- only one WASDE vintage exists so far; prior-month columns will diverge once a second release lands"
    If InStr(txt, stalePhrase) > 0 Then
        txt = Replace(txt, stalePhrase, "reflects the change vs. the prior month's WASDE")
    End If

    cell.Value = txt
    On Error GoTo 0
End Sub

' =============================================================================
' MISC HELPERS
' =============================================================================

Private Function SheetExists(sheetName As String) As Boolean
    Dim ws As Worksheet
    On Error Resume Next
    Set ws = ThisWorkbook.Sheets(sheetName)
    On Error GoTo 0
    SheetExists = Not (ws Is Nothing)
End Function

' =============================================================================
' KEYBOARD SHORTCUT
' =============================================================================
' Application.OnKey is a single binding for the whole Excel session, not
' per-workbook. With all three WASDE workbooks open at once, each one's
' Workbook_Open re-registers "^+w", so whichever workbook happened to open
' (or re-trigger this) LAST silently wins -- Ctrl+Shift+W would keep running
' that one workbook's macro regardless of which window is actually active.
'
' Fix: every copy binds to the SAME dispatcher name (UpdateWASDEComp_Dispatch).
' It doesn't matter which workbook's copy of the dispatcher actually ends up
' registered -- the dispatcher itself looks at ActiveWorkbook (not
' ThisWorkbook) and explicitly re-routes to *that* workbook's own
' UpdateWASDEComp by fully-qualified name, so it's always correct regardless
' of open order.

Public Sub AssignWASDEShortcut()
    Application.OnKey "^+w", "UpdateWASDEComp_Dispatch"
End Sub

Public Sub RemoveWASDEShortcut()
    Application.OnKey "^+w"
End Sub

Public Sub UpdateWASDEComp_Dispatch()
    Dim targetName As String
    targetName = ActiveWorkbook.Name

    On Error Resume Next
    Application.Run "'" & targetName & "'!WASDECompUpdater.UpdateWASDEComp"
    If Err.Number <> 0 Then
        MsgBox "Couldn't find the WASDE updater in '" & targetName & "'." & vbCrLf & vbCrLf & _
               "Make sure this is one of the three WASDE workbooks and the macro was imported correctly.", _
               vbExclamation, "WASDE Comp Updater"
    End If
    On Error GoTo 0
End Sub
