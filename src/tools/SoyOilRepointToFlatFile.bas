Attribute VB_Name = "SoyOilRepointToFlatFile"
' =====================================================================================
' SoyOilRepointToFlatFile  --  retire eia_data.xlsm and the ff_ mirror tabs
' =====================================================================================
' Ruled by Tore 2026-07-21: "just have the DB output to the flat file, which we link to
' the balance sheets."
'
' WHY THIS IS VBA AND NOT PYTHON: openpyxl rewrites an .xlsm wholesale and can silently
' drop charts, conditional formatting, defined names and VBA. This workbook is the one
' Tore actually works in, so the edit happens IN Excel, in place, touching only formulas.
'
' WHAT IT REPLACES
'   1. 2,292 refs to [1]eia_data.xlsm (biodiesel / renewable_diesel / SAF / co-processing
'      blocks). eia_data.xlsm was 4,078 mil lb light on CY2024 soybean oil because its RD
'      tab was never refreshed from its own source table -- that stale intermediate is the
'      whole reason for retiring it.
'   2. 6,720 SUMIFS/COUNTIFS/MAXIFS against the ff_sbo_supply / ff_sbo_demand mirror tabs,
'      which themselves hold ~144,000 external refs to the flat file. The mirrors existed
'      only because SUMIFS cannot read a CLOSED workbook -- plain cell refs can. The flat
'      file now ships pre-pivoted _wide tabs, so plain refs work and the mirrors can go.
'
' GRID ALIGNMENT: the _wide tabs are anchored to THIS sheet's grid -- column B = MY 1990/91,
' so column AI = MY 2023/24 in both. Columns map 1:1; only the row offset differs, and that
' is read from the flat file's _wide_index tab rather than assumed.
'
' Blocks are located BY TITLE, never by hardcoded row -- this sheet's rows have shifted
' before (the biodiesel block moved from 108 to 115) and will again.
'
' RUN ORDER
'   1. RepointSoyOilPreview   -- reports every change it WOULD make. Changes nothing.
'   2. RepointSoyOilApply     -- makes them. Prompts, and saves a timestamped backup first.
'   3. RepointSoyOilCleanup   -- only after you have checked the numbers: deletes the ff_
'                               mirror tabs and breaks the eia_data.xlsm link. Irreversible.
' =====================================================================================
Option Explicit

' Unattended mode: when gSilent is True, dialogs are captured into gResult instead of
' shown, so the macro can be driven headless (COM/Application.Run) for verification and
' automated apply. The interactive wrappers below leave gSilent False and behave as before.
Public gSilent As Boolean
Public gResult As String

Private Const BAL_SHEET As String = "soyoil_balance_sheet"
Private Const FF_NAME As String = "us_soybean_oil_supply_demand.xlsx"
Private Const N_MONTHS As Long = 12
Private Const MY_ANCHOR As Long = 1990   ' marketing year in column B, BOTH grids
Private Const MY_COL0 As Long = 2        ' column B

' All module-level Const/Dim MUST stay above the first procedure (the declarations
' section); a Const placed after a Sub compiles as "Variable not defined".
Private Sub Say(ByVal msg As String, ByVal style As VbMsgBoxStyle)
    If gSilent Then
        gResult = gResult & msg & vbLf & "----" & vbLf
    Else
        MsgBox msg, style, "Repoint soyoil balance sheet"
    End If
End Sub

' block title prefix | wide tab | series key in _wide_index
Private Function BlockMap() As Variant
    BlockMap = Array( _
        Array("US SOYBEAN OIL PRODUCTION", "soybean_oil_supply_wide", "production"), _
        Array("US SOYBEAN OIL IMPORTS", "soybean_oil_supply_wide", "imports"), _
        Array("US SOYBEAN OIL EXPORTS", "soybean_oil_supply_wide", "exports"), _
        Array("US SOYBEAN OIL BIODIESEL USE", "soybean_oil_demand_wide", "biofuel_use_biodiesel"), _
        Array("US SOYBEAN OIL RENEWABLE DIESEL USE", "soybean_oil_demand_wide", "biofuel_use_renewable_diesel"), _
        Array("US SOYBEAN OIL SUSTAINABLE AVIATION FUEL USE", "soybean_oil_demand_wide", "biofuel_use_saf"), _
        Array("US SOYBEAN OIL CO-PROCESSING USE", "soybean_oil_demand_wide", "biofuel_use_coprocessing"), _
        Array("US SOYBEAN OIL SALAD AND COOKING OIL DOMESTIC USE", "soybean_oil_demand_wide", "nonbiofuel_use_salad_cooking_oil"), _
        Array("US SOYBEAN OIL BAKING AND FRYING DOMESTIC USE", "soybean_oil_demand_wide", "nonbiofuel_use_baking_frying_fats"), _
        Array("US SOYBEAN OIL MARGARINE DOMESTIC USE", "soybean_oil_demand_wide", "nonbiofuel_use_margarine"), _
        Array("US SOYBEAN OIL MISC EDIBLE DOMESTIC USE", "soybean_oil_demand_wide", "nonbiofuel_use_other_edible"), _
        Array("US SOYBEAN OIL PLASTICS AND RESINS DOMESTIC USE", "soybean_oil_demand_wide", "nonbiofuel_use_resins_plastics"), _
        Array("US SOYBEAN OIL PAINT AND VARNISH OIL DOMESTIC USE", "soybean_oil_demand_wide", "nonbiofuel_use_paint_varnish"), _
        Array("US SOYBEAN OIL OTHER INEDIBLE", "soybean_oil_demand_wide", "nonbiofuel_use_other_inedible"))
    ' DELIBERATELY NOT TOUCHED (all derived on-sheet, or fed from another chain):
    '   YIELD, BIOMASS-BASED DIESEL USE (=sum of the four fuel blocks),
    '   NON-BIOFUEL DOMESTIC USE (=domestic use - BBD), DOMESTIC USE,
    '   MONTH-ENDING STOCKS (fed from us_soybean_production.xlsx, a separate chain).
End Function

Private Function FlatFilePath() As String
    FlatFilePath = ThisWorkbook.Path & Application.PathSeparator & FF_NAME
End Function

' Find the row of a block title in column A (prefix match, case-insensitive). 0 if absent.
Private Function FindBlockRow(ws As Worksheet, titlePrefix As String) As Long
    Dim lastRow As Long, r As Long, v As String
    lastRow = ws.Cells(ws.Rows.Count, 1).End(xlUp).Row
    For r = 1 To lastRow
        v = Trim$(CStr(ws.Cells(r, 1).Value))
        If Len(v) >= Len(titlePrefix) Then
            If StrComp(Left$(v, Len(titlePrefix)), titlePrefix, vbTextCompare) = 0 Then
                FindBlockRow = r
                Exit Function
            End If
        End If
    Next r
    FindBlockRow = 0
End Function

' Read series anchors from the flat file's _wide_index tab, and compute each series'
' LASTACTUAL rank by scanning its wide block. Keys per "tab|series":
'   (bare)       -> first_month_row
'   |LASTMY      -> last marketing year the DB renders (the write cap)
'   |LASTACTUAL  -> chronological rank (col-MY_COL0)*12 + month-offset of the last non-blank
'                   cell. Blanks at/before it are historical GAPS (write 0 so downstream
'                   arithmetic works); blanks after it are the forward FORECAST HOLE (write
'                   "" so the missing forecast stays visibly blank). -1 if the series is empty.
Private Function LoadWideAnchors(ffWb As Workbook) As Object
    Dim d As Object, ws As Worksheet, r As Long, lastRow As Long
    Dim tabName As String, seriesName As String, firstRow As Long, lastMy As Long
    Dim dataWs As Worksheet, col As Long, k As Long, endCol As Long, rnk As Long, maxRnk As Long
    Set d = CreateObject("Scripting.Dictionary")
    On Error Resume Next
    Set ws = ffWb.Worksheets("_wide_index")
    On Error GoTo 0
    If ws Is Nothing Then Exit Function
    lastRow = ws.Cells(ws.Rows.Count, 1).End(xlUp).Row
    For r = 2 To lastRow
        tabName = Trim$(CStr(ws.Cells(r, 1).Value))
        seriesName = Trim$(CStr(ws.Cells(r, 2).Value))
        If Len(tabName) > 0 And Len(seriesName) > 0 Then
            ' col 5 = first_month_row, col 10 = last_my (the last MY the DB actually renders)
            firstRow = CLng(ws.Cells(r, 5).Value)
            lastMy = CLng(ws.Cells(r, 10).Value)
            d(tabName & "|" & seriesName) = firstRow
            d(tabName & "|" & seriesName & "|LASTMY") = lastMy
            maxRnk = -1
            endCol = MY_COL0 + (lastMy - MY_ANCHOR)
            On Error Resume Next
            Set dataWs = ffWb.Worksheets(tabName)
            On Error GoTo 0
            If Not dataWs Is Nothing Then
                For col = MY_COL0 To endCol
                    For k = 0 To N_MONTHS - 1
                        If Len(Trim$(CStr(dataWs.Cells(firstRow + k, col).Value))) > 0 Then
                            rnk = (col - MY_COL0) * N_MONTHS + k
                            If rnk > maxRnk Then maxRnk = rnk
                        End If
                    Next k
                Next col
            End If
            d(tabName & "|" & seriesName & "|LASTACTUAL") = maxRnk
            Set dataWs = Nothing
        End If
    Next r
    Set LoadWideAnchors = d
End Function

' Last populated marketing-year column on the balance sheet, read from a block's header row.
Private Function LastMYCol(ws As Worksheet, headerRow As Long) As Long
    Dim c As Long
    c = ws.Cells(headerRow, ws.Columns.Count).End(xlToLeft).Column
    If c < 2 Then c = 2
    LastMYCol = c
End Function

Private Sub RunRepoint(ByVal applyIt As Boolean)
    Dim ws As Worksheet, ffWb As Workbook, anchors As Object
    Dim m As Variant, i As Long, blockRow As Long, wideRow As Long
    Dim col As Long, lastCol As Long, k As Long
    Dim ffPath As String, refBase As String, msg As String
    Dim nCells As Long, nBlocks As Long, missing As String, skipped As String
    Dim capCol As Long
    Dim wasOpen As Boolean, bak As String
    Dim laRank As Long, rnk As Long, refCell As String, emptyVal As String

    ffPath = FlatFilePath()
    If Len(Dir$(ffPath)) = 0 Then
        Say "Flat file not found:" & vbLf & ffPath & vbLf & vbLf & _
               "Run scripts/write_oils_supply_flat_files.py first.", vbCritical
        Exit Sub
    End If

    Set ws = ThisWorkbook.Worksheets(BAL_SHEET)

    ' open the flat file read-only just long enough to read the wide anchors
    On Error Resume Next
    Set ffWb = Workbooks(FF_NAME)
    On Error GoTo 0
    wasOpen = Not ffWb Is Nothing
    If Not wasOpen Then Set ffWb = Workbooks.Open(ffPath, ReadOnly:=True, UpdateLinks:=0)
    Set anchors = LoadWideAnchors(ffWb)
    If anchors Is Nothing Then
        If Not wasOpen Then ffWb.Close SaveChanges:=False
        Say "_wide_index tab not found in " & FF_NAME & ". Re-run the flat-file writer.", vbCritical
        Exit Sub
    End If

    If applyIt Then
        bak = ThisWorkbook.Path & Application.PathSeparator & _
              Replace(ThisWorkbook.Name, ".xlsm", "") & "_backup_" & Format$(Now, "yyyymmdd_hhnnss") & ".xlsm"
        ThisWorkbook.SaveCopyAs bak
    End If

    refBase = "'" & ThisWorkbook.Path & Application.PathSeparator & "[" & FF_NAME & "]"

    Application.ScreenUpdating = False
    m = BlockMap()
    For i = LBound(m) To UBound(m)
        blockRow = FindBlockRow(ws, CStr(m(i)(0)))
        If blockRow = 0 Then
            missing = missing & "  block not found: " & m(i)(0) & vbLf
        ElseIf Not anchors.Exists(CStr(m(i)(1)) & "|" & CStr(m(i)(2))) Then
            missing = missing & "  series not in _wide_index: " & m(i)(2) & vbLf
        Else
            wideRow = anchors(CStr(m(i)(1)) & "|" & CStr(m(i)(2)))
            lastCol = LastMYCol(ws, blockRow + 1)
            ' HARD CAP -- do not write past the last marketing year the DB renders.
            ' The balance sheet runs out to ~MY2045/46 with TORE'S OWN forward extensions in
            ' those columns; the wide tabs stop at our forecast horizon. Without this cap the
            ' repoint would blank his projections. Never widen this without asking.
            capCol = MY_COL0 + (anchors(CStr(m(i)(1)) & "|" & CStr(m(i)(2)) & "|LASTMY") - MY_ANCHOR)
            If lastCol > capCol Then
                skipped = skipped & "  " & m(i)(2) & ": left cols " & _
                          ws.Cells(1, capCol + 1).Address(False, False) & " onward untouched " & _
                          "(past MY" & anchors(CStr(m(i)(1)) & "|" & CStr(m(i)(2)) & "|LASTMY") & ")" & vbLf
                lastCol = capCol
            End If
            nBlocks = nBlocks + 1
            laRank = anchors(CStr(m(i)(1)) & "|" & CStr(m(i)(2)) & "|LASTACTUAL")
            For col = 2 To lastCol
                For k = 0 To N_MONTHS - 1
                    If applyIt Then
                        ' Columns align 1:1 (both grids anchored at col B = MY 1990/91), so only
                        ' the row differs. PERIOD-AWARE empty handling: a blank flat-file cell
                        ' AT/BEFORE the series' last actual month is a historical gap -> 0 (so
                        ' downstream +/- arithmetic works, e.g. pre-2006 biofuel, pre-1993 supply);
                        ' a blank AFTER it is the forward forecast hole -> "" (stays visibly blank).
                        refCell = refBase & CStr(m(i)(1)) & "'!" & _
                                  ws.Cells(wideRow + k, col).Address(True, True)
                        rnk = (col - MY_COL0) * N_MONTHS + k
                        If rnk <= laRank Then
                            emptyVal = "0"
                        Else
                            emptyVal = Chr(34) & Chr(34)
                        End If
                        ws.Cells(blockRow + 2 + k, col).Formula = _
                            "=IF(" & refCell & "=" & Chr(34) & Chr(34) & "," & _
                            emptyVal & "," & refCell & ")"
                    End If
                    nCells = nCells + 1
                Next k
            Next col
        End If
    Next i
    Application.ScreenUpdating = True

    If Not wasOpen Then ffWb.Close SaveChanges:=False

    msg = IIf(applyIt, "APPLIED", "PREVIEW (nothing changed)") & vbLf & vbLf & _
          nBlocks & " blocks, " & nCells & " cells -> " & FF_NAME & " _wide tabs" & vbLf & _
          "Columns map 1:1 (col B = MY 1990/91 in both grids)." & vbLf
    If Len(skipped) > 0 Then msg = msg & vbLf & "LEFT ALONE (your forward extensions, past the DB horizon):" & vbLf & skipped
    If Len(missing) > 0 Then msg = msg & vbLf & "NOT DONE:" & vbLf & missing
    If applyIt Then
        msg = msg & vbLf & "Backup saved:" & vbLf & bak & vbLf & vbLf & _
              "CHECK THE NUMBERS, then run RepointSoyOilCleanup to delete the ff_ mirror" & vbLf & _
              "tabs and break the eia_data.xlsm link."
    End If
    Say msg, vbInformation
End Sub

Public Sub RepointSoyOilPreview()
    RunRepoint False
End Sub

' Headless entry points. Return the captured dialog text so a COM driver can read the
' preview/apply report via Application.Run. gSilent suppresses the confirm prompt too.
Public Function RepointSoyOilPreviewSilent() As String
    gSilent = True: gResult = ""
    RunRepoint False
    gSilent = False
    RepointSoyOilPreviewSilent = gResult
End Function

Public Function RepointSoyOilApplySilent() As String
    gSilent = True: gResult = ""
    RunRepoint True
    gSilent = False
    RepointSoyOilApplySilent = gResult
End Function

Public Sub RepointSoyOilApply()
    If MsgBox("Repoint the soyoil balance sheet at " & FF_NAME & " _wide tabs?" & vbLf & vbLf & _
              "A timestamped backup is saved first.", vbYesNo + vbQuestion) = vbYes Then
        RunRepoint True
    End If
End Sub

' Run ONLY after verifying the repointed numbers. Deletes the mirror tabs and drops the
' eia_data.xlsm link. Not reversible except from the backup.
Public Sub RepointSoyOilCleanup()
    Dim ws As Worksheet, links As Variant, i As Long, n As Long, s As String
    If MsgBox("Delete ff_sbo_supply / ff_sbo_demand and break the eia_data.xlsm link?" & vbLf & _
              "Do this only after checking the repointed numbers.", vbYesNo + vbExclamation) <> vbYes Then Exit Sub

    Application.DisplayAlerts = False
    For Each ws In ThisWorkbook.Worksheets
        If Left$(ws.Name, 7) = "ff_sbo_" Then
            s = s & "  deleted tab " & ws.Name & vbLf
            ws.Delete
        End If
    Next ws
    Application.DisplayAlerts = True

    links = ThisWorkbook.LinkSources(xlExcelLinks)
    If Not IsEmpty(links) Then
        For i = 1 To UBound(links)
            If InStr(1, links(i), "eia_data", vbTextCompare) > 0 Then
                ThisWorkbook.BreakLink Name:=links(i), Type:=xlLinkTypeExcelLinks
                s = s & "  broke link " & links(i) & vbLf
                n = n + 1
            End If
        Next i
    End If
    MsgBox "Cleanup done." & vbLf & vbLf & s & vbLf & _
           "Chain is now: DB -> flat file -> balance sheet.", vbInformation
End Sub

' =====================================================================================
' BlankBBDForecastHole -- deliverable 3 (session 5). Run AFTER RepointSoyOilApply.
' The 4 fuel-use blocks are blank for months past the last raked EIA month (biofuel has no
' forecast). The monthly BBD line (=fuel1+fuel2+fuel3+fuel4) turned those blanks into 0.0
' (silently, when fed from eia_data) or #VALUE! (once repointed to "" cells) -- both HIDE
' the missing forecast. This rewrites the BBD line to blank LOUDLY when no fuel block has a
' value, and SUM (blank-safe) when any does. Idempotent; also repairs the AL133/AM133
' copy-paste error in the old MY2027 formula. Behavior-neutral for history.
Public Sub BlankBBDForecastHole()
    Dim ws As Worksheet, bbdRow As Long, lastCol As Long, col As Long, k As Long
    Dim rBio As Long, rRd As Long, rSaf As Long, rCo As Long, n As Long, c1 As String
    Set ws = ThisWorkbook.Worksheets(BAL_SHEET)
    bbdRow = FindBlockRow(ws, "US SOYBEAN OIL BIOMASS-BASED DIESEL USE")
    rBio = FindBlockRow(ws, "US SOYBEAN OIL BIODIESEL USE")
    rRd = FindBlockRow(ws, "US SOYBEAN OIL RENEWABLE DIESEL USE")
    rSaf = FindBlockRow(ws, "US SOYBEAN OIL SUSTAINABLE AVIATION FUEL USE")
    rCo = FindBlockRow(ws, "US SOYBEAN OIL CO-PROCESSING USE")
    If bbdRow = 0 Or rBio = 0 Or rRd = 0 Or rSaf = 0 Or rCo = 0 Then
        MsgBox "Could not locate the BBD line or one of the four fuel blocks -- nothing changed.", vbCritical
        Exit Sub
    End If
    lastCol = LastMYCol(ws, bbdRow + 1)
    Application.ScreenUpdating = False
    For col = 2 To lastCol
        For k = 0 To N_MONTHS - 1
            c1 = ws.Cells(rBio + 2 + k, col).Address(True, True) & "," & _
                 ws.Cells(rRd + 2 + k, col).Address(True, True) & "," & _
                 ws.Cells(rSaf + 2 + k, col).Address(True, True) & "," & _
                 ws.Cells(rCo + 2 + k, col).Address(True, True)
            ws.Cells(bbdRow + 2 + k, col).Formula = _
                "=IF(COUNT(" & c1 & ")=0,""""," & "SUM(" & c1 & "))"
            n = n + 1
        Next k
    Next col
    Application.ScreenUpdating = True
    MsgBox "BBD line reblanked: " & n & " cells now show blank when all four fuel blocks are" & vbLf & _
           "blank (missing biofuel forecast), SUM otherwise. Row " & bbdRow & ".", vbInformation
End Sub
