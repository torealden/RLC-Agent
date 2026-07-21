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

Private Const BAL_SHEET As String = "soyoil_balance_sheet"
Private Const FF_NAME As String = "us_soybean_oil_supply_demand.xlsx"
Private Const N_MONTHS As Long = 12
Private Const MY_ANCHOR As Long = 1990   ' marketing year in column B, BOTH grids
Private Const MY_COL0 As Long = 2        ' column B

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

' Read series -> first_month_row from the flat file's _wide_index tab.
Private Function LoadWideAnchors(ffWb As Workbook) As Object
    Dim d As Object, ws As Worksheet, r As Long, lastRow As Long
    Set d = CreateObject("Scripting.Dictionary")
    On Error Resume Next
    Set ws = ffWb.Worksheets("_wide_index")
    On Error GoTo 0
    If ws Is Nothing Then Exit Function
    lastRow = ws.Cells(ws.Rows.Count, 1).End(xlUp).Row
    For r = 2 To lastRow
        If Len(Trim$(CStr(ws.Cells(r, 1).Value))) > 0 And Len(Trim$(CStr(ws.Cells(r, 2).Value))) > 0 Then
            ' col 5 = first_month_row, col 10 = last_my (the last MY the DB actually renders)
            d(CStr(ws.Cells(r, 1).Value) & "|" & CStr(ws.Cells(r, 2).Value)) = CLng(ws.Cells(r, 5).Value)
            d(CStr(ws.Cells(r, 1).Value) & "|" & CStr(ws.Cells(r, 2).Value) & "|LASTMY") = CLng(ws.Cells(r, 10).Value)
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

    ffPath = FlatFilePath()
    If Len(Dir$(ffPath)) = 0 Then
        MsgBox "Flat file not found:" & vbLf & ffPath & vbLf & vbLf & _
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
        MsgBox "_wide_index tab not found in " & FF_NAME & ". Re-run the flat-file writer.", vbCritical
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
            For col = 2 To lastCol
                For k = 0 To N_MONTHS - 1
                    If applyIt Then
                        ' columns align 1:1 (both grids anchored at col B = MY 1990/91),
                        ' so only the row differs. Blank-safe: "" instead of 0.
                        ws.Cells(blockRow + 2 + k, col).Formula = _
                            "=IF(" & refBase & CStr(m(i)(1)) & "'!" & _
                            ws.Cells(wideRow + k, col).Address(True, True) & "=""""," & """""," & _
                            refBase & CStr(m(i)(1)) & "'!" & _
                            ws.Cells(wideRow + k, col).Address(True, True) & ")"
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
    MsgBox msg, vbInformation, "Repoint soyoil balance sheet"
End Sub

Public Sub RepointSoyOilPreview()
    RunRepoint False
End Sub

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
