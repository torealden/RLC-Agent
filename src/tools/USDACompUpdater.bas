Attribute VB_Name = "USDACompUpdater"
' =============================================================================
' USDACompUpdater - Universal usda_comp refresher (Pure VBA + PostgreSQL ODBC)
' =============================================================================
' Refreshes the usda_comp tab in WHICHEVER balance-sheet workbook it is
' imported into -- one shared module for all ~38 books, not per-book copies.
' Reads gold.psd_wasde_vintages (all countries, all commodities, one shared
' vintage ladder; higher vintage_rank = more recent, rank ties at 79 broken
' by psd_cycle -- migrations 168/169).
'
' DIVISION OF LABOUR vs scripts/build_usda_comp_tabs.py:
'   * The Python builder BUILDS the tab (layout, formulas, formatting,
'     unit derivation with the magnitude cross-check). Run it after each
'     WASDE for a fleet-wide refresh, and whenever marketing years roll.
'   * This macro REFRESHES VALUES IN PLACE for the book you are looking at:
'     it never creates layout, never touches a formula cell, and derives
'     everything it needs (country, commodity, marketing years, units) from
'     what the builder already wrote into the sheet. If the sheet's
'     marketing years no longer match the ladder's active years, it tells
'     you to re-run the builder instead of guessing.
'
' Layout contract it reads (donor: argentina_soybean_complex usda_comp):
'   block title row      A: "<COUNTRY> <COMMODITY> SUPPLY AND DEMAND"
'   MY header row        B: "2025/26"  E: "2026/27"  I: prior cycle month
'   column header row    A: "(units)"  B: "USDA" ... I/J: short MY labels
'   data rows            A: row label; B/E current USDA, I/J prior USDA,
'                        C/F deltas (formulas), D/G RLC links (formulas)
'   note row             A: "USDA columns: WASDE_XXX_YY (Month Year cycle,
'                        pulled YYYY-MM-DD) ..."
'   stamp row (sheet)    A: "USDA (PSD/WASDE) vs RLC comparison - refreshed"
'
' Only overwrites cells that are NOT formulas (leaf USDA inputs). Total
' Supply / Other Domestic Use / Total Demand / Stocks-to-Use and the
' RLC-linked and delta columns are formulas and are never touched.
'
' Meta-stamp: every run rewrites the note rows (vintage, cycle month, pull
' date) and the sheet stamp line -- a refresh that changes nothing still
' stamps (feedback_timestamp_every_touch, ruled 2026-08-03).
'
' Keyboard shortcut: Ctrl+Shift+U (bound via Workbook_Open -- see
' USDACompWorkbookEvents.bas, paste into ThisWorkbook)
'
' DB CREDENTIALS: read at runtime from a local file OUTSIDE the network
' share (%USERPROFILE%\.rlc_db_credentials) -- same file WASDECompUpdater
' and FatsOilsUpdaterSQL already use. Never hardcoded here.
'
' Requirements:
' - PostgreSQL ODBC Driver installed (psqlODBC x64)
' - Late-bound ADODB (CreateObject) -- no references needed
'
' NOTE ON ENCODING: this file is deliberately pure ASCII. VBA's Import reads
' .bas files as ANSI/cp1252; a literal UTF-8 delta or em-dash corrupts on
' import. All non-ASCII characters are built at runtime with ChrW().
' =============================================================================

Option Explicit

Private Const SHEET_NAME As String = "usda_comp"

' =============================================================================
' MAIN ENTRY POINT
' =============================================================================

Public Sub UpdateUSDAComp()
    ' Keyboard shortcut: Ctrl+Shift+U

    Dim confirmMsg As VbMsgBoxResult
    confirmMsg = MsgBox("Refresh usda_comp with the latest USDA vintages?" & vbCrLf & vbCrLf & _
                         "A timestamped backup of this file will be saved to the same folder first.", _
                         vbYesNo + vbQuestion, "USDA Comp Updater")
    If confirmMsg <> vbYes Then Exit Sub

    Dim summary As String
    summary = DoUpdate()

    If summary <> "" Then
        MsgBox "usda_comp refreshed." & vbCrLf & vbCrLf & summary & vbCrLf & _
               "Review the changes, then save the workbook.", vbInformation, "USDA Comp Updater"
    End If
End Sub

' Test-only entry point: same update, no dialogs, for automated end-to-end
' testing via COM on workbook copies. Never wired to a shortcut.
Public Function UpdateUSDAComp_Silent() As String
    UpdateUSDAComp_Silent = DoUpdate(True)
End Function

Private Function DoUpdate(Optional silent As Boolean = False) As String
    Dim conn As Object

    On Error GoTo ErrHandler

    If Not SheetExists(SHEET_NAME) Then
        If Not silent Then
            MsgBox "This workbook has no '" & SHEET_NAME & "' tab." & vbCrLf & vbCrLf & _
                   "Build it first with: python scripts/build_usda_comp_tabs.py --only <book>", _
                   vbExclamation, "USDA Comp Updater"
        End If
        DoUpdate = ""
        Exit Function
    End If

    Dim countryCode As String
    countryCode = CountryCodeFromFolder()
    If countryCode = "" Then
        If Not silent Then
            MsgBox "Could not map this workbook's folder ('" & FolderName() & _
                   "') to a PSD country code. Update CountryCodeFromFolder in USDACompUpdater.", _
                   vbCritical, "USDA Comp Updater"
        End If
        DoUpdate = ""
        Exit Function
    End If

    If Not silent Then BackupWorkbookCopy

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
    Set ws = ThisWorkbook.Sheets(SHEET_NAME)

    Application.StatusBar = "Refreshing usda_comp..."
    DoEvents

    Dim summary As String
    Dim r As Long, lastRow As Long
    lastRow = ws.Cells(ws.Rows.Count, 1).End(xlUp).Row
    If lastRow < 4 Then lastRow = 4

    ' A block is announced by its column-header row: B = "USDA".
    Dim blocks As Long
    blocks = 0
    r = 2
    Do While r <= lastRow + 2
        If UCase(Trim(CStr(ws.Cells(r, 2).Value))) = "USDA" Then
            summary = summary & UpdateBlock(conn, ws, countryCode, r)
            blocks = blocks + 1
        End If
        r = r + 1
    Loop

    conn.Close

    If blocks = 0 Then
        summary = "  No comp blocks found on " & SHEET_NAME & " -- run the Python builder." & vbCrLf
    Else
        StampSheet ws, lastRow
    End If

    Application.StatusBar = False
    Application.Cursor = xlDefault

    DoUpdate = summary
    Exit Function

ErrHandler:
    Application.StatusBar = False
    Application.Cursor = xlDefault
    If Not silent Then
        MsgBox "Update failed:" & vbCrLf & vbCrLf & Err.Description, vbCritical, "USDA Comp Updater"
    End If
    If Not conn Is Nothing Then
        On Error Resume Next
        conn.Close
    End If
    DoUpdate = ""
End Function

' =============================================================================
' PER-BLOCK UPDATE (hdrRow = the row whose column B says "USDA")
' =============================================================================

Private Function UpdateBlock(conn As Object, ws As Worksheet, countryCode As String, _
                              hdrRow As Long) As String

    Dim titleRow As Long, myRow As Long
    titleRow = hdrRow - 2
    myRow = hdrRow - 1

    Dim blockTitle As String
    blockTitle = UCase(Trim(CStr(ws.Cells(titleRow, 1).Value)))

    Dim commodity As String
    commodity = TitleToCommodity(blockTitle)
    If commodity = "" Then
        UpdateBlock = "  " & blockTitle & ": unrecognized commodity title, skipped." & vbCrLf
        Exit Function
    End If

    ' Marketing years from the merged green headers ("2025/26" -> 2025)
    Dim my1 As Long, my2 As Long
    my1 = ParseMY(CStr(ws.Cells(myRow, 2).Value))
    my2 = ParseMY(CStr(ws.Cells(myRow, 5).Value))
    If my1 = 0 Or my2 = 0 Then
        UpdateBlock = "  " & commodity & ": could not read marketing years from headers, skipped." & vbCrLf
        Exit Function
    End If

    ' Units from the "(...)" line the builder wrote next to the headers
    Dim valFactor As Double, areaFactor As Double
    If Not ParseUnits(CStr(ws.Cells(hdrRow, 1).Value), commodity, valFactor, areaFactor) Then
        UpdateBlock = "  " & commodity & ": unit line '" & CStr(ws.Cells(hdrRow, 1).Value) & _
                      "' not recognized, skipped (never guessed)." & vbCrLf
        Exit Function
    End If

    Dim vintages As Object
    Set vintages = LoadVintages(conn, commodity, countryCode, my1, my2)

    Dim cur1 As Object, prior1 As Object, cur2 As Object, prior2 As Object
    Set cur1 = GetVintage(vintages, my1, 1)
    Set prior1 = GetVintage(vintages, my1, 2)
    Set cur2 = GetVintage(vintages, my2, 1)
    Set prior2 = GetVintage(vintages, my2, 2)

    If cur1 Is Nothing Or cur2 Is Nothing Then
        UpdateBlock = "  " & commodity & ": sheet MYs " & my1 & "/" & my2 & _
                      " not both active on the ladder -- marketing years rolled;" & _
                      " re-run scripts/build_usda_comp_tabs.py." & vbCrLf
        Exit Function
    End If

    ' Data rows: from hdrRow+1 down while column A carries a label
    Dim cellsUpdated As Long
    cellsUpdated = 0
    Dim r As Long
    r = hdrRow + 1
    Do While Len(Trim(CStr(ws.Cells(r, 1).Value))) > 0
        Dim field As String
        field = LabelToField(CStr(ws.Cells(r, 1).Value))
        If field <> "" Then
            Dim f As Double
            If field = "area_harvested" Then f = areaFactor Else f = valFactor
            cellsUpdated = cellsUpdated + WriteIfLiteral(ws.Cells(r, 2), FieldValue(cur1, field), f)   ' B current MY1
            cellsUpdated = cellsUpdated + WriteIfLiteral(ws.Cells(r, 9), FieldValue(prior1, field), f) ' I prior   MY1
            cellsUpdated = cellsUpdated + WriteIfLiteral(ws.Cells(r, 5), FieldValue(cur2, field), f)   ' E current MY2
            cellsUpdated = cellsUpdated + WriteIfLiteral(ws.Cells(r, 10), FieldValue(prior2, field), f) ' J prior  MY2
        End If
        r = r + 1
    Loop

    ' Header labels: prior cycle month (merged I:J) + delta headers
    Dim priorMonthName As String, curMonthName As String, curYear As String
    priorMonthName = CycleMonthName(prior1)
    curMonthName = CycleMonthName(cur1)
    curYear = CycleYear(cur1)

    ' Merged I:J header carries the full month name; delta headers use the
    ' ABBREVIATED month (matches the builder -- long months clip at grid width)
    UpdateIfLiteral ws.Cells(myRow, 9), priorMonthName
    If priorMonthName <> "" Then
        UpdateIfLiteral ws.Cells(hdrRow, 3), ChrW(916) & " from " & Left(priorMonthName, 3)
        UpdateIfLiteral ws.Cells(hdrRow, 6), ChrW(916) & " from " & Left(priorMonthName, 3)
    End If

    ' Note row: first row after the data whose A starts "USDA columns:"
    Dim noteRow As Long
    noteRow = FindRowByPrefix(ws, r, r + 4, "USDA columns:")
    If noteRow > 0 Then
        RewriteNote ws, noteRow, cur1, prior1, curMonthName, curYear
    End If

    ' NOT IIf: VBA's IIf evaluates BOTH branches, so touching prior1("vintage")
    ' inside it blows up (error 91) whenever a block has no prior vintage.
    Dim vsTxt As String
    If prior1 Is Nothing Then
        vsTxt = ""
    Else
        vsTxt = " vs " & CStr(prior1("vintage"))
    End If
    UpdateBlock = "  " & commodity & ": " & cellsUpdated & " cells (" & _
                  CStr(cur1("vintage")) & vsTxt & ")" & vbCrLf
End Function

' =============================================================================
' DATA LOADING -- gold.psd_wasde_vintages, rank ties broken by psd_cycle
' =============================================================================

Private Function LoadVintages(conn As Object, commodity As String, countryCode As String, _
                               my1 As Long, my2 As Long) As Object
    Dim dict As Object
    Set dict = CreateObject("Scripting.Dictionary")

    Dim seenPerMY As Object
    Set seenPerMY = CreateObject("Scripting.Dictionary")

    ' vintage_rank is HIGHER = MORE RECENT (mig 149). With the mig-168
    ' archive union an active MY can carry 20+ cycles and everything past
    ' the 19th caps at rank 79 -- psd_cycle DESC breaks those ties (US corn
    ' MY2012 has seventeen cycles tied at 79). Do NOT drop the tie-break.
    Dim sql As String
    sql = "SELECT marketing_year, report_date, psd_cycle, vintage, vintage_rank, " & _
          "area_harvested, beginning_stocks, production, imports, crush, " & _
          "domestic_consumption, exports, ending_stocks " & _
          "FROM gold.psd_wasde_vintages " & _
          "WHERE commodity = '" & commodity & "' AND country_code = '" & countryCode & "' " & _
          "AND is_active_my AND marketing_year IN (" & my1 & ", " & my2 & ") " & _
          "ORDER BY marketing_year, vintage_rank DESC, psd_cycle DESC"

    Dim rs As Object
    Set rs = CreateObject("ADODB.Recordset")
    rs.Open sql, conn

    Dim fieldNames As Variant
    fieldNames = Array("area_harvested", "beginning_stocks", "production", "imports", _
                        "crush", "domestic_consumption", "exports", "ending_stocks")

    Do While Not rs.EOF
        Dim rowDict As Object
        Set rowDict = CreateObject("Scripting.Dictionary")

        Dim fn As Variant
        For Each fn In fieldNames
            rowDict.Add CStr(fn), rs.Fields(CStr(fn)).Value
        Next fn
        rowDict.Add "report_date", rs.Fields("report_date").Value
        rowDict.Add "psd_cycle", rs.Fields("psd_cycle").Value
        rowDict.Add "vintage", rs.Fields("vintage").Value

        ' rank DESC (+ cycle tie-break) within each MY -> first row seen for
        ' a MY is its newest vintage. Callers ask for ordinals 1/2.
        Dim myVal As Long
        myVal = CLng(rs.Fields("marketing_year").Value)
        Dim ordinal As Long
        If seenPerMY.Exists(myVal) Then
            ordinal = CLng(seenPerMY(myVal)) + 1
            seenPerMY(myVal) = ordinal
        Else
            ordinal = 1
            seenPerMY.Add myVal, ordinal
        End If

        If ordinal <= 2 Then
            dict.Add CStr(myVal) & "|" & CStr(ordinal), rowDict
        End If

        rs.MoveNext
    Loop
    rs.Close

    Set LoadVintages = dict
End Function

Private Function GetVintage(vintages As Object, my As Long, ordinal As Integer) As Object
    Dim key As String
    key = CStr(my) & "|" & CStr(ordinal)
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

' =============================================================================
' SHEET-DERIVED CONTEXT: country, commodity, marketing years, units
' =============================================================================

Private Function FolderName() As String
    Dim p As String
    p = ThisWorkbook.Path
    If Len(p) = 0 Then Exit Function
    FolderName = Mid(p, InStrRev(p, "\") + 1)
End Function

Private Function CountryCodeFromFolder() As String
    ' Folder name -> PSD (FIPS-style) country code. Mirrors COUNTRY_CODES in
    ' scripts/build_usda_comp_tabs.py -- PSD's own codes, NOT ISO (CH not CN,
    ' E4 not EU, RS not RU...). Extend both together.
    Dim d As Object
    Set d = CreateObject("Scripting.Dictionary")
    d("UNITED STATES") = "US"
    d("ARGENTINA") = "AR"
    d("AUSTRALIA") = "AS"
    d("BRAZIL") = "BR"
    d("CANADA") = "CA"
    d("CHINA") = "CH"
    d("EU") = "E4"
    d("INDIA") = "IN"
    d("INDONESIA") = "ID"
    d("JAPAN") = "JA"
    d("MALAYSIA") = "MY"
    d("MEXICO") = "MX"
    d("PARAGUAY") = "PA"
    d("PHILIPPINES") = "RP"
    d("RUSSIA") = "RS"
    d("UKRAINE") = "UP"
    d("URUGUAY") = "UY"

    Dim key As String
    key = UCase(Trim(FolderName()))
    If d.Exists(key) Then CountryCodeFromFolder = d(key) Else CountryCodeFromFolder = ""
End Function

Private Function TitleToCommodity(title As String) As String
    ' Ordered most-specific-first; mirrors TITLE_TO_COMMODITY in the builder.
    ' Sequential If chain, NOT one Array(...) literal: VBA caps a statement
    ' at 24 line continuations and the pair list is longer than that --
    ' a single Array literal makes the module UNIMPORTABLE (this bit once).
    If InStr(title, "PALM KERNEL CAKE") > 0 Then
        TitleToCommodity = "palm_kernel_meal"
    ElseIf InStr(title, "PALM KERNEL MEAL") > 0 Then
        TitleToCommodity = "palm_kernel_meal"
    ElseIf InStr(title, "PALM KERNEL OIL") > 0 Then
        TitleToCommodity = "palm_kernel_oil"
    ElseIf InStr(title, "PALM KERNEL") > 0 Then
        TitleToCommodity = "palm_kernel"
    ElseIf InStr(title, "PALM OIL") > 0 Then
        TitleToCommodity = "palm_oil"
    ElseIf InStr(title, "COPRA MEAL") > 0 Then
        TitleToCommodity = "copra_meal"
    ElseIf InStr(title, "COPRA") > 0 Then
        TitleToCommodity = "copra"
    ElseIf InStr(title, "COCONUT OIL") > 0 Then
        TitleToCommodity = "coconut_oil"
    ElseIf InStr(title, "COCONUT") > 0 Then
        TitleToCommodity = "copra"
    ElseIf InStr(title, "RAPESEED MEAL") > 0 Or InStr(title, "CANOLA MEAL") > 0 Then
        TitleToCommodity = "rapeseed_meal"
    ElseIf InStr(title, "RAPESEED OIL") > 0 Or InStr(title, "CANOLA OIL") > 0 Then
        TitleToCommodity = "rapeseed_oil"
    ElseIf InStr(title, "RAPESEED") > 0 Or InStr(title, "CANOLA") > 0 Then
        TitleToCommodity = "rapeseed"
    ElseIf InStr(title, "SUNFLOWERSEED MEAL") > 0 Or InStr(title, "SUNFLOWER MEAL") > 0 Then
        TitleToCommodity = "sunflowerseed_meal"
    ElseIf InStr(title, "SUNFLOWERSEED OIL") > 0 Or InStr(title, "SUNFLOWER OIL") > 0 Then
        TitleToCommodity = "sunflowerseed_oil"
    ElseIf InStr(title, "SUNFLOWER") > 0 Then
        TitleToCommodity = "sunflowerseed"
    ElseIf InStr(title, "COTTONSEED MEAL") > 0 Then
        TitleToCommodity = "cottonseed_meal"
    ElseIf InStr(title, "COTTONSEED OIL") > 0 Then
        TitleToCommodity = "cottonseed_oil"
    ElseIf InStr(title, "COTTONSEED") > 0 Then
        TitleToCommodity = "cottonseed"
    ElseIf InStr(title, "COTTON") > 0 Then
        TitleToCommodity = "cotton"
    ElseIf InStr(title, "PEANUT MEAL") > 0 Then
        TitleToCommodity = "peanut_meal"
    ElseIf InStr(title, "PEANUT OIL") > 0 Then
        TitleToCommodity = "peanut_oil"
    ElseIf InStr(title, "PEANUT") > 0 Then
        TitleToCommodity = "peanuts"
    ElseIf InStr(title, "SOYBEAN MEAL") > 0 Then
        TitleToCommodity = "soybean_meal"
    ElseIf InStr(title, "SOYBEAN OIL") > 0 Then
        TitleToCommodity = "soybean_oil"
    ElseIf InStr(title, "SOYBEAN") > 0 Then
        TitleToCommodity = "soybeans"
    Else
        TitleToCommodity = ""
    End If
End Function

Private Function ParseMY(label As String) As Long
    ' "2025/26" -> 2025; anything else -> 0
    Dim s As String
    s = Trim(label)
    Dim slashPos As Long
    slashPos = InStr(s, "/")
    If slashPos < 2 Then Exit Function
    Dim head As String
    head = Trim(Left(s, slashPos - 1))
    If IsNumeric(head) And Len(head) = 4 Then ParseMY = CLng(head)
End Function

Private Function ParseUnits(unitLine As String, commodity As String, _
                             ByRef valFactor As Double, ByRef areaFactor As Double) As Boolean
    ' "(million hectares, thousand tonnes)" or "(thousand tonnes)".
    ' Factors are PSD-native -> book units: PSD serves 1000 MT (area 1000 HA),
    ' except cotton in 1000 480-lb bales. Mirrors the builder's factor tables.
    Dim s As String
    s = Trim(unitLine)
    Dim p1 As Long, p2 As Long
    p1 = InStr(s, "(")
    p2 = InStrRev(s, ")")
    If p1 = 0 Or p2 <= p1 Then Exit Function
    s = LCase(Mid(s, p1 + 1, p2 - p1 - 1))

    Dim valTxt As String
    areaFactor = 0.001   ' million hectares default (only used when area row exists)
    Dim commaPos As Long
    commaPos = InStr(s, ",")
    If commaPos > 0 Then
        Dim areaTxt As String
        areaTxt = Trim(Left(s, commaPos - 1))
        valTxt = Trim(Mid(s, commaPos + 1))
        Select Case areaTxt
            Case "million hectares": areaFactor = 0.001
            Case "million acres": areaFactor = 0.00247105
            Case "thousand hectares": areaFactor = 1#
            Case "thousand acres": areaFactor = 2.47105
            Case Else: Exit Function
        End Select
    Else
        valTxt = Trim(s)
    End If

    If commodity = "cotton" Then
        Select Case valTxt
            Case "million 480-lb bales", "million 480 lb bales": valFactor = 0.001
            Case "thousand 480-lb bales", "thousand 480 lb bales": valFactor = 1#
            Case Else: Exit Function
        End Select
        ParseUnits = True
        Exit Function
    End If

    Select Case valTxt
        Case "thousand tonnes", "thousand metric tons": valFactor = 1#
        Case "million pounds": valFactor = 2.204623
        Case "thousand short tons", "thousand tons": valFactor = 1.102311
        Case "million bushels (60 lb)", "million bushels": valFactor = 0.0367437
        Case "million bushels (56 lb)": valFactor = 0.0393683
        Case "million tonnes": valFactor = 0.001
        Case Else: Exit Function
    End Select
    ParseUnits = True
End Function

Private Function LabelToField(label As String) As String
    Dim L As String
    L = LCase(Trim(label))

    If InStr(L, "harvested area") > 0 Then
        LabelToField = "area_harvested"
    ElseIf InStr(L, "beginning stocks") > 0 Then
        LabelToField = "beginning_stocks"
    ElseIf InStr(L, "production") > 0 Then
        LabelToField = "production"
    ElseIf InStr(L, "imports") > 0 Then
        LabelToField = "imports"
    ElseIf InStr(L, "crush") > 0 Then
        LabelToField = "crush"
    ElseIf InStr(L, "domestic use") > 0 And InStr(L, "other") = 0 And InStr(L, "total") = 0 Then
        LabelToField = "domestic_consumption"
    ElseIf InStr(L, "exports") > 0 Then
        LabelToField = "exports"
    ElseIf InStr(L, "ending stocks") > 0 Then
        LabelToField = "ending_stocks"
    Else
        LabelToField = ""   ' Total Supply, Other Domestic Use, Total Demand,
                             ' Stocks-to-Use -- formulas; never touched anyway.
    End If
End Function

' =============================================================================
' CELL WRITE HELPERS (never overwrite a formula)
' =============================================================================

Private Function WriteIfLiteral(cell As Range, rawValue As Variant, factor As Double) As Long
    If cell.HasFormula Then
        WriteIfLiteral = 0
        Exit Function
    End If
    If IsNull(rawValue) Then
        WriteIfLiteral = 0
        Exit Function
    End If
    cell.Value = Round(CDbl(rawValue) * factor, 3)
    WriteIfLiteral = 1
End Function

Private Sub UpdateIfLiteral(cell As Range, newText As String)
    If cell.HasFormula Then Exit Sub
    If Len(newText) = 0 Then Exit Sub
    cell.Value = newText
End Sub

' =============================================================================
' NOTE + STAMP REWRITES (the meta-stamp: every touch re-stamps)
' =============================================================================

Private Function FindRowByPrefix(ws As Worksheet, fromRow As Long, toRow As Long, _
                                  prefix As String) As Long
    Dim r As Long
    For r = fromRow To toRow
        If Left(Trim(CStr(ws.Cells(r, 1).Value)), Len(prefix)) = prefix Then
            FindRowByPrefix = r
            Exit Function
        End If
    Next r
    FindRowByPrefix = 0
End Function

Private Sub RewriteNote(ws As Worksheet, noteRow As Long, cur1 As Object, prior1 As Object, _
                         curMonthName As String, curYear As String)
    ' Surgical rewrite of the builder's note line: swap the vintage/cycle/pull
    ' fragment and the delta-vs fragment, keep the RLC-link and unit-check
    ' text (which this macro cannot re-derive) intact.
    On Error Resume Next

    Dim cell As Range
    Set cell = ws.Cells(noteRow, 1)
    If cell.HasFormula Then Exit Sub

    Dim txt As String
    txt = CStr(cell.Value)
    If Len(txt) = 0 Then Exit Sub

    Dim anchor As String
    anchor = " from gold.psd_wasde_vintages"

    Dim p1 As Long, p2 As Long
    p1 = InStr(txt, "USDA columns: ")
    p2 = InStr(txt, anchor)
    If p1 > 0 And p2 > p1 Then
        Dim pullTxt As String
        pullTxt = Format(CDate(cur1("report_date")), "yyyy-mm-dd")
        txt = Left(txt, p1 - 1) & "USDA columns: " & CStr(cur1("vintage")) & _
              " (" & curMonthName & " " & curYear & " cycle, pulled " & pullTxt & ")" & _
              Mid(txt, p2)
    End If

    Dim deltaCh As String
    deltaCh = ChrW(916)

    If Not prior1 Is Nothing Then
        Dim noPrior As String
        noPrior = "; no prior vintage yet -- " & deltaCh & " columns blank"
        If InStr(txt, noPrior) > 0 Then
            txt = Replace(txt, noPrior, "; " & deltaCh & " vs " & CStr(prior1("vintage")))
        Else
            ' "; <delta> vs WASDE_XXX_YY." -> swap the vintage token
            Dim q1 As Long, q2 As Long
            q1 = InStr(txt, deltaCh & " vs ")
            If q1 > 0 Then
                q1 = q1 + Len(deltaCh & " vs ")
                q2 = InStr(q1, txt, ".")
                If q2 > q1 Then
                    txt = Left(txt, q1 - 1) & CStr(prior1("vintage")) & Mid(txt, q2)
                End If
            End If
        End If
    End If

    cell.Value = txt
    On Error GoTo 0
End Sub

Private Sub StampSheet(ws As Worksheet, lastRow As Long)
    ' The single sheet-level stamp line ("USDA (PSD/WASDE) vs RLC comparison
    ' ... refreshed ..."). Rewritten on EVERY run, changed data or not.
    Dim r As Long
    r = FindRowByPrefix(ws, 2, lastRow + 2, "USDA (PSD/WASDE) vs RLC comparison")
    If r = 0 Then Exit Sub
    Dim dash As String
    dash = " " & ChrW(8212) & " "
    ws.Cells(r, 1).Value = "USDA (PSD/WASDE) vs RLC comparison" & dash & _
        "refreshed " & Format(Now, "yyyy-mm-dd hh:nn") & _
        " by USDACompUpdater.bas (Ctrl+Shift+U)" & dash & "source gold.psd_wasde_vintages"
End Sub

' =============================================================================
' VINTAGE LABEL HELPERS -- month labels come from psd_cycle (the WASDE cycle
' the values belong to), NOT report_date (the pull that carried them), mig 166.
' =============================================================================

Private Function CycleMonthName(rowDict As Object) As String
    If rowDict Is Nothing Then
        CycleMonthName = ""
        Exit Function
    End If
    Dim d As Variant
    d = rowDict("psd_cycle")
    If IsNull(d) Then d = rowDict("report_date")
    If IsNull(d) Then
        CycleMonthName = ""
        Exit Function
    End If
    CycleMonthName = MonthName(Month(CDate(d)))
End Function

Private Function CycleYear(rowDict As Object) As String
    If rowDict Is Nothing Then
        CycleYear = ""
        Exit Function
    End If
    Dim d As Variant
    d = rowDict("psd_cycle")
    If IsNull(d) Then d = rowDict("report_date")
    If IsNull(d) Then
        CycleYear = ""
        Exit Function
    End If
    CycleYear = CStr(Year(CDate(d)))
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
               "DB_USER=..." & vbCrLf & "DB_PASSWORD=...", vbCritical, "USDA Comp Updater"
        Set GetConnection = Nothing
        Exit Function
    End If

    Dim required As Variant, rk As Variant
    required = Array("DB_SERVER", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD")
    For Each rk In required
        If Not creds.Exists(CStr(rk)) Then
            MsgBox "Credential file is missing " & rk & ":" & vbCrLf & CredentialFilePath(), _
                   vbCritical, "USDA Comp Updater"
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
    backupPath = ThisWorkbook.Path & "\" & baseName & "_backup_" & _
                 Format(Now, "yyyymmdd_hhnnss") & ext

    ThisWorkbook.SaveCopyAs backupPath
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
' KEYBOARD SHORTCUT -- dispatcher pattern (same reasoning as WASDECompUpdater:
' Application.OnKey is session-global; every open book's copy binds the same
' dispatcher name, and the dispatcher re-routes to ActiveWorkbook's own copy,
' so the binding is correct regardless of open order).
' =============================================================================

Public Sub AssignUSDACompShortcut()
    Application.OnKey "^+u", "UpdateUSDAComp_Dispatch"

    On Error Resume Next
    ShortcutsHelper.ShowShortcutBanner "USDA Comp Updater", "Ctrl+Shift+U", "Refresh usda_comp (current vs prior USDA vintage, both marketing years)"
    On Error GoTo 0
End Sub

Public Sub RemoveUSDACompShortcut()
    Application.OnKey "^+u"
End Sub

Public Sub UpdateUSDAComp_Dispatch()
    Dim targetName As String
    targetName = ActiveWorkbook.Name

    On Error Resume Next
    Application.Run "'" & targetName & "'!USDACompUpdater.UpdateUSDAComp"
    If Err.Number <> 0 Then
        MsgBox "Couldn't find the USDA comp updater in '" & targetName & "'." & vbCrLf & vbCrLf & _
               "Import USDACompUpdater.bas into that workbook (VBE > File > Import File).", _
               vbExclamation, "USDA Comp Updater"
    End If
    On Error GoTo 0
End Sub
