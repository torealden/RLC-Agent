Attribute VB_Name = "FatsOilsUpdaterSQL"
' =============================================================================
' FatsOilsUpdaterSQL - Universal Header-Matching Fats & Oils Updater
' =============================================================================
' Generic updater for ALL commodities in the Fats & Oils / Crush report.
' Works with any sheet layout by reading column headers at runtime.
'
' Target: us_oilseed_crush.xlsm (any tab: soy_crush, canola_crush, etc.)
' Layout: Row 3 = headers, Row 4 = units
'         Column A = dates (1st of month, as date serial)
'
' Data source: gold.fats_oils_crush_matrix view
' Mapping: silver.crush_attribute_reference table (header_pattern column)
'
' How it works:
'   1. Reads the sheet name to determine the commodity
'   2. Reads row 3 headers and builds a header-to-column map
'   3. Queries the database for that commodity's data
'   4. Matches returned header_pattern values to actual headers
'   5. Places data in the matching columns
'
' Supports: soybeans, canola, cottonseed, corn, sunflower, palm,
'           coconut, safflower, palm_kernel, peanut, tallow, lard,
'           choice_white_grease, yellow_grease, poultry_fat, and more
'
' Requirements:
' - PostgreSQL ODBC Driver installed (psqlODBC)
' - Reference to "Microsoft ActiveX Data Objects" (Tools > References)
' =============================================================================

Option Explicit

' Database connection settings
Private Const DB_SERVER As String = "rlc-commodities.c16c6wm826t7.us-east-2.rds.amazonaws.com"
Private Const DB_PORT As String = "5432"
Private Const DB_NAME As String = "rlc_commodities"
Private Const DB_USER As String = "postgres"
Private Const DB_PASSWORD As String = "SoupBoss1"

' Spreadsheet structure
' NOTE: HEADER_ROW and DATA_START_ROW are now auto-detected at runtime.
'       The legacy constants are kept as fallback defaults only.
Private Const DEFAULT_HEADER_ROW As Integer = 3
Private Const DEFAULT_DATA_START_ROW As Integer = 5
Private Const DATE_COLUMN As Integer = 1      ' Column A = dates

' ── User-defined types must be declared at module scope BEFORE any procedure
'    that references them. VBA is strict about this. ──
Private Type CommodityUpdateResult
    cellsUpdated As Long
    headersUnmatched As Long
    unmatchedList As String
    excelHeadersRead As String   ' Diagnostic: what headers the updater actually saw in this block
End Type

' =============================================================================
' LAYOUT AUTO-DETECTION
' =============================================================================

Private Function DetectDataStartRow(ws As Worksheet) As Integer
    ' Find the first row in col A that contains a real date (= first data row).
    ' Header is assumed to be at DataStart - 2, units row at DataStart - 1.
    Dim r As Integer
    For r = 2 To 12
        Dim v As Variant
        v = ws.Cells(r, DATE_COLUMN).Value
        If IsDate(v) Then
            DetectDataStartRow = r
            Exit Function
        End If
    Next r
    DetectDataStartRow = DEFAULT_DATA_START_ROW
End Function

Private Function DetectHeaderRow(ws As Worksheet) As Integer
    Dim ds As Integer
    ds = DetectDataStartRow(ws)
    DetectHeaderRow = ds - 2
    If DetectHeaderRow < 1 Then DetectHeaderRow = DEFAULT_HEADER_ROW
End Function

' =============================================================================
' SHEET NAME TO COMMODITY MAPPING
' =============================================================================

' Same lookup logic as GetCommodityFromSheet, but takes a row-1 group header
' string (e.g., "Palm Kernel Oil", "Corn Oil") used in multi-commodity tabs.
Private Function GetCommodityFromHeaderText(ByVal headerText As String) As String
    Dim t As String
    t = LCase(Trim(headerText))
    If t = "" Then
        GetCommodityFromHeaderText = ""
        Exit Function
    End If
    Select Case True
        Case t Like "*palm*kernel*"
            GetCommodityFromHeaderText = "palm_kernel"
        Case t Like "*palm*"
            GetCommodityFromHeaderText = "palm"
        Case t Like "*coconut*"
            GetCommodityFromHeaderText = "coconut"
        Case t Like "*safflower*"
            GetCommodityFromHeaderText = "safflower"
        Case t Like "*corn*oil*"
            GetCommodityFromHeaderText = "corn"
        ' NOTE: "peanut" intentionally NOT matched here — peanut oil has its
        ' own dedicated `peanut_crush` tab. On multi-commodity tabs (like
        ' NASS Other Veg Oils) the peanut column group will be skipped so
        ' its cells remain untouched by Ctrl+U.
        Case t Like "*soybean*oil*", t Like "*soy*oil*"
            GetCommodityFromHeaderText = "soybeans"
        Case t Like "*sunflower*"
            GetCommodityFromHeaderText = "sunflower"
        Case t Like "*canola*", t Like "*rapeseed*"
            GetCommodityFromHeaderText = "canola"
        Case t Like "*cottonseed*"
            GetCommodityFromHeaderText = "cottonseed"
        Case t Like "*linseed*", t Like "*flaxseed*"
            GetCommodityFromHeaderText = "linseed"  ' NASS does not publish — will return 0 rows
        Case Else
            GetCommodityFromHeaderText = ""
    End Select
End Function

Private Function GetCommodityFromSheet(ByVal sheetName As String) As String
    ' Maps sheet names to commodity codes in the database
    Dim sn As String
    sn = LCase(Trim(sheetName))

    Select Case True
        Case sn Like "*soy*crush*" Or sn Like "*soy_crush*"
            GetCommodityFromSheet = "soybeans"
        Case sn Like "*canola*"
            GetCommodityFromSheet = "canola"
        Case sn Like "*cottonseed*"
            GetCommodityFromSheet = "cottonseed"
        Case sn Like "*corn*crush*" Or sn Like "*corn_grind*" Or sn Like "*grain*crush*"
            GetCommodityFromSheet = "corn_grind"
        Case sn Like "*corn*oil*" Or sn Like "*corn_oil*"
            GetCommodityFromSheet = "corn"
        Case sn Like "*sunflower*"
            GetCommodityFromSheet = "sunflower"
        Case sn Like "*palm_kernel*" Or sn Like "*palmkernel*"
            GetCommodityFromSheet = "palm_kernel"
        Case sn Like "*palm*" And Not (sn Like "*palm_kernel*" Or sn Like "*palmkernel*")
            GetCommodityFromSheet = "palm"
        Case sn Like "*coconut*"
            GetCommodityFromSheet = "coconut"
        Case sn Like "*safflower*"
            GetCommodityFromSheet = "safflower"
        Case sn Like "*peanut*"
            GetCommodityFromSheet = "peanut"
        Case sn Like "*tallow*edible*"
            GetCommodityFromSheet = "tallow_edible"
        Case sn Like "*tallow*inedible*"
            GetCommodityFromSheet = "tallow_inedible"
        Case sn Like "*tallow*technical*"
            GetCommodityFromSheet = "tallow_technical"
        Case sn Like "*tallow*"
            GetCommodityFromSheet = "tallow"
        Case sn Like "*lard*"
            GetCommodityFromSheet = "lard"
        Case sn Like "*choice*white*" Or sn Like "*cwg*"
            GetCommodityFromSheet = "choice_white_grease"
        Case sn Like "*yellow*grease*"
            GetCommodityFromSheet = "yellow_grease"
        Case sn Like "*poultry*fat*"
            GetCommodityFromSheet = "poultry_fat"
        Case sn Like "*poultry*meal*" Or sn Like "*poultry*by*product*"
            GetCommodityFromSheet = "poultry_byproduct_meal"
        Case sn Like "*feather*meal*"
            GetCommodityFromSheet = "feather_meal"
        Case sn Like "*meat*meal*"
            GetCommodityFromSheet = "meat_meal"
        Case sn Like "*other*grease*"
            GetCommodityFromSheet = "other_grease"
        Case Else
            GetCommodityFromSheet = ""
    End Select
End Function

Private Function IsNASSLowCITab(ByVal sheetName As String) As Boolean
    ' Match "NASS Low CI" sheet (case-insensitive). Pattern is intentionally
    ' narrow — must start with "nass" and contain both "low" and "ci" — so it
    ' can't accidentally swallow other tabs (e.g., "NASS Other Veg Oils").
    Dim sn As String
    sn = LCase(Trim(sheetName))
    IsNASSLowCITab = (sn = "nass low ci") Or (sn Like "nass*low*ci*")
End Function

' =============================================================================
' MAIN UPDATE FUNCTIONS
' =============================================================================

Public Sub UpdateFatsOilsData()
    ' Quick update - latest 6 months of available data
    ' Keyboard shortcut: Ctrl+U
    ' On multi-commodity tabs: updates ALL commodity blocks
    DoUpdate 6, False
End Sub

Public Sub UpdateFatsOilsDataCustom()
    ' Custom update with user-specified months
    ' Keyboard shortcut: Ctrl+Shift+U
    ' On multi-commodity tabs: prompts "all vs cursor only"
    Dim monthCount As String
    monthCount = InputBox("How many months of data to update?" & vbCrLf & vbCrLf & _
                          "Enter a number (e.g., 12 for last 12 months)" & vbCrLf & _
                          "Or enter 0 to update ALL available data", _
                          "Fats & Oils Updater", "6")
    If monthCount = "" Then Exit Sub
    If Not IsNumeric(monthCount) Then
        MsgBox "Please enter a valid number.", vbExclamation, "Fats & Oils Updater"
        Exit Sub
    End If
    DoUpdate CInt(monthCount), True
End Sub

' Figure out which commodity block contains the given column. Returns the
' block key ("" if column is not inside any detected block).
Private Function FindBlockForColumn(blocks As Object, ByVal col As Integer) As String
    Dim k As Variant
    For Each k In blocks.Keys
        Dim sCol As Integer, eCol As Integer
        sCol = CInt(blocks(k)(0))
        eCol = CInt(blocks(k)(1))
        If col >= sCol And col <= eCol Then
            FindBlockForColumn = CStr(k)
            Exit Function
        End If
    Next k
    FindBlockForColumn = ""
End Function

' Single dispatch: figure out whether the active sheet is single-commodity
' (commodity from sheet name) or multi-commodity (commodity blocks in row 1).
' offerCursorScope: when True and on multi-commodity tab, offer to update
' only the block containing ActiveCell.
Private Sub DoUpdate(monthCount As Integer, offerCursorScope As Boolean)
    Dim ws As Worksheet
    Set ws = ActiveSheet

    ' NASS Low CI tab needs a dedicated handler: its commodities (CWG, lard,
    ' tallow varieties, yellow grease, etc.) live in gold.nass_low_ci_matrix,
    ' NOT in gold.fats_oils_crush_matrix / silver.crush_attribute_reference.
    ' The matrix view returns raw pounds — we divide by 1000 to display 000 lbs,
    ' matching the unit convention already in use on NASS Other Veg Oils.
    If IsNASSLowCITab(ws.name) Then
        Dim lowCIResp As VbMsgBoxResult
        lowCIResp = MsgBox("Update " & ws.name & " with the latest " & monthCount & _
                           " months? (NASS Low CI uses gold.nass_low_ci_matrix; values" & _
                           " will be displayed in 000 lbs.)", _
                           vbYesNo + vbQuestion, "Fats & Oils Updater")
        If lowCIResp <> vbYes Then Exit Sub
        Dim lowCIRes As CommodityUpdateResult
        lowCIRes = UpdateNASSLowCI(monthCount)
        ShowSingleResult "NASS Low CI", lowCIRes
        Exit Sub
    End If

    Dim commodity As String
    commodity = GetCommodityFromSheet(ws.name)

    If commodity <> "" Then
        ' Single-commodity tab — use full column range
        Dim resp As VbMsgBoxResult
        resp = MsgBox("Update " & ws.name & " (" & commodity & ") with the latest " & _
                      monthCount & " months?", vbYesNo + vbQuestion, "Fats & Oils Updater")
        If resp <> vbYes Then Exit Sub
        Dim single_r As CommodityUpdateResult
        single_r = UpdateFromDatabase(commodity, monthCount, 1, ws.Columns.Count)
        ShowSingleResult commodity, single_r
        Exit Sub
    End If

    ' Multi-commodity tab: scan row 1 for commodity blocks
    Dim blocks As Object
    Set blocks = ScanRow1ForCommodityBlocks(ws)

    If blocks.Count = 0 Then
        MsgBox "Cannot determine commodity from sheet name: " & ws.name & vbCrLf & vbCrLf & _
               "Expected names like soy_crush, canola_crush, etc., OR a multi-commodity " & _
               "tab with commodity headers in row 1 (e.g., 'Palm Oil', 'Coconut Oil').", _
               vbExclamation, "Fats & Oils Updater"
        Exit Sub
    End If

    ' If Ctrl+Shift+U on a multi-commodity tab, offer to narrow to cursor block
    Dim cursorCommodity As String
    cursorCommodity = ""
    If offerCursorScope Then
        cursorCommodity = FindBlockForColumn(blocks, ActiveCell.Column)
        If cursorCommodity <> "" Then
            Dim scope As VbMsgBoxResult
            scope = MsgBox("Multi-commodity tab detected (" & blocks.Count & " commodities)." & vbCrLf & vbCrLf & _
                           "Cursor is inside the '" & cursorCommodity & "' block." & vbCrLf & vbCrLf & _
                           "  YES   Update only " & cursorCommodity & vbCrLf & _
                           "  NO    Update ALL " & blocks.Count & " commodities" & vbCrLf & _
                           "  CANCEL  Abort", _
                           vbYesNoCancel + vbQuestion, "Fats & Oils Updater")
            If scope = vbCancel Then Exit Sub
            If scope = vbNo Then cursorCommodity = ""  ' fall through to all-commodity flow
        End If
    End If

    ' Build the update list — either just the cursor commodity or all
    Dim targets As Object
    Set targets = CreateObject("Scripting.Dictionary")
    targets.CompareMode = vbTextCompare

    If cursorCommodity <> "" Then
        targets(cursorCommodity) = blocks(cursorCommodity)
    Else
        ' Summary + confirmation for ALL-commodity path
        Dim summary As String
        summary = "Multi-commodity tab detected. Update " & blocks.Count & " commodities " & _
                  "with latest " & monthCount & " months?" & vbCrLf & vbCrLf
        Dim kk As Variant
        For Each kk In blocks.Keys
            summary = summary & "  " & kk & " (cols " & ColLetter(CInt(blocks(kk)(0))) & _
                      "-" & ColLetter(CInt(blocks(kk)(1))) & ")" & vbCrLf
        Next kk
        If MsgBox(summary, vbYesNo + vbQuestion, "Fats & Oils Updater") <> vbYes Then Exit Sub
        Dim kk2 As Variant
        For Each kk2 In blocks.Keys
            targets(kk2) = blocks(kk2)
        Next kk2
    End If

    Dim totalCells As Long, totalSkipped As Long
    Dim allUnmatched As String
    totalCells = 0
    totalSkipped = 0
    Dim k As Variant
    For Each k In targets.Keys
        Dim r As CommodityUpdateResult
        Dim sCol As Integer, eCol As Integer
        sCol = CInt(targets(k)(0))
        eCol = CInt(targets(k)(1))
        r = UpdateFromDatabase(CStr(k), monthCount, sCol, eCol)
        totalCells = totalCells + r.cellsUpdated
        totalSkipped = totalSkipped + r.headersUnmatched
        If r.unmatchedList <> "" Or r.excelHeadersRead <> "" Then
            allUnmatched = allUnmatched & vbCrLf & vbCrLf & "[" & k & "]  (cols " & _
                           ColLetter(sCol) & "-" & ColLetter(eCol) & ")"
            If r.unmatchedList <> "" Then
                allUnmatched = allUnmatched & vbCrLf & "  DB patterns not matched:" & r.unmatchedList
            End If
            If r.excelHeadersRead <> "" Then
                allUnmatched = allUnmatched & vbCrLf & "  Excel columns in block:" & r.excelHeadersRead
            End If
        End If
    Next k

    Dim scopeLabel As String
    If cursorCommodity <> "" Then
        scopeLabel = "single commodity (" & cursorCommodity & ")"
    Else
        scopeLabel = "all " & targets.Count & " commodities"
    End If

    Dim msg As String
    msg = "Update complete for " & scopeLabel & "." & vbCrLf & vbCrLf & _
          "Cells updated:          " & totalCells & vbCrLf & _
          "Headers not matched:    " & totalSkipped
    If allUnmatched <> "" Then
        msg = msg & vbCrLf & allUnmatched
    End If
    MsgBox msg, vbInformation, "Fats & Oils Updater"
End Sub

Private Sub ShowSingleResult(ByVal commodity As String, r As CommodityUpdateResult)
    Dim msg As String
    msg = "Update complete for " & commodity & "." & vbCrLf & vbCrLf & _
          "Cells updated:          " & r.cellsUpdated & vbCrLf & _
          "Headers not matched:    " & r.headersUnmatched
    If r.unmatchedList <> "" Then
        msg = msg & vbCrLf & vbCrLf & "DB patterns not matched:" & r.unmatchedList
    End If
    If r.excelHeadersRead <> "" Then
        msg = msg & vbCrLf & vbCrLf & "Excel headers read:" & r.excelHeadersRead
    End If
    MsgBox msg, vbInformation, "Fats & Oils Updater"
End Sub

Private Function ColLetter(ByVal col As Integer) As String
    ' Convert column number to letter (1=A, 27=AA, etc.)
    Dim s As String
    s = ""
    Do While col > 0
        Dim rem_ As Integer
        rem_ = ((col - 1) Mod 26)
        s = Chr(65 + rem_) & s
        col = (col - 1) \ 26
    Loop
    ColLetter = s
End Function

' Walk row 1 of the active sheet, looking for commodity-block headers
' (e.g., "Palm Kernel Oil", "Corn Oil"). Returns a Dictionary keyed by
' commodity code with values = Array(start_col, end_col).
Private Function ScanRow1ForCommodityBlocks(ws As Worksheet) As Object
    Dim blocks As Object
    Set blocks = CreateObject("Scripting.Dictionary")
    blocks.CompareMode = vbTextCompare

    Dim lastCol As Integer
    lastCol = ws.Cells(1, ws.Columns.Count).End(xlToLeft).Column
    If lastCol < 2 Then
        Set ScanRow1ForCommodityBlocks = blocks
        Exit Function
    End If

    ' Collect (commodity, start_col) pairs in scan order
    Dim starts As Object
    Set starts = CreateObject("Scripting.Dictionary")

    Dim c As Integer, lastComm As String, lastStart As Integer
    lastComm = ""
    lastStart = 0
    For c = 1 To lastCol
        Dim raw As String
        raw = Trim(CStr(ws.Cells(1, c).Value & ""))
        If raw <> "" Then
            Dim comm As String
            comm = GetCommodityFromHeaderText(raw)
            If comm <> "" Then
                ' Close out previous block
                If lastComm <> "" Then
                    blocks(lastComm) = Array(lastStart, c - 1)
                End If
                lastComm = comm
                lastStart = c
            End If
        End If
    Next c
    ' Close out the final block
    If lastComm <> "" Then
        blocks(lastComm) = Array(lastStart, lastCol)
    End If

    Set ScanRow1ForCommodityBlocks = blocks
End Function

' =============================================================================
' DATABASE CONNECTION
' =============================================================================

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
' HEADER MATCHING ENGINE
' =============================================================================

Private Function BuildHeaderMap(ws As Worksheet, ByVal headerRow As Integer, _
                                 ByVal startCol As Integer, ByVal endCol As Integer) As Object
    ' Reads the specified header row in the column range [startCol, endCol]
    ' and builds a dictionary of header_text -> column_number.
    ' Used by single-commodity tabs (full row) AND multi-commodity tabs
    ' (one block at a time).

    Dim headerMap As Object
    Set headerMap = CreateObject("Scripting.Dictionary")
    headerMap.CompareMode = vbTextCompare

    If endCol < startCol Then
        endCol = ws.Cells(headerRow, ws.Columns.Count).End(xlToLeft).Column
    End If

    Dim c As Integer
    Dim headerText As String
    For c = startCol To endCol
        headerText = Trim(CStr(ws.Cells(headerRow, c).Value & ""))
        If headerText <> "" Then
            If Not headerMap.Exists(headerText) Then
                headerMap.Add headerText, c
            End If
        End If
    Next c

    Set BuildHeaderMap = headerMap
End Function

Private Function FindColumnForHeader(headerMap As Object, headerPattern As String) As Integer
    ' Find the column number for a given header pattern
    ' First tries exact match, then tries partial matching

    ' Exact match (case-insensitive due to CompareMode)
    If headerMap.Exists(headerPattern) Then
        FindColumnForHeader = headerMap(headerPattern)
        Exit Function
    End If

    ' Partial match - check if any header contains the pattern or vice versa
    Dim key As Variant
    Dim lowerPattern As String
    lowerPattern = LCase(headerPattern)

    For Each key In headerMap.Keys
        If InStr(1, LCase(CStr(key)), lowerPattern, vbTextCompare) > 0 Then
            FindColumnForHeader = headerMap(key)
            Exit Function
        End If
        If InStr(1, lowerPattern, LCase(CStr(key)), vbTextCompare) > 0 Then
            FindColumnForHeader = headerMap(key)
            Exit Function
        End If
    Next key

    FindColumnForHeader = 0  ' Not found
End Function

' =============================================================================
' MAIN UPDATE LOGIC
' =============================================================================

' Updates one commodity's columns within [startCol, endCol] in the active sheet.
' For single-commodity tabs pass startCol=1, endCol=ws.Columns.Count.
' For multi-commodity tabs (e.g., NASS Other Veg Oils) pass the block range
' detected from row 1 commodity headers.
Private Function UpdateFromDatabase(ByVal commodity As String, ByVal monthCount As Integer, _
                                     ByVal startCol As Integer, ByVal endCol As Integer) _
                                     As CommodityUpdateResult
    Dim conn As Object
    Dim rs As Object
    Dim ws As Worksheet
    Dim sql As String
    Dim res As CommodityUpdateResult
    res.cellsUpdated = 0
    res.headersUnmatched = 0
    res.unmatchedList = ""
    res.excelHeadersRead = ""

    Application.StatusBar = "Connecting to database..."
    Application.Cursor = xlWait
    DoEvents

    Set conn = GetConnection()
    If conn Is Nothing Then
        Application.StatusBar = False
        Application.Cursor = xlDefault
        UpdateFromDatabase = res
        Exit Function
    End If

    Set ws = ActiveSheet

    ' Auto-detect header row from data start row
    Dim headerRow As Integer
    Dim dataStartRow As Integer
    headerRow = DetectHeaderRow(ws)
    dataStartRow = DetectDataStartRow(ws)

    Application.StatusBar = "Reading column headers (row " & headerRow & ", cols " & _
                            startCol & "-" & endCol & ")..."
    DoEvents
    Dim headerMap As Object
    Set headerMap = BuildHeaderMap(ws, headerRow, startCol, endCol)

    ' Capture what we saw for diagnostics
    Dim hk As Variant
    For Each hk In headerMap.Keys
        res.excelHeadersRead = res.excelHeadersRead & vbCrLf & "  - " & _
                               CStr(hk) & " (" & ColLetter(CInt(headerMap(hk))) & ")"
    Next hk

    Application.StatusBar = "Fetching " & commodity & " data..."
    DoEvents

    ' Pull attribute_code alongside header_pattern so we can dedupe
    ' primary/alt siblings (e.g., crude_oil_refined vs crude_oil_refined_alt
    ' are the same metric — don't report both as unmatched).
    ' ORDER BY puts alt rows FIRST so the short-form names that the spreadsheet
    ' typically uses get tried before the long-form primaries.
    If monthCount > 0 Then
        sql = "SELECT year, month, attribute_code, header_pattern, display_value " & _
              "FROM gold.fats_oils_crush_matrix " & _
              "WHERE commodity = '" & commodity & "' " & _
              "AND display_value IS NOT NULL " & _
              "AND (year, month) IN (" & _
              "  SELECT DISTINCT year, month FROM gold.fats_oils_crush_matrix " & _
              "  WHERE commodity = '" & commodity & "' AND display_value IS NOT NULL " & _
              "  ORDER BY year DESC, month DESC LIMIT " & monthCount & _
              ") ORDER BY year, month, " & _
              "  CASE WHEN attribute_code LIKE '%\_alt' ESCAPE '\' THEN 0 ELSE 1 END, " & _
              "  attribute_code"
    Else
        sql = "SELECT year, month, attribute_code, header_pattern, display_value " & _
              "FROM gold.fats_oils_crush_matrix " & _
              "WHERE commodity = '" & commodity & "' AND display_value IS NOT NULL " & _
              "ORDER BY year, month, " & _
              "  CASE WHEN attribute_code LIKE '%\_alt' ESCAPE '\' THEN 0 ELSE 1 END, " & _
              "  attribute_code"
    End If

    On Error GoTo QueryError
    Set rs = CreateObject("ADODB.Recordset")
    rs.Open sql, conn

    If rs.EOF Then
        rs.Close
        conn.Close
        Application.StatusBar = False
        Application.Cursor = xlDefault
        UpdateFromDatabase = res
        Exit Function
    End If

    Application.StatusBar = "Updating " & commodity & " cells..."
    DoEvents

    ' Track unmatched at metric-key level (attribute_code minus _alt suffix)
    ' so siblings don't double-report. A metric is "satisfied" if ANY of its
    ' header_patterns matched a column.
    Dim unmatchedByMetric As Object
    Dim satisfiedMetrics As Object
    Dim writtenCells As Object   ' avoid double-writing same row+col when sibling matches
    Set unmatchedByMetric = CreateObject("Scripting.Dictionary")
    Set satisfiedMetrics = CreateObject("Scripting.Dictionary")
    Set writtenCells = CreateObject("Scripting.Dictionary")

    Do While Not rs.EOF
        Dim yr As Integer, mo As Integer
        Dim headerPattern As String
        Dim attrCode As String
        Dim metricKey As String
        Dim displayValue As Variant

        yr = rs("year")
        mo = rs("month")
        attrCode = rs("attribute_code") & ""
        headerPattern = rs("header_pattern") & ""
        displayValue = rs("display_value")

        ' Strip trailing "_alt" to get the underlying metric
        If Right(attrCode, 4) = "_alt" Then
            metricKey = Left(attrCode, Len(attrCode) - 4)
        Else
            metricKey = attrCode
        End If

        Dim targetCol As Integer
        targetCol = FindColumnForHeader(headerMap, headerPattern)

        If targetCol > 0 Then
            Dim targetRow As Integer
            targetRow = FindRowForDate(ws, yr, mo, dataStartRow)

            If targetRow > 0 Then
                If Not IsNull(displayValue) And displayValue <> "" Then
                    Dim cellKey As String
                    cellKey = targetRow & ":" & targetCol
                    If Not writtenCells.Exists(cellKey) Then
                        ws.Cells(targetRow, targetCol).Value = CDbl(displayValue)
                        writtenCells.Add cellKey, True
                        res.cellsUpdated = res.cellsUpdated + 1
                    End If
                End If
            End If
            satisfiedMetrics(metricKey) = True
        Else
            If headerPattern <> "" Then
                ' Record the FIRST unmatched pattern per metric for the report
                If Not unmatchedByMetric.Exists(metricKey) Then
                    unmatchedByMetric.Add metricKey, headerPattern
                End If
            End If
        End If

        rs.MoveNext
    Loop

    rs.Close
    conn.Close

    Application.StatusBar = False
    Application.Cursor = xlDefault

    ' Only report metrics where NEITHER primary nor alt matched
    Dim k As Variant
    For Each k In unmatchedByMetric.Keys
        If Not satisfiedMetrics.Exists(k) Then
            res.unmatchedList = res.unmatchedList & vbCrLf & "  - " & unmatchedByMetric(k)
            res.headersUnmatched = res.headersUnmatched + 1
        End If
    Next k

    UpdateFromDatabase = res
    Exit Function

QueryError:
    Application.StatusBar = False
    Application.Cursor = xlDefault
    MsgBox "Query error for " & commodity & ":" & vbCrLf & vbCrLf & Err.Description, _
           vbCritical, "Fats & Oils Updater"
    If Not conn Is Nothing Then
        On Error Resume Next
        conn.Close
    End If
    UpdateFromDatabase = res
End Function

' =============================================================================
' NASS LOW CI TAB HANDLER
' =============================================================================

' Map (commodity_text_in_row_1, item_text_in_row_2) -> field name in
' gold.nass_low_ci_matrix. Built once per call. Keys are lowercased to be
' resilient to header capitalization changes.
Private Function BuildNASSLowCIFieldMap() As Object
    Dim m As Object
    Set m = CreateObject("Scripting.Dictionary")
    m.CompareMode = vbTextCompare

    ' Choice White Grease
    m("choice white grease|production") = "cwg_production"
    m("choice white grease|processing use") = "cwg_processing_use"
    m("choice white grease|removal for processing") = "cwg_processing_use"
    m("choice white grease|end-of-month stocks") = "cwg_stocks"
    m("choice white grease|stocks") = "cwg_stocks"
    m("cwg|production") = "cwg_production"
    m("cwg|processing use") = "cwg_processing_use"
    m("cwg|stocks") = "cwg_stocks"

    ' Feather Meal
    m("feather meal|production") = "feather_meal_production"
    m("feather meal|end-of-month stocks") = "feather_meal_stocks"
    m("feather meal|stocks") = "feather_meal_stocks"

    ' Lard
    m("lard|production") = "lard_production"
    m("lard|processing use") = "lard_processing_use"
    m("lard|removal for processing") = "lard_processing_use"
    m("lard|end-of-month stocks") = "lard_stocks"
    m("lard|stocks") = "lard_stocks"

    ' Meat and Bone Meal
    m("meat and bone meal|production") = "mbm_production"
    m("meat and bone meal|end-of-month stocks") = "mbm_stocks"
    m("meat and bone meal|stocks") = "mbm_stocks"
    m("meat & bone meal|production") = "mbm_production"
    m("meat & bone meal|stocks") = "mbm_stocks"
    m("mbm|production") = "mbm_production"
    m("mbm|stocks") = "mbm_stocks"

    ' Other Grease (= grease excluding the named varieties)
    m("other grease|production") = "other_grease_production"
    m("other grease|processing use") = "other_grease_processing_use"
    m("other grease|removal for processing") = "other_grease_processing_use"
    m("other grease|end-of-month stocks") = "other_grease_stocks"
    m("other grease|stocks") = "other_grease_stocks"

    ' Poultry Fats
    m("poultry fats|production") = "poultry_fat_production"
    m("poultry fat|production") = "poultry_fat_production"
    m("poultry fats|processing use") = "poultry_fat_processing_use"
    m("poultry fat|processing use") = "poultry_fat_processing_use"
    m("poultry fats|removal for processing") = "poultry_fat_processing_use"
    m("poultry fat|removal for processing") = "poultry_fat_processing_use"
    m("poultry fats|end-of-month stocks") = "poultry_fat_stocks"
    m("poultry fat|end-of-month stocks") = "poultry_fat_stocks"
    m("poultry fats|stocks") = "poultry_fat_stocks"
    m("poultry fat|stocks") = "poultry_fat_stocks"

    ' Poultry By-Product
    m("poultry by-product|production") = "poultry_byproduct_production"
    m("poultry by-product|end-of-month stocks") = "poultry_byproduct_stocks"
    m("poultry by-product|stocks") = "poultry_byproduct_stocks"
    m("poultry byproduct|production") = "poultry_byproduct_production"
    m("poultry byproduct|stocks") = "poultry_byproduct_stocks"
    m("poultry by-product meal|production") = "poultry_byproduct_production"
    m("poultry by-product meal|stocks") = "poultry_byproduct_stocks"

    ' Edible Tallow
    m("edible tallow|production") = "edible_tallow_production"
    m("edible tallow|processing use") = "edible_tallow_processing_use"
    m("edible tallow|removal for processing") = "edible_tallow_processing_use"
    m("edible tallow|end-of-month stocks") = "edible_tallow_stocks"
    m("edible tallow|stocks") = "edible_tallow_stocks"

    ' Inedible Tallow
    m("inedible tallow|production") = "inedible_tallow_production"
    m("inedible tallow|processing use") = "inedible_tallow_processing_use"
    m("inedible tallow|removal for processing") = "inedible_tallow_processing_use"
    m("inedible tallow|end-of-month stocks") = "inedible_tallow_stocks"
    m("inedible tallow|stocks") = "inedible_tallow_stocks"

    ' Technical Tallow
    m("technical tallow|production") = "technical_tallow_production"
    m("technical tallow|processing use") = "technical_tallow_processing_use"
    m("technical tallow|removal for processing") = "technical_tallow_processing_use"
    m("technical tallow|end-of-month stocks") = "technical_tallow_stocks"
    m("technical tallow|stocks") = "technical_tallow_stocks"

    ' Yellow Grease
    m("yellow grease|production") = "yellow_grease_production"
    m("yellow grease|processing use") = "yellow_grease_processing_use"
    m("yellow grease|removal for processing") = "yellow_grease_processing_use"
    m("yellow grease|end-of-month stocks") = "yellow_grease_stocks"
    m("yellow grease|stocks") = "yellow_grease_stocks"

    Set BuildNASSLowCIFieldMap = m
End Function

' Walk row 1 (commodity) and row 2 (item) of the active sheet to identify which
' (commodity, item) pair lives in each spreadsheet column. The most recent
' commodity name in row 1 sticks to subsequent columns until another commodity
' name appears (the row-1 headers are typically merged across each block).
Private Function ScanLowCIColumnLayout(ws As Worksheet, fieldMap As Object) As Object
    Dim layout As Object
    Set layout = CreateObject("Scripting.Dictionary")  ' col -> field_name

    Dim lastCol As Long
    lastCol = ws.Cells(1, ws.Columns.Count).End(xlToLeft).Column
    If lastCol < 2 Then lastCol = ws.Cells(2, ws.Columns.Count).End(xlToLeft).Column

    Dim currentCommodity As String
    currentCommodity = ""

    Dim c As Long
    For c = 2 To lastCol
        Dim row1Text As String, row2Text As String
        row1Text = LCase(Trim(CStr(ws.Cells(1, c).Value & "")))
        row2Text = LCase(Trim(CStr(ws.Cells(2, c).Value & "")))
        If row1Text <> "" Then currentCommodity = row1Text
        If currentCommodity <> "" And row2Text <> "" Then
            Dim key As String
            key = currentCommodity & "|" & row2Text
            If fieldMap.Exists(key) Then
                layout(c) = fieldMap(key)
            End If
        End If
    Next c

    Set ScanLowCIColumnLayout = layout
End Function

Private Function UpdateNASSLowCI(ByVal monthCount As Integer) As CommodityUpdateResult
    ' Pulls from gold.nass_low_ci_matrix (returns RAW POUNDS) and writes
    ' value/1000 = 000 lbs to the matching cells. Tab layout:
    '   Row 1: commodity name (spans the metric columns for that commodity)
    '   Row 2: metric name (Production / Processing Use / End-of-Month Stocks)
    '   Row 3: unit label (set to "000 lbs" by this routine)
    '   Row 4+: dates in column A, data in commodity columns

    Dim res As CommodityUpdateResult
    res.cellsUpdated = 0
    res.headersUnmatched = 0
    res.unmatchedList = ""
    res.excelHeadersRead = ""

    Dim ws As Worksheet
    Set ws = ActiveSheet

    Dim conn As Object
    Application.StatusBar = "Connecting to database..."
    Application.Cursor = xlWait
    DoEvents
    Set conn = GetConnection()
    If conn Is Nothing Then
        Application.StatusBar = False
        Application.Cursor = xlDefault
        UpdateNASSLowCI = res
        Exit Function
    End If

    Application.StatusBar = "Mapping NASS Low CI columns..."
    DoEvents
    Dim fieldMap As Object
    Set fieldMap = BuildNASSLowCIFieldMap()
    Dim layout As Object
    Set layout = ScanLowCIColumnLayout(ws, fieldMap)

    ' Capture diagnostic info on what got mapped
    Dim k As Variant
    For Each k In layout.Keys
        res.excelHeadersRead = res.excelHeadersRead & vbCrLf & "  - col " & _
                                ColLetter(CInt(k)) & " -> " & layout(k)
    Next k

    If layout.Count = 0 Then
        conn.Close
        Application.StatusBar = False
        Application.Cursor = xlDefault
        res.unmatchedList = vbCrLf & "  No (commodity, metric) pairs in row 1/2 matched the gold.nass_low_ci_matrix fields."
        UpdateNASSLowCI = res
        Exit Function
    End If

    Application.StatusBar = "Fetching NASS Low CI data..."
    DoEvents

    Dim sql As String
    If monthCount > 0 Then
        sql = "SELECT calendar_year, month, " & _
              "cwg_production, cwg_processing_use, cwg_stocks, " & _
              "feather_meal_production, feather_meal_stocks, " & _
              "lard_production, lard_processing_use, lard_stocks, " & _
              "mbm_production, mbm_stocks, " & _
              "other_grease_production, other_grease_processing_use, other_grease_stocks, " & _
              "poultry_fat_production, poultry_fat_processing_use, poultry_fat_stocks, " & _
              "poultry_byproduct_production, poultry_byproduct_stocks, " & _
              "edible_tallow_production, edible_tallow_processing_use, edible_tallow_stocks, " & _
              "inedible_tallow_production, inedible_tallow_processing_use, inedible_tallow_stocks, " & _
              "technical_tallow_production, technical_tallow_processing_use, technical_tallow_stocks, " & _
              "yellow_grease_production, yellow_grease_processing_use, yellow_grease_stocks " & _
              "FROM gold.nass_low_ci_matrix " & _
              "ORDER BY calendar_year DESC, month DESC " & _
              "LIMIT " & monthCount
    Else
        sql = "SELECT calendar_year, month, " & _
              "cwg_production, cwg_processing_use, cwg_stocks, " & _
              "feather_meal_production, feather_meal_stocks, " & _
              "lard_production, lard_processing_use, lard_stocks, " & _
              "mbm_production, mbm_stocks, " & _
              "other_grease_production, other_grease_processing_use, other_grease_stocks, " & _
              "poultry_fat_production, poultry_fat_processing_use, poultry_fat_stocks, " & _
              "poultry_byproduct_production, poultry_byproduct_stocks, " & _
              "edible_tallow_production, edible_tallow_processing_use, edible_tallow_stocks, " & _
              "inedible_tallow_production, inedible_tallow_processing_use, inedible_tallow_stocks, " & _
              "technical_tallow_production, technical_tallow_processing_use, technical_tallow_stocks, " & _
              "yellow_grease_production, yellow_grease_processing_use, yellow_grease_stocks " & _
              "FROM gold.nass_low_ci_matrix " & _
              "ORDER BY calendar_year, month"
    End If

    On Error GoTo LowCIQueryError
    Dim rs As Object
    Set rs = CreateObject("ADODB.Recordset")
    rs.Open sql, conn

    If rs.EOF Then
        rs.Close
        conn.Close
        Application.StatusBar = False
        Application.Cursor = xlDefault
        UpdateNASSLowCI = res
        Exit Function
    End If

    ' Detect data start row using existing helper (looks for first date in col A)
    Dim dataStartRow As Integer
    dataStartRow = DetectDataStartRow(ws)
    If dataStartRow < 4 Then dataStartRow = 4   ' Low CI tab has data starting at row 4

    Dim writtenCells As Object
    Set writtenCells = CreateObject("Scripting.Dictionary")

    Application.StatusBar = "Updating cells (raw lbs / 1000 -> 000 lbs)..."
    DoEvents

    Do While Not rs.EOF
        Dim yr As Integer, mo As Integer
        yr = rs("calendar_year")
        mo = rs("month")

        Dim targetRow As Integer
        targetRow = FindRowForDate(ws, yr, mo, dataStartRow)

        If targetRow > 0 Then
            Dim colVar As Variant
            For Each colVar In layout.Keys
                Dim col As Integer
                col = CInt(colVar)
                Dim fieldName As String
                fieldName = CStr(layout(col))
                Dim raw As Variant
                raw = rs(fieldName)
                If Not IsNull(raw) And raw <> "" Then
                    Dim cellKey As String
                    cellKey = targetRow & ":" & col
                    If Not writtenCells.Exists(cellKey) Then
                        ' Convert raw pounds to 000 lbs
                        ws.Cells(targetRow, col).Value = CDbl(raw) / 1000#
                        writtenCells.Add cellKey, True
                        res.cellsUpdated = res.cellsUpdated + 1
                    End If
                End If
            Next colVar
        End If

        rs.MoveNext
    Loop

    rs.Close
    conn.Close

    ' Stamp the unit row (row 3) with "000 lbs" for any column we just wrote
    Dim col2 As Variant
    For Each col2 In layout.Keys
        ws.Cells(3, CInt(col2)).Value = "000 lbs"
    Next col2

    Application.StatusBar = False
    Application.Cursor = xlDefault
    UpdateNASSLowCI = res
    Exit Function

LowCIQueryError:
    Application.StatusBar = False
    Application.Cursor = xlDefault
    MsgBox "Query error on NASS Low CI:" & vbCrLf & vbCrLf & Err.Description, _
           vbCritical, "Fats & Oils Updater"
    On Error Resume Next
    If Not conn Is Nothing Then conn.Close
    UpdateNASSLowCI = res
End Function

' =============================================================================
' HELPER FUNCTIONS
' =============================================================================

Private Function FindRowForDate(ws As Worksheet, ByVal yr As Integer, ByVal mo As Integer, _
                                 Optional ByVal dataStartRow As Integer = 0) As Integer
    ' Find row number for a given year/month by scanning the date column (A)

    Dim row As Integer
    Dim cellVal As Variant
    Dim cellDate As Date
    Dim lastRow As Integer

    If dataStartRow <= 0 Then dataStartRow = DEFAULT_DATA_START_ROW
    lastRow = ws.Cells(ws.Rows.Count, DATE_COLUMN).End(xlUp).row

    For row = dataStartRow To lastRow
        cellVal = ws.Cells(row, DATE_COLUMN).Value

        If IsDate(cellVal) Then
            cellDate = CDate(cellVal)
            If Year(cellDate) = yr And Month(cellDate) = mo Then
                FindRowForDate = row
                Exit Function
            End If
        ElseIf IsNumeric(cellVal) And cellVal > 0 Then
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

Public Sub AssignFatsOilsShortcuts()
    Application.OnKey "^u", "FatsOilsUpdaterSQL.UpdateFatsOilsData"
    Application.OnKey "^+u", "FatsOilsUpdaterSQL.UpdateFatsOilsDataCustom"

    MsgBox "Keyboard shortcuts assigned:" & vbCrLf & vbCrLf & _
           "Ctrl+U        Quick update (latest 6 months, all commodities)" & vbCrLf & _
           "Ctrl+Shift+U  Custom update (choose month count; on multi-commodity" & vbCrLf & _
           "              tabs, choose 'cursor block only' vs 'all')", _
           vbInformation, "Fats & Oils Updater"
End Sub

Public Sub RemoveFatsOilsShortcuts()
    Application.OnKey "^u"
    Application.OnKey "^+u"
End Sub
