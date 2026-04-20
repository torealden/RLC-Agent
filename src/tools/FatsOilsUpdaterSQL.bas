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

' =============================================================================
' MAIN UPDATE FUNCTIONS
' =============================================================================

Public Sub UpdateFatsOilsData()
    ' Quick update - latest 6 months of available data
    ' Keyboard shortcut: Ctrl+U
    DoUpdate 6
End Sub

Public Sub UpdateFatsOilsDataCustom()
    ' Custom update with user-specified months
    ' Keyboard shortcut: Ctrl+Shift+U
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
    DoUpdate CInt(monthCount)
End Sub

' Single dispatch: figure out whether the active sheet is single-commodity
' (commodity from sheet name) or multi-commodity (commodity blocks in row 1).
Private Sub DoUpdate(monthCount As Integer)
    Dim ws As Worksheet
    Set ws = ActiveSheet

    Dim commodity As String
    commodity = GetCommodityFromSheet(ws.name)

    If commodity <> "" Then
        ' Single-commodity tab — use full column range
        Dim resp As VbMsgBoxResult
        resp = MsgBox("Update " & ws.name & " (" & commodity & ") with the latest " & _
                      monthCount & " months?", vbYesNo + vbQuestion, "Fats & Oils Updater")
        If resp <> vbYes Then Exit Sub
        UpdateFromDatabase commodity, monthCount, 1, ws.Columns.Count
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

    Dim summary As String
    summary = "Multi-commodity tab detected. Update " & blocks.Count & " commodities " & _
              "with latest " & monthCount & " months?" & vbCrLf & vbCrLf
    Dim k As Variant
    For Each k In blocks.Keys
        summary = summary & "  " & k & " (cols " & blocks(k)(0) & "-" & blocks(k)(1) & ")" & vbCrLf
    Next k

    If MsgBox(summary, vbYesNo + vbQuestion, "Fats & Oils Updater") <> vbYes Then Exit Sub

    Dim totalCells As Long, totalSkipped As Long
    Dim allUnmatched As String
    totalCells = 0
    totalSkipped = 0
    For Each k In blocks.Keys
        Dim r As CommodityUpdateResult
        Dim sCol As Integer, eCol As Integer
        sCol = CInt(blocks(k)(0))
        eCol = CInt(blocks(k)(1))
        r = UpdateFromDatabase(CStr(k), monthCount, sCol, eCol)
        totalCells = totalCells + r.cellsUpdated
        totalSkipped = totalSkipped + r.headersUnmatched
        If r.unmatchedList <> "" Then
            allUnmatched = allUnmatched & vbCrLf & "[" & k & "]" & r.unmatchedList
        End If
    Next k

    Dim msg As String
    msg = "Multi-commodity update complete!" & vbCrLf & vbCrLf & _
          "Total cells updated: " & totalCells & vbCrLf & _
          "Total headers not matched: " & totalSkipped
    If allUnmatched <> "" Then
        msg = msg & vbCrLf & vbCrLf & "Unmatched headers:" & allUnmatched
    End If
    MsgBox msg, vbInformation, "Fats & Oils Updater"
End Sub

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

    Application.StatusBar = "Fetching " & commodity & " data..."
    DoEvents

    If monthCount > 0 Then
        sql = "SELECT year, month, header_pattern, display_value " & _
              "FROM gold.fats_oils_crush_matrix " & _
              "WHERE commodity = '" & commodity & "' " & _
              "AND display_value IS NOT NULL " & _
              "AND (year, month) IN (" & _
              "  SELECT DISTINCT year, month FROM gold.fats_oils_crush_matrix " & _
              "  WHERE commodity = '" & commodity & "' AND display_value IS NOT NULL " & _
              "  ORDER BY year DESC, month DESC LIMIT " & monthCount & _
              ") ORDER BY year, month, header_pattern"
    Else
        sql = "SELECT year, month, header_pattern, display_value " & _
              "FROM gold.fats_oils_crush_matrix " & _
              "WHERE commodity = '" & commodity & "' AND display_value IS NOT NULL " & _
              "ORDER BY year, month, header_pattern"
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

    Dim unmatchedSet As Object
    Set unmatchedSet = CreateObject("Scripting.Dictionary")

    Do While Not rs.EOF
        Dim yr As Integer, mo As Integer
        Dim headerPattern As String
        Dim displayValue As Variant

        yr = rs("year")
        mo = rs("month")
        headerPattern = rs("header_pattern") & ""
        displayValue = rs("display_value")

        Dim targetCol As Integer
        targetCol = FindColumnForHeader(headerMap, headerPattern)

        If targetCol > 0 Then
            Dim targetRow As Integer
            targetRow = FindRowForDate(ws, yr, mo, dataStartRow)

            If targetRow > 0 Then
                If Not IsNull(displayValue) And displayValue <> "" Then
                    ws.Cells(targetRow, targetCol).Value = CDbl(displayValue)
                    res.cellsUpdated = res.cellsUpdated + 1
                End If
            End If
        Else
            If headerPattern <> "" And Not unmatchedSet.Exists(headerPattern) Then
                unmatchedSet.Add headerPattern, True
                res.headersUnmatched = res.headersUnmatched + 1
            End If
        End If

        rs.MoveNext
    Loop

    rs.Close
    conn.Close

    Application.StatusBar = False
    Application.Cursor = xlDefault

    Dim k As Variant
    For Each k In unmatchedSet.Keys
        res.unmatchedList = res.unmatchedList & vbCrLf & "  - " & k
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
    Application.OnKey "^u", "UpdateFatsOilsData"
    Application.OnKey "^+u", "UpdateFatsOilsDataCustom"

    MsgBox "Keyboard shortcuts assigned:" & vbCrLf & vbCrLf & _
           "Ctrl+U = Quick update (latest 6 months)" & vbCrLf & _
           "Ctrl+Shift+U = Custom month count", vbInformation, "Fats & Oils Updater"
End Sub

Public Sub RemoveFatsOilsShortcuts()
    Application.OnKey "^u"
    Application.OnKey "^+u"
End Sub
