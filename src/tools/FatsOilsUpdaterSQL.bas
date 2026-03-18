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
Private Const HEADER_ROW As Integer = 3       ' Row with attribute headers
Private Const DATA_START_ROW As Integer = 5   ' First data row
Private Const DATE_COLUMN As Integer = 1      ' Column A = dates

' =============================================================================
' SHEET NAME TO COMMODITY MAPPING
' =============================================================================

Private Function GetCommodityFromSheet(sheetName As String) As String
    ' Maps sheet names to commodity codes in the database
    Dim name As String
    name = LCase(Trim(sheetName))

    Select Case True
        Case name Like "*soy*crush*" Or name Like "*soy_crush*"
            GetCommodityFromSheet = "soybeans"
        Case name Like "*canola*"
            GetCommodityFromSheet = "canola"
        Case name Like "*cottonseed*"
            GetCommodityFromSheet = "cottonseed"
        Case name Like "*corn*oil*" Or name Like "*corn_oil*"
            GetCommodityFromSheet = "corn"
        Case name Like "*sunflower*"
            GetCommodityFromSheet = "sunflower"
        Case name Like "*palm_kernel*" Or name Like "*palmkernel*"
            GetCommodityFromSheet = "palm_kernel"
        Case name Like "*palm*" And Not (name Like "*palm_kernel*" Or name Like "*palmkernel*")
            GetCommodityFromSheet = "palm"
        Case name Like "*coconut*"
            GetCommodityFromSheet = "coconut"
        Case name Like "*safflower*"
            GetCommodityFromSheet = "safflower"
        Case name Like "*peanut*"
            GetCommodityFromSheet = "peanut"
        Case name Like "*tallow*edible*"
            GetCommodityFromSheet = "tallow_edible"
        Case name Like "*tallow*inedible*"
            GetCommodityFromSheet = "tallow_inedible"
        Case name Like "*tallow*technical*"
            GetCommodityFromSheet = "tallow_technical"
        Case name Like "*tallow*"
            GetCommodityFromSheet = "tallow"
        Case name Like "*lard*"
            GetCommodityFromSheet = "lard"
        Case name Like "*choice*white*" Or name Like "*cwg*"
            GetCommodityFromSheet = "choice_white_grease"
        Case name Like "*yellow*grease*"
            GetCommodityFromSheet = "yellow_grease"
        Case name Like "*poultry*fat*"
            GetCommodityFromSheet = "poultry_fat"
        Case name Like "*poultry*meal*" Or name Like "*poultry*by*product*"
            GetCommodityFromSheet = "poultry_byproduct_meal"
        Case name Like "*feather*meal*"
            GetCommodityFromSheet = "feather_meal"
        Case name Like "*meat*meal*"
            GetCommodityFromSheet = "meat_meal"
        Case name Like "*other*grease*"
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

    Dim commodity As String
    commodity = GetCommodityFromSheet(ActiveSheet.name)

    If commodity = "" Then
        MsgBox "Cannot determine commodity from sheet name: " & ActiveSheet.name & vbCrLf & vbCrLf & _
               "Expected names like: soy_crush, canola_crush, cottonseed, corn_oil, " & _
               "sunflower, palm, tallow_edible, lard, etc.", _
               vbExclamation, "Fats & Oils Updater"
        Exit Sub
    End If

    Dim result As VbMsgBoxResult
    result = MsgBox("Update " & ActiveSheet.name & " (" & commodity & ") with the latest 6 months?", _
                    vbYesNo + vbQuestion, "Fats & Oils Updater")

    If result = vbYes Then
        UpdateFromDatabase commodity, 6
    End If
End Sub

Public Sub UpdateFatsOilsDataCustom()
    ' Custom update with user-specified months
    ' Keyboard shortcut: Ctrl+Shift+U

    Dim commodity As String
    commodity = GetCommodityFromSheet(ActiveSheet.name)

    If commodity = "" Then
        MsgBox "Cannot determine commodity from sheet name: " & ActiveSheet.name, _
               vbExclamation, "Fats & Oils Updater"
        Exit Sub
    End If

    Dim monthCount As String
    monthCount = InputBox("How many months of " & commodity & " data to update?" & vbCrLf & vbCrLf & _
                          "Enter a number (e.g., 12 for last 12 months)" & vbCrLf & _
                          "Or enter 0 to update ALL available data", _
                          "Fats & Oils Updater", "6")

    If monthCount = "" Then Exit Sub

    If Not IsNumeric(monthCount) Then
        MsgBox "Please enter a valid number.", vbExclamation, "Fats & Oils Updater"
        Exit Sub
    End If

    UpdateFromDatabase commodity, CInt(monthCount)
End Sub

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

Private Function BuildHeaderMap(ws As Worksheet) As Object
    ' Reads row 3 and builds a dictionary of header_text -> column_number
    ' Uses case-insensitive matching

    Dim headerMap As Object
    Set headerMap = CreateObject("Scripting.Dictionary")
    headerMap.CompareMode = vbTextCompare  ' Case-insensitive

    Dim lastCol As Integer
    lastCol = ws.Cells(HEADER_ROW, ws.Columns.Count).End(xlToLeft).Column

    Dim c As Integer
    Dim headerText As String
    For c = 1 To lastCol
        headerText = Trim(CStr(ws.Cells(HEADER_ROW, c).Value & ""))
        If headerText <> "" Then
            ' Store the first occurrence of each header
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

Private Sub UpdateFromDatabase(commodity As String, monthCount As Integer)
    Dim conn As Object
    Dim rs As Object
    Dim ws As Worksheet
    Dim sql As String
    Dim cellsUpdated As Long
    Dim rowsNotFound As Long
    Dim headersNotFound As Long
    Dim unmatchedHeaders As String

    Application.StatusBar = "Connecting to database..."
    Application.Cursor = xlWait
    DoEvents

    Set conn = GetConnection()
    If conn Is Nothing Then
        Application.StatusBar = False
        Application.Cursor = xlDefault
        Exit Sub
    End If

    Set ws = ActiveSheet

    ' Build header map from row 3
    Application.StatusBar = "Reading column headers..."
    DoEvents
    Dim headerMap As Object
    Set headerMap = BuildHeaderMap(ws)

    Application.StatusBar = "Fetching " & commodity & " data..."
    DoEvents

    ' Build query using the generic view (exclude NULLs to avoid writing "D" placeholders)
    If monthCount > 0 Then
        sql = "SELECT " & _
              "    year, month, header_pattern, display_value " & _
              "FROM gold.fats_oils_crush_matrix " & _
              "WHERE commodity = '" & commodity & "' " & _
              "AND display_value IS NOT NULL " & _
              "AND (year, month) IN ( " & _
              "    SELECT DISTINCT year, month " & _
              "    FROM gold.fats_oils_crush_matrix " & _
              "    WHERE commodity = '" & commodity & "' " & _
              "    AND display_value IS NOT NULL " & _
              "    ORDER BY year DESC, month DESC " & _
              "    LIMIT " & monthCount & _
              ") " & _
              "ORDER BY year, month, header_pattern"
    Else
        sql = "SELECT " & _
              "    year, month, header_pattern, display_value " & _
              "FROM gold.fats_oils_crush_matrix " & _
              "WHERE commodity = '" & commodity & "' " & _
              "AND display_value IS NOT NULL " & _
              "ORDER BY year, month, header_pattern"
    End If

    On Error GoTo QueryError
    Set rs = CreateObject("ADODB.Recordset")
    rs.Open sql, conn

    If rs.EOF Then
        MsgBox "No data found for commodity: " & commodity & vbCrLf & vbCrLf & _
               "Check that the commodity is configured in the database " & _
               "(silver.crush_attribute_reference table).", _
               vbInformation, "Fats & Oils Updater"
        rs.Close
        conn.Close
        Application.StatusBar = False
        Application.Cursor = xlDefault
        Exit Sub
    End If

    Application.StatusBar = "Updating cells..."
    DoEvents

    cellsUpdated = 0
    rowsNotFound = 0
    headersNotFound = 0
    unmatchedHeaders = ""

    ' Track which headers we could not match (to report once)
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

        ' Find the spreadsheet column by matching header
        Dim targetCol As Integer
        targetCol = FindColumnForHeader(headerMap, headerPattern)

        If targetCol > 0 Then
            ' Find the row for this (year, month)
            Dim targetRow As Integer
            targetRow = FindRowForDate(ws, yr, mo)

            If targetRow > 0 Then
                If Not IsNull(displayValue) And displayValue <> "" Then
                    ws.Cells(targetRow, targetCol).Value = CDbl(displayValue)
                    cellsUpdated = cellsUpdated + 1
                End If
            Else
                rowsNotFound = rowsNotFound + 1
            End If
        Else
            If headerPattern <> "" And Not unmatchedSet.Exists(headerPattern) Then
                unmatchedSet.Add headerPattern, True
                headersNotFound = headersNotFound + 1
            End If
        End If

        rs.MoveNext
    Loop

    rs.Close
    conn.Close

    Application.StatusBar = False
    Application.Cursor = xlDefault

    ' Build unmatched headers list for reporting
    If unmatchedSet.Count > 0 Then
        Dim k As Variant
        For Each k In unmatchedSet.Keys
            unmatchedHeaders = unmatchedHeaders & vbCrLf & "  - " & k
        Next k
    End If

    ' Report results
    Dim msg As String
    msg = "Update complete for " & commodity & "!" & vbCrLf & vbCrLf & _
          "Cells updated: " & cellsUpdated & vbCrLf & _
          "Months not found in sheet: " & rowsNotFound

    If headersNotFound > 0 Then
        msg = msg & vbCrLf & vbCrLf & _
              "Headers not matched (" & headersNotFound & "):" & unmatchedHeaders & vbCrLf & vbCrLf & _
              "These data fields exist in the database but no matching " & _
              "column header was found in row 3."
    End If

    MsgBox msg, vbInformation, "Fats & Oils Updater"

    Exit Sub

QueryError:
    Application.StatusBar = False
    Application.Cursor = xlDefault
    MsgBox "Query error:" & vbCrLf & vbCrLf & Err.Description, vbCritical, "Fats & Oils Updater"
    If Not conn Is Nothing Then conn.Close
End Sub

' =============================================================================
' HELPER FUNCTIONS
' =============================================================================

Private Function FindRowForDate(ws As Worksheet, yr As Integer, mo As Integer) As Integer
    ' Find row number for a given year/month by scanning the date column (A)

    Dim row As Integer
    Dim cellVal As Variant
    Dim cellDate As Date
    Dim lastRow As Integer

    lastRow = ws.Cells(ws.Rows.Count, DATE_COLUMN).End(xlUp).row

    For row = DATA_START_ROW To lastRow
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
