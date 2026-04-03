' =============================================================================
' CrushDataUpdater - Updates NASS crush, fats, and oils data from PostgreSQL
' =============================================================================
' Works with us_oilseed_crush.xlsm (or whatever we rename it to).
' Detects which tab is active and queries the correct data.
'
' Keyboard shortcut: Ctrl+U / Ctrl+Shift+U (custom months)
'
' Tab mappings:
'   soy_crush          -> NASS soybean crush data
'   canola_crush       -> NASS canola crush data
'   sunflower_crush    -> NASS sunflower crush data
'   cottonseed_crush    -> NASS cottonseed crush data
'   peanut_crush       -> NASS peanut crush data
'   NASS Low CI        -> NASS fats & greases (multi-commodity)
'   NASS Other Veg Oils -> NASS veg oil refining/stocks (multi-commodity)
'   Census Crush       -> Historical Census data (not updated via macro)
'   NOPA US Crush      -> NOPA monthly data (separate update)
' =============================================================================

Option Explicit

' Database connection settings
Private Const DB_SERVER As String = "rlc-commodities.c16c6wm826t7.us-east-2.rds.amazonaws.com"
Private Const DB_PORT As String = "5432"
Private Const DB_NAME As String = "rlc_commodities"
Private Const DB_USER As String = "postgres"
Private Const DB_PASSWORD As String = "SoupBoss1"

' =============================================================================
' MAIN UPDATE FUNCTIONS
' =============================================================================

Public Sub UpdateCrushData()
    ' Quick update - latest 3 months of available data
    ' Keyboard shortcut: Ctrl+U

    Dim result As VbMsgBoxResult
    result = MsgBox("Update " & ActiveSheet.Name & " with the latest 3 months of NASS data?", _
                    vbYesNo + vbQuestion, "Crush Data Updater")

    If result = vbYes Then
        UpdateFromDatabase 3
    End If
End Sub

Public Sub UpdateCrushDataCustom()
    ' Custom update with user-specified months
    ' Keyboard shortcut: Ctrl+Shift+U

    Dim monthCount As String
    monthCount = InputBox("How many months of data to update?" & vbCrLf & vbCrLf & _
                          "Enter a number (e.g., 12 for last 12 months, or 120 for full history)", _
                          "Crush Data Updater", "3")

    If monthCount = "" Then Exit Sub

    If Not IsNumeric(monthCount) Then
        MsgBox "Please enter a valid number.", vbExclamation, "Crush Data Updater"
        Exit Sub
    End If

    UpdateFromDatabase CInt(monthCount)
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
' MAIN UPDATE LOGIC
' =============================================================================

Private Sub UpdateFromDatabase(monthCount As Integer)
    Dim conn As Object
    Dim rs As Object
    Dim ws As Worksheet
    Dim tabName As String
    Dim sql As String
    Dim cellsUpdated As Long

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
    tabName = LCase(ws.Name)

    ' Build query based on active tab
    sql = BuildQueryForTab(tabName, monthCount)

    If sql = "" Then
        MsgBox "Tab '" & ws.Name & "' is not configured for automatic updates." & vbCrLf & vbCrLf & _
               "Supported tabs: soy_crush, canola_crush, sunflower_crush, " & _
               "cottonseed_crush, peanut_crush, NASS Low CI, NASS Other Veg Oils", _
               vbInformation, "Crush Data Updater"
        conn.Close
        Application.StatusBar = False
        Application.Cursor = xlDefault
        Exit Sub
    End If

    Application.StatusBar = "Fetching " & ws.Name & " data..."
    DoEvents

    On Error GoTo QueryError
    Set rs = CreateObject("ADODB.Recordset")
    rs.Open sql, conn

    If rs.BOF And rs.EOF Then
        rs.Close
        conn.Close
        Application.StatusBar = False
        Application.Cursor = xlDefault
        MsgBox "No data found for " & ws.Name & "." & vbCrLf & vbCrLf & _
               "The data may not have been collected yet.", vbInformation, "Crush Data Updater"
        Exit Sub
    End If

    ' Write data based on tab type
    If tabName = "nass low ci" Then
        cellsUpdated = WriteLowCIData(ws, rs)
    ElseIf tabName = "nass other veg oils" Then
        cellsUpdated = WriteMultiCommodityData(ws, rs)
    Else
        cellsUpdated = WriteSingleCommodityData(ws, rs)
    End If

    rs.Close
    conn.Close

    Application.StatusBar = False
    Application.Cursor = xlDefault

    MsgBox "Update complete!" & vbCrLf & vbCrLf & _
           "Cells updated: " & cellsUpdated, vbInformation, "Crush Data Updater"

    Exit Sub

QueryError:
    Application.StatusBar = False
    Application.Cursor = xlDefault
    MsgBox "Query error:" & vbCrLf & vbCrLf & Err.Description, vbCritical, "Crush Data Updater"
    If Not conn Is Nothing Then conn.Close
End Sub

' =============================================================================
' QUERY BUILDERS
' =============================================================================

Private Function BuildQueryForTab(tabName As String, monthCount As Integer) As String
    Dim sql As String

    Select Case tabName
        Case "soy_crush"
            sql = "SELECT calendar_year, month, attribute, realized_value " & _
                  "FROM silver.monthly_realized " & _
                  "WHERE commodity = 'soybeans' " & _
                  "AND source IN ('NASS_SOY_CRUSH', 'NASS_FATS_OILS') " & _
                  "ORDER BY calendar_year DESC, month DESC " & _
                  "LIMIT " & (monthCount * 20)

        Case "canola_crush"
            sql = "SELECT calendar_year, month, attribute, realized_value " & _
                  "FROM silver.monthly_realized " & _
                  "WHERE commodity = 'canola' " & _
                  "AND source IN ('NASS_SOY_CRUSH', 'NASS_FATS_OILS') " & _
                  "ORDER BY calendar_year DESC, month DESC " & _
                  "LIMIT " & (monthCount * 10)

        Case "sunflower_crush"
            sql = "SELECT calendar_year, month, attribute, realized_value " & _
                  "FROM silver.monthly_realized " & _
                  "WHERE commodity = 'sunflower' " & _
                  "AND source IN ('NASS_FATS_OILS') " & _
                  "ORDER BY calendar_year DESC, month DESC " & _
                  "LIMIT " & (monthCount * 10)

        Case "cottonseed_crush"
            sql = "SELECT calendar_year, month, attribute, realized_value " & _
                  "FROM silver.monthly_realized " & _
                  "WHERE commodity = 'cottonseed' " & _
                  "AND source IN ('NASS_FATS_OILS') " & _
                  "ORDER BY calendar_year DESC, month DESC " & _
                  "LIMIT " & (monthCount * 10)

        Case "peanut_crush"
            sql = "SELECT calendar_year, month, attribute, realized_value " & _
                  "FROM silver.monthly_realized " & _
                  "WHERE commodity = 'peanuts' " & _
                  "AND source IN ('NASS_PEANUT', 'NASS_FATS_OILS') " & _
                  "ORDER BY calendar_year DESC, month DESC " & _
                  "LIMIT " & (monthCount * 10)

        Case "nass low ci"
            ' Uses gold.nass_low_ci_matrix which pivots NASS data into spreadsheet columns
            ' Column order: cwg_production, cwg_processing_use, cwg_stocks,
            '   feather_meal_production, feather_meal_stocks,
            '   lard_production, lard_processing_use, lard_stocks,
            '   mbm_production, mbm_stocks,
            '   other_grease_production, other_grease_processing_use, other_grease_stocks,
            '   poultry_fat_production, poultry_fat_processing_use, poultry_fat_stocks,
            '   poultry_byproduct_production, poultry_byproduct_stocks,
            '   edible_tallow_production, edible_tallow_processing_use, edible_tallow_stocks,
            '   inedible_tallow_production, inedible_tallow_processing_use, inedible_tallow_stocks,
            '   technical_tallow_production, technical_tallow_processing_use, technical_tallow_stocks,
            '   yellow_grease_production, yellow_grease_processing_use, yellow_grease_stocks
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

        Case "nass other veg oils"
            ' TODO: Build gold.nass_other_veg_oils_matrix view
            sql = ""

        Case Else
            sql = ""
    End Select

    BuildQueryForTab = sql
End Function

' =============================================================================
' DATA WRITERS
' =============================================================================

Private Function WriteSingleCommodityData(ws As Worksheet, rs As Object) As Long
    ' For single-commodity tabs (soy_crush, canola_crush, etc.)
    ' Dates are in column A, data series in subsequent columns
    ' Find the date row and write the value to the correct column

    Dim cellsUpdated As Long
    Dim targetRow As Integer
    Dim targetCol As Integer

    cellsUpdated = 0

    Do While Not rs.EOF
        Dim yr As Integer
        Dim mo As Integer
        Dim attr As String
        Dim val As Variant

        yr = rs("calendar_year")
        mo = rs("month")
        attr = rs("attribute")
        val = rs("realized_value")

        ' Find the row for this date
        targetRow = FindDateRow(ws, yr, mo)

        If targetRow > 0 And Not IsNull(val) Then
            ' Find the column for this attribute
            targetCol = FindAttributeColumn(ws, attr)

            If targetCol > 0 Then
                ws.Cells(targetRow, targetCol).Value = val
                cellsUpdated = cellsUpdated + 1
            End If
        End If

        rs.MoveNext
    Loop

    WriteSingleCommodityData = cellsUpdated
End Function

Private Function WriteLowCIData(ws As Worksheet, rs As Object) As Long
    ' For NASS Low CI tab - gold.nass_low_ci_matrix columns map directly
    ' to spreadsheet columns. The gold view columns are in the exact order
    ' matching the spreadsheet layout.
    '
    ' Spreadsheet columns (from the tab we created):
    '   Col 2-4:   CWG (Production, Processing Use, Stocks)
    '   Col 6-7:   Feather Meal (Production, Stocks)
    '   Col 9-11:  Lard (Production, Processing Use, Stocks)
    '   Col 13-14: MBM (Production, Stocks)
    '   Col 16-18: Other Grease (Production, Processing Use, Stocks)
    '   Col 20-22: Poultry Fats (Production, Processing Use, Stocks)
    '   Col 24-25: Poultry By-Product (Production, Stocks)
    '   Col 27-29: Edible Tallow (Production, Processing Use, Stocks)
    '   Col 31-33: Inedible Tallow (Production, Processing Use, Stocks)
    '   Col 35-37: Technical Tallow (Production, Processing Use, Stocks)
    '   Col 39-41: Yellow Grease (Production, Processing Use, Stocks)

    ' Map gold view field index (after year/month) to spreadsheet column
    Dim colMap(1 To 30) As Integer
    colMap(1) = 2:  colMap(2) = 3:  colMap(3) = 4     ' CWG
    colMap(4) = 6:  colMap(5) = 7                      ' Feather Meal
    colMap(6) = 9:  colMap(7) = 10: colMap(8) = 11     ' Lard
    colMap(9) = 13: colMap(10) = 14                    ' MBM
    colMap(11) = 16: colMap(12) = 17: colMap(13) = 18  ' Other Grease
    colMap(14) = 20: colMap(15) = 21: colMap(16) = 22  ' Poultry Fats
    colMap(17) = 24: colMap(18) = 25                   ' Poultry By-Product
    colMap(19) = 27: colMap(20) = 28: colMap(21) = 29  ' Edible Tallow
    colMap(22) = 31: colMap(23) = 32: colMap(24) = 33  ' Inedible Tallow
    colMap(25) = 35: colMap(26) = 36: colMap(27) = 37  ' Technical Tallow
    colMap(28) = 39: colMap(29) = 40: colMap(30) = 41  ' Yellow Grease

    Dim cellsUpdated As Long
    cellsUpdated = 0

    Do While Not rs.EOF
        Dim yr As Integer
        Dim mo As Integer
        yr = rs("calendar_year")
        mo = rs("month")

        Dim targetRow As Integer
        targetRow = FindDateRow(ws, yr, mo)

        If targetRow > 0 Then
            Dim fieldIdx As Integer
            For fieldIdx = 1 To 30
                Dim fieldName As String
                Select Case fieldIdx
                    Case 1: fieldName = "cwg_production"
                    Case 2: fieldName = "cwg_processing_use"
                    Case 3: fieldName = "cwg_stocks"
                    Case 4: fieldName = "feather_meal_production"
                    Case 5: fieldName = "feather_meal_stocks"
                    Case 6: fieldName = "lard_production"
                    Case 7: fieldName = "lard_processing_use"
                    Case 8: fieldName = "lard_stocks"
                    Case 9: fieldName = "mbm_production"
                    Case 10: fieldName = "mbm_stocks"
                    Case 11: fieldName = "other_grease_production"
                    Case 12: fieldName = "other_grease_processing_use"
                    Case 13: fieldName = "other_grease_stocks"
                    Case 14: fieldName = "poultry_fat_production"
                    Case 15: fieldName = "poultry_fat_processing_use"
                    Case 16: fieldName = "poultry_fat_stocks"
                    Case 17: fieldName = "poultry_byproduct_production"
                    Case 18: fieldName = "poultry_byproduct_stocks"
                    Case 19: fieldName = "edible_tallow_production"
                    Case 20: fieldName = "edible_tallow_processing_use"
                    Case 21: fieldName = "edible_tallow_stocks"
                    Case 22: fieldName = "inedible_tallow_production"
                    Case 23: fieldName = "inedible_tallow_processing_use"
                    Case 24: fieldName = "inedible_tallow_stocks"
                    Case 25: fieldName = "technical_tallow_production"
                    Case 26: fieldName = "technical_tallow_processing_use"
                    Case 27: fieldName = "technical_tallow_stocks"
                    Case 28: fieldName = "yellow_grease_production"
                    Case 29: fieldName = "yellow_grease_processing_use"
                    Case 30: fieldName = "yellow_grease_stocks"
                End Select

                Dim val As Variant
                val = rs(fieldName)
                If Not IsNull(val) Then
                    ws.Cells(targetRow, colMap(fieldIdx)).Value = val
                    cellsUpdated = cellsUpdated + 1
                End If
            Next fieldIdx
        End If

        rs.MoveNext
    Loop

    WriteLowCIData = cellsUpdated
End Function

Private Function WriteMultiCommodityData(ws As Worksheet, rs As Object) As Long
    ' For multi-commodity tabs (NASS Low CI, NASS Other Veg Oils)
    ' Same approach but also needs to match commodity to the right column group

    Dim cellsUpdated As Long
    cellsUpdated = 0

    Do While Not rs.EOF
        Dim yr As Integer
        Dim mo As Integer
        Dim commodity As String
        Dim attr As String
        Dim val As Variant

        yr = rs("calendar_year")
        mo = rs("month")
        commodity = rs("commodity")
        attr = rs("attribute")
        val = rs("realized_value")

        Dim targetRow As Integer
        targetRow = FindDateRow(ws, yr, mo)

        If targetRow > 0 And Not IsNull(val) Then
            ' For multi-commodity, find column by matching commodity in row 1
            ' and attribute in row 2
            Dim targetCol As Integer
            targetCol = FindMultiCommodityColumn(ws, commodity, attr)

            If targetCol > 0 Then
                ws.Cells(targetRow, targetCol).Value = val
                cellsUpdated = cellsUpdated + 1
            End If
        End If

        rs.MoveNext
    Loop

    WriteMultiCommodityData = cellsUpdated
End Function

' =============================================================================
' HELPER FUNCTIONS
' =============================================================================

Private Function FindDateRow(ws As Worksheet, yr As Integer, mo As Integer) As Integer
    ' Find the row number for a given year/month in column A

    Dim row As Integer
    Dim cellVal As Variant

    For row = 4 To ws.UsedRange.Rows.Count + 4
        cellVal = ws.Cells(row, 1).Value

        If IsDate(cellVal) Then
            If Year(CDate(cellVal)) = yr And Month(CDate(cellVal)) = mo Then
                FindDateRow = row
                Exit Function
            End If
        End If
    Next row

    FindDateRow = 0
End Function

Private Function FindAttributeColumn(ws As Worksheet, attr As String) As Integer
    ' Find column matching attribute name in row 3 (variable headers)

    Dim col As Integer
    Dim cellVal As String
    Dim searchAttr As String

    searchAttr = LCase(Trim(attr))

    For col = 2 To ws.UsedRange.Columns.Count + 2
        cellVal = LCase(Trim(CStr(ws.Cells(3, col).Value)))
        If cellVal = searchAttr Or InStr(cellVal, searchAttr) > 0 Then
            FindAttributeColumn = col
            Exit Function
        End If
    Next col

    FindAttributeColumn = 0
End Function

Private Function FindMultiCommodityColumn(ws As Worksheet, commodity As String, attr As String) As Integer
    ' Find column by matching commodity in row 1 and attribute description in row 2
    ' attr is the full short_desc from NASS, which contains the commodity and variable

    Dim col As Integer
    Dim r1Val As String
    Dim r2Val As String
    Dim searchAttr As String

    searchAttr = LCase(Trim(attr))

    ' For multi-commodity tabs, we search row 2 headers which contain
    ' the variable name (Production, Stocks, etc.)
    ' Row 1 has the commodity name as a section header

    ' Find the commodity section first
    Dim commStartCol As Integer
    Dim commEndCol As Integer
    Dim searchComm As String

    searchComm = LCase(Trim(commodity))
    commStartCol = 0

    For col = 2 To 50
        r1Val = LCase(Trim(CStr(ws.Cells(1, col).Value)))
        If InStr(r1Val, searchComm) > 0 Or InStr(searchComm, r1Val) > 0 Then
            If commStartCol = 0 Then commStartCol = col
        ElseIf commStartCol > 0 And r1Val <> "" Then
            commEndCol = col - 1
            Exit For
        End If
    Next col

    If commStartCol = 0 Then
        FindMultiCommodityColumn = 0
        Exit Function
    End If

    If commEndCol = 0 Then commEndCol = commStartCol + 5

    ' Now find the attribute within that section
    For col = commStartCol To commEndCol
        r2Val = LCase(Trim(CStr(ws.Cells(2, col).Value)))
        If InStr(searchAttr, r2Val) > 0 Or InStr(r2Val, "production") > 0 And InStr(searchAttr, "production") > 0 Then
            FindMultiCommodityColumn = col
            Exit Function
        End If
    Next col

    FindMultiCommodityColumn = 0
End Function

' =============================================================================
' KEYBOARD SHORTCUTS
' =============================================================================

Public Sub AssignCrushShortcuts()
    Application.OnKey "^u", "UpdateCrushData"
    Application.OnKey "^+u", "UpdateCrushDataCustom"

    MsgBox "Keyboard shortcuts assigned:" & vbCrLf & vbCrLf & _
           "Ctrl+U = Quick update (latest 3 months)" & vbCrLf & _
           "Ctrl+Shift+U = Custom month count", vbInformation, "Crush Data Updater"
End Sub

Public Sub RemoveCrushShortcuts()
    Application.OnKey "^u"
    Application.OnKey "^+u"
End Sub
