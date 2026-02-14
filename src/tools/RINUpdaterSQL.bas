' =============================================================================
' RINUpdaterSQL - Pure VBA with PostgreSQL Connection
' =============================================================================
' Connects directly to PostgreSQL database to update RIN data sheets.
' No Python required. No closing/reopening workbook.
'
' Target: us_rin_data.xlsm (4 sheets)
' Sheets:
'   1. "RIN Monthly"       - Monthly generation by D-code (rows = months)
'   2. "Annual Generation"  - Annual generation totals (rows = years)
'   3. "RIN Balance"        - Annual gen/ret/avail by D-code (rows = years)
'   4. "D4 Fuel Mix"        - D4 fuel production breakdown (rows = years)
'
' Layout: Row 3 = headers, Row 4 = units, Data starts Row 5
'         Column A = dates (1st of month) or years (integer)
'
' Data sources:
'   gold.rin_monthly_matrix     - Monthly RIN generation pivoted by D-code
'   gold.rin_generation_summary - Annual generation totals (pre-existing)
'   gold.rin_annual_balance     - Annual balance (gen/ret/avail)
'   gold.d4_fuel_matrix         - D4 fuel production by fuel type
'
' Key similarity to CrushUpdaterSQL:
'   Both are rows = time periods, columns = data attributes
'
' Requirements:
' - PostgreSQL ODBC Driver installed (psqlODBC)
' - Reference to "Microsoft ActiveX Data Objects" (Tools > References)
'
' Installation:
' 1. Download psqlODBC from https://www.postgresql.org/ftp/odbc/versions/msi/
' 2. Install the 64-bit version (psqlodbc_x64.msi)
' 3. In VBA: Tools > References > Check "Microsoft ActiveX Data Objects 6.1 Library"
' =============================================================================

Option Explicit

' Database connection settings
Private Const DB_SERVER As String = "localhost"
Private Const DB_PORT As String = "5432"
Private Const DB_NAME As String = "rlc_commodities"
Private Const DB_USER As String = "postgres"
Private Const DB_PASSWORD As String = "SoupBoss1"

' Spreadsheet structure
Private Const DATE_COLUMN As Integer = 1      ' Column A = dates or years
Private Const HEADER_ROW As Integer = 3       ' Row with attribute headers
Private Const UNIT_ROW As Integer = 4         ' Row with units
Private Const DATA_START_ROW As Integer = 5   ' First data row

' =============================================================================
' MAIN UPDATE FUNCTIONS
' =============================================================================

Public Sub UpdateRINData()
    ' Quick update - latest 6 months for monthly sheet, all years for annual
    ' Keyboard shortcut: Ctrl+R

    Dim result As VbMsgBoxResult
    result = MsgBox("Update all RIN sheets?" & vbCrLf & vbCrLf & _
                    "Monthly: latest 6 months" & vbCrLf & _
                    "Annual: all available years", _
                    vbYesNo + vbQuestion, "RIN Updater")

    If result = vbYes Then
        UpdateFromDatabase 6
    End If
End Sub

Public Sub UpdateRINDataCustom()
    ' Custom update with user-specified months for monthly sheet
    ' Keyboard shortcut: Ctrl+Shift+R

    Dim monthCount As String
    monthCount = InputBox("How many months of MONTHLY data to update?" & vbCrLf & vbCrLf & _
                          "Enter a number (e.g., 12 for last 12 months)" & vbCrLf & _
                          "Or enter 0 to update ALL available data" & vbCrLf & vbCrLf & _
                          "(Annual sheets always update all years)", _
                          "RIN Updater", "6")

    If monthCount = "" Then Exit Sub

    If Not IsNumeric(monthCount) Then
        MsgBox "Please enter a valid number.", vbExclamation, "RIN Updater"
        Exit Sub
    End If

    UpdateFromDatabase CInt(monthCount)
End Sub

' =============================================================================
' DATABASE CONNECTION
' =============================================================================

Private Function GetConnection() As Object
    ' Create and return ADODB Connection to PostgreSQL

    Dim conn As Object
    Dim connString As String

    Set conn = CreateObject("ADODB.Connection")

    ' PostgreSQL ODBC connection string
    connString = "Driver={PostgreSQL UNICODE(x64)};" & _
                 "Server=" & DB_SERVER & ";" & _
                 "Port=" & DB_PORT & ";" & _
                 "Database=" & DB_NAME & ";" & _
                 "Uid=" & DB_USER & ";" & _
                 "Pwd=" & DB_PASSWORD & ";"

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
    ' Fetch data from database and update all sheets

    Dim conn As Object
    Dim totalCells As Long
    Dim sheetCount As Integer

    ' Show status
    Application.StatusBar = "Connecting to database..."
    Application.Cursor = xlWait
    DoEvents

    ' Get connection
    Set conn = GetConnection()
    If conn Is Nothing Then
        Application.StatusBar = False
        Application.Cursor = xlDefault
        Exit Sub
    End If

    totalCells = 0
    sheetCount = 0

    On Error Resume Next

    ' Sheet 1: Monthly RIN generation
    Application.StatusBar = "Updating RIN Monthly..."
    DoEvents
    totalCells = totalCells + UpdateRINMonthly(conn, monthCount)
    sheetCount = sheetCount + 1

    ' Sheet 2: Annual generation totals
    Application.StatusBar = "Updating Annual Generation..."
    DoEvents
    totalCells = totalCells + UpdateAnnualGeneration(conn)
    sheetCount = sheetCount + 1

    ' Sheet 3: Annual balance (gen/ret/avail)
    Application.StatusBar = "Updating RIN Balance..."
    DoEvents
    totalCells = totalCells + UpdateRINBalance(conn)
    sheetCount = sheetCount + 1

    ' Sheet 4: D4 fuel production breakdown
    Application.StatusBar = "Updating D4 Fuel Mix..."
    DoEvents
    totalCells = totalCells + UpdateD4FuelMix(conn)
    sheetCount = sheetCount + 1

    On Error GoTo 0

    conn.Close

    ' Reset status
    Application.StatusBar = False
    Application.Cursor = xlDefault

    ' Report results
    Dim monthLabel As String
    If monthCount = 0 Then
        monthLabel = "all available"
    Else
        monthLabel = "latest " & monthCount & " months"
    End If

    MsgBox "Update complete!" & vbCrLf & vbCrLf & _
           "Sheets updated: " & sheetCount & vbCrLf & _
           "Cells updated: " & totalCells & vbCrLf & _
           "Monthly period: " & monthLabel & vbCrLf & _
           "Annual: all years", vbInformation, "RIN Updater"
End Sub

' =============================================================================
' HELPER FUNCTIONS
' =============================================================================

Private Function FindRowForDate(ws As Worksheet, yr As Integer, mo As Integer) As Integer
    ' Find row number for a given year/month by scanning the date column (A)
    ' Dates in column A are 1st-of-month date serial numbers

    Dim row As Integer
    Dim cellVal As Variant
    Dim cellDate As Date
    Dim lastRow As Integer

    ' Find last row with data in column A
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
            ' Excel date serial number
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

    FindRowForDate = 0  ' Not found
End Function

Private Function FindRowForYear(ws As Worksheet, yr As Integer) As Integer
    ' Find row number for a given year by scanning column A
    ' Column A contains year as integer

    Dim row As Integer
    Dim cellVal As Variant
    Dim lastRow As Integer

    ' Find last row with data in column A
    lastRow = ws.Cells(ws.Rows.Count, DATE_COLUMN).End(xlUp).row

    For row = DATA_START_ROW To lastRow
        cellVal = ws.Cells(row, DATE_COLUMN).Value

        If IsNumeric(cellVal) Then
            If CInt(cellVal) = yr Then
                FindRowForYear = row
                Exit Function
            End If
        End If
    Next row

    FindRowForYear = 0  ' Not found
End Function

' =============================================================================
' SHEET UPDATERS
' Each returns the number of cells updated
' =============================================================================

'--- Sheet 1: RIN Monthly Generation ---
Private Function UpdateRINMonthly(conn As Object, monthCount As Integer) As Long
    ' Source: gold.rin_monthly_matrix
    ' Layout: Col A=Date, B=D3 RINs, C=D4, D=D5, E=D6, F=D7, G=Total(formula),
    '         H=D3 Vol, I=D4 Vol, J=D5 Vol, K=D6 Vol, L=D7 Vol, M=Total Vol(formula)

    Dim ws As Worksheet
    Dim rs As Object
    Dim sql As String
    Dim cellsUpdated As Long

    On Error GoTo SheetError
    Set ws = ThisWorkbook.Sheets("RIN Monthly")

    ' Build query with optional month limit
    If monthCount > 0 Then
        sql = "SELECT year, month, " & _
              "    d3_rins, d4_rins, d5_rins, d6_rins, d7_rins, " & _
              "    d3_volume, d4_volume, d5_volume, d6_volume, d7_volume " & _
              "FROM gold.rin_monthly_matrix " & _
              "WHERE (year, month) IN ( " & _
              "    SELECT DISTINCT year, month " & _
              "    FROM gold.rin_monthly_matrix " & _
              "    ORDER BY year DESC, month DESC " & _
              "    LIMIT " & monthCount & _
              ") " & _
              "ORDER BY year, month"
    Else
        sql = "SELECT year, month, " & _
              "    d3_rins, d4_rins, d5_rins, d6_rins, d7_rins, " & _
              "    d3_volume, d4_volume, d5_volume, d6_volume, d7_volume " & _
              "FROM gold.rin_monthly_matrix " & _
              "ORDER BY year, month"
    End If

    Set rs = CreateObject("ADODB.Recordset")
    rs.Open sql, conn

    If rs.EOF Then
        rs.Close
        UpdateRINMonthly = 0
        Exit Function
    End If

    cellsUpdated = 0

    Do While Not rs.EOF
        Dim targetRow As Integer
        targetRow = FindRowForDate(ws, CInt(rs("year")), CInt(rs("month")))

        If targetRow > 0 Then
            ' RIN Quantities: B=D3, C=D4, D=D5, E=D6, F=D7
            If Not IsNull(rs("d3_rins")) Then ws.Cells(targetRow, 2).Value = CDbl(rs("d3_rins")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("d4_rins")) Then ws.Cells(targetRow, 3).Value = CDbl(rs("d4_rins")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("d5_rins")) Then ws.Cells(targetRow, 4).Value = CDbl(rs("d5_rins")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("d6_rins")) Then ws.Cells(targetRow, 5).Value = CDbl(rs("d6_rins")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("d7_rins")) Then ws.Cells(targetRow, 6).Value = CDbl(rs("d7_rins")): cellsUpdated = cellsUpdated + 1
            ' Col G = Total RINs (formula, skip)
            ' Batch Volumes: H=D3, I=D4, J=D5, K=D6, L=D7
            If Not IsNull(rs("d3_volume")) Then ws.Cells(targetRow, 8).Value = CDbl(rs("d3_volume")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("d4_volume")) Then ws.Cells(targetRow, 9).Value = CDbl(rs("d4_volume")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("d5_volume")) Then ws.Cells(targetRow, 10).Value = CDbl(rs("d5_volume")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("d6_volume")) Then ws.Cells(targetRow, 11).Value = CDbl(rs("d6_volume")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("d7_volume")) Then ws.Cells(targetRow, 12).Value = CDbl(rs("d7_volume")): cellsUpdated = cellsUpdated + 1
            ' Col M = Total Volume (formula, skip)
        End If

        rs.MoveNext
    Loop

    rs.Close
    UpdateRINMonthly = cellsUpdated
    Exit Function

SheetError:
    Debug.Print "RIN Monthly error: " & Err.Description
    UpdateRINMonthly = 0
End Function

'--- Sheet 2: Annual Generation ---
Private Function UpdateAnnualGeneration(conn As Object) As Long
    ' Source: gold.rin_generation_summary (pre-existing view)
    ' Layout: Col A=Year, B=D3, C=D4, D=D5, E=D6, F=D7, G=Total, H=Advanced

    Dim ws As Worksheet
    Dim rs As Object
    Dim sql As String
    Dim cellsUpdated As Long

    On Error GoTo SheetError
    Set ws = ThisWorkbook.Sheets("Annual Generation")

    sql = "SELECT year, " & _
          "    d3_cellulosic, d4_biodiesel, d5_advanced, d6_renewable, " & _
          "    d7_cellulosic_diesel, total_all_rins, total_advanced " & _
          "FROM gold.rin_generation_summary " & _
          "ORDER BY year"

    Set rs = CreateObject("ADODB.Recordset")
    rs.Open sql, conn

    If rs.EOF Then
        rs.Close
        UpdateAnnualGeneration = 0
        Exit Function
    End If

    cellsUpdated = 0

    Do While Not rs.EOF
        Dim targetRow As Integer
        targetRow = FindRowForYear(ws, CInt(rs("year")))

        If targetRow > 0 Then
            ' Col B=D3, C=D4, D=D5, E=D6, F=D7, G=Total, H=Advanced
            If Not IsNull(rs("d3_cellulosic")) Then ws.Cells(targetRow, 2).Value = CDbl(rs("d3_cellulosic")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("d4_biodiesel")) Then ws.Cells(targetRow, 3).Value = CDbl(rs("d4_biodiesel")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("d5_advanced")) Then ws.Cells(targetRow, 4).Value = CDbl(rs("d5_advanced")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("d6_renewable")) Then ws.Cells(targetRow, 5).Value = CDbl(rs("d6_renewable")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("d7_cellulosic_diesel")) Then ws.Cells(targetRow, 6).Value = CDbl(rs("d7_cellulosic_diesel")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("total_all_rins")) Then ws.Cells(targetRow, 7).Value = CDbl(rs("total_all_rins")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("total_advanced")) Then ws.Cells(targetRow, 8).Value = CDbl(rs("total_advanced")): cellsUpdated = cellsUpdated + 1
        End If

        rs.MoveNext
    Loop

    rs.Close
    UpdateAnnualGeneration = cellsUpdated
    Exit Function

SheetError:
    Debug.Print "Annual Generation error: " & Err.Description
    UpdateAnnualGeneration = 0
End Function

'--- Sheet 3: RIN Balance (annual gen/ret/avail by D-code) ---
Private Function UpdateRINBalance(conn As Object) As Long
    ' Source: gold.rin_annual_balance
    ' Layout: Col A=Year
    '   D3: B=Generated, C=Retired, D=Available
    '   D4: E=Generated, F=Retired, G=Available
    '   D5: H=Generated, I=Retired, J=Available
    '   D6: K=Generated, L=Retired, M=Available
    '   Totals: N=Generated, O=Retired, P=Available

    Dim ws As Worksheet
    Dim rs As Object
    Dim sql As String
    Dim cellsUpdated As Long

    On Error GoTo SheetError
    Set ws = ThisWorkbook.Sheets("RIN Balance")

    sql = "SELECT year, " & _
          "    d3_generated, d3_retired, d3_available, " & _
          "    d4_generated, d4_retired, d4_available, " & _
          "    d5_generated, d5_retired, d5_available, " & _
          "    d6_generated, d6_retired, d6_available, " & _
          "    total_generated, total_retired, total_available " & _
          "FROM gold.rin_annual_balance " & _
          "ORDER BY year"

    Set rs = CreateObject("ADODB.Recordset")
    rs.Open sql, conn

    If rs.EOF Then
        rs.Close
        UpdateRINBalance = 0
        Exit Function
    End If

    cellsUpdated = 0

    Do While Not rs.EOF
        Dim targetRow As Integer
        targetRow = FindRowForYear(ws, CInt(rs("year")))

        If targetRow > 0 Then
            ' D3: B=Generated, C=Retired, D=Available
            If Not IsNull(rs("d3_generated")) Then ws.Cells(targetRow, 2).Value = CDbl(rs("d3_generated")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("d3_retired")) Then ws.Cells(targetRow, 3).Value = CDbl(rs("d3_retired")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("d3_available")) Then ws.Cells(targetRow, 4).Value = CDbl(rs("d3_available")): cellsUpdated = cellsUpdated + 1
            ' D4: E=Generated, F=Retired, G=Available
            If Not IsNull(rs("d4_generated")) Then ws.Cells(targetRow, 5).Value = CDbl(rs("d4_generated")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("d4_retired")) Then ws.Cells(targetRow, 6).Value = CDbl(rs("d4_retired")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("d4_available")) Then ws.Cells(targetRow, 7).Value = CDbl(rs("d4_available")): cellsUpdated = cellsUpdated + 1
            ' D5: H=Generated, I=Retired, J=Available
            If Not IsNull(rs("d5_generated")) Then ws.Cells(targetRow, 8).Value = CDbl(rs("d5_generated")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("d5_retired")) Then ws.Cells(targetRow, 9).Value = CDbl(rs("d5_retired")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("d5_available")) Then ws.Cells(targetRow, 10).Value = CDbl(rs("d5_available")): cellsUpdated = cellsUpdated + 1
            ' D6: K=Generated, L=Retired, M=Available
            If Not IsNull(rs("d6_generated")) Then ws.Cells(targetRow, 11).Value = CDbl(rs("d6_generated")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("d6_retired")) Then ws.Cells(targetRow, 12).Value = CDbl(rs("d6_retired")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("d6_available")) Then ws.Cells(targetRow, 13).Value = CDbl(rs("d6_available")): cellsUpdated = cellsUpdated + 1
            ' Totals: N=Generated, O=Retired, P=Available
            If Not IsNull(rs("total_generated")) Then ws.Cells(targetRow, 14).Value = CDbl(rs("total_generated")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("total_retired")) Then ws.Cells(targetRow, 15).Value = CDbl(rs("total_retired")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("total_available")) Then ws.Cells(targetRow, 16).Value = CDbl(rs("total_available")): cellsUpdated = cellsUpdated + 1
        End If

        rs.MoveNext
    Loop

    rs.Close
    UpdateRINBalance = cellsUpdated
    Exit Function

SheetError:
    Debug.Print "RIN Balance error: " & Err.Description
    UpdateRINBalance = 0
End Function

'--- Sheet 4: D4 Fuel Mix ---
Private Function UpdateD4FuelMix(conn As Object) As Long
    ' Source: gold.d4_fuel_matrix
    ' Layout: Col A=Year
    '   Volumes (gallons): B=Biodiesel, C=RD(EV1.7), D=RD(EV1.6), E=Ren Jet, F=Other, G=Total(formula)
    '   RINs:              H=Biodiesel, I=RD(EV1.7), J=RD(EV1.6), K=Ren Jet, L=Other, M=Total(formula)

    Dim ws As Worksheet
    Dim rs As Object
    Dim sql As String
    Dim cellsUpdated As Long

    On Error GoTo SheetError
    Set ws = ThisWorkbook.Sheets("D4 Fuel Mix")

    sql = "SELECT year, " & _
          "    biodiesel_vol, rd_ev17_vol, rd_ev16_vol, ren_jet_vol, other_vol, " & _
          "    biodiesel_rins, rd_ev17_rins, rd_ev16_rins, ren_jet_rins, other_rins " & _
          "FROM gold.d4_fuel_matrix " & _
          "ORDER BY year"

    Set rs = CreateObject("ADODB.Recordset")
    rs.Open sql, conn

    If rs.EOF Then
        rs.Close
        UpdateD4FuelMix = 0
        Exit Function
    End If

    cellsUpdated = 0

    Do While Not rs.EOF
        Dim targetRow As Integer
        targetRow = FindRowForYear(ws, CInt(rs("year")))

        If targetRow > 0 Then
            ' Volumes: B=Biodiesel, C=RD(1.7), D=RD(1.6), E=Jet, F=Other
            If Not IsNull(rs("biodiesel_vol")) Then ws.Cells(targetRow, 2).Value = CDbl(rs("biodiesel_vol")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("rd_ev17_vol")) Then ws.Cells(targetRow, 3).Value = CDbl(rs("rd_ev17_vol")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("rd_ev16_vol")) Then ws.Cells(targetRow, 4).Value = CDbl(rs("rd_ev16_vol")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("ren_jet_vol")) Then ws.Cells(targetRow, 5).Value = CDbl(rs("ren_jet_vol")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("other_vol")) Then ws.Cells(targetRow, 6).Value = CDbl(rs("other_vol")): cellsUpdated = cellsUpdated + 1
            ' Col G = Total Volume (formula, skip)
            ' RINs: H=Biodiesel, I=RD(1.7), J=RD(1.6), K=Jet, L=Other
            If Not IsNull(rs("biodiesel_rins")) Then ws.Cells(targetRow, 8).Value = CDbl(rs("biodiesel_rins")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("rd_ev17_rins")) Then ws.Cells(targetRow, 9).Value = CDbl(rs("rd_ev17_rins")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("rd_ev16_rins")) Then ws.Cells(targetRow, 10).Value = CDbl(rs("rd_ev16_rins")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("ren_jet_rins")) Then ws.Cells(targetRow, 11).Value = CDbl(rs("ren_jet_rins")): cellsUpdated = cellsUpdated + 1
            If Not IsNull(rs("other_rins")) Then ws.Cells(targetRow, 12).Value = CDbl(rs("other_rins")): cellsUpdated = cellsUpdated + 1
            ' Col M = Total RINs (formula, skip)
        End If

        rs.MoveNext
    Loop

    rs.Close
    UpdateD4FuelMix = cellsUpdated
    Exit Function

SheetError:
    Debug.Print "D4 Fuel Mix error: " & Err.Description
    UpdateD4FuelMix = 0
End Function

' =============================================================================
' KEYBOARD SHORTCUTS
' =============================================================================

Public Sub AssignRINShortcuts()
    ' Assign Ctrl+R and Ctrl+Shift+R shortcuts

    Application.OnKey "^r", "UpdateRINData"
    Application.OnKey "^+r", "UpdateRINDataCustom"

    MsgBox "Keyboard shortcuts assigned:" & vbCrLf & vbCrLf & _
           "Ctrl+R = Quick update (latest 6 months + all annual)" & vbCrLf & _
           "Ctrl+Shift+R = Custom month count", vbInformation, "RIN Updater"
End Sub

Public Sub RemoveRINShortcuts()
    Application.OnKey "^r"
    Application.OnKey "^+r"
End Sub
