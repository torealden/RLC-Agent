' =============================================================================
' TradeUpdater VBA Module
' =============================================================================
' This module integrates with the Python excel_trade_updater.py script
' to update trade data from the PostgreSQL database.
'
' Installation:
' 1. Open Excel workbook
' 2. Press Alt+F11 to open VBA Editor
' 3. File > Import File > Select this .bas file
' 4. Save workbook as .xlsm (macro-enabled)
'
' Usage:
' - Press Ctrl+I to update current sheet with recent months
' - Press Ctrl+Shift+I to open dialog for custom date selection
' =============================================================================

Option Explicit

' Configuration - Update these paths for your system
Private Const PYTHON_PATH As String = "python"
Private Const SCRIPT_PATH As String = "C:\Users\torem\RLC Dropbox\RLC Team Folder\RLC-Agent\src\tools\excel_trade_updater.py"
Private Const BATCH_PATH As String = "C:\Users\torem\RLC Dropbox\RLC Team Folder\RLC-Agent\src\tools\trade_update_runner.bat"

' =============================================================================
' MAIN UPDATE FUNCTIONS
' =============================================================================

Public Sub UpdateTradeData()
    ' Quick update - latest 3 months of available data from database
    ' Keyboard shortcut: Ctrl+I

    Dim result As Variant

    ' Confirm with user
    result = MsgBox("Update " & ActiveSheet.Name & " with the latest 3 months of available data?" & vbCrLf & vbCrLf & _
                    "(Uses most recent data in database, not calendar months)" & vbCrLf & vbCrLf & _
                    "Continue?", vbYesNo + vbQuestion, "Trade Updater")

    If result = vbYes Then
        RunPythonUpdater "", False, True  ' useLatest = True
    End If
End Sub


Public Sub UpdateTradeDataCustom()
    ' Custom update with date selection dialog
    ' Keyboard shortcut: Ctrl+Shift+I

    Dim startMonth As String
    Dim endMonth As String
    Dim months As String

    ' Get start month from user
    startMonth = InputBox("Enter START month (YYYY-MM format):" & vbCrLf & vbCrLf & _
                          "Example: 2024-09", "Trade Updater - Start Month", _
                          Format(DateAdd("m", -6, Date), "yyyy-mm"))

    If startMonth = "" Then Exit Sub

    ' Validate format
    If Not IsValidMonthFormat(startMonth) Then
        MsgBox "Invalid format. Please use YYYY-MM format.", vbExclamation, "Trade Updater"
        Exit Sub
    End If

    ' Get end month from user
    endMonth = InputBox("Enter END month (YYYY-MM format):" & vbCrLf & vbCrLf & _
                        "Example: 2024-12", "Trade Updater - End Month", _
                        Format(DateAdd("m", -1, Date), "yyyy-mm"))

    If endMonth = "" Then Exit Sub

    If Not IsValidMonthFormat(endMonth) Then
        MsgBox "Invalid format. Please use YYYY-MM format.", vbExclamation, "Trade Updater"
        Exit Sub
    End If

    ' Build month range
    months = BuildMonthRange(startMonth, endMonth)

    If months = "" Then
        MsgBox "Invalid date range.", vbExclamation, "Trade Updater"
        Exit Sub
    End If

    ' Confirm and run
    Dim result As Variant
    result = MsgBox("Update " & ActiveSheet.Name & " with data for:" & vbCrLf & vbCrLf & _
                    Replace(months, ",", vbCrLf) & vbCrLf & vbCrLf & _
                    "Continue?", vbYesNo + vbQuestion, "Trade Updater")

    If result = vbYes Then
        RunPythonUpdater months
    End If
End Sub


Public Sub UpdateAllAvailableData()
    ' Update with all available data from database

    Dim result As Variant
    result = MsgBox("Update " & ActiveSheet.Name & " with ALL available data from database?" & vbCrLf & vbCrLf & _
                    "This may take a while.", vbYesNo + vbQuestion, "Trade Updater")

    If result = vbYes Then
        RunPythonUpdater "", True
    End If
End Sub


' =============================================================================
' PYTHON INTEGRATION
' =============================================================================

Private Sub RunPythonUpdater(months As String, Optional useAll As Boolean = False, Optional useLatest As Boolean = False)
    ' Execute the Python script via batch file
    ' Batch file runs after Excel closes, then reopens the workbook

    Dim cmd As String
    Dim filePath As String
    Dim sheetName As String
    Dim wsh As Object
    Dim mode As String

    ' Save workbook first
    On Error Resume Next
    ThisWorkbook.Save
    If Err.Number <> 0 Then
        MsgBox "Please save the workbook before updating.", vbExclamation, "Trade Updater"
        Exit Sub
    End If
    On Error GoTo ErrorHandler

    filePath = ThisWorkbook.FullName
    sheetName = ActiveSheet.Name

    ' Determine mode
    If useAll Then
        mode = "all"
    ElseIf useLatest Then
        mode = "latest"
    Else
        mode = "months"
    End If

    ' Build batch command
    ' Batch file takes: filepath, sheetname, mode, months(optional)
    cmd = """" & BATCH_PATH & """ """ & filePath & """ """ & sheetName & """ " & mode
    If mode = "months" Then
        cmd = cmd & " """ & months & """"
    End If

    ' Show status
    Application.StatusBar = "Launching updater... Workbook will close and reopen."
    Application.Cursor = xlWait
    DoEvents

    ' Launch batch file (0 = hidden would hide it, 1 = show window)
    Set wsh = CreateObject("WScript.Shell")
    wsh.Run cmd, 1, False  ' False = don't wait, let batch run independently

    ' Small delay to let batch start
    Application.Wait Now + TimeValue("00:00:01")

    ' Close workbook - batch file will reopen it after Python finishes
    Application.DisplayAlerts = False
    ThisWorkbook.Close SaveChanges:=True

    ' Note: Code stops here because workbook closes
    ' Batch file handles reopening

    Exit Sub

ErrorHandler:
    Application.StatusBar = False
    Application.Cursor = xlDefault
    Application.DisplayAlerts = True
    MsgBox "Error: " & Err.Description, vbCritical, "Trade Updater"
End Sub


Private Sub ReloadWorkbook()
    ' Reload the current workbook to see external changes

    Dim filePath As String
    filePath = ThisWorkbook.FullName

    Application.DisplayAlerts = False
    ThisWorkbook.Close SaveChanges:=False
    Workbooks.Open filePath
    Application.DisplayAlerts = True
End Sub


' =============================================================================
' HELPER FUNCTIONS
' =============================================================================

Private Function GetRecentMonths(numMonths As Integer) As String
    ' Get comma-separated list of recent months

    Dim months() As String
    Dim i As Integer
    Dim d As Date

    ReDim months(numMonths - 1)

    For i = numMonths To 1 Step -1
        d = DateAdd("m", -i, Date)
        months(numMonths - i) = Format(d, "yyyy-mm")
    Next i

    GetRecentMonths = Join(months, ",")
End Function


Private Function BuildMonthRange(startMonth As String, endMonth As String) As String
    ' Build comma-separated list of months between start and end

    Dim months As String
    Dim startDate As Date
    Dim endDate As Date
    Dim currentDate As Date

    ' Parse dates
    startDate = DateValue(startMonth & "-01")
    endDate = DateValue(endMonth & "-01")

    If startDate > endDate Then
        BuildMonthRange = ""
        Exit Function
    End If

    ' Build list
    currentDate = startDate
    Do While currentDate <= endDate
        If months <> "" Then months = months & ","
        months = months & Format(currentDate, "yyyy-mm")
        currentDate = DateAdd("m", 1, currentDate)
    Loop

    BuildMonthRange = months
End Function


Private Function IsValidMonthFormat(monthStr As String) As Boolean
    ' Validate YYYY-MM format

    If Len(monthStr) <> 7 Then
        IsValidMonthFormat = False
        Exit Function
    End If

    If Mid(monthStr, 5, 1) <> "-" Then
        IsValidMonthFormat = False
        Exit Function
    End If

    Dim yearPart As String
    Dim monthPart As String

    yearPart = Left(monthStr, 4)
    monthPart = Right(monthStr, 2)

    If Not IsNumeric(yearPart) Or Not IsNumeric(monthPart) Then
        IsValidMonthFormat = False
        Exit Function
    End If

    Dim monthNum As Integer
    monthNum = CInt(monthPart)

    If monthNum < 1 Or monthNum > 12 Then
        IsValidMonthFormat = False
        Exit Function
    End If

    IsValidMonthFormat = True
End Function


' =============================================================================
' KEYBOARD SHORTCUTS
' =============================================================================

Public Sub AssignKeyboardShortcuts()
    ' Assign Ctrl+I and Ctrl+Shift+I shortcuts
    ' Run this once to set up shortcuts

    Application.OnKey "^i", "UpdateTradeData"           ' Ctrl+I
    Application.OnKey "^+i", "UpdateTradeDataCustom"    ' Ctrl+Shift+I

    MsgBox "Keyboard shortcuts assigned:" & vbCrLf & vbCrLf & _
           "Ctrl+I = Quick update (last 3 months)" & vbCrLf & _
           "Ctrl+Shift+I = Custom date range", vbInformation, "Trade Updater"
End Sub


Public Sub RemoveKeyboardShortcuts()
    ' Remove the keyboard shortcuts

    Application.OnKey "^i"
    Application.OnKey "^+i"

    MsgBox "Keyboard shortcuts removed.", vbInformation, "Trade Updater"
End Sub


' =============================================================================
' AUTO-RUN ON WORKBOOK OPEN
' =============================================================================
' To automatically assign shortcuts when workbook opens, add this to
' ThisWorkbook module:
'
' Private Sub Workbook_Open()
'     AssignKeyboardShortcuts
' End Sub
'
' Private Sub Workbook_BeforeClose(Cancel As Boolean)
'     RemoveKeyboardShortcuts
' End Sub
' =============================================================================
