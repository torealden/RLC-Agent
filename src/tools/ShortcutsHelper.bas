Attribute VB_Name = "ShortcutsHelper"
' =============================================================================
' ShortcutsHelper - Shared "what shortcut is this?" banner
' =============================================================================
' Each updater's Assign*Shortcuts sub calls ShowShortcutBanner so the user
' sees the shortcut(s) for THIS workbook every time the file opens (the
' Workbook_Open event calls the Assign sub, which then calls this helper).
'
' Pattern (called from inside any Assign*Shortcuts sub):
'
'   ShortcutsHelper.ShowShortcutBanner _
'       "Trade Updater (Census)", _
'       "Ctrl+I", "Quick update - latest 6 months", _
'       "Ctrl+Shift+I", "Custom update - choose months"
'
' The banner title includes ThisWorkbook.Name so when multiple trade files
' are open at once it's obvious which workbook owns the shortcut.
'
' Installation:
'   1. Import this module into each workbook (VBE > File > Import File >
'      ShortcutsHelper.bas)
'   2. Re-import the relevant updater .bas (they call this helper) so the
'      Assign*Shortcuts subs pick up the new lines.
' =============================================================================

Option Explicit

Public Sub ShowShortcutBanner(updaterName As String, _
                              primaryKey As String, primaryDesc As String, _
                              Optional customKey As String = "", _
                              Optional customDesc As String = "")
    Dim msg As String
    Dim wbName As String

    On Error Resume Next
    wbName = ThisWorkbook.Name
    If Err.Number <> 0 Then wbName = "(unknown)"
    On Error GoTo 0

    msg = updaterName & " shortcuts (file: " & wbName & ")" & vbCrLf & vbCrLf
    msg = msg & "  " & primaryKey & vbTab & vbTab & primaryDesc

    If Len(customKey) > 0 Then
        msg = msg & vbCrLf & "  " & customKey & vbTab & customDesc
    End If

    MsgBox msg, vbInformation, updaterName
End Sub

' Variant for workbooks that have MULTIPLE updaters — call once after
' wiring all of them. Pass a single pre-built body string.
Public Sub ShowMultiShortcutBanner(workbookLabel As String, body As String)
    Dim wbName As String

    On Error Resume Next
    wbName = ThisWorkbook.Name
    If Err.Number <> 0 Then wbName = "(unknown)"
    On Error GoTo 0

    MsgBox workbookLabel & " (file: " & wbName & ")" & vbCrLf & vbCrLf & body, _
           vbInformation, workbookLabel
End Sub
