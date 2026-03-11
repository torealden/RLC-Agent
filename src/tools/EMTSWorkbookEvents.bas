' =============================================================================
' EMTSWorkbookEvents - Paste into ThisWorkbook module
' =============================================================================
' This code goes into the ThisWorkbook module (not a standard module).
' In VBA Editor: double-click "ThisWorkbook" in the Project Explorer,
' then paste this code.
' =============================================================================

Private Sub Workbook_Open()
    AssignEMTSShortcuts
End Sub

Private Sub Workbook_BeforeClose(Cancel As Boolean)
    RemoveEMTSShortcuts
End Sub
