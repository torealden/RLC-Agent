' =============================================================================
' EIAFeedstockWorkbookEvents - Paste into ThisWorkbook module
' =============================================================================
' This code goes into the ThisWorkbook module (not a standard module).
' In VBA Editor: double-click "ThisWorkbook" in the Project Explorer,
' then paste this code.
'
' Note: If you already have Workbook_Open / Workbook_BeforeClose handlers
' (e.g., from EMTSDataUpdater), add the feedstock calls to those existing
' handlers instead of creating duplicate event handlers.
' =============================================================================

Private Sub Workbook_Open()
    AssignFeedstockShortcuts
End Sub

Private Sub Workbook_BeforeClose(Cancel As Boolean)
    RemoveFeedstockShortcuts
End Sub
