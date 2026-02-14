' =============================================================================
' ThisWorkbook Event Code for us_biofuel_holding_sheet.xlsm
' =============================================================================
' Paste ONLY the two Sub procedures below into the ThisWorkbook module.
' In VBA Editor: double-click "ThisWorkbook" in Project Explorer, then paste.
' =============================================================================

Private Sub Workbook_Open()
    BiofuelDataUpdater.AssignBiofuelShortcuts
End Sub

Private Sub Workbook_BeforeClose(Cancel As Boolean)
    BiofuelDataUpdater.RemoveBiofuelShortcuts
End Sub
