' =============================================================================
' ThisWorkbook Event Code for us_fuel_trade.xlsm
' =============================================================================
' Paste ONLY the two Sub procedures below into the ThisWorkbook module of
' us_fuel_trade.xlsm.
'
' In VBA Editor:
'   1. Open us_fuel_trade.xlsm
'   2. Alt+F11 to open VBA editor
'   3. In Project Explorer (left side), expand "VBAProject (us_fuel_trade.xlsm)"
'   4. Double-click "ThisWorkbook"
'   5. Replace any existing AssignXXXShortcuts / RemoveXXXShortcuts code with
'      the two Subs below, EXACTLY as written
'   6. Save (Ctrl+S)
'
' What this fixes: previously ThisWorkbook called "AssignXXXShortcuts" (no
' module prefix, no defined procedure) so the keyboard shortcuts were never
' registered. Now Ctrl+Y / Ctrl+Shift+Y are wired correctly.
'
' Requires: EnergyTradeUpdater.bas module already imported into the workbook.
' =============================================================================

Private Sub Workbook_Open()
    On Error Resume Next
    EnergyTradeUpdater.AssignEnergyShortcuts
    On Error GoTo 0
End Sub

Private Sub Workbook_BeforeClose(Cancel As Boolean)
    On Error Resume Next
    EnergyTradeUpdater.RemoveEnergyShortcuts
    On Error GoTo 0
End Sub
