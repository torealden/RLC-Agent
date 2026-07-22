Attribute VB_Name = "CornGrindWorkbookEvents"
' =============================================================================
' CornGrindWorkbookEvents - Paste into ThisWorkbook module of us_grain_crush.xlsm
' =============================================================================
' THIS FILE CANNOT BE IMPORTED. ThisWorkbook is a document module; VBE >
' File > Import File creates a NEW standard module and the events never fire.
' In the VBA editor: double-click "ThisWorkbook" in the Project Explorer,
' then paste the two procedures below into the code pane.
'
' Why this module has to exist at all:
'   Application.OnKey assignments are RUNTIME state. They are not saved in the
'   workbook. Every time the file opens, something has to call
'   AssignCornGrindShortcuts again -- and Workbook_Open is that something.
'   Importing CornGrindUpdater.bas alone makes the macros appear under Alt+F8
'   (they are there, and running them by name works) while Ctrl+K does nothing,
'   because nothing ever bound Ctrl+K.
'
' To verify before closing the file: Alt+F8 > run AssignCornGrindShortcuts.
' The shortcut banner appears and Ctrl+K works immediately, in that session.
'
' Note: If you already have Workbook_Open / Workbook_BeforeClose handlers in
' this workbook, ADD the calls to the existing handlers rather than pasting
' duplicate event procedures -- VBA allows only one of each per module.
' =============================================================================

Private Sub Workbook_Open()
    AssignCornGrindShortcuts
End Sub

Private Sub Workbook_BeforeClose(Cancel As Boolean)
    RemoveCornGrindShortcuts
End Sub
