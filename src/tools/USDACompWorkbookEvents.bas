Attribute VB_Name = "USDACompWorkbookEvents"
' =============================================================================
' USDACompWorkbookEvents - Paste into ThisWorkbook of any usda_comp workbook
' =============================================================================
' THIS FILE CANNOT BE IMPORTED. ThisWorkbook is a document module; VBE >
' File > Import File creates a NEW standard module and the events never fire.
' In the VBA editor: double-click "ThisWorkbook" in the Project Explorer,
' then paste the two procedures below into the code pane.
'
' Why this module has to exist at all:
'   Application.OnKey assignments are RUNTIME state. They are not saved in the
'   workbook. Every time the file opens, something has to call
'   AssignUSDACompShortcut again -- and Workbook_Open is that something.
'   Importing USDACompUpdater.bas alone makes the macros appear under Alt+F8
'   (running them by name works) while Ctrl+Shift+U does nothing, because
'   nothing ever bound it.
'
' To verify before closing the file: Alt+F8 > run AssignUSDACompShortcut.
' The shortcut banner appears and Ctrl+Shift+U works immediately, that session.
'
' Note: If you already have Workbook_Open / Workbook_BeforeClose handlers in
' this workbook (the soybean complex book binds Ctrl+Shift+W there), ADD the
' calls to the existing handlers rather than pasting duplicate event
' procedures -- VBA allows only one of each per module.
' =============================================================================

Private Sub Workbook_Open()
    AssignUSDACompShortcut
End Sub

Private Sub Workbook_BeforeClose(Cancel As Boolean)
    RemoveUSDACompShortcut
End Sub
