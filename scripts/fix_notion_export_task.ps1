$PythonExe = 'C:\Users\torem\AppData\Local\Programs\Python\Python311\python.exe'
$WorkingDir = 'C:\dev\rlc-agent\rlc_scheduler'
try {
    $newAction = New-ScheduledTaskAction -Execute $PythonExe -Argument 'daily_activity_log.py export' -WorkingDirectory $WorkingDir
    Set-ScheduledTask -TaskName 'RLC_DailyNotionExport' -Action $newAction
    Write-Host 'SUCCESS'
    Get-ScheduledTask -TaskName 'RLC_DailyNotionExport' | Select-Object -ExpandProperty Actions | Format-List Execute, Arguments, WorkingDirectory
} catch {
    Write-Host "FAILED: $_"
}
