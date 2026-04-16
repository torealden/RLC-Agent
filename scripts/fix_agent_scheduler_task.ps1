$PythonExe = 'C:\Users\torem\AppData\Local\Programs\Python\Python311\python.exe'
$WorkingDir = 'C:\dev\rlc-agent\rlc_scheduler'
try {
    $newAction = New-ScheduledTaskAction -Execute $PythonExe -Argument 'agent_scheduler.py run' -WorkingDirectory $WorkingDir
    Set-ScheduledTask -TaskName 'RLC_AgentScheduler' -Action $newAction
    Write-Host 'SUCCESS'
    Get-ScheduledTask -TaskName 'RLC_AgentScheduler' | Select-Object -ExpandProperty Actions | Format-List Execute, Arguments, WorkingDirectory
    Start-ScheduledTask -TaskName 'RLC_AgentScheduler'
    Start-Sleep -Seconds 4
    Get-ScheduledTaskInfo -TaskName 'RLC_AgentScheduler' | Format-List LastRunTime, LastTaskResult
    Get-ScheduledTask -TaskName 'RLC_AgentScheduler' | Select-Object TaskName, State
} catch {
    Write-Host "FAILED: $_"
}
