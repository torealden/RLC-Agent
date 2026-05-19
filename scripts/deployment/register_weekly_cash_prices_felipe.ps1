$ErrorActionPreference = 'Stop'

$taskName = 'Weekly Cash Prices to Felipe'
$taskPath = '\RLC\'
$pythonExe = (Get-Command python).Source
$scriptPath = 'C:\dev\RLC-Agent\scripts\email_cash_prices_to_felipe.py'
$logPath = 'C:\dev\RLC-Agent\logs\weekly_cash_prices_felipe.log'

# Remove old task if present
$existing = Get-ScheduledTask -TaskPath $taskPath -TaskName $taskName -ErrorAction SilentlyContinue
if ($existing) {
    Unregister-ScheduledTask -TaskPath $taskPath -TaskName $taskName -Confirm:$false
    Write-Host "Removed existing task"
}

# Wednesday 6:30pm ET — after the dispatcher's 18:00 ET cash_prices_generation
$trigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Wednesday -At 6:30PM
$action = New-ScheduledTaskAction `
    -Execute $pythonExe `
    -Argument "$scriptPath >> `"$logPath`" 2>&1" `
    -WorkingDirectory 'C:\dev\RLC-Agent'
$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 10)
$principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType S4U

Register-ScheduledTask `
    -TaskName $taskName `
    -TaskPath $taskPath `
    -Trigger $trigger `
    -Action $action `
    -Settings $settings `
    -Principal $principal `
    -Description 'Generate weekly cash prices xlsx, copy to HB Dropbox folder, email Felipe + Tore (cc). Fires Wed 6:30pm ET.'

Write-Host "Registered $taskPath$taskName"
Get-ScheduledTask -TaskPath $taskPath -TaskName $taskName | Select-Object TaskName, State | Format-List
