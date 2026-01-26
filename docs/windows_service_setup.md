# Windows Service Setup for RLC Scheduler

This document explains how to set up the RLC Agent Scheduler to run automatically on Windows startup.

---

## Option 1: Task Scheduler (Recommended for Most Users)

Windows Task Scheduler is the simplest approach and doesn't require additional software.

### Setup Steps

1. **Open Task Scheduler**
   - Press `Win + R`, type `taskschd.msc`, press Enter

2. **Create a New Task**
   - Click "Create Task" (not "Create Basic Task")

3. **General Tab**
   - Name: `RLC Agent Scheduler`
   - Description: `Runs the RLC commodity data scheduler`
   - Select: "Run whether user is logged on or not"
   - Check: "Run with highest privileges"

4. **Triggers Tab**
   - Click "New"
   - Begin the task: "At startup"
   - Delay task for: `30 seconds` (gives network time to connect)
   - Enabled: Yes

5. **Actions Tab**
   - Click "New"
   - Action: "Start a program"
   - Program/script: `python`
   - Add arguments: `rlc_scheduler/agent_scheduler.py run`
   - Start in: `C:\Users\torem\Dropbox\RLC Documents\LLM Model and Documents\Projects\RLC-Agent`

6. **Conditions Tab**
   - Uncheck: "Start only if the computer is on AC power"
   - Check: "Start only if the following network connection is available" > "Any connection"

7. **Settings Tab**
   - Check: "Allow task to be run on demand"
   - Check: "If the task fails, restart every: 1 minute"
   - Attempt to restart up to: 3 times
   - Check: "If the running task does not end when requested, force it to stop"

8. **Click OK**
   - Enter your Windows password when prompted

### Managing the Task

```powershell
# Start the scheduler manually
schtasks /run /tn "RLC Agent Scheduler"

# Stop the scheduler
schtasks /end /tn "RLC Agent Scheduler"

# Check status
schtasks /query /tn "RLC Agent Scheduler" /v

# Delete the task
schtasks /delete /tn "RLC Agent Scheduler" /f
```

---

## Option 2: Windows Service with NSSM

NSSM (Non-Sucking Service Manager) wraps any executable as a Windows service.

### Install NSSM

1. Download from: https://nssm.cc/download
2. Extract to `C:\nssm`
3. Add to PATH or use full path

### Create Service

```powershell
# Open PowerShell as Administrator
cd C:\nssm\win64

# Install the service
.\nssm.exe install RLCScheduler

# In the GUI that opens:
# Path: C:\Users\torem\AppData\Local\Programs\Python\Python312\python.exe
# Startup directory: C:\Users\torem\Dropbox\RLC Documents\LLM Model and Documents\Projects\RLC-Agent
# Arguments: rlc_scheduler\agent_scheduler.py run

# Or via command line:
.\nssm.exe install RLCScheduler "C:\Users\torem\AppData\Local\Programs\Python\Python312\python.exe" "rlc_scheduler\agent_scheduler.py run"
.\nssm.exe set RLCScheduler AppDirectory "C:\Users\torem\Dropbox\RLC Documents\LLM Model and Documents\Projects\RLC-Agent"
.\nssm.exe set RLCScheduler DisplayName "RLC Agent Scheduler"
.\nssm.exe set RLCScheduler Description "Commodity data agent scheduler"
.\nssm.exe set RLCScheduler Start SERVICE_AUTO_START
.\nssm.exe set RLCScheduler AppStdout "C:\Users\torem\Dropbox\RLC Documents\LLM Model and Documents\Projects\RLC-Agent\logs\scheduler_stdout.log"
.\nssm.exe set RLCScheduler AppStderr "C:\Users\torem\Dropbox\RLC Documents\LLM Model and Documents\Projects\RLC-Agent\logs\scheduler_stderr.log"
.\nssm.exe set RLCScheduler AppRotateFiles 1
.\nssm.exe set RLCScheduler AppRotateBytes 5000000
```

### Manage Service

```powershell
# Start service
net start RLCScheduler

# Stop service
net stop RLCScheduler

# Check status
sc query RLCScheduler

# Remove service
.\nssm.exe remove RLCScheduler confirm
```

---

## Option 3: Python-Based Windows Service

Create a native Windows service using `pywin32`.

### Install pywin32

```bash
pip install pywin32
```

### Service Script

Save as `rlc_scheduler/scheduler_service.py`:

```python
import win32serviceutil
import win32service
import win32event
import servicemanager
import socket
import sys
import os
import subprocess
from pathlib import Path

class RLCSchedulerService(win32serviceutil.ServiceFramework):
    _svc_name_ = "RLCScheduler"
    _svc_display_name_ = "RLC Agent Scheduler"
    _svc_description_ = "Runs commodity data collection agents on schedule"

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        socket.setdefaulttimeout(60)
        self.process = None

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.hWaitStop)
        if self.process:
            self.process.terminate()

    def SvcDoRun(self):
        servicemanager.LogMsg(
            servicemanager.EVENTLOG_INFORMATION_TYPE,
            servicemanager.PYS_SERVICE_STARTED,
            (self._svc_name_, '')
        )
        self.main()

    def main(self):
        # Change to RLC-Agent directory
        rlc_dir = Path(r"C:\Users\torem\Dropbox\RLC Documents\LLM Model and Documents\Projects\RLC-Agent")
        os.chdir(rlc_dir)

        # Start scheduler
        self.process = subprocess.Popen(
            [sys.executable, "rlc_scheduler/agent_scheduler.py", "run"],
            cwd=str(rlc_dir)
        )

        # Wait for stop signal
        win32event.WaitForSingleObject(self.hWaitStop, win32event.INFINITE)

if __name__ == '__main__':
    if len(sys.argv) == 1:
        servicemanager.Initialize()
        servicemanager.PrepareToHostSingle(RLCSchedulerService)
        servicemanager.StartServiceCtrlDispatcher()
    else:
        win32serviceutil.HandleCommandLine(RLCSchedulerService)
```

### Install and Run

```powershell
# Install service (run as Administrator)
python rlc_scheduler\scheduler_service.py install

# Start service
python rlc_scheduler\scheduler_service.py start

# Stop service
python rlc_scheduler\scheduler_service.py stop

# Remove service
python rlc_scheduler\scheduler_service.py remove
```

---

## Troubleshooting

### Common Issues

1. **Python not found**
   - Use full path to python.exe
   - Typical location: `C:\Users\torem\AppData\Local\Programs\Python\Python312\python.exe`

2. **Module not found errors**
   - Ensure virtual environment is activated or use system Python with all dependencies
   - Consider creating a batch wrapper script

3. **Network/API timeouts**
   - Scheduler has retry logic built in
   - Check logs for specific errors

4. **Permission issues**
   - Run as Administrator when installing
   - Task Scheduler: use "Run with highest privileges"

### Batch Wrapper Script

Save as `rlc_scheduler/run_scheduler.bat`:

```batch
@echo off
cd /d "C:\Users\torem\Dropbox\RLC Documents\LLM Model and Documents\Projects\RLC-Agent"
python rlc_scheduler\agent_scheduler.py run
```

Use this batch file in Task Scheduler instead of calling Python directly.

### Check Logs

```powershell
# View scheduler log (if configured)
Get-Content "C:\Users\torem\Dropbox\RLC Documents\LLM Model and Documents\Projects\RLC-Agent\logs\scheduler.log" -Tail 50

# View Windows Event Log (for service mode)
Get-EventLog -LogName Application -Source RLCScheduler -Newest 10
```

---

## Recommended Setup

For your work computer:
1. Use **Task Scheduler** (Option 1) - most reliable, no extra software
2. Set startup delay to 30-60 seconds
3. Enable restart on failure
4. Keep a manual shortcut to run `agent_scheduler.py run` for testing

---

*Document created: January 19, 2026*
