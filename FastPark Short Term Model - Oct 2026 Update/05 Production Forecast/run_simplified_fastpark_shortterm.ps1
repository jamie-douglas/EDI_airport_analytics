param(
    [switch]$ForceMondayTasks,
    [switch]$NoPause,

    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$ForwardedPythonArguments
)

$ErrorActionPreference = "Stop"
# In PowerShell 7, native command non-zero exits can become terminating errors.
# This script relies on $LASTEXITCODE checks for controlled flow.
if (Get-Variable -Name PSNativeCommandUseErrorActionPreference -ErrorAction SilentlyContinue) {
    $PSNativeCommandUseErrorActionPreference = $false
}
$pauseOnExit = -not $NoPause
$runStartedAt = Get-Date
$script:pywin32Status = "not-checked"
$script:mondayTasksStatus = "not-run"

function Exit-Script {
    param(
        [int]$Code,
        [string]$Summary = ""
    )

    if ($Summary) {
        Write-Output ""
        Write-Output $Summary
    }

    if ($pauseOnExit) {
        Write-Output ""
        try {
            if ([Environment]::UserInteractive) {
                Read-Host "Press Enter to close this window" | Out-Null
            }
            else {
                Write-Output "No interactive console detected; skipping pause."
            }
        }
        catch {
            Write-Output "Pause prompt unavailable in this host; closing now."
        }
    }

    exit $Code
}

function Fail {
    param([string]$Message)
    Write-Output "ERROR: $Message"
    Exit-Script -Code 1
}

function Test-PythonRuntime {
    param([string]$PythonExecutable)

    if (-not $PythonExecutable -or -not (Test-Path $PythonExecutable)) {
        return $false
    }

    try {
        & $PythonExecutable -c "import encodings, site" 2>$null
        return ($LASTEXITCODE -eq 0)
    }
    catch {
        return $false
    }
}

function Resolve-BasePython {
    $candidates = @()

    # 1) Explicit override for locked-down machines.
    if ($env:FASTPARK_BASE_PYTHON) {
        $candidates += $env:FASTPARK_BASE_PYTHON
    }

    # 2) If launched from an activated venv, try that interpreter first.
    if ($env:VIRTUAL_ENV) {
        $activeVenvPython = Join-Path $env:VIRTUAL_ENV "Scripts\python.exe"
        $candidates += $activeVenvPython
    }

    # 2b) uv-managed Python (if uv is installed).
    $uvCommand = Get-Command uv.exe -ErrorAction SilentlyContinue
    if ($uvCommand) {
        try {
            $uvResolved = & $uvCommand.Source python find 2>$null
            if ($LASTEXITCODE -eq 0 -and $uvResolved) {
                $uvCandidate = ($uvResolved | Select-Object -Last 1).Trim()
                if ($uvCandidate) {
                    $candidates += $uvCandidate
                }
            }
        }
        catch {
            # Continue with other sources.
        }
    }

    # 3) Python launcher (usually most reliable on Windows).
    $pyLauncher = Get-Command py.exe -ErrorAction SilentlyContinue
    if ($pyLauncher) {
        try {
            $resolved = & $pyLauncher.Source -3 -c "import sys, encodings; print(sys.executable)" 2>$null
            if ($LASTEXITCODE -eq 0 -and $resolved) {
                $candidate = ($resolved | Select-Object -Last 1).Trim()
                if ($candidate) {
                    $candidates += $candidate
                }
            }
        }
        catch {
            # Fall through to PATH-based discovery.
        }
    }

    # 4) PATH python(s).
    $pathPythons = Get-Command python.exe -All -ErrorAction SilentlyContinue
    if ($pathPythons) {
        $candidates += ($pathPythons | Select-Object -ExpandProperty Source)
    }

    # 5) Python installs registered in Windows registry (PEP 514).
    $registryRoots = @(
        "HKCU:\Software\Python\PythonCore",
        "HKLM:\Software\Python\PythonCore",
        "HKLM:\Software\Wow6432Node\Python\PythonCore"
    )

    foreach ($root in $registryRoots) {
        if (-not (Test-Path $root)) {
            continue
        }

        $versions = Get-ChildItem -Path $root -ErrorAction SilentlyContinue
        foreach ($version in $versions) {
            $installPathKey = Join-Path $version.PSPath "InstallPath"
            if (-not (Test-Path $installPathKey)) {
                continue
            }

            try {
                $installInfo = Get-ItemProperty -Path $installPathKey -ErrorAction SilentlyContinue
                if ($installInfo.'(default)') {
                    $candidates += (Join-Path $installInfo.'(default)' "python.exe")
                }
                if ($installInfo.ExecutablePath) {
                    $candidates += $installInfo.ExecutablePath
                }
            }
            catch {
                # Keep searching other keys.
            }
        }
    }

    # 6) Common per-user install location if nothing is on PATH.
    $userPythonRoot = Join-Path $env:LOCALAPPDATA "Programs\Python"
    if (Test-Path $userPythonRoot) {
        $installedPythons = Get-ChildItem -Path $userPythonRoot -Directory -Filter "Python*" -ErrorAction SilentlyContinue |
            Sort-Object Name -Descending

        foreach ($pythonDir in $installedPythons) {
            $candidates += (Join-Path $pythonDir.FullName "python.exe")
        }
    }

    # 7) Common machine-wide install locations.
    $machinePythonRoots = @(
        (Join-Path $env:ProgramFiles "Python"),
        (Join-Path ${env:ProgramFiles(x86)} "Python")
    )

    foreach ($root in $machinePythonRoots) {
        if (-not $root -or -not (Test-Path $root)) {
            continue
        }

        $installedPythons = Get-ChildItem -Path $root -Directory -Filter "Python*" -ErrorAction SilentlyContinue |
            Sort-Object Name -Descending

        foreach ($pythonDir in $installedPythons) {
            $candidates += (Join-Path $pythonDir.FullName "python.exe")
        }
    }

    foreach ($candidate in ($candidates | Select-Object -Unique)) {
        if (Test-PythonRuntime -PythonExecutable $candidate) {
            return $candidate
        }
    }

    return $null
}

function Install-UvPython {
    param([string]$Version = "3.11")

    $uvCommand = Get-Command uv.exe -ErrorAction SilentlyContinue
    if (-not $uvCommand) {
        return $false
    }

    Write-Output "No healthy Python found. Attempting uv-managed install for Python $Version..."
    & $uvCommand.Source python install $Version

    return ($LASTEXITCODE -eq 0)
}

function Invoke-PythonScript {
    param(
        [string]$StepName,
        [string]$ScriptPath,
        [string[]]$StepArguments = @()
    )

    if (-not (Test-Path $ScriptPath)) {
        Fail "Step '$StepName' script not found: $ScriptPath"
    }

    Write-Output ""
    Write-Output "Running step: $StepName"
    Write-Output "Script: $ScriptPath"

    & $venvPython -B $ScriptPath @StepArguments

    if ($LASTEXITCODE -ne 0) {
        Fail "Step '$StepName' failed with exit code $LASTEXITCODE"
    }

    Write-Output "Step complete: $StepName"
}

function Install-NotebookExecutionPackages {
    Write-Output "Checking notebook execution packages..."

    $notebookPackagesAvailable = $false
    try {
        & $venvPython -c "import nbconvert, nbclient, nbformat" 2>$null
        $notebookPackagesAvailable = ($LASTEXITCODE -eq 0)
    }
    catch {
        $notebookPackagesAvailable = $false
    }

    if ($notebookPackagesAvailable) {
        Write-Output "Notebook packages already available."
        return
    }

    Write-Output "Installing notebook execution packages..."
    & $venvPython -m pip install jupyter nbconvert nbclient nbformat ipykernel

    if ($LASTEXITCODE -ne 0) {
        Fail "Failed to install notebook execution packages."
    }
}

function Install-PowerPointAutomationPackages {
    Write-Output "Checking PowerPoint automation packages..."

    $pywin32Available = $false
    try {
        & $venvPython -c "import win32com.client" 2>$null
        $pywin32Available = ($LASTEXITCODE -eq 0)
    }
    catch {
        $pywin32Available = $false
    }

    if ($pywin32Available) {
        $script:pywin32Status = "ok"
        Write-Output "PowerPoint automation packages already available."
        return
    }

    Write-Output "Installing PowerPoint automation package (pywin32)..."
    & $venvPython -m pip install pywin32

    if ($LASTEXITCODE -ne 0) {
        Fail "Failed to install pywin32 for PowerPoint automation."
    }

    $script:pywin32Status = "installed"
}

function Install-ChristmasAdjustmentPackages {
    # The Christmas adjustment (07 Christmas Adjustment) draws a chart with
    # matplotlib. Without it the adjustment fails and the model forecast is
    # saved unadjusted, so make sure existing venvs have it too.
    Write-Output "Checking Christmas adjustment packages..."

    $matplotlibAvailable = $false
    try {
        & $venvPython -c "import matplotlib" 2>$null
        $matplotlibAvailable = ($LASTEXITCODE -eq 0)
    }
    catch {
        $matplotlibAvailable = $false
    }

    if ($matplotlibAvailable) {
        Write-Output "Christmas adjustment packages already available."
        return
    }

    Write-Output "Installing matplotlib for the Christmas adjustment..."
    & $venvPython -m pip install matplotlib

    if ($LASTEXITCODE -ne 0) {
        Fail "Failed to install matplotlib for the Christmas adjustment."
    }
}

function Invoke-ForecastAccuracyNotebook {
    param([string]$NotebookPath)

    if (-not (Test-Path $NotebookPath)) {
        Fail "Forecast accuracy notebook not found: $NotebookPath"
    }

    Install-NotebookExecutionPackages

    Write-Output ""
    Write-Output "Running step: Forecast accuracy notebook"
    Write-Output "Notebook: $NotebookPath"

    & $venvPython -m jupyter nbconvert --to notebook --execute --inplace --ExecutePreprocessor.timeout=1800 $NotebookPath

    if ($LASTEXITCODE -ne 0) {
        Fail "Forecast accuracy notebook failed with exit code $LASTEXITCODE"
    }

    Write-Output "Step complete: Forecast accuracy notebook"
}

try {
    Write-Output "============================================================"
    Write-Output "FastPark Short-Term Forecast Launcher"
    Write-Output "============================================================"

    $scriptDir = $PSScriptRoot
    $modelScript = Join-Path $scriptDir "simplified_fastpark_shortterm.py"

    $modelRoot = Split-Path -Path $scriptDir -Parent
    $shortTermModelsRoot = Split-Path -Path $modelRoot -Parent

    $comparisonScript = Join-Path $modelRoot "06 Comparison Framework\compare_live_forecasts.py"
    $forecastAccuracyNotebook = Join-Path $modelRoot "06 Comparison Framework\Forecast accuracy\forecast accuracy.ipynb"
    $powerPointScript = Join-Path $modelRoot "06 Comparison Framework\update_weekly_powerpoint.py"
    $oldModelScript = Join-Path $shortTermModelsRoot "FastPark Short-Term Model Rebuild 2025\FP_short_term_model.py"

    if (-not (Test-Path $modelScript)) {
        Fail "Model script not found: $modelScript"
    }

    # Prefer a local Documents venv and fall back to the shared/script location.
    $venvName = "highs"
    $documentsPath = [Environment]::GetFolderPath("MyDocuments")

    if (Test-Path $documentsPath) {
        $venvPath = Join-Path $documentsPath "Venv FastPark ST model\$venvName"
        Write-Output "Using local Documents folder for virtual environment:"
        Write-Output $venvPath
    }
    else {
        $venvPath = Join-Path $scriptDir $venvName
        Write-Output "Local Documents folder not found. Using script/shared folder for virtual environment:"
        Write-Output $venvPath
    }

    $venvRoot = Split-Path -Path $venvPath -Parent
    $venvPython = Join-Path $venvPath "Scripts\python.exe"
    $venvConfig = Join-Path $venvPath "pyvenv.cfg"

    Write-Output "Model script: $modelScript"
    Write-Output "Venv path:    $venvPath"
    Write-Output ""

    $systemPython = Resolve-BasePython
    if (-not $systemPython) {
        if (Install-UvPython -Version "3.11") {
            $systemPython = Resolve-BasePython
        }
    }

    if (-not $systemPython) {
        Fail "No healthy base Python interpreter was found. The launcher attempted uv install automatically. If using uv, run 'uv python install 3.11' manually and rerun, or set FASTPARK_BASE_PYTHON to a known good interpreter path. Otherwise install/repair Python so 'py -3' or 'python.exe' can import encodings."
    }

    Write-Output "Base Python: $systemPython"
    $recreateVenv = $false

    if (-not (Test-Path $venvPath)) {
        Write-Output "Virtual environment not found."
        $recreateVenv = $true
    }
    elseif (-not (Test-Path $venvPython)) {
        Write-Output "Virtual environment found, but python.exe is missing."
        $recreateVenv = $true
    }
    elseif (-not (Test-Path $venvConfig)) {
        Write-Output "Virtual environment found, but pyvenv.cfg is missing."
        $recreateVenv = $true
    }
    else {
        if (-not (Test-PythonRuntime -PythonExecutable $venvPython)) {
            Write-Output "Virtual environment exists but failed runtime validation (often due to missing encodings)."
            $recreateVenv = $true
        }
        else {
            Write-Output "Existing virtual environment is valid."
        }
    }

    if ($recreateVenv) {
        if (Test-Path $venvPath) {
            Write-Output "Removing invalid virtual environment..."
            Remove-Item -Path $venvPath -Recurse -Force
        }

        Write-Output "Creating virtual environment..."
        New-Item -Path $venvRoot -ItemType Directory -Force | Out-Null
        & $systemPython -m venv $venvPath

        if ($LASTEXITCODE -ne 0 -or -not (Test-Path $venvPython)) {
            Fail "Failed to create virtual environment at: $venvPath"
        }

        Write-Output "Upgrading pip..."
        & $venvPython -m pip install --upgrade pip
        if ($LASTEXITCODE -ne 0) {
            Fail "Failed to upgrade pip."
        }

        Write-Output "Installing required packages..."
        & $venvPython -m pip install numpy pandas openpyxl sqlalchemy pyodbc python-dotenv pywin32 matplotlib
        if ($LASTEXITCODE -ne 0) {
            Fail "Failed to install required packages."
        }
    }

    Install-PowerPointAutomationPackages
    Install-ChristmasAdjustmentPackages

    Invoke-PythonScript -StepName "New forecast model" -ScriptPath $modelScript -StepArguments $ForwardedPythonArguments
    Invoke-PythonScript -StepName "Old forecast model" -ScriptPath $oldModelScript
    Invoke-PythonScript -StepName "Live forecast comparison" -ScriptPath $comparisonScript

    $isMonday = ((Get-Date).DayOfWeek -eq [System.DayOfWeek]::Monday)
    $runMondayTasks = $isMonday -or $ForceMondayTasks

    Write-Output ""
    if ($runMondayTasks) {
        if ($ForceMondayTasks -and -not $isMonday) {
            Write-Output "Monday-only tasks forced on a non-Monday run."
        }
        else {
            Write-Output "Today is Monday. Running Monday-only tasks."
        }

        Invoke-ForecastAccuracyNotebook -NotebookPath $forecastAccuracyNotebook
        Invoke-PythonScript -StepName "PowerPoint archive/update" -ScriptPath $powerPointScript
        $script:mondayTasksStatus = "ran"
    }
    else {
        Write-Output "Today is not Monday. Skipping forecast accuracy notebook and deck archive."
        $script:mondayTasksStatus = "skipped"
    }

    $runFinishedAt = Get-Date
    $durationSec = [math]::Round((New-TimeSpan -Start $runStartedAt -End $runFinishedAt).TotalSeconds, 1)
    $runLine = "{0},{1},{2},{3},{4},{5}" -f $runFinishedAt.ToString("yyyy-MM-dd HH:mm:ss"),$durationSec,$isMonday,$ForceMondayTasks,$script:mondayTasksStatus,$script:pywin32Status

    Write-Output "Run summary: $runLine"

    Exit-Script -Code 0 -Summary "Pipeline complete."
}
catch {
    Write-Output ""
    Write-Output "Pipeline failed."
    Write-Output "ERROR: $($_.Exception.Message)"
    Write-Output ""
    Write-Output "Error record details:"
    Write-Output ($_ | Out-String)
    Exit-Script -Code 1 -Summary "Pipeline failed. See error details above."
}
