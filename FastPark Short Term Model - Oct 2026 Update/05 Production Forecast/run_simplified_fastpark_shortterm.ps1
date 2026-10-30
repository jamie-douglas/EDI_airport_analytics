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
        Read-Host "Press Enter to close this window" | Out-Null
    }

    exit $Code
}

function Fail {
    param([string]$Message)
    Write-Output "ERROR: $Message"
    Exit-Script -Code 1
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

    $pythonCommand = Get-Command python.exe -ErrorAction SilentlyContinue
    if (-not $pythonCommand) {
        Fail "python.exe was not found on PATH. Install Python first."
    }

    $systemPython = $pythonCommand.Source
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
        & $venvPython --version | Out-Null
        if ($LASTEXITCODE -ne 0) {
            Write-Output "Virtual environment exists but failed validation."
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
        & $venvPython -m pip install numpy pandas openpyxl sqlalchemy pyodbc python-dotenv pywin32
        if ($LASTEXITCODE -ne 0) {
            Fail "Failed to install required packages."
        }
    }

    Install-PowerPointAutomationPackages

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
