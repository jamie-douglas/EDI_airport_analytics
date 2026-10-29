param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$PythonArgs
)

$ErrorActionPreference = "Stop"

function Fail {
    param([string]$Message)
    Write-Error $Message
    exit 1
}

Write-Output "============================================================"
Write-Output "FastPark Short-Term Forecast Launcher"
Write-Output "============================================================"

$scriptDir = $PSScriptRoot
$modelScript = Join-Path $scriptDir "simplified_fastpark_shortterm.py"

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
    & $venvPython -m pip install numpy pandas openpyxl sqlalchemy pyodbc python-dotenv
    if ($LASTEXITCODE -ne 0) {
        Fail "Failed to install required packages."
    }
}

Write-Output ""
Write-Output "Running model..."
& $venvPython -B $modelScript @PythonArgs
$exitCode = $LASTEXITCODE

Write-Output ""
Write-Output "Model exit code: $exitCode"
exit $exitCode
