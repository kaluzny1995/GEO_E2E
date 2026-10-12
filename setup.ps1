<#
Writes the machine-specific env file (paths, uid, Airflow UI login) detected from this checkout,
so the stack runs wherever the repo was cloned. Git-ignored; docker compose reads it through
`--env-file .env.local` (see README — Setup). In a Docker container it writes .env.docker
instead, non-interactively (defaults only).

Usage: .\setup.ps1 [-Force] [-Help]
  -Force  regenerate an existing env file (keeps the Airflow login already stored there)
  -Help   print this message
If script execution is blocked: powershell -ExecutionPolicy Bypass -File .\setup.ps1
#>
[CmdletBinding()]
param(
    [switch] $Force,
    [switch] $Help,
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]] $Rest
)

$ErrorActionPreference = 'Stop'

function Show-Usage {
    $lines = Get-Content -LiteralPath $PSCommandPath
    $out = @()
    $inBlock = $false
    foreach ($line in $lines) {
        if (-not $inBlock) {
            if ($line -match '^<#') { $inBlock = $true }
            continue
        }
        if ($line -match '#>') { break }
        $out += ($line -replace '^#\s?', '')
    }
    Write-Output ($out -join [Environment]::NewLine)
}

if ($Help) {
    Show-Usage
    exit 0
}
if ($Rest -and $Rest.Count -gt 0) {
    [Console]::Error.WriteLine("unknown argument: $($Rest[0]) (try -Help)")
    exit 2
}

function Test-InContainer {
    if (Test-Path -LiteralPath '/.dockerenv') { return $true }
    if (Test-Path -LiteralPath 'Env:container') {
        if ((Get-Item -LiteralPath 'Env:container').Value) { return $true }
    }
    if (Test-Path -LiteralPath '/proc/1/cgroup') {
        $content = Get-Content -LiteralPath '/proc/1/cgroup' -Raw -ErrorAction SilentlyContinue
        if ($content -and $content -match 'docker|containerd|kubepods|lxc|podman|libpod') { return $true }
    }
    return $false
}

function Get-EnvValue {
    param([string] $Name)
    $path = "Env:$Name"
    if (Test-Path -LiteralPath $path) {
        $value = (Get-Item -LiteralPath $path).Value
        if ($value) { return $value }
    }
    return $null
}

function Get-PreviousValue {
    param([string] $Key)
    if (Test-Path -LiteralPath $script:envPath) {
        foreach ($line in Get-Content -LiteralPath $script:envPath) {
            if ($line.StartsWith("$Key=")) { return $line.Substring($Key.Length + 1) }
        }
    }
    return $null
}

function Read-PromptValue {
    param([string] $Default, [string] $Message)
    if ($script:inContainer) { return $Default }
    if ([Console]::IsInputRedirected) { return $Default }
    $value = Read-Host "$Message [$Default]"
    if ([string]::IsNullOrWhiteSpace($value)) { return $Default }
    return $value
}

$inContainer = Test-InContainer
$envFile = '.env.local'
if ($inContainer) { $envFile = '.env.docker' }

$projectDir = Split-Path -Parent $PSCommandPath
$geoRoot = Split-Path -Parent $projectDir
$envPath = Join-Path $projectDir $envFile

if ((Test-Path -LiteralPath $envPath) -and -not $Force) {
    Write-Output "$envFile already exists in $projectDir — nothing to do."
    Write-Output "Edit it manually, or rerun .\setup.ps1 -Force to regenerate it from this checkout."
    exit 0
}

foreach ($sibling in @('GEO_Download', 'GEO_ETL')) {
    if (-not (Test-Path -LiteralPath (Join-Path $geoRoot $sibling))) {
        Write-Warning "$sibling not found under $geoRoot — the $sibling project must be a sibling of $projectDir"
    }
}

$condaRoot = Get-EnvValue 'CONDA_ROOT'
if (-not $condaRoot) {
    $condaCmd = Get-Command conda -ErrorAction SilentlyContinue
    if ($condaCmd) {
        $out = @(& $condaCmd.Source info --base 2>&1)
        if ($LASTEXITCODE -eq 0 -and $out.Count -gt 0 -and $out[0]) {
            $condaRoot = "$($out[0])".Trim()
        }
    }
}
if (-not $condaRoot) {
    $condaRoots = @()
    foreach ($key in @('USERPROFILE', 'ProgramData')) {
        $base = Get-EnvValue $key
        if ($base) { $condaRoots += $base }
    }
    foreach ($base in $condaRoots) {
        foreach ($leaf in @('anaconda3', 'miniconda3', 'miniforge3')) {
            $candidate = Join-Path $base $leaf
            if (Test-Path -LiteralPath $candidate) { $condaRoot = $candidate; break }
        }
        if ($condaRoot) { break }
    }
}
if (-not $condaRoot) {
    if ($inContainer) {
        $condaRoot = '/opt/conda'
    } else {
        $condaRoot = Read-PromptValue '/absolute/path/to/anaconda3' 'Conda base dir (CONDA_ROOT)'
    }
}

$airflowUid = Get-EnvValue 'AIRFLOW_UID'
$uidComment = '# Host uid of your user so containers can write to bind mounts (AIRFLOW_UID from the environment)'
if (-not $airflowUid) {
    $idCmd = Get-Command id -ErrorAction SilentlyContinue
    if ($idCmd) {
        $out = @(& $idCmd.Source -u 2>&1)
        if ($LASTEXITCODE -eq 0 -and $out.Count -gt 0 -and $out[0]) {
            $airflowUid = "$($out[0])".Trim()
            $uidComment = '# Host uid of your user so containers can write to bind mounts (id -u)'
        }
    }
}
if (-not $airflowUid) {
    $airflowUid = '1000'
    $uidComment = '# uid 1000 — no id command here (Windows); Docker Desktop handles file ownership'
}

if ($inContainer) {
    $condaComment = '# Conda installation inside the container'
    $fileComment = 'Regenerate with .\setup.ps1 -Force, or edit by hand; read by the containerized solution (in-container run).'
    $nextLine = "Wrote $envPath (in-container run — compose step not applicable)"
} else {
    $condaComment = '# Conda installation on the host (bind-mounted into the Airflow containers)'
    $fileComment = 'Regenerate with .\setup.ps1 -Force, or edit by hand; docker compose reads it through the --env-file .env.local flag.'
    $nextLine = 'Next: docker compose --env-file .env.local up -d --build'
}

if (-not (Test-Path -LiteralPath $condaRoot)) {
    if ($inContainer) {
        Write-Warning "CONDA_ROOT=$condaRoot does not exist (no conda on PATH, /opt/conda missing)"
    } else {
        Write-Warning "CONDA_ROOT=$condaRoot does not exist — the Airflow image bind-mounts it"
    }
}

$geoDataDir = Join-Path (Join-Path $geoRoot 'GEO_Download') 'data'
$defaultUser = Get-EnvValue '_AIRFLOW_WWW_USER_USERNAME'
if (-not $defaultUser) { $defaultUser = Get-PreviousValue '_AIRFLOW_WWW_USER_USERNAME' }
if (-not $defaultUser) { $defaultUser = 'airflow_admin' }
$username = Read-PromptValue $defaultUser 'Airflow UI login (_AIRFLOW_WWW_USER_USERNAME)'
$defaultPassword = Get-EnvValue '_AIRFLOW_WWW_USER_PASSWORD'
if (-not $defaultPassword) { $defaultPassword = Get-PreviousValue '_AIRFLOW_WWW_USER_PASSWORD' }
if (-not $defaultPassword) { $defaultPassword = 'airflow_admin' }
$password = Read-PromptValue $defaultPassword 'Airflow UI password (_AIRFLOW_WWW_USER_PASSWORD)'

$stamp = [DateTime]::UtcNow.ToString('yyyy-MM-ddTHH:mm:ssZ')
$content = @"
# Generated by .\setup.ps1 on $stamp from $projectDir
# Machine-specific values (paths, uid, Airflow UI credentials) — git-ignored.
# $fileComment

# Parent dir of the sibling projects (GEO_Download, GEO_E2E, GEO_ETL)
GEO_ROOT=$geoRoot
# Data folder shared with the Spark containers (read-only mount)
GEO_DATA_DIR=$geoDataDir
$condaComment
CONDA_ROOT=$condaRoot
$uidComment
AIRFLOW_UID=$airflowUid

# Airflow UI login (airflow-init creates this admin user on startup)
_AIRFLOW_WWW_USER_USERNAME=$username
_AIRFLOW_WWW_USER_PASSWORD=$password
"@

$utf8NoBom = New-Object System.Text.UTF8Encoding($false)
[IO.File]::WriteAllText($envPath, $content, $utf8NoBom)

Write-Output "Wrote ${envPath}:"
$keyLine = '^(GEO_ROOT|CONDA_ROOT|AIRFLOW_UID|_AIRFLOW_WWW_USER_USERNAME)='
Get-Content -LiteralPath $envPath | Where-Object { $_ -match $keyLine } | ForEach-Object { Write-Output "  $_" }
Write-Output ''
Write-Output $nextLine
