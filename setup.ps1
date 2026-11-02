<#
Writes the machine-specific env file (paths, uid, Airflow UI login, Supabase credentials)
detected from this checkout, so the stack runs wherever the repo was cloned — and mirrors the
Supabase credentials into the sibling GEO_ETL/.env and GEO_API/.env (setup lives here).
Git-ignored; docker compose auto-loads it from the project root (see README — Docker setup).
In a Docker container it writes .env.docker instead, non-interactively (defaults only).

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
$envFile = '.env'
if ($inContainer) { $envFile = '.env.docker' }

$projectDir = Split-Path -Parent $PSCommandPath
$geoRoot = Split-Path -Parent $projectDir
$envPath = Join-Path $projectDir $envFile

$geoEtlEnv = Join-Path (Join-Path $geoRoot 'GEO_ETL') '.env'
$geoApiEnv = Join-Path (Join-Path $geoRoot 'GEO_API') '.env'
$supabaseEnvFiles = @($geoEtlEnv, $geoApiEnv)
$supabaseKeys = @(
    'SUPABASE_DB_USER',
    'SUPABASE_DB_PASSWORD',
    'SUPABASE_DB_HOST',
    'SUPABASE_DB_PORT',
    'SUPABASE_DB_NAME'
)

# Current Supabase credentials as stored in this checkout's env file (whatever ran before us).
function Get-SupabaseValueMap {
    $map = @{}
    foreach ($k in $supabaseKeys) { $map[$k] = Get-PreviousValue $k }
    return $map
}

# GEO_ETL and GEO_API read the same five Supabase values from their own gitignored .env files —
# GEO_E2E is where they are set up, so mirror them into both sibling files (upsert: other lines
# are preserved, a missing file is created with a pointer comment).
function Sync-SiblingEnv {
    param([hashtable] $Values)
    $any = $false
    foreach ($k in $supabaseKeys) { if ($Values[$k]) { $any = $true; break } }
    if (-not $any) { return } # nothing to mirror — the warning below already told the user

    foreach ($target in $supabaseEnvFiles) {
        $targetDir = Split-Path -Parent $target
        if (-not (Test-Path -LiteralPath $targetDir)) {
            Write-Warning "$targetDir not found — skipped writing its .env"
            continue
        }
        $lines = New-Object 'System.Collections.Generic.List[string]'
        if (Test-Path -LiteralPath $target) {
            foreach ($line in Get-Content -LiteralPath $target) {
                $isSupabaseKey = $false
                foreach ($k in $supabaseKeys) {
                    if ($line.StartsWith("$k=")) { $isSupabaseKey = $true; break }
                }
                if (-not $isSupabaseKey) { $lines.Add($line) }
            }
        }
        else {
            $lines.Add("# Supabase PostgreSQL credentials — mirrored from GEO_E2E/.env by GEO_E2E's setup.ps1")
            $lines.Add('# Set them in GEO_E2E/.env first (see GEO_E2E/.env.example); this file is gitignored.')
        }
        foreach ($k in $supabaseKeys) { if ($Values[$k]) { $lines.Add("$k=$($Values[$k])") } }
        $utf8NoBomEnv = New-Object System.Text.UTF8Encoding($false)
        [IO.File]::WriteAllLines($target, $lines.ToArray(), $utf8NoBomEnv)
        Write-Output "Mirrored the Supabase credentials into ${target}:"
        foreach ($k in @('SUPABASE_DB_USER', 'SUPABASE_DB_HOST', 'SUPABASE_DB_PORT')) {
            Write-Output "  $k=$($Values[$k])"
        }
    }
}

if ((Test-Path -LiteralPath $envPath) -and -not $Force) {
    Write-Output "$envFile already exists in $projectDir — nothing to do."
    Write-Output "Edit it manually, or rerun .\setup.ps1 -Force to regenerate it from this checkout."
    # The env file itself is untouched, but the sibling .env files are still refreshed from it
    Sync-SiblingEnv (Get-SupabaseValueMap)
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
    $condaComment = '# Conda installation on the host (optional — used by local, non-container runs)'
    $fileComment = 'Regenerate with .\setup.ps1 -Force, or edit by hand; docker compose auto-loads .env from this directory.'
    $nextLine = 'Next: docker compose up -d --build'
}

if (-not (Test-Path -LiteralPath $condaRoot)) {
    if ($inContainer) {
        Write-Warning "CONDA_ROOT=$condaRoot does not exist (no conda on PATH, /opt/conda missing)"
    } else {
        Write-Warning "CONDA_ROOT=$condaRoot does not exist — the Airflow image bind-mounts it"
    }
}

$geoDataDir = Join-Path (Join-Path $geoRoot 'GEO_Download') 'data'
# The data folder is bind-mounted into the containers, so it must exist before Compose starts.
New-Item -ItemType Directory -Force -Path $geoDataDir | Out-Null
$defaultUser = Get-EnvValue '_AIRFLOW_WWW_USER_USERNAME'
if (-not $defaultUser) { $defaultUser = Get-PreviousValue '_AIRFLOW_WWW_USER_USERNAME' }
if (-not $defaultUser) { $defaultUser = 'airflow_admin' }
$username = Read-PromptValue $defaultUser 'Airflow UI login (_AIRFLOW_WWW_USER_USERNAME)'
$defaultPassword = Get-EnvValue '_AIRFLOW_WWW_USER_PASSWORD'
if (-not $defaultPassword) { $defaultPassword = Get-PreviousValue '_AIRFLOW_WWW_USER_PASSWORD' }
if (-not $defaultPassword) { $defaultPassword = 'airflow_admin' }
$password = Read-PromptValue $defaultPassword 'Airflow UI password (_AIRFLOW_WWW_USER_PASSWORD)'

# Supabase credentials are secrets: never prompted (they would land in the scrollback) —
# the primary place to set them is this file (.env); a previous value of .env or
# of a sibling GEO_ETL/.env / GEO_API/.env is re-used, else they stay empty (compose then
# fails with a message naming the missing variable). Sync-SiblingEnv mirrors the result back
# into both siblings, so every project runs with the same credentials.
function Get-SupabaseValue {
    param([string] $Key)
    $value = Get-PreviousValue $Key
    if (-not $value) {
        foreach ($path in $supabaseEnvFiles) {
            if (-not (Test-Path -LiteralPath $path)) { continue }
            foreach ($line in Get-Content -LiteralPath $path) {
                if ($line.StartsWith("$Key=")) { $value = $line.Substring($Key.Length + 1); break }
            }
            if ($value) { break }
        }
    }
    return $value
}
$supabaseUser = Get-SupabaseValue 'SUPABASE_DB_USER'
$supabasePassword = Get-SupabaseValue 'SUPABASE_DB_PASSWORD'
$supabaseHost = Get-SupabaseValue 'SUPABASE_DB_HOST'
$supabasePort = Get-SupabaseValue 'SUPABASE_DB_PORT'
$supabaseName = Get-SupabaseValue 'SUPABASE_DB_NAME'
$schema = Get-PreviousValue 'AIRFLOW_DB_SCHEMA'
if (-not $schema) { $schema = 'geo_airflow' }
if (-not $supabaseUser -or -not $supabasePassword -or -not $supabaseHost) {
    Write-Warning "SUPABASE_DB_* not found in $envFile, $geoEtlEnv or $geoApiEnv — fill them in $envFile (template: .env.example) before docker compose"
}

$stamp = [DateTime]::UtcNow.ToString('yyyy-MM-ddTHH:mm:ssZ')
$content = @"
# Generated by .\setup.ps1 on $stamp from $projectDir
# Machine-specific values (paths, uid, Airflow UI login, Supabase credentials) — git-ignored.
# $fileComment

# Parent dir of the sibling projects (GEO_Download, GEO_E2E, GEO_ETL)
GEO_ROOT=$geoRoot
# Data folder bind-mounted into Airflow and Spark as /opt/geo/GEO_Download/data
GEO_DATA_DIR=$geoDataDir
$condaComment
CONDA_ROOT=$condaRoot
$uidComment
AIRFLOW_UID=$airflowUid

# Airflow UI login (airflow-init creates this admin user on startup)
_AIRFLOW_WWW_USER_USERNAME=$username
_AIRFLOW_WWW_USER_PASSWORD=$password

# Supabase PostgreSQL — Airflow's metadata DB (no local Postgres container)
SUPABASE_DB_USER=$supabaseUser
SUPABASE_DB_PASSWORD=$supabasePassword
SUPABASE_DB_HOST=$supabaseHost
SUPABASE_DB_PORT=$supabasePort
SUPABASE_DB_NAME=$supabaseName
# Schema of that database holding Airflow's metadata tables (created by airflow-init)
AIRFLOW_DB_SCHEMA=$schema
"@

$utf8NoBom = New-Object System.Text.UTF8Encoding($false)
[IO.File]::WriteAllText($envPath, $content, $utf8NoBom)

Write-Output "Wrote ${envPath}:"
$keyLine = '^(GEO_ROOT|CONDA_ROOT|AIRFLOW_UID|_AIRFLOW_WWW_USER_USERNAME|SUPABASE_DB_USER|SUPABASE_DB_HOST|SUPABASE_DB_PORT|AIRFLOW_DB_SCHEMA)='
Get-Content -LiteralPath $envPath | Where-Object { $_ -match $keyLine } | ForEach-Object { Write-Output "  $_" }
Write-Output ''
Sync-SiblingEnv (Get-SupabaseValueMap)
Write-Output ''
Write-Output $nextLine
