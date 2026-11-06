<#
Runs the GEO E2E preflight check before `docker compose up`:
  * all six GEO repositories are checked out side by side
  * GEO_E2E/.env carries the five Supabase credentials

`up.ps1` calls this automatically. Run it alone with:
  .\check.ps1

Usage: .\check.ps1 [preflight.py options]
#>
[CmdletBinding()]
param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]] $Rest
)

$ErrorActionPreference = 'Stop'
if ($null -eq $Rest) { $Rest = @() }

$python = Get-Command python -ErrorAction SilentlyContinue
$pythonArgs = @()
if (-not $python) {
    $python = Get-Command python3 -ErrorAction SilentlyContinue
}
if (-not $python) {
    $py = Get-Command py -ErrorAction SilentlyContinue
    if ($py) {
        $python = $py
        $pythonArgs += '-3'
    }
}
if (-not $python) {
    [Console]::Error.WriteLine('error: Python 3 is required for the preflight check (python/python3/py not found)')
    exit 2
}

$preflight = Join-Path $PSScriptRoot 'preflight.py'
& $python.Source @pythonArgs $preflight @Rest
exit $LASTEXITCODE
