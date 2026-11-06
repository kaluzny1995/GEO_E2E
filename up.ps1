<#
Start the GEO stack only after the preflight checks pass.

The check verifies that all six repositories are present side by side and that
GEO_E2E/.env contains the five Supabase credentials. If something is missing, it
prints what to fix and exits before Docker starts any container.

Usage: .\up.ps1 [docker compose up options...]
  .\up.ps1 -d              # start in the background (normal use)
  .\up.ps1 -d --build      # rebuild changed images and start
#>
[CmdletBinding()]
param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]] $Rest
)

$ErrorActionPreference = 'Stop'
if ($null -eq $Rest) { $Rest = @() }

& (Join-Path $PSScriptRoot 'check.ps1')
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

& docker compose up @Rest
exit $LASTEXITCODE
