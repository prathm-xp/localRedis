# Run every quality gate, in the order that fails fastest.
#
#   powershell -File scripts/check.ps1            everything
#   powershell -File scripts/check.ps1 -Fast      skip the slow tests
param([switch]$Fast)

$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

Write-Host "==> ruff format --check"
python -m ruff format --check .
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "==> ruff check"
python -m ruff check .
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "==> mypy"
python -m mypy
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "==> pytest"
if ($Fast) { python -m pytest -q -m "not slow" } else { python -m pytest -q }
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host ""
Write-Host "All checks passed."
