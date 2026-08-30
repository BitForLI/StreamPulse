[CmdletBinding()]
param(
    [string]$ComposeFile = "compose.yaml",
    [string]$Migration = "infra/clickhouse/init/003_recommendation_audit.sql"
)

$ErrorActionPreference = "Stop"
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$composePath = (Resolve-Path (Join-Path $repoRoot $ComposeFile)).Path
$migrationPath = (Resolve-Path (Join-Path $repoRoot $Migration)).Path
$sql = Get-Content -Raw -LiteralPath $migrationPath
$password = if ([string]::IsNullOrEmpty($env:CLICKHOUSE_PASSWORD)) { "streampulse-local" } else { $env:CLICKHOUSE_PASSWORD }

$sql | & docker compose -f $composePath exec -T clickhouse clickhouse-client `
    --multiquery `
    --user streampulse `
    --password $password
if ($LASTEXITCODE -ne 0) {
    throw "ClickHouse migration failed: $Migration"
}

Write-Output "Applied $Migration"
