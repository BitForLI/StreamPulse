[CmdletBinding()]
param(
    [string]$ComposeFile = "compose.yaml",
    [string]$Scenario = "experiments/scenarios/clickhouse-catchup.yaml",
    [string]$OutputDir = "experiments/results/clickhouse-catchup",
    [string]$StartTime = "2026-09-02T00:00:00Z",
    [int64]$Seed = 20260903,
    [string]$ClickHouseUrl = "http://localhost:8123",
    [string]$ClickHouseUser = "streampulse",
    [string]$ClickHousePassword = "streampulse-local"
)

$ErrorActionPreference = "Stop"
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$composePath = (Resolve-Path (Join-Path $repoRoot $ComposeFile)).Path
$scenarioPath = (Resolve-Path (Join-Path $repoRoot $Scenario)).Path
$outputPath = [System.IO.Path]::GetFullPath((Join-Path $repoRoot $OutputDir))
$go = (Resolve-Path (Join-Path $repoRoot ".tools/go/bin/go.exe")).Path
$generator = Join-Path $repoRoot ".tmp/event-generator-catchup-linux-amd64"
$manifestPath = Join-Path $outputPath "manifest.json"
$containerManifest = "/tmp/clickhouse-catchup-manifest.json"

if (Test-Path (Join-Path $outputPath "result.json")) {
    throw "Refusing to overwrite completed evidence at $outputPath; choose another -OutputDir"
}
New-Item -ItemType Directory -Force -Path $outputPath | Out-Null

$authBytes = [Text.Encoding]::ASCII.GetBytes("${ClickHouseUser}:${ClickHousePassword}")
$headers = @{ Authorization = "Basic $([Convert]::ToBase64String($authBytes))" }
$clickHouseStopped = $false

function Get-RepoRelativePath {
    param([string]$Path)
    $fullPath = [System.IO.Path]::GetFullPath($Path)
    if (-not $fullPath.StartsWith($repoRoot + [System.IO.Path]::DirectorySeparatorChar)) {
        throw "path is outside repository: $fullPath"
    }
    return $fullPath.Substring($repoRoot.Length + 1).Replace("\", "/")
}

function Invoke-Compose {
    param([Parameter(ValueFromRemainingArguments = $true)][string[]]$Arguments)
    & docker compose -f $composePath @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "docker compose failed: $($Arguments -join ' ')"
    }
}

function Write-UTF8NoBOM {
    param([string]$Path, [string]$Content)
    [System.IO.File]::WriteAllText(
        $Path,
        $Content,
        [System.Text.UTF8Encoding]::new($false)
    )
}

function Invoke-ClickHouseJSON {
    param([string]$Query)
    $uri = "$ClickHouseUrl/?query=$([Uri]::EscapeDataString("$Query FORMAT JSONEachRow"))"
    return Invoke-RestMethod -Uri $uri -Headers $headers -TimeoutSec 15
}

function Get-LagSnapshot {
    $previousErrorAction = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    $raw = & docker compose -f $composePath exec -T kafka `
        /opt/kafka/bin/kafka-consumer-groups.sh `
        --bootstrap-server kafka:9092 --all-groups --describe 2>&1
    $exitCode = $LASTEXITCODE
    $ErrorActionPreference = $previousErrorAction
    if ($exitCode -ne 0) { throw "failed to read Kafka consumer lag" }
    $clickHouseLag = 0L
    $flinkLag = 0L
    foreach ($line in $raw) {
        $columns = @($line -split "\s+" | Where-Object { $_ })
        if ($columns.Count -lt 6 -or $columns[5] -notmatch "^\d+$") { continue }
        if ($columns[0] -like "streampulse-clickhouse-*") {
            $clickHouseLag += [int64]$columns[5]
        }
        if ($columns[0] -eq "streampulse-cdn-analytics-v1") {
            $flinkLag += [int64]$columns[5]
        }
    }
    return [pscustomobject]@{
        clickhouse_lag = $clickHouseLag
        flink_lag = $flinkLag
        raw = @($raw)
    }
}

function Wait-ClickHouse {
    for ($attempt = 1; $attempt -le 60; $attempt++) {
        try {
            $ping = Invoke-RestMethod -Uri "$ClickHouseUrl/ping" -Headers $headers -TimeoutSec 3
            if ([string]$ping -match "Ok") { return }
        }
        catch {}
        Start-Sleep -Seconds 2
    }
    throw "ClickHouse did not become healthy"
}

Push-Location $repoRoot
try {
    $jobs = Invoke-RestMethod -Uri "http://localhost:8081/jobs/overview" -TimeoutSec 10
    $analytics = @($jobs.jobs | Where-Object {
        $_.name -eq "StreamPulse CDN Analytics v1" -and $_.state -eq "RUNNING"
    })
    if ($analytics.Count -ne 1) {
        throw "expected exactly one RUNNING StreamPulse CDN Analytics v1 job"
    }

    $commit = (& git rev-parse HEAD).Trim()
    if ((& git status --porcelain).Count -gt 0) { $commit = "$commit-dirty" }
    $env:GOOS = "linux"
    $env:GOARCH = "amd64"
    $env:CGO_ENABLED = "0"
    $env:GOPATH = Join-Path $repoRoot ".tmp/go-path"
    $env:GOCACHE = Join-Path $repoRoot ".tmp/go-build"
    $env:GOMODCACHE = Join-Path $repoRoot ".tmp/go-mod"
    & $go -C services/event-generator build -trimpath -o $generator ./cmd/generator
    if ($LASTEXITCODE -ne 0) { throw "failed to build Linux event generator" }
    Invoke-Compose -Arguments @("cp", $generator, "kafka:/tmp/event-generator-catchup")
    Invoke-Compose -Arguments @("cp", $scenarioPath, "kafka:/tmp/clickhouse-catchup.yaml")
    Invoke-Compose -Arguments @("exec", "-T", "-u", "0", "kafka", "chmod", "0755", "/tmp/event-generator-catchup")

    $start = [DateTimeOffset]::Parse($StartTime).UtcDateTime
    $end = $start.AddMinutes(2).AddSeconds(10)
    $startSql = $start.ToString("yyyy-MM-dd HH:mm:ss.fff")
    $endSql = $end.ToString("yyyy-MM-dd HH:mm:ss.fff")
    $countsQuery = @"
SELECT
  (SELECT count() FROM streampulse.raw_delivery FINAL WHERE event_time >= parseDateTime64BestEffort('$startSql') AND event_time < parseDateTime64BestEffort('$endSql')) AS delivery,
  (SELECT count() FROM streampulse.raw_routing FINAL WHERE event_time >= parseDateTime64BestEffort('$startSql') AND event_time < parseDateTime64BestEffort('$endSql')) AS routing,
  (SELECT count() FROM streampulse.raw_player FINAL WHERE event_time >= parseDateTime64BestEffort('$startSql') AND event_time < parseDateTime64BestEffort('$endSql')) AS player,
  (SELECT count() FROM streampulse.node_metrics_1m FINAL WHERE window_start >= parseDateTime64BestEffort('$startSql') AND window_start < parseDateTime64BestEffort('$endSql')) AS node_metrics,
  (SELECT count() FROM streampulse.network_metrics_1m FINAL WHERE window_start >= parseDateTime64BestEffort('$startSql') AND window_start < parseDateTime64BestEffort('$endSql')) AS network_metrics
"@
    $before = Invoke-ClickHouseJSON $countsQuery
    if (([int64]$before.delivery + [int64]$before.routing + [int64]$before.player + [int64]$before.node_metrics + [int64]$before.network_metrics) -ne 0) {
        throw "catch-up time range already contains rows; choose another -StartTime/-Seed"
    }
    $preLag = Get-LagSnapshot
    Write-UTF8NoBOM (Join-Path $outputPath "lag-before.txt") (($preLag.raw -join "`n") + "`n")

    Invoke-Compose -Arguments @("stop", "clickhouse")
    $clickHouseStopped = $true
    $pausedAt = (Get-Date).ToUniversalTime()
    $generatorArguments = @(
        "exec", "-T",
        "-e", "FLINK_VERSION=1.16.0",
        "-e", "KAFKA_VERSION=3.9.0",
        "-e", "CLICKHOUSE_VERSION=26.7.3.19",
        "kafka", "/tmp/event-generator-catchup",
        "-config", "/tmp/clickhouse-catchup.yaml",
        "-seed", [string]$Seed,
        "-start-time", $StartTime,
        "-git-commit", $commit,
        "-output", "kafka",
        "-brokers", "kafka:9092",
        "-manifest", $containerManifest
    )
    Invoke-Compose -Arguments $generatorArguments
    Invoke-Compose -Arguments @("cp", "kafka:$containerManifest", $manifestPath)

    $pausedLag = $null
    for ($attempt = 1; $attempt -le 60; $attempt++) {
        $snapshot = Get-LagSnapshot
        if ($snapshot.flink_lag -eq 0 -and $snapshot.clickhouse_lag -gt 0) {
            $pausedLag = $snapshot
            break
        }
        Start-Sleep -Seconds 2
    }
    if ($null -eq $pausedLag) { throw "did not observe ClickHouse lag after Flink caught up" }
    Write-UTF8NoBOM (Join-Path $outputPath "lag-while-paused.txt") (($pausedLag.raw -join "`n") + "`n")

    $restartAt = (Get-Date).ToUniversalTime()
    Invoke-Compose -Arguments @("start", "clickhouse")
    $clickHouseStopped = $false
    Wait-ClickHouse
    $healthyAt = (Get-Date).ToUniversalTime()
    $postLag = $null
    for ($attempt = 1; $attempt -le 90; $attempt++) {
        $snapshot = Get-LagSnapshot
        if ($snapshot.clickhouse_lag -eq 0) {
            $postLag = $snapshot
            break
        }
        Start-Sleep -Seconds 2
    }
    if ($null -eq $postLag) { throw "ClickHouse consumer groups did not catch up" }
    $caughtUpAt = (Get-Date).ToUniversalTime()
    Write-UTF8NoBOM (Join-Path $outputPath "lag-after.txt") (($postLag.raw -join "`n") + "`n")
    $after = Invoke-ClickHouseJSON $countsQuery
    if ([int64]$after.delivery -le 0 -or [int64]$after.routing -le 0 -or
        [int64]$after.player -le 0 -or [int64]$after.node_metrics -le 0 -or
        [int64]$after.network_metrics -le 0) {
        throw "catch-up completed with missing target-table rows"
    }

    $result = [ordered]@{
        generated_at_utc = (Get-Date).ToUniversalTime().ToString("o")
        scenario = Get-RepoRelativePath $scenarioPath
        manifest = Get-RepoRelativePath $manifestPath
        seed = $Seed
        simulation_start_utc = $StartTime
        clickhouse_pause_seconds = [math]::Round(($restartAt - $pausedAt).TotalSeconds, 3)
        clickhouse_unavailable_seconds = [math]::Round(($healthyAt - $pausedAt).TotalSeconds, 3)
        catchup_after_restart_seconds = [math]::Round(($caughtUpAt - $restartAt).TotalSeconds, 3)
        total_stop_to_caught_up_seconds = [math]::Round(($caughtUpAt - $pausedAt).TotalSeconds, 3)
        observed_lag_while_paused = [int64]$pausedLag.clickhouse_lag
        final_clickhouse_lag = [int64]$postLag.clickhouse_lag
        rows_before = $before
        rows_after = $after
        limitations = @(
            "Local synthetic run only.",
            "Kafka lag reaching zero proves consumer catch-up, not production recovery capacity.",
            "No ClickHouse data was deleted or tables truncated."
        )
    }
    Write-UTF8NoBOM (Join-Path $outputPath "result.json") (($result | ConvertTo-Json -Depth 6) + "`n")
    $report = @(
        "# ClickHouse pause and catch-up",
        "",
        "ClickHouse was stopped before the workload and restarted after Flink input lag reached zero.",
        "",
        "- Observed ClickHouse consumer lag while paused: $($result.observed_lag_while_paused)",
        "- Final ClickHouse consumer lag: $($result.final_clickhouse_lag)",
        "- Intentional pause before restart: $($result.clickhouse_pause_seconds) seconds",
        "- Stop-to-healthy duration: $($result.clickhouse_unavailable_seconds) seconds",
        "- Restart-to-zero-lag catch-up: $($result.catchup_after_restart_seconds) seconds",
        "- Rows after catch-up: delivery=$($after.delivery), routing=$($after.routing), player=$($after.player), node=$($after.node_metrics), network=$($after.network_metrics)",
        "",
        "This local synthetic result does not establish production recovery capacity."
    )
    Write-UTF8NoBOM (Join-Path $outputPath "report.md") (($report -join "`n") + "`n")
}
finally {
    if ($clickHouseStopped) {
        & docker compose -f $composePath start clickhouse | Out-Null
    }
    Pop-Location
}
