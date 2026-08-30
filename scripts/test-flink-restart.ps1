[CmdletBinding()]
param(
    [string]$ComposeFile = "compose.yaml",
    [string]$Scenario = "experiments/scenarios/clickhouse-catchup.yaml",
    [string]$OutputDir = "experiments/results/flink-restart",
    [string]$StartTime = "2026-09-04T00:00:00Z",
    [int64]$Seed = 20260905,
    [int64]$FlushSeed = 20260906,
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
$generator = Join-Path $repoRoot ".tmp/event-generator-restart-linux-amd64"
$taskManagerStopped = $false

if (Test-Path (Join-Path $outputPath "result.json")) {
    throw "Refusing to overwrite completed evidence at $outputPath; choose another -OutputDir"
}
New-Item -ItemType Directory -Force -Path $outputPath | Out-Null

$authBytes = [Text.Encoding]::ASCII.GetBytes("${ClickHouseUser}:${ClickHousePassword}")
$headers = @{ Authorization = "Basic $([Convert]::ToBase64String($authBytes))" }

function Write-UTF8NoBOM {
    param([string]$Path, [string]$Content)
    [System.IO.File]::WriteAllText($Path, $Content, [System.Text.UTF8Encoding]::new($false))
}

function Get-RepoRelativePath {
    param([string]$Path)
    $fullPath = [System.IO.Path]::GetFullPath($Path)
    if (-not $fullPath.StartsWith($repoRoot + [System.IO.Path]::DirectorySeparatorChar)) {
        throw "path is outside repository: $fullPath"
    }
    return $fullPath.Substring($repoRoot.Length + 1).Replace("\", "/")
}

function Invoke-Compose {
    param([string[]]$Arguments)
    $output = & docker compose -f $composePath @Arguments
    if ($LASTEXITCODE -ne 0) { throw "docker compose failed: $($Arguments -join ' ')" }
    if ($null -ne $output) { $output | Out-Host }
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
    $sourceLag = 0L
    foreach ($line in $raw) {
        $columns = @($line -split "\s+" | Where-Object { $_ })
        if ($columns.Count -lt 6 -or $columns[5] -notmatch "^\d+$") { continue }
        if ($columns[0] -like "streampulse-clickhouse-*") {
            $clickHouseLag += [int64]$columns[5]
        }
        if ($columns[0] -eq "streampulse-cdn-analytics-v1") {
            $sourceLag += [int64]$columns[5]
        }
    }
    return [pscustomobject]@{ source_lag = $sourceLag; clickhouse_lag = $clickHouseLag; raw = @($raw) }
}

function Wait-LagZero {
    param([switch]$IncludeClickHouse, [int]$Attempts = 90)
    for ($attempt = 1; $attempt -le $Attempts; $attempt++) {
        $snapshot = Get-LagSnapshot
        if ($snapshot.source_lag -eq 0 -and
            (-not $IncludeClickHouse -or $snapshot.clickhouse_lag -eq 0)) {
            return $snapshot
        }
        Start-Sleep -Seconds 2
    }
    throw "Kafka lag did not return to zero"
}

function Get-AnalyticsJob {
    $overview = Invoke-RestMethod -Uri "http://localhost:8081/jobs/overview" -TimeoutSec 10
    $jobs = @($overview.jobs | Where-Object {
        $_.name -eq "StreamPulse CDN Analytics v1" -and
        $_.state -notin @("FAILED", "CANCELED", "FINISHED")
    })
    if ($jobs.Count -ne 1) { throw "expected exactly one active StreamPulse analytics job" }
    return $jobs[0]
}

function Get-JobDetail {
    param([string]$JobID)
    return Invoke-RestMethod -Uri "http://localhost:8081/jobs/$JobID" -TimeoutSec 10
}

function Get-Checkpoints {
    param([string]$JobID)
    return Invoke-RestMethod -Uri "http://localhost:8081/jobs/$JobID/checkpoints" -TimeoutSec 10
}

function Wait-NewCheckpoint {
    param([string]$JobID, [int64]$AfterID, [int]$Attempts = 30)
    for ($attempt = 1; $attempt -le $Attempts; $attempt++) {
        $checkpoints = Get-Checkpoints $JobID
        if ($null -ne $checkpoints.latest.completed -and
            [int64]$checkpoints.latest.completed.id -gt $AfterID) {
            return $checkpoints
        }
        Start-Sleep -Seconds 2
    }
    throw "no new completed checkpoint after $AfterID"
}

function Invoke-Generator {
    param([int64]$RunSeed, [string]$RunStart, [string]$ManifestName, [string]$Commit)
    $containerManifest = "/tmp/$ManifestName"
    $arguments = @(
        "exec", "-T",
        "-e", "FLINK_VERSION=1.16.0",
        "-e", "KAFKA_VERSION=3.9.0",
        "-e", "CLICKHOUSE_VERSION=26.7.3.19",
        "kafka", "/tmp/event-generator-restart",
        "-config", "/tmp/flink-restart.yaml",
        "-seed", [string]$RunSeed,
        "-start-time", $RunStart,
        "-git-commit", $Commit,
        "-output", "kafka",
        "-brokers", "kafka:9092",
        "-manifest", $containerManifest
    )
    Invoke-Compose -Arguments $arguments
    $localManifest = Join-Path $outputPath $ManifestName
    Invoke-Compose -Arguments @("cp", "kafka:$containerManifest", $localManifest)
    return $localManifest
}

Push-Location $repoRoot
try {
    $job = Get-AnalyticsJob
    if ($job.state -ne "RUNNING") { throw "analytics job is not RUNNING" }
    $jobID = [string]$job.jid
    $detailBefore = Get-JobDetail $jobID
    if ([int]$detailBefore.'status-counts'.RUNNING -ne 6) { throw "expected 6 RUNNING vertices" }

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
    Invoke-Compose -Arguments @("cp", $generator, "kafka:/tmp/event-generator-restart")
    Invoke-Compose -Arguments @("cp", $scenarioPath, "kafka:/tmp/flink-restart.yaml")
    Invoke-Compose -Arguments @("exec", "-T", "-u", "0", "kafka", "chmod", "0755", "/tmp/event-generator-restart")

    $start = [DateTimeOffset]::Parse($StartTime).UtcDateTime
    $end = $start.AddMinutes(5).AddSeconds(10)
    $startSql = $start.ToString("yyyy-MM-dd HH:mm:ss.fff")
    $endSql = $end.ToString("yyyy-MM-dd HH:mm:ss.fff")
    $preexisting = Invoke-ClickHouseJSON @"
SELECT count() AS rows FROM streampulse.raw_delivery FINAL
WHERE event_time >= parseDateTime64BestEffort('$startSql')
  AND event_time < parseDateTime64BestEffort('$endSql')
"@
    if ([int64]$preexisting.rows -ne 0) { throw "restart range already contains rows" }

    $initialManifest = Invoke-Generator $Seed $StartTime "initial-manifest.json" $commit
    $initialLag = Wait-LagZero -IncludeClickHouse
    Write-UTF8NoBOM (Join-Path $outputPath "lag-after-initial.txt") (($initialLag.raw -join "`n") + "`n")
    $checkpointBeforeRun = Get-Checkpoints $jobID
    $checkpointBeforeID = [int64]$checkpointBeforeRun.latest.completed.id
    $checkpointBefore = Wait-NewCheckpoint $jobID $checkpointBeforeID
    Write-UTF8NoBOM (Join-Path $outputPath "checkpoint-before.json") (($checkpointBefore | ConvertTo-Json -Depth 8) + "`n")

    $stoppedAt = (Get-Date).ToUniversalTime()
    Invoke-Compose -Arguments @("stop", "taskmanager")
    $taskManagerStopped = $true
    Start-Sleep -Seconds 6
    $detailDuring = Get-JobDetail $jobID
    if ([int]$detailDuring.'status-counts'.RUNNING -eq 6) {
        throw "job never exposed a non-running vertex state during TaskManager outage"
    }
    Write-UTF8NoBOM (Join-Path $outputPath "job-during-outage.json") (($detailDuring | ConvertTo-Json -Depth 8) + "`n")

    $replayManifest = Invoke-Generator $Seed $StartTime "replay-manifest.json" $commit
    $outageLag = Get-LagSnapshot
    if ([int64]$outageLag.source_lag -le 0) { throw "source lag did not grow during TaskManager outage" }
    Write-UTF8NoBOM (Join-Path $outputPath "lag-during-outage.txt") (($outageLag.raw -join "`n") + "`n")

    $startedAt = (Get-Date).ToUniversalTime()
    Invoke-Compose -Arguments @("start", "taskmanager")
    $taskManagerStopped = $false
    $recoveredDetail = $null
    for ($attempt = 1; $attempt -le 60; $attempt++) {
        $candidate = Get-JobDetail $jobID
        if ($candidate.state -eq "RUNNING" -and [int]$candidate.'status-counts'.RUNNING -eq 6) {
            $recoveredDetail = $candidate
            break
        }
        Start-Sleep -Seconds 1
    }
    if ($null -eq $recoveredDetail) { throw "analytics job did not recover all six vertices" }
    $recoveredAt = (Get-Date).ToUniversalTime()
    $recoveredJob = Get-AnalyticsJob
    if ([string]$recoveredJob.jid -ne $jobID) { throw "job ID changed across TaskManager recovery" }
    $lagAfterRecovery = Wait-LagZero -IncludeClickHouse
    Write-UTF8NoBOM (Join-Path $outputPath "lag-after-recovery.txt") (($lagAfterRecovery.raw -join "`n") + "`n")
    Write-UTF8NoBOM (Join-Path $outputPath "job-after-recovery.json") (($recoveredDetail | ConvertTo-Json -Depth 8) + "`n")

    $afterRecoveryCheckpoint = Wait-NewCheckpoint $jobID ([int64]$checkpointBefore.latest.completed.id)
    if ($null -eq $afterRecoveryCheckpoint.latest.restored) {
        throw "Flink did not report restoration from a completed checkpoint"
    }
    Write-UTF8NoBOM (Join-Path $outputPath "checkpoint-after.json") (($afterRecoveryCheckpoint | ConvertTo-Json -Depth 8) + "`n")

    $flushStart = $start.AddMinutes(3).ToString("yyyy-MM-ddTHH:mm:ssZ")
    $flushManifest = Invoke-Generator $FlushSeed $flushStart "flush-manifest.json" $commit
    $finalLag = Wait-LagZero -IncludeClickHouse
    Write-UTF8NoBOM (Join-Path $outputPath "lag-final.txt") (($finalLag.raw -join "`n") + "`n")

    $minuteStart = $start.AddMinutes(1).ToString("yyyy-MM-dd HH:mm:ss.fff")
    $minuteEnd = $start.AddMinutes(2).ToString("yyyy-MM-dd HH:mm:ss.fff")
    $consistency = $null
    for ($attempt = 1; $attempt -le 45; $attempt++) {
        $consistency = Invoke-ClickHouseJSON @"
SELECT
  (SELECT count() FROM streampulse.raw_delivery FINAL
   WHERE event_time >= parseDateTime64BestEffort('$minuteStart')
     AND event_time < parseDateTime64BestEffort('$minuteEnd')) AS unique_delivery,
  (SELECT sum(requests) FROM streampulse.node_metrics_1m FINAL
   WHERE window_start = parseDateTime64BestEffort('$minuteStart')) AS node_requests,
  (SELECT sum(requests) FROM streampulse.network_metrics_1m FINAL
   WHERE window_start = parseDateTime64BestEffort('$minuteStart')) AS network_requests,
  (SELECT count() FROM streampulse.node_metrics_1m FINAL
   WHERE window_start = parseDateTime64BestEffort('$minuteStart')) AS node_rows,
  (SELECT count() FROM streampulse.network_metrics_1m FINAL
   WHERE window_start = parseDateTime64BestEffort('$minuteStart')) AS network_rows
"@
        if ([int64]$consistency.unique_delivery -gt 0 -and
            [int64]$consistency.node_requests -eq [int64]$consistency.unique_delivery -and
            [int64]$consistency.network_requests -eq [int64]$consistency.unique_delivery) {
            break
        }
        Start-Sleep -Seconds 2
    }
    if ([int64]$consistency.node_requests -ne [int64]$consistency.unique_delivery -or
        [int64]$consistency.network_requests -ne [int64]$consistency.unique_delivery) {
        throw "post-restart aggregates contain a missing or duplicate request"
    }

    $initialHash = (Get-FileHash -LiteralPath $initialManifest -Algorithm SHA256).Hash.ToLowerInvariant()
    $replayHash = (Get-FileHash -LiteralPath $replayManifest -Algorithm SHA256).Hash.ToLowerInvariant()
    if ($initialHash -ne $replayHash) { throw "replay manifest is not byte-identical to initial manifest" }

    $result = [ordered]@{
        generated_at_utc = (Get-Date).ToUniversalTime().ToString("o")
        job_id_before = $jobID
        job_id_after = [string]$recoveredJob.jid
        vertices_before = [int]$detailBefore.'status-counts'.RUNNING
        vertices_during_outage = [int]$detailDuring.'status-counts'.RUNNING
        vertices_after = [int]$recoveredDetail.'status-counts'.RUNNING
        checkpoint_before = [int64]$checkpointBefore.latest.completed.id
        checkpoint_after = [int64]$afterRecoveryCheckpoint.latest.completed.id
        checkpoint_state_size_bytes = [int64]$afterRecoveryCheckpoint.latest.completed.state_size
        restored_checkpoint_id = [int64]$afterRecoveryCheckpoint.latest.restored.id
        restored_checkpoint_path = [string]$afterRecoveryCheckpoint.latest.restored.external_path
        taskmanager_stop_seconds = [math]::Round(($startedAt - $stoppedAt).TotalSeconds, 3)
        taskmanager_start_to_vertices_running_seconds = [math]::Round(($recoveredAt - $startedAt).TotalSeconds, 3)
        peak_observed_source_lag = [int64]$outageLag.source_lag
        final_source_lag = [int64]$finalLag.source_lag
        initial_manifest_sha256 = $initialHash
        replay_manifest_sha256 = $replayHash
        exact_replay_confirmed = ($initialHash -eq $replayHash)
        consistency_window_start = $minuteStart
        consistency = $consistency
        aggregate_equality_passed = $true
        manifests = @(
            (Get-RepoRelativePath $initialManifest),
            (Get-RepoRelativePath $replayManifest),
            (Get-RepoRelativePath $flushManifest)
        )
        limitations = @(
            "Local single-TaskManager synthetic test only.",
            "The equality check covers one completed node/network minute and does not establish production recovery capacity.",
            "Raw ClickHouse tables use FINAL unique event IDs as the comparison oracle."
        )
    }
    Write-UTF8NoBOM (Join-Path $outputPath "result.json") (($result | ConvertTo-Json -Depth 8) + "`n")
    $report = @(
        "# Flink TaskManager restart and exact replay",
        "",
        "The only TaskManager was stopped after a completed checkpoint. The exact same generator run was replayed while it was down, then the TaskManager was restarted.",
        "",
        "- Job ID remained: $jobID",
        "- Vertices: $($result.vertices_before) running -> $($result.vertices_during_outage) during outage -> $($result.vertices_after) running",
        "- Checkpoint: $($result.checkpoint_before) -> $($result.checkpoint_after); post-restore checkpoint state size $($result.checkpoint_state_size_bytes) bytes",
        "- Flink reported restored checkpoint: $($result.restored_checkpoint_id)",
        "- Peak observed delivery-source lag: $($result.peak_observed_source_lag); final lag: $($result.final_source_lag)",
        "- TaskManager start to 6/6 vertices RUNNING: $($result.taskmanager_start_to_vertices_running_seconds) seconds",
        "- Exact replay manifests SHA-256 matched: $($result.exact_replay_confirmed)",
        "- Completed minute unique delivery/node/network requests: $($consistency.unique_delivery)/$($consistency.node_requests)/$($consistency.network_requests)",
        "",
        "The equality is scoped to a local synthetic minute and is not a production availability claim."
    )
    Write-UTF8NoBOM (Join-Path $outputPath "report.md") (($report -join "`n") + "`n")
}
finally {
    if ($taskManagerStopped) {
        & docker compose -f $composePath start taskmanager | Out-Null
    }
    Pop-Location
}
