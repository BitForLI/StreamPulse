[CmdletBinding()]
param(
    [string]$ComposeFile = "compose.yaml",
    [string]$BaseScenario = "experiments/scenarios/watermark-idle-base.yaml",
    [string]$ProbeScenario = "experiments/scenarios/watermark-probe.yaml",
    [string]$OutputDir = "experiments/results/watermark-lateness",
    [string]$Location = "au-watermark-test",
    [string]$Network = "as-synthetic-watermark",
    [string]$JobName = "StreamPulse Watermark Lateness E2E v1"
)

$ErrorActionPreference = "Stop"
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$composePath = (Resolve-Path (Join-Path $repoRoot $ComposeFile)).Path
$baseScenarioPath = (Resolve-Path (Join-Path $repoRoot $BaseScenario)).Path
$probeScenarioPath = (Resolve-Path (Join-Path $repoRoot $ProbeScenario)).Path
$outputPath = Join-Path $repoRoot $OutputDir
$go = (Resolve-Path (Join-Path $repoRoot ".tools/go/bin/go.exe")).Path
$maven = (Resolve-Path (Join-Path $repoRoot ".tools/apache-maven-3.9.16/bin/mvn.cmd")).Path
$generator = Join-Path $repoRoot ".tmp/event-generator-watermark-linux-amd64"
$analyticsJar = Join-Path $repoRoot "jobs/cdn-analytics/target/cdn-analytics-0.1.0-SNAPSHOT.jar"
$containerJar = "/tmp/streampulse-watermark-e2e.jar"
$flinkBaseUri = "http://localhost:8081"
$clickHousePassword = if ([string]::IsNullOrEmpty($env:CLICKHOUSE_PASSWORD)) { "streampulse-local" } else { $env:CLICKHOUSE_PASSWORD }

$runSuffix = (Get-Date).ToUniversalTime().ToString("yyyyMMddHHmmss")
$deliveryTopic = "cdn.delivery.watermark-test.$runSuffix.v1"
$deadLetterTopic = "cdn.dead-letter.watermark-test.$runSuffix.v1"
$groupId = "streampulse-watermark-e2e-$runSuffix"

function Invoke-Compose {
    param([Parameter(ValueFromRemainingArguments = $true)][string[]]$Arguments)
    & docker compose -f $composePath @Arguments
    if ($LASTEXITCODE -ne 0) { throw "docker compose failed: $($Arguments -join ' ')" }
}

function Wait-FlinkJob {
    param([string]$Name, [int]$Attempts = 90)
    for ($attempt = 1; $attempt -le $Attempts; $attempt++) {
        $overview = Invoke-RestMethod -Uri "$flinkBaseUri/jobs/overview" -TimeoutSec 10
        $matches = @($overview.jobs | Where-Object { $_.name -eq $Name -and $_.state -eq "RUNNING" })
        if ($matches.Count -eq 1) { return $matches[0] }
        if ($matches.Count -gt 1) { throw "multiple RUNNING jobs named '$Name'" }
        Start-Sleep -Seconds 2
    }
    throw "Flink job '$Name' did not become RUNNING"
}

function Stop-FlinkJob {
    param([string]$JobId)
    if (-not $JobId) { return }
    try {
        Invoke-RestMethod -Uri "$flinkBaseUri/jobs/${JobId}?mode=cancel" -Method Patch -TimeoutSec 10 | Out-Null
    }
    catch {
        $current = Invoke-RestMethod -Uri "$flinkBaseUri/jobs/$JobId" -TimeoutSec 10
        if ($current.state -notin @("CANCELED", "FAILED", "FINISHED")) { throw }
        return
    }
    for ($attempt = 1; $attempt -le 30; $attempt++) {
        $current = Invoke-RestMethod -Uri "$flinkBaseUri/jobs/$JobId" -TimeoutSec 10
        if ($current.state -in @("CANCELED", "FAILED", "FINISHED")) { return }
        Start-Sleep -Seconds 1
    }
    throw "Flink job $JobId did not stop"
}

function Wait-TaskManagers {
    param([int]$Expected, [int]$Attempts = 90)
    for ($attempt = 1; $attempt -le $Attempts; $attempt++) {
        $overview = Invoke-RestMethod -Uri "$flinkBaseUri/overview" -TimeoutSec 10
        if ([int]$overview.taskmanagers -eq $Expected) { return $overview }
        Start-Sleep -Seconds 2
    }
    throw "Flink cluster did not reach $Expected TaskManagers"
}

function Invoke-ClickHouseJSON {
    param([string]$Query, [hashtable]$Parameters = @{})
    $arguments = @(
        "exec", "-T", "clickhouse", "clickhouse-client",
        "--user", "streampulse", "--password", $clickHousePassword
    )
    foreach ($name in ($Parameters.Keys | Sort-Object)) {
        $arguments += "--param_$name"
        $arguments += [string]$Parameters[$name]
    }
    $arguments += "--query"
    $arguments += $Query
    $raw = (Invoke-Compose -Arguments $arguments) -join "`n"
    return $raw | ConvertFrom-Json
}

function Get-WindowSummary {
    param([string]$WindowStart)
    $query = @'
SELECT sum(requests) AS requests, max(revision) AS max_revision, count() AS node_rows
FROM streampulse.node_metrics_1m FINAL
WHERE location = {location:String} AND network_id = {network:String}
  AND window_start = parseDateTime64BestEffort({window_start:String}, 3, 'UTC')
FORMAT JSON
'@
    $result = Invoke-ClickHouseJSON -Query $query -Parameters @{
        location = $Location
        network = $Network
        window_start = $WindowStart
    }
    return $result.data[0]
}

function Wait-WindowSummary {
    param([string]$WindowStart, [int64]$ExpectedRequests, [int]$ExpectedRevision, [int]$Attempts = 60)
    for ($attempt = 1; $attempt -le $Attempts; $attempt++) {
        $summary = Get-WindowSummary -WindowStart $WindowStart
        if ([int64]$summary.requests -eq $ExpectedRequests -and [int]$summary.max_revision -eq $ExpectedRevision) {
            return $summary
        }
        Start-Sleep -Seconds 2
    }
    throw "window $WindowStart did not reach requests=$ExpectedRequests revision=$ExpectedRevision; last=$($summary | ConvertTo-Json -Compress)"
}

function Get-TopicOffsets {
    param([string]$Topic)
    $lines = @(Invoke-Compose -Arguments @(
        "exec", "-T", "kafka", "/opt/kafka/bin/kafka-get-offsets.sh",
        "--bootstrap-server", "kafka:9092", "--topic", $Topic
    ))
    $total = [int64]0
    foreach ($line in $lines) {
        if ($line -notmatch ':(\d+)$') { throw "unexpected Kafka offset line: $line" }
        $total += [int64]$Matches[1]
    }
    return [pscustomobject]@{ lines = $lines; total = $total }
}

function Wait-TopicOffset {
    param([string]$Topic, [int64]$Minimum, [int]$Attempts = 60)
    for ($attempt = 1; $attempt -le $Attempts; $attempt++) {
        $offsets = Get-TopicOffsets -Topic $Topic
        if ($offsets.total -ge $Minimum) { return $offsets }
        Start-Sleep -Seconds 2
    }
    throw "topic $Topic did not reach offset $Minimum"
}

function Run-Generator {
    param([string]$ConfigInContainer, [string]$StartTime, [int64]$Seed, [string]$ManifestName)
    Invoke-Compose -Arguments @(
        "exec", "-T", "kafka", "/tmp/event-generator-watermark",
        "-config", $ConfigInContainer,
        "-start-time", $StartTime,
        "-seed", [string]$Seed,
        "-delivery-topic", $deliveryTopic,
        "-output", "kafka",
        "-brokers", "kafka:9092",
        "-manifest", "/tmp/$ManifestName"
    )
}

Push-Location $repoRoot
$e2eJobId = $null
$scaledTaskManagers = $false
try {
    New-Item -ItemType Directory -Force -Path $outputPath | Out-Null
    $initialCluster = Invoke-RestMethod -Uri "$flinkBaseUri/overview" -TimeoutSec 10
    $initialTaskManagers = [int]$initialCluster.taskmanagers
    $mainBefore = Wait-FlinkJob -Name "StreamPulse CDN Analytics v1"

    & $maven -f jobs/cdn-analytics/pom.xml package
    if ($LASTEXITCODE -ne 0) { throw "failed to test and package analytics job" }
    $analyticsJar = (Resolve-Path $analyticsJar).Path

    $env:GOPATH = Join-Path $repoRoot ".tmp/go-path"
    $env:GOCACHE = Join-Path $repoRoot ".tmp/go-build"
    $env:GOMODCACHE = Join-Path $repoRoot ".tmp/go-mod"
    $env:GOOS = "linux"
    $env:GOARCH = "amd64"
    $env:CGO_ENABLED = "0"
    & $go -C services/event-generator build -trimpath -o $generator ./cmd/generator
    if ($LASTEXITCODE -ne 0) { throw "failed to build event generator" }

    Invoke-Compose -Arguments @("up", "-d", "--scale", "taskmanager=2", "taskmanager")
    $scaledTaskManagers = $initialTaskManagers -ne 2
    $scaledCluster = Wait-TaskManagers -Expected 2
    if ([int]$scaledCluster.'slots-total' -lt 4) { throw "scaled Flink cluster has fewer than four slots" }

    foreach ($topic in @($deliveryTopic, $deadLetterTopic)) {
        Invoke-Compose -Arguments @(
            "exec", "-T", "kafka", "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-server", "kafka:9092", "--create", "--if-not-exists",
            "--topic", $topic, "--partitions", "2", "--replication-factor", "1",
            "--config", "retention.ms=86400000"
        )
    }

    Invoke-Compose -Arguments @("cp", $analyticsJar, "jobmanager:$containerJar")
    Invoke-Compose -Arguments @("cp", $generator, "kafka:/tmp/event-generator-watermark")
    Invoke-Compose -Arguments @("cp", $baseScenarioPath, "kafka:/tmp/watermark-base.yaml")
    Invoke-Compose -Arguments @("cp", $probeScenarioPath, "kafka:/tmp/watermark-probe.yaml")
    Invoke-Compose -Arguments @("exec", "-T", "-u", "0", "kafka", "chmod", "0755", "/tmp/event-generator-watermark")

    Invoke-Compose -Arguments @(
        "exec", "-T",
        "-e", "KAFKA_GROUP_ID=$groupId",
        "-e", "KAFKA_STARTING_OFFSETS=latest",
        "-e", "KAFKA_DELIVERY_TOPIC=$deliveryTopic",
        "-e", "KAFKA_DEAD_LETTER_TOPIC=$deadLetterTopic",
        "-e", "FLINK_JOB_NAME=$JobName",
        "-e", "FLINK_PARALLELISM=2",
        "jobmanager", "flink", "run", "-d", $containerJar
    )
    $e2eJob = Wait-FlinkJob -Name $JobName
    $e2eJobId = $e2eJob.jid
    $jobDetails = Invoke-RestMethod -Uri "$flinkBaseUri/jobs/$e2eJobId" -TimeoutSec 10
    if (@($jobDetails.vertices | Where-Object { [int]$_.parallelism -ne 2 }).Count -ne 0) {
        throw "not every watermark E2E vertex has parallelism two"
    }

    $anchor = (Get-Date).ToUniversalTime().AddMinutes(-3)
    $anchor = [DateTimeOffset]::new($anchor.Year, $anchor.Month, $anchor.Day, $anchor.Hour, $anchor.Minute, 0, [TimeSpan]::Zero)
    $windowStart = $anchor.AddMinutes(1)
    $windowEnd = $anchor.AddMinutes(2)
    $windowStartText = $windowStart.ToString("yyyy-MM-ddTHH:mm:ssZ")

    Run-Generator -ConfigInContainer "/tmp/watermark-base.yaml" -StartTime $anchor.ToString("yyyy-MM-ddTHH:mm:ssZ") -Seed 20260910 -ManifestName "watermark-base-manifest.json"
    $flushSentAt = (Get-Date).ToUniversalTime()
    Run-Generator -ConfigInContainer "/tmp/watermark-probe.yaml" -StartTime $windowEnd.AddSeconds(12).ToString("yyyy-MM-ddTHH:mm:ssZ") -Seed 20260911 -ManifestName "watermark-flush-within-manifest.json"

    $initial = Wait-WindowSummary -WindowStart $windowStartText -ExpectedRequests 600 -ExpectedRevision 0 -Attempts 60
    $idleAdvanceSeconds = ((Get-Date).ToUniversalTime() - $flushSentAt).TotalSeconds
    $inputOffsets = Get-TopicOffsets -Topic $deliveryTopic
    if (@($inputOffsets.lines | Where-Object { $_ -match ':0$' }).Count -ne 1) {
        throw "expected exactly one idle Kafka partition: $($inputOffsets.lines -join ',')"
    }

    Run-Generator -ConfigInContainer "/tmp/watermark-probe.yaml" -StartTime $windowStart.AddSeconds(30).ToString("yyyy-MM-ddTHH:mm:ssZ") -Seed 20260912 -ManifestName "watermark-allowed-late-manifest.json"
    $afterAllowed = Wait-WindowSummary -WindowStart $windowStartText -ExpectedRequests 601 -ExpectedRevision 1 -Attempts 30

    Run-Generator -ConfigInContainer "/tmp/watermark-probe.yaml" -StartTime $windowEnd.AddSeconds(20).ToString("yyyy-MM-ddTHH:mm:ssZ") -Seed 20260913 -ManifestName "watermark-flush-expired-manifest.json"
    Run-Generator -ConfigInContainer "/tmp/watermark-probe.yaml" -StartTime $windowStart.AddSeconds(40).ToString("yyyy-MM-ddTHH:mm:ssZ") -Seed 20260914 -ManifestName "watermark-too-late-manifest.json"
    $dlqOffsets = Wait-TopicOffset -Topic $deadLetterTopic -Minimum 1 -Attempts 30
    $afterTooLate = Get-WindowSummary -WindowStart $windowStartText
    if ([int64]$afterTooLate.requests -ne 601 -or [int]$afterTooLate.max_revision -ne 1) {
        throw "too-late event changed finalized aggregate: $($afterTooLate | ConvertTo-Json -Compress)"
    }

    $dlqRaw = (Invoke-Compose -Arguments @(
        "exec", "-T", "kafka", "/opt/kafka/bin/kafka-console-consumer.sh",
        "--bootstrap-server", "kafka:9092", "--topic", $deadLetterTopic,
        "--from-beginning", "--max-messages", "1"
    )) -join "`n"
    $dlq = $dlqRaw | ConvertFrom-Json
    if ($dlq.error_code -ne "TOO_LATE" -or $dlq.source_topic -ne $deliveryTopic) {
        throw "unexpected isolated DLQ record: $dlqRaw"
    }

    foreach ($manifest in @(
        "watermark-base-manifest.json",
        "watermark-flush-within-manifest.json",
        "watermark-allowed-late-manifest.json",
        "watermark-flush-expired-manifest.json",
        "watermark-too-late-manifest.json"
    )) {
        Invoke-Compose -Arguments @("cp", "kafka:/tmp/$manifest", (Join-Path $outputPath $manifest))
    }
    Set-Content -LiteralPath (Join-Path $outputPath "too-late-dlq.json") -Value $dlqRaw -Encoding utf8

    $evidence = [ordered]@{
        measured_at_utc = (Get-Date).ToUniversalTime().ToString("o")
        flink_job_id = $e2eJobId
        flink_parallelism = 2
        main_job_id_before = $mainBefore.jid
        delivery_topic = $deliveryTopic
        dead_letter_topic = $deadLetterTopic
        delivery_offsets = $inputOffsets
        idle_partition_count = 1
        idle_timeout_config_seconds = 30
        seconds_from_flush_to_window_visible = [Math]::Round($idleAdvanceSeconds, 3)
        target_window_start = $windowStartText
        target_window_end = $windowEnd.ToString("yyyy-MM-ddTHH:mm:ssZ")
        initial_window = $initial
        after_allowed_late = $afterAllowed
        after_too_late = $afterTooLate
        dlq_offsets = $dlqOffsets
        dlq_record = $dlq
    }
    $evidence | ConvertTo-Json -Depth 10 | Set-Content -LiteralPath (Join-Path $outputPath "result.json") -Encoding utf8
}
finally {
    if ($e2eJobId) {
        try { Stop-FlinkJob -JobId $e2eJobId }
        catch { Write-Warning "failed to stop watermark E2E job: $($_.Exception.Message)" }
    }
    if ($scaledTaskManagers) {
        try {
            Invoke-Compose -Arguments @("up", "-d", "--scale", "taskmanager=$initialTaskManagers", "taskmanager")
            Wait-TaskManagers -Expected $initialTaskManagers | Out-Null
            $mainAfter = Wait-FlinkJob -Name "StreamPulse CDN Analytics v1"
            if ($mainBefore -and $mainAfter.jid -ne $mainBefore.jid) {
                Write-Warning "main StreamPulse job ID changed during TaskManager scale-down"
            }
            Wait-FlinkJob -Name "Click Event Count" | Out-Null
        }
        catch { Write-Warning "failed to restore initial TaskManager count: $($_.Exception.Message)" }
    }
    Pop-Location
}

Get-Content -Raw -LiteralPath (Join-Path $outputPath "result.json")
