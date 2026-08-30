[CmdletBinding()]
param(
    [string]$ComposeFile = "compose.yaml",
    [string]$Scenario = "experiments/scenarios/recommendation-fault.yaml",
    [string]$Location = "au-sydney",
    [string]$Network = "as-synthetic-1221",
    [string]$OutputPath = "experiments/reports/recommendation-api/e2e-result.json",
    [string]$E2EJobName = "StreamPulse Recommendation E2E Analytics v1",
    [string]$E2EGroupId = "streampulse-recommendation-e2e-v1",
    [switch]$CompactOutput
)

$ErrorActionPreference = "Stop"
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$composePath = (Resolve-Path (Join-Path $repoRoot $ComposeFile)).Path
$scenarioPath = (Resolve-Path (Join-Path $repoRoot $Scenario)).Path

function Resolve-DevelopmentTool {
    param([string]$RepositoryPath, [string]$CommandName)
    $localPath = Join-Path $repoRoot $RepositoryPath
    if (Test-Path -LiteralPath $localPath) {
        return (Resolve-Path -LiteralPath $localPath).Path
    }
    $command = Get-Command $CommandName -ErrorAction SilentlyContinue
    if (-not $command) {
        throw "required tool '$CommandName' was not found in .tools or PATH"
    }
    return $command.Source
}

$go = Resolve-DevelopmentTool -RepositoryPath ".tools/go/bin/go.exe" -CommandName "go"
$maven = Resolve-DevelopmentTool -RepositoryPath ".tools/apache-maven-3.9.16/bin/mvn.cmd" -CommandName "mvn"
$migrationScript = (Resolve-Path (Join-Path $repoRoot "scripts/apply-clickhouse-migrations.ps1")).Path
$linuxGenerator = Join-Path $repoRoot ".tmp/event-generator-recommendation-linux-amd64"
$mavenRepository = Join-Path $repoRoot ".tmp/maven-repository"
$analyticsJar = Join-Path $repoRoot "jobs/cdn-analytics/target/cdn-analytics-0.1.0-SNAPSHOT.jar"
$containerAnalyticsJar = "/tmp/streampulse-recommendation-e2e.jar"
$flinkBaseUri = "http://localhost:8081"

function Invoke-Compose {
    param([Parameter(ValueFromRemainingArguments = $true)][string[]]$Arguments)
    & docker compose -f $composePath @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "docker compose failed: $($Arguments -join ' ')"
    }
}

function Wait-HttpJSON {
    param([string]$Uri, [int]$Attempts = 30, [int]$DelaySeconds = 2)
    for ($attempt = 1; $attempt -le $Attempts; $attempt++) {
        try {
            return Invoke-RestMethod -Uri $Uri -TimeoutSec 5
        }
        catch {
            if ($attempt -eq $Attempts) { throw }
            Start-Sleep -Seconds $DelaySeconds
        }
    }
}

function Wait-FlinkJob {
    param(
        [string]$Name,
        [string[]]$States = @("RUNNING"),
        [int]$Attempts = 60,
        [int]$DelaySeconds = 2
    )
    for ($attempt = 1; $attempt -le $Attempts; $attempt++) {
        $overview = Invoke-RestMethod -Uri "$flinkBaseUri/jobs/overview" -TimeoutSec 10
        $matches = @($overview.jobs | Where-Object { $_.name -eq $Name -and $States -contains $_.state })
        if ($matches.Count -eq 1) { return $matches[0] }
        if ($matches.Count -gt 1) { throw "multiple Flink jobs named '$Name' are in states $($States -join ',')" }
        Start-Sleep -Seconds $DelaySeconds
    }
    throw "Flink job '$Name' did not reach $($States -join ',')"
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

function Get-KafkaTopicOffsets {
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

Push-Location $repoRoot
$e2eJobId = $null
$suspendedClickCount = $false
try {
    New-Item -ItemType Directory -Force -Path $mavenRepository | Out-Null
    & $maven "-Dmaven.repo.local=$mavenRepository" -f jobs/cdn-analytics/pom.xml package
    if ($LASTEXITCODE -ne 0) { throw "failed to test and package Flink analytics job" }
    $analyticsJar = (Resolve-Path $analyticsJar).Path

    $env:GOPATH = Join-Path $repoRoot ".tmp/go-path"
    $env:GOCACHE = Join-Path $repoRoot ".tmp/go-build"
    $env:GOMODCACHE = Join-Path $repoRoot ".tmp/go-mod"
    $env:GOOS = "linux"
    $env:GOARCH = "amd64"
    $env:CGO_ENABLED = "0"
    & $go -C services/event-generator build -trimpath -o $linuxGenerator ./cmd/generator
    if ($LASTEXITCODE -ne 0) { throw "failed to build Linux event generator" }

    Invoke-Compose -Arguments @("up", "-d", "--wait", "clickhouse")
    & $migrationScript -ComposeFile $ComposeFile
    if ($LASTEXITCODE -ne 0) { throw "failed to apply ClickHouse migrations" }
    Invoke-Compose -Arguments @("up", "-d", "--build", "recommendation-api")
    $ready = Wait-HttpJSON -Uri "http://localhost:8090/readyz" -Attempts 45
    if ($ready.status -ne "ready") { throw "recommendation API is not ready" }

    $jobs = Invoke-RestMethod -Uri "$flinkBaseUri/jobs/overview" -TimeoutSec 10
    $staleE2E = @($jobs.jobs | Where-Object { $_.name -eq $E2EJobName -and $_.state -eq "RUNNING" })
    if ($staleE2E.Count -gt 0) {
        throw "an existing '$E2EJobName' job is already running; refusing to reuse ambiguous state"
    }

    $cluster = Invoke-RestMethod -Uri "$flinkBaseUri/overview" -TimeoutSec 10
    if ([int]$cluster.'slots-available' -lt 1) {
        $clickCount = @($jobs.jobs | Where-Object { $_.name -eq "Click Event Count" -and $_.state -eq "RUNNING" })
        if ($clickCount.Count -ne 1) {
            throw "no free Flink slot and no single optional Click Event Count job can be suspended"
        }
        Stop-FlinkJob -JobId $clickCount[0].jid
        $suspendedClickCount = $true
    }

    Invoke-Compose -Arguments @("cp", $analyticsJar, "jobmanager:$containerAnalyticsJar")
    Invoke-Compose -Arguments @(
        "exec", "-T",
        "-e", "KAFKA_GROUP_ID=$E2EGroupId",
        "-e", "KAFKA_STARTING_OFFSETS=latest",
        "-e", "FLINK_JOB_NAME=$E2EJobName",
        "jobmanager", "flink", "run", "-d", $containerAnalyticsJar
    )
    $e2eJob = Wait-FlinkJob -Name $E2EJobName
    $e2eJobId = $e2eJob.jid

    $recommendationOffsetsBefore = Get-KafkaTopicOffsets -Topic "cdn.recommendations.v1"

    Invoke-Compose -Arguments @("cp", $linuxGenerator, "kafka:/tmp/event-generator-recommendation")
    Invoke-Compose -Arguments @("cp", $scenarioPath, "kafka:/tmp/recommendation-fault.yaml")
    Invoke-Compose -Arguments @("exec", "-T", "-u", "0", "kafka", "chmod", "0755", "/tmp/event-generator-recommendation")

    # The workload takes roughly two minutes to publish locally. Ending the
    # seven-minute simulation two minutes after launch keeps the final completed
    # windows fresh when evaluation starts without changing the API clock.
    $startTime = (Get-Date).ToUniversalTime().AddMinutes(-5).ToString("yyyy-MM-ddTHH:mm:ssZ")
    Invoke-Compose -Arguments @(
        "exec", "-T", "kafka", "/tmp/event-generator-recommendation",
        "-config", "/tmp/recommendation-fault.yaml",
        "-start-time", $startTime,
        "-output", "kafka",
        "-brokers", "kafka:9092",
        "-manifest", "/tmp/recommendation-fault-manifest.json"
    )

    $body = @{
        scope = @{ location = $Location; network_id = $Network }
        current_weights = @{
            "edge-syd-a" = 1.0 / 3
            "edge-syd-b" = 1.0 / 3
            "edge-mel-a" = 1.0 / 3
        }
    } | ConvertTo-Json -Depth 5

    $evaluation = $null
    for ($attempt = 1; $attempt -le 30; $attempt++) {
        $evaluation = Invoke-RestMethod `
            -Uri "http://localhost:8090/v1/recommendations/evaluate" `
            -Method Post `
            -ContentType "application/json" `
            -Body $body `
            -TimeoutSec 10
        if ($evaluation.generated -or $evaluation.reason_codes -notcontains "NO_ANOMALY") { break }
        Start-Sleep -Seconds 2
    }
    if (-not $evaluation.generated) {
        throw "fault scenario produced no recommendation: $($evaluation.reason_codes -join ',')"
    }

    $recommendation = $evaluation.recommendation
    if ($recommendation.mode -ne "shadow") { throw "recommendation is not shadow-only" }
    $created = [DateTimeOffset]::Parse($recommendation.created_at)
    $validUntil = [DateTimeOffset]::Parse($recommendation.valid_until)
    if (($validUntil - $created).TotalSeconds -ne 120) { throw "recommendation TTL is not 120 seconds" }
    foreach ($property in $recommendation.proposed.PSObject.Properties) {
        $current = [double]$recommendation.current.($property.Name)
        if ([Math]::Abs([double]$property.Value - $current) -gt 0.200000001) {
            throw "weight step exceeded for $($property.Name)"
        }
    }

    $latestUri = "http://localhost:8090/v1/scopes/$Location/$Network/recommendations/latest"
    $persisted = $null
    for ($attempt = 1; $attempt -le 30; $attempt++) {
        try {
            $candidate = Invoke-RestMethod -Uri $latestUri -TimeoutSec 10
            if ($candidate.recommendation_id -eq $recommendation.recommendation_id) {
                $persisted = $candidate
                break
            }
        }
        catch {
            # The Kafka Engine materialized view may still be catching up.
        }
        Start-Sleep -Seconds 2
    }
    if (-not $persisted) { throw "recommendation did not reach ClickHouse" }
    if ($persisted.config_hash -ne $recommendation.config_hash -or
        $persisted.query_version -ne $recommendation.query_version -or
        -not $persisted.evidence) {
        throw "ClickHouse recommendation is missing audit evidence/query/config fields"
    }

    $ackBody = @{
        actor = "streampulse-e2e"
        status = "observed"
    } | ConvertTo-Json
    Invoke-RestMethod `
        -Uri "http://localhost:8090/v1/recommendations/$($recommendation.recommendation_id)/ack" `
        -Method Post -ContentType "application/json" -Body $ackBody -TimeoutSec 10 | Out-Null

    $outcomeBody = @{
        observed_delta = @{
            p95_ttfb_delta_ms = 0
            error_rate_delta = 0
            cost_units_delta = 0
        }
        notes = "synthetic persistence check only; no causal outcome claimed"
    } | ConvertTo-Json -Depth 4
    Invoke-RestMethod `
        -Uri "http://localhost:8090/v1/recommendations/$($recommendation.recommendation_id)/outcome" `
        -Method Post -ContentType "application/json" -Body $outcomeBody -TimeoutSec 10 | Out-Null

    $clickHousePassword = if ([string]::IsNullOrEmpty($env:CLICKHOUSE_PASSWORD)) { "streampulse-local" } else { $env:CLICKHOUSE_PASSWORD }
    $auditQuery = @'
SELECT
    (SELECT count() FROM streampulse.recommendation_acknowledgements
     WHERE recommendation_id = {recommendation_id:String}) AS acknowledgement_count,
    (SELECT count() FROM streampulse.recommendation_outcomes
     WHERE recommendation_id = {recommendation_id:String}) AS outcome_count
FORMAT JSON
'@
    $auditJSON = (Invoke-Compose -Arguments @(
        "exec", "-T", "clickhouse", "clickhouse-client",
        "--user", "streampulse", "--password", $clickHousePassword,
        "--param_recommendation_id", $recommendation.recommendation_id,
        "--query", $auditQuery
    )) -join "`n"
    $auditPersistence = $auditJSON | ConvertFrom-Json
    $auditRow = $auditPersistence.data[0]
    if ([int64]$auditRow.acknowledgement_count -lt 1 -or [int64]$auditRow.outcome_count -lt 1) {
        throw "ack/outcome did not persist in ClickHouse"
    }

    $recommendationOffsetsAfter = Get-KafkaTopicOffsets -Topic "cdn.recommendations.v1"
    if ($recommendationOffsetsAfter.total -le $recommendationOffsetsBefore.total) {
        throw "Kafka recommendation topic offsets did not advance"
    }

    $absoluteOutput = Join-Path $repoRoot $OutputPath
    $outputDirectory = Split-Path -Parent $absoluteOutput
    New-Item -ItemType Directory -Force -Path $outputDirectory | Out-Null
    $manifestPath = Join-Path $outputDirectory "fault-manifest.json"
    Invoke-Compose -Arguments @("cp", "kafka:/tmp/recommendation-fault-manifest.json", $manifestPath)
    $manifestSha256 = (Get-FileHash -LiteralPath $manifestPath -Algorithm SHA256).Hash.ToLowerInvariant()

    $evidence = [ordered]@{
        measured_at_utc = (Get-Date).ToUniversalTime().ToString("o")
        scenario = $Scenario
        start_time_override = $startTime
        flink_job = [ordered]@{
            id = $e2eJobId
            name = $E2EJobName
            kafka_group_id = $E2EGroupId
            starting_offsets = "latest"
        }
        kafka_recommendation_offsets = [ordered]@{
            before = $recommendationOffsetsBefore
            after = $recommendationOffsetsAfter
            delta = $recommendationOffsetsAfter.total - $recommendationOffsetsBefore.total
        }
        generator_manifest_sha256 = $manifestSha256
        evaluation = $evaluation
        clickhouse_persisted = $persisted
        audit_persisted = $auditRow
    }
    $evidence | ConvertTo-Json -Depth 12 | Set-Content -LiteralPath $absoluteOutput -Encoding utf8
    if ($CompactOutput) {
        Write-Output "Recommendation E2E passed: $($recommendation.recommendation_id)"
    }
    else {
        $evidence | ConvertTo-Json -Depth 12
    }
}
finally {
    if ($e2eJobId) {
        try { Stop-FlinkJob -JobId $e2eJobId }
        catch { Write-Warning "failed to stop isolated recommendation E2E job: $($_.Exception.Message)" }
    }
    if ($suspendedClickCount) {
        try {
            Invoke-Compose -Arguments @("run", "--rm", "client")
            Wait-FlinkJob -Name "Click Event Count" | Out-Null
        }
        catch { Write-Warning "failed to restore optional Click Event Count job: $($_.Exception.Message)" }
    }
    Pop-Location
}
