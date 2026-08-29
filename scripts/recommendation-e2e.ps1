[CmdletBinding()]
param(
    [string]$ComposeFile = "compose.yaml",
    [string]$Scenario = "experiments/scenarios/recommendation-fault.yaml",
    [string]$Location = "au-sydney",
    [string]$Network = "as-synthetic-1221",
    [string]$OutputPath = "experiments/reports/recommendation-api/e2e-result.json"
)

$ErrorActionPreference = "Stop"
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$composePath = (Resolve-Path (Join-Path $repoRoot $ComposeFile)).Path
$scenarioPath = (Resolve-Path (Join-Path $repoRoot $Scenario)).Path
$go = (Resolve-Path (Join-Path $repoRoot ".tools/go/bin/go.exe")).Path
$linuxGenerator = Join-Path $repoRoot ".tmp/event-generator-recommendation-linux-amd64"

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

Push-Location $repoRoot
try {
    $env:GOPATH = Join-Path $repoRoot ".tmp/go-path"
    $env:GOCACHE = Join-Path $repoRoot ".tmp/go-build"
    $env:GOMODCACHE = Join-Path $repoRoot ".tmp/go-mod"
    $env:GOOS = "linux"
    $env:GOARCH = "amd64"
    $env:CGO_ENABLED = "0"
    & $go -C services/event-generator build -trimpath -o $linuxGenerator ./cmd/generator
    if ($LASTEXITCODE -ne 0) { throw "failed to build Linux event generator" }

    Invoke-Compose up -d --build recommendation-api
    $ready = Wait-HttpJSON -Uri "http://localhost:8090/readyz" -Attempts 45
    if ($ready.status -ne "ready") { throw "recommendation API is not ready" }

    $jobs = Invoke-RestMethod -Uri "http://localhost:8081/jobs/overview" -TimeoutSec 10
    $analytics = @($jobs.jobs | Where-Object { $_.name -eq "StreamPulse CDN Analytics v1" -and $_.state -eq "RUNNING" })
    if ($analytics.Count -ne 1) {
        throw "expected exactly one RUNNING StreamPulse CDN Analytics v1 job"
    }

    Invoke-Compose cp $linuxGenerator "kafka:/tmp/event-generator-recommendation"
    Invoke-Compose cp $scenarioPath "kafka:/tmp/recommendation-fault.yaml"
    Invoke-Compose exec -T -u 0 kafka chmod 0755 /tmp/event-generator-recommendation

    # The workload takes roughly two minutes to publish locally. Ending the
    # seven-minute simulation two minutes after launch keeps the final completed
    # windows fresh when evaluation starts without changing the API clock.
    $startTime = (Get-Date).ToUniversalTime().AddMinutes(-5).ToString("yyyy-MM-ddTHH:mm:ssZ")
    Invoke-Compose exec -T kafka /tmp/event-generator-recommendation `
        -config /tmp/recommendation-fault.yaml `
        -start-time $startTime `
        -output kafka `
        -brokers kafka:9092 `
        -manifest /tmp/recommendation-fault-manifest.json

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

    $evidence = [ordered]@{
        measured_at_utc = (Get-Date).ToUniversalTime().ToString("o")
        scenario = $Scenario
        start_time_override = $startTime
        flink_job_id = $analytics[0].jid
        evaluation = $evaluation
        clickhouse_persisted = $persisted
    }
    $absoluteOutput = Join-Path $repoRoot $OutputPath
    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $absoluteOutput) | Out-Null
    $evidence | ConvertTo-Json -Depth 12 | Set-Content -LiteralPath $absoluteOutput -Encoding utf8
    $evidence | ConvertTo-Json -Depth 12
}
finally {
    Pop-Location
}
