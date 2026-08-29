[CmdletBinding()]
param(
    [string]$ComposeFile = "compose.yaml",
    [string]$Scenario = "experiments/scenarios/detector-comparison.yaml",
    [string]$OutputDir = "experiments/results/detector-comparison",
    [int[]]$Seeds = @(20260831, 20260901, 20260902),
    [string[]]$StartTimes = @(
        "2026-08-30T00:00:00Z",
        "2026-08-31T00:00:00Z",
        "2026-09-01T00:00:00Z"
    ),
    [string]$ClickHouseUrl = "http://localhost:8123",
    [string]$ClickHouseUser = "streampulse",
    [string]$ClickHousePassword = "streampulse-local"
)

$ErrorActionPreference = "Stop"
if ($Seeds.Count -ne $StartTimes.Count -or $Seeds.Count -lt 3) {
    throw "Seeds and StartTimes must have the same length and contain at least three runs"
}

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$composePath = (Resolve-Path (Join-Path $repoRoot $ComposeFile)).Path
$scenarioPath = (Resolve-Path (Join-Path $repoRoot $Scenario)).Path
$outputPath = [System.IO.Path]::GetFullPath((Join-Path $repoRoot $OutputDir))
$go = (Resolve-Path (Join-Path $repoRoot ".tools/go/bin/go.exe")).Path
$generator = Join-Path $repoRoot ".tmp/event-generator-detector-linux-amd64"

function Get-RepoRelativePath {
    param([string]$Path)
    $fullPath = [System.IO.Path]::GetFullPath($Path)
    if (-not $fullPath.StartsWith($repoRoot + [System.IO.Path]::DirectorySeparatorChar)) {
        throw "path is outside repository: $fullPath"
    }
    return $fullPath.Substring($repoRoot.Length + 1).Replace("\", "/")
}

if (Test-Path (Join-Path $outputPath "summary.json")) {
    throw "Refusing to overwrite completed evidence at $outputPath; choose another -OutputDir"
}
New-Item -ItemType Directory -Force -Path $outputPath | Out-Null

$authBytes = [Text.Encoding]::ASCII.GetBytes("${ClickHouseUser}:${ClickHousePassword}")
$headers = @{ Authorization = "Basic $([Convert]::ToBase64String($authBytes))" }

function Invoke-Compose {
    param([Parameter(ValueFromRemainingArguments = $true)][string[]]$Arguments)
    & docker compose -f $composePath @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "docker compose failed: $($Arguments -join ' ')"
    }
}

function Get-ClickHouseUri {
    param([string]$Query)
    return "$ClickHouseUrl/?query=$([Uri]::EscapeDataString($Query))"
}

function Invoke-ClickHouseJSON {
    param([string]$Query)
    return Invoke-RestMethod -Uri (Get-ClickHouseUri "$Query FORMAT JSONEachRow") -Headers $headers -TimeoutSec 15
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
    if ((& git status --porcelain).Count -gt 0) {
        $commit = "$commit-dirty"
    }

    $env:GOOS = "linux"
    $env:GOARCH = "amd64"
    $env:CGO_ENABLED = "0"
    $env:GOPATH = Join-Path $repoRoot ".tmp/go-path"
    $env:GOCACHE = Join-Path $repoRoot ".tmp/go-build"
    $env:GOMODCACHE = Join-Path $repoRoot ".tmp/go-mod"
    & $go -C services/event-generator build -trimpath -o $generator ./cmd/generator
    if ($LASTEXITCODE -ne 0) { throw "failed to build Linux event generator" }

    Invoke-Compose cp $generator "kafka:/tmp/event-generator-detector"
    Invoke-Compose cp $scenarioPath "kafka:/tmp/detector-comparison.yaml"
    Invoke-Compose exec -T -u 0 kafka chmod 0755 /tmp/event-generator-detector

    $evaluationArgs = @("scripts/evaluate_detectors.py")
    for ($index = 0; $index -lt $Seeds.Count; $index++) {
        $ordinal = $index + 1
        $runName = "run-{0:d2}-seed-{1}" -f $ordinal, $Seeds[$index]
        $runPath = Join-Path $outputPath $runName
        New-Item -ItemType Directory -Force -Path $runPath | Out-Null
        $manifestPath = Join-Path $runPath "manifest.json"
        $metricsPath = Join-Path $runPath "node-metrics.jsonl"
        $containerManifest = "/tmp/$runName-manifest.json"

        if (-not (Test-Path -LiteralPath $manifestPath)) {
            $generatorArguments = @(
                "exec", "-T",
                "-e", "FLINK_VERSION=1.16.0",
                "-e", "KAFKA_VERSION=3.9.0",
                "-e", "CLICKHOUSE_VERSION=26.7.3.19",
                "-e", "GRAFANA_VERSION=13.1.3",
                "kafka", "/tmp/event-generator-detector",
                "-config", "/tmp/detector-comparison.yaml",
                "-seed", [string]$Seeds[$index],
                "-start-time", $StartTimes[$index],
                "-git-commit", $commit,
                "-output", "kafka",
                "-brokers", "kafka:9092",
                "-manifest", $containerManifest
            )
            Invoke-Compose @generatorArguments
            Invoke-Compose cp "kafka:$containerManifest" $manifestPath
        }

        $manifest = Get-Content -LiteralPath $manifestPath -Raw | ConvertFrom-Json
        if ([int64]$manifest.seed -ne [int64]$Seeds[$index] -or
            $manifest.simulation_start_utc -ne $StartTimes[$index]) {
            throw "$runName manifest does not match the requested seed/start time"
        }
        $start = [DateTimeOffset]::Parse($manifest.simulation_start_utc).UtcDateTime
        $end = [DateTimeOffset]::Parse($manifest.simulation_end_utc).UtcDateTime
        $completedThrough = $start.AddMinutes(13)
        $startSql = $start.ToString("yyyy-MM-dd HH:mm:ss.fff")
        $endSql = $end.ToString("yyyy-MM-dd HH:mm:ss.fff")
        $ready = $false
        for ($attempt = 1; $attempt -le 60; $attempt++) {
            $statsQuery = @"
SELECT count() AS rows, max(window_end) AS max_window_end
FROM streampulse.node_metrics_1m FINAL
WHERE window_start >= parseDateTime64BestEffort('$startSql')
  AND window_start < parseDateTime64BestEffort('$endSql')
"@
            $stats = Invoke-ClickHouseJSON $statsQuery
            $maxWindowEnd = [DateTime]::SpecifyKind(
                [DateTime]::ParseExact(
                    $stats.max_window_end,
                    "yyyy-MM-dd HH:mm:ss.fff",
                    [Globalization.CultureInfo]::InvariantCulture
                ),
                [DateTimeKind]::Utc
            )
            if ([int]$stats.rows -ge 156 -and $maxWindowEnd -ge $completedThrough) {
                $ready = $true
                break
            }
            Start-Sleep -Seconds 2
        }
        if (-not $ready) {
            throw "$runName did not expose 13 complete node-metric windows in ClickHouse"
        }

        $exportQuery = @"
SELECT window_start, window_end, location, network_id, node_id, requests,
       error_5xx_rate, cache_hit_ratio, ttfb_p95_ms
FROM streampulse.node_metrics_1m FINAL
WHERE window_start >= parseDateTime64BestEffort('$startSql')
  AND window_start < parseDateTime64BestEffort('$endSql')
ORDER BY window_start, location, network_id, node_id
FORMAT JSONEachRow
"@
        Invoke-WebRequest -Uri (Get-ClickHouseUri $exportQuery) -Headers $headers `
            -TimeoutSec 30 -OutFile $metricsPath
        $evaluationArgs += @(
            "--run",
            $runName,
            (Get-RepoRelativePath $metricsPath),
            (Get-RepoRelativePath $scenarioPath),
            (Get-RepoRelativePath $manifestPath)
        )
    }

    $evaluationArgs += @(
        "--output-dir", (Get-RepoRelativePath $outputPath)
    )
    & python @evaluationArgs
    if ($LASTEXITCODE -ne 0) { throw "detector evaluation failed" }
}
finally {
    Pop-Location
}
