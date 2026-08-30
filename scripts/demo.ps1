[CmdletBinding()]
param([string]$ComposeFile = "compose.yaml")

$ErrorActionPreference = "Stop"
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$composePath = (Resolve-Path (Join-Path $repoRoot $ComposeFile)).Path
$e2eScript = (Resolve-Path (Join-Path $repoRoot "scripts/recommendation-e2e.ps1")).Path
$topicScript = (Resolve-Path (Join-Path $repoRoot "infra/kafka/create-topics.sh")).Path
$flinkBaseUri = "http://localhost:8081"

function Invoke-Compose {
    param([Parameter(ValueFromRemainingArguments = $true)][string[]]$Arguments)
    & docker compose -f $composePath @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "docker compose failed: $($Arguments -join ' ')"
    }
}

function Wait-Endpoint {
    param([string]$Uri, [int]$Attempts = 60)
    for ($attempt = 1; $attempt -le $Attempts; $attempt++) {
        try {
            Invoke-RestMethod -Uri $Uri -TimeoutSec 5 | Out-Null
            return
        }
        catch {
            if ($attempt -eq $Attempts) { throw }
            Start-Sleep -Seconds 2
        }
    }
}

Push-Location $repoRoot
try {
    Invoke-Compose -Arguments @(
        "up", "-d", "--wait", "jobmanager", "taskmanager", "kafka",
        "clickhouse", "grafana"
    )
    Wait-Endpoint -Uri "$flinkBaseUri/overview"

    Invoke-Compose -Arguments @("cp", $topicScript, "kafka:/tmp/streampulse-create-topics.sh")
    Invoke-Compose -Arguments @("exec", "-T", "kafka", "sh", "/tmp/streampulse-create-topics.sh")

    $stamp = (Get-Date).ToUniversalTime().ToString("yyyyMMddHHmmss")
    $outputPath = ".tmp/demo/e2e-result-$stamp.json"
    & $e2eScript `
        -ComposeFile $ComposeFile `
        -OutputPath $outputPath `
        -E2EJobName "StreamPulse Five Minute Demo $stamp" `
        -E2EGroupId "streampulse-demo-$stamp" `
        -CompactOutput
    if ($LASTEXITCODE -ne 0) { throw "recommendation demo gate failed" }

    $result = Get-Content -Raw -LiteralPath (Join-Path $repoRoot $outputPath) | ConvertFrom-Json
    $recommendation = $result.evaluation.recommendation
    Write-Output ""
    Write-Output "StreamPulse demo passed."
    Write-Output "Recommendation: $($recommendation.recommendation_id)"
    Write-Output "Mode/TTL: $($recommendation.mode) / 120 seconds"
    Write-Output "Reason codes: $($recommendation.reason_codes -join ', ')"
    Write-Output "Evidence: $outputPath"
    Write-Output "Grafana: http://localhost:3000/d/streampulse-overview"
    Write-Output "Flink: http://localhost:8081"
    Write-Output "Recommendation API: http://localhost:8090/healthz"
}
finally {
    Pop-Location
}
