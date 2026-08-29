[CmdletBinding()]
param(
    [int]$HotIterations = 20,
    [string]$ClickHouseUrl = "http://localhost:8123/",
    [string]$User = "streampulse",
    [string]$Password = $(if ($env:CLICKHOUSE_PASSWORD) { $env:CLICKHOUSE_PASSWORD } else { "streampulse-local" }),
    [string]$OutputPath = "experiments/reports/clickhouse-grafana/query-benchmark.json"
)

$ErrorActionPreference = "Stop"

function Get-PercentileNearestRank {
    param([double[]]$Values, [double]$Percentile)

    $sorted = @($Values | Sort-Object)
    if ($sorted.Count -eq 0) {
        return 0.0
    }
    $index = [Math]::Max(0, [Math]::Ceiling($Percentile * $sorted.Count) - 1)
    return [double]$sorted[$index]
}

$basic = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes("${User}:${Password}"))
$headers = @{ Authorization = "Basic $basic" }

function Invoke-ClickHouseQuery {
    param([string]$Sql)

    $stopwatch = [Diagnostics.Stopwatch]::StartNew()
    $response = Invoke-WebRequest `
        -Uri $ClickHouseUrl `
        -Method Post `
        -Headers $headers `
        -ContentType "text/plain" `
        -Body $Sql `
        -TimeoutSec 30
    $stopwatch.Stop()

    $summary = $null
    $summaryHeader = $response.Headers["X-ClickHouse-Summary"]
    if ($summaryHeader) {
        $summary = $summaryHeader | ConvertFrom-Json
    }

    return [pscustomobject]@{
        elapsed_ms = [Math]::Round($stopwatch.Elapsed.TotalMilliseconds, 3)
        read_rows = if ($summary) { [uint64]$summary.read_rows } else { 0 }
        read_bytes = if ($summary) { [uint64]$summary.read_bytes } else { 0 }
    }
}

$queries = [ordered]@{
    node_quality_15m = @"
WITH (SELECT max(window_start) FROM streampulse.node_metrics_1m FINAL) AS anchor
SELECT
    node_id,
    sum(requests) AS requests,
    round(avg(ttfb_p95_ms), 3) AS p95_ttfb_ms,
    round(100 * avg(error_5xx_rate), 4) AS error_percent,
    round(100 * avg(cache_hit_ratio), 4) AS hit_percent
FROM streampulse.node_metrics_1m FINAL
WHERE window_start >= anchor - INTERVAL 15 MINUTE
GROUP BY node_id
ORDER BY node_id
FORMAT Null
"@
    network_anomalies_24h = @"
WITH (SELECT max(window_start) FROM streampulse.network_metrics_1m FINAL) AS anchor
SELECT
    window_start,
    location,
    network_id,
    ttfb_p95_ms,
    error_5xx_rate,
    cache_hit_ratio
FROM streampulse.network_metrics_1m FINAL
WHERE window_start >= anchor - INTERVAL 24 HOUR
  AND (ttfb_p95_ms >= 80 OR error_5xx_rate >= 0.05)
ORDER BY window_start, location, network_id
FORMAT Null
"@
    content_across_nodes = @"
WITH (
    SELECT content_id
    FROM streampulse.raw_delivery FINAL
    GROUP BY content_id
    ORDER BY count() DESC
    LIMIT 1
) AS target_content
SELECT
    content_id,
    node_id,
    count() AS requests,
    countIf(cache_status != 'HIT') AS origin_requests,
    sumIf(bytes_sent, cache_status != 'HIT') AS origin_bytes,
    round(avg(ttfb_ms), 3) AS avg_ttfb_ms
FROM streampulse.raw_delivery FINAL
WHERE content_id = target_content
GROUP BY content_id, node_id
ORDER BY node_id
FORMAT Null
"@
}

$cacheReset = [ordered]@{}
foreach ($command in @(
    "SYSTEM DROP MARK CACHE",
    "SYSTEM DROP UNCOMPRESSED CACHE",
    "SYSTEM DROP QUERY CONDITION CACHE",
    "SYSTEM DROP FILESYSTEM CACHE"
)) {
    try {
        Invoke-ClickHouseQuery -Sql $command | Out-Null
        $cacheReset[$command] = "ok"
    }
    catch {
        $cacheReset[$command] = "unsupported-or-denied"
    }
}

$results = [ordered]@{}
foreach ($entry in $queries.GetEnumerator()) {
    $cold = Invoke-ClickHouseQuery -Sql $entry.Value
    $hot = @()
    for ($i = 0; $i -lt $HotIterations; $i++) {
        $hot += Invoke-ClickHouseQuery -Sql $entry.Value
    }

    $durations = [double[]]@($hot | ForEach-Object elapsed_ms)
    $results[$entry.Key] = [ordered]@{
        cold_ms = $cold.elapsed_ms
        hot_iterations = $HotIterations
        hot_p50_ms = [Math]::Round((Get-PercentileNearestRank -Values $durations -Percentile 0.50), 3)
        hot_p95_ms = [Math]::Round((Get-PercentileNearestRank -Values $durations -Percentile 0.95), 3)
        read_rows = [uint64]($hot | Measure-Object -Property read_rows -Maximum).Maximum
        read_bytes = [uint64]($hot | Measure-Object -Property read_bytes -Maximum).Maximum
    }
}

$tableResponse = Invoke-RestMethod `
    -Uri $ClickHouseUrl `
    -Method Post `
    -Headers $headers `
    -ContentType "text/plain" `
    -Body "SELECT table, sum(rows) AS rows FROM system.parts WHERE database = 'streampulse' AND active GROUP BY table ORDER BY table FORMAT JSON" `
    -TimeoutSec 30
$tableRows = @($tableResponse.data)

$report = [ordered]@{
    measured_at_utc = (Get-Date).ToUniversalTime().ToString("o")
    clickhouse_url = $ClickHouseUrl
    cache_reset = $cacheReset
    table_rows = $tableRows
    queries = $results
    limitations = @(
        "Local single-node Docker benchmark; not a production capacity claim.",
        "Synthetic event time is anchored to each table's maximum timestamp.",
        "Cold measurement follows supported ClickHouse cache resets; OS page cache is not forcibly cleared.",
        "Elapsed time includes localhost HTTP client overhead."
    )
}

$parent = Split-Path -Parent $OutputPath
if ($parent) {
    New-Item -ItemType Directory -Force -Path $parent | Out-Null
}
$report | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $OutputPath -Encoding utf8
$report | ConvertTo-Json -Depth 8
