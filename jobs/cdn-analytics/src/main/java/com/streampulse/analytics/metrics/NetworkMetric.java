package com.streampulse.analytics.metrics;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;

public class NetworkMetric implements Serializable {
    @JsonProperty("schema_version") public int schemaVersion = 1;
    @JsonProperty("window_start") public String windowStart;
    @JsonProperty("window_end") public String windowEnd;
    public int revision;
    public String location;
    @JsonProperty("network_id") public String networkId;
    public long requests;
    @JsonProperty("error_5xx_rate") public double error5xxRate;
    @JsonProperty("cache_hit_ratio") public double cacheHitRatio;
    @JsonProperty("bytes_sent") public long bytesSent;
    @JsonProperty("origin_ms_total") public double originMsTotal;
    @JsonProperty("ttfb_p50_ms") public double ttfbP50Ms;
    @JsonProperty("ttfb_p95_ms") public double ttfbP95Ms;
    @JsonProperty("ttfb_p99_ms") public double ttfbP99Ms;
}
