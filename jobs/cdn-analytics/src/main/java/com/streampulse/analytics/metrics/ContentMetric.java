package com.streampulse.analytics.metrics;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;

public class ContentMetric implements Serializable {
    @JsonProperty("schema_version") public int schemaVersion = 1;
    @JsonProperty("window_start") public String windowStart;
    @JsonProperty("window_end") public String windowEnd;
    public int revision;
    @JsonProperty("content_id") public String contentId;
    public long requests;
    @JsonProperty("cache_hit_ratio") public double cacheHitRatio;
    @JsonProperty("bytes_sent") public long bytesSent;
    @JsonProperty("origin_ms_total") public double originMsTotal;
}
