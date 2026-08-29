package com.streampulse.analytics.schema;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;

@JsonIgnoreProperties(ignoreUnknown = true)
public class DeliveryEvent {
    @JsonProperty("schema_version") public int schemaVersion;
    @JsonProperty("event_id") public String eventId;
    @JsonProperty("event_time") public String eventTime;
    @JsonProperty("ingest_time") public String ingestTime;
    @JsonProperty("request_id") public String requestId;
    @JsonProperty("content_id") public String contentId;
    @JsonProperty("node_id") public String nodeId;
    public String location;
    @JsonProperty("network_id") public String networkId;
    @JsonProperty("cache_status") public String cacheStatus;
    @JsonProperty("http_status") public int httpStatus;
    @JsonProperty("bytes_sent") public long bytesSent;
    @JsonProperty("ttfb_ms") public double ttfbMs;
    @JsonProperty("transfer_ms") public double transferMs;
    @JsonProperty("origin_ms") public double originMs;

    public long eventTimestampMillis() {
        return Instant.parse(eventTime).toEpochMilli();
    }

    public void validate() {
        require(eventId, "event_id");
        require(eventTime, "event_time");
        require(ingestTime, "ingest_time");
        require(requestId, "request_id");
        require(contentId, "content_id");
        require(nodeId, "node_id");
        require(location, "location");
        require(networkId, "network_id");
        require(cacheStatus, "cache_status");
        if (schemaVersion != 1) throw new IllegalArgumentException("INVALID_SCHEMA_VERSION");
        if (httpStatus < 100 || httpStatus > 599) throw new IllegalArgumentException("INVALID_HTTP_STATUS");
        if (bytesSent < 0 || ttfbMs < 0 || transferMs < 0 || originMs < 0) {
            throw new IllegalArgumentException("NEGATIVE_MEASUREMENT");
        }
        Instant event = Instant.parse(eventTime);
        if (Instant.parse(ingestTime).isBefore(event)) throw new IllegalArgumentException("INGEST_BEFORE_EVENT");
        if (!isCacheStatus(cacheStatus)) throw new IllegalArgumentException("INVALID_CACHE_STATUS");
    }

    private static boolean isCacheStatus(String value) {
        switch (value) {
            case "HIT": case "MISS": case "BYPASS": case "EXPIRED":
            case "STALE": case "UPDATING": case "REVALIDATED": case "UNKNOWN": return true;
            default: return false;
        }
    }

    private static void require(String value, String field) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException("MISSING_" + field.toUpperCase());
    }
}
