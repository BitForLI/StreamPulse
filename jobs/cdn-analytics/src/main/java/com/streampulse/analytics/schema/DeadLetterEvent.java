package com.streampulse.analytics.schema;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DeadLetterEvent {
    @JsonProperty("source_topic")
    public String sourceTopic;
    @JsonProperty("observed_at")
    public String observedAt;
    @JsonProperty("error_code")
    public String errorCode;
    @JsonProperty("error_message")
    public String errorMessage;
    @JsonProperty("payload_sha256")
    public String payloadSha256;
    @JsonProperty("payload_preview_redacted")
    public String payloadPreviewRedacted;

    public DeadLetterEvent() {}

    public DeadLetterEvent(String sourceTopic, String observedAt, String errorCode,
                           String errorMessage, String payloadSha256, String preview) {
        this.sourceTopic = sourceTopic;
        this.observedAt = observedAt;
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
        this.payloadSha256 = payloadSha256;
        this.payloadPreviewRedacted = preview;
    }
}
