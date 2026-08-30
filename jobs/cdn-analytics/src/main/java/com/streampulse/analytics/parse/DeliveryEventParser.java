package com.streampulse.analytics.parse;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streampulse.analytics.schema.DeadLetterEvent;
import com.streampulse.analytics.schema.DeliveryEvent;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class DeliveryEventParser extends ProcessFunction<String, DeliveryEvent> {
    public static final OutputTag<DeadLetterEvent> DEAD_LETTER =
            new OutputTag<DeadLetterEvent>("delivery-dead-letter") {};
    private final String sourceTopic;
    private transient ObjectMapper mapper;

    public DeliveryEventParser() {
        this("cdn.delivery.v1");
    }

    public DeliveryEventParser(String sourceTopic) {
        this.sourceTopic = sourceTopic;
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) {
        mapper = new ObjectMapper();
    }

    @Override
    public void processElement(String raw, Context context, Collector<DeliveryEvent> output) {
        try {
            DeliveryEvent event = mapper.readValue(raw, DeliveryEvent.class);
            event.validate();
            output.collect(event);
        } catch (Exception error) {
            context.output(DEAD_LETTER, new DeadLetterEvent(
                    sourceTopic, Instant.now().toString(), classify(error), safeMessage(error),
                    sha256(raw), redact(raw)));
        }
    }

    private static String classify(Exception error) {
        if (error instanceof IllegalArgumentException && error.getMessage() != null) return error.getMessage();
        return "INVALID_JSON";
    }

    private static String safeMessage(Exception error) {
        if (error instanceof IllegalArgumentException && error.getMessage() != null
                && error.getMessage().matches("[A-Z0-9_]{3,64}")) {
            return error.getMessage();
        }
        // Parser exception messages can echo input; never copy them into the DLQ.
        return error.getClass().getSimpleName();
    }

    static String redact(String raw) {
        return "redacted:length=" + raw.length();
    }

    static String sha256(String raw) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(raw.getBytes(StandardCharsets.UTF_8));
            StringBuilder result = new StringBuilder(64);
            for (byte value : digest) result.append(String.format("%02x", value));
            return result.toString();
        } catch (Exception impossible) {
            throw new IllegalStateException(impossible);
        }
    }
}
