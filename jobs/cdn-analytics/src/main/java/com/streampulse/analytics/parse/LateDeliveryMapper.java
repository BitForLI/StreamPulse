package com.streampulse.analytics.parse;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streampulse.analytics.schema.DeadLetterEvent;
import com.streampulse.analytics.schema.DeliveryEvent;
import java.time.Instant;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

public class LateDeliveryMapper extends RichMapFunction<DeliveryEvent, DeadLetterEvent> {
    private final String sourceTopic;
    private transient ObjectMapper mapper;

    public LateDeliveryMapper() {
        this("cdn.delivery.v1");
    }

    public LateDeliveryMapper(String sourceTopic) {
        this.sourceTopic = sourceTopic;
    }

    @Override public void open(Configuration parameters) { mapper = new ObjectMapper(); }

    @Override
    public DeadLetterEvent map(DeliveryEvent event) throws Exception {
        String raw = mapper.writeValueAsString(event);
        return new DeadLetterEvent(sourceTopic, Instant.now().toString(), "TOO_LATE",
                "event exceeded watermark and allowed lateness", DeliveryEventParser.sha256(raw),
                "redacted:event_id=" + event.eventId);
    }
}
