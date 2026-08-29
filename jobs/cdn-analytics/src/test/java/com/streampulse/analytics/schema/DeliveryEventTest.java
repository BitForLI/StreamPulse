package com.streampulse.analytics.schema;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class DeliveryEventTest {
    @Test
    void unknownCacheStatusIsRejectedByFrozenV1Contract() {
        DeliveryEvent event = validEvent();
        event.cacheStatus = "NEW_UPSTREAM_VALUE";
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class, event::validate);
        org.junit.jupiter.api.Assertions.assertEquals("INVALID_CACHE_STATUS", error.getMessage());
    }

    @Test
    void negativeMeasurementsEnterValidationFailurePath() {
        DeliveryEvent event = validEvent();
        event.bytesSent = -1;
        assertThrows(IllegalArgumentException.class, event::validate);
    }

    @Test
    void ingestCannotPrecedeEventTime() {
        DeliveryEvent event = validEvent();
        event.ingestTime = "2026-08-26T23:59:59Z";
        assertThrows(IllegalArgumentException.class, event::validate);
    }

    private static DeliveryEvent validEvent() {
        DeliveryEvent event = new DeliveryEvent();
        event.schemaVersion = 1;
        event.eventId = "delivery-0001";
        event.eventTime = "2026-08-27T00:00:00Z";
        event.ingestTime = "2026-08-27T00:00:01Z";
        event.requestId = "request-0001";
        event.contentId = "content-1";
        event.nodeId = "edge-a";
        event.location = "au-sydney";
        event.networkId = "as-synthetic-1";
        event.cacheStatus = "HIT";
        event.httpStatus = 200;
        event.bytesSent = 100;
        event.ttfbMs = 20;
        event.transferMs = 30;
        return event;
    }
}
