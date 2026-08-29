package com.streampulse.analytics.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.streampulse.analytics.schema.DeliveryEvent;
import org.junit.jupiter.api.Test;

class NodeMetricAggregateTest {
    @Test
    void handCalculatedMetricsMatch() {
        NodeMetricAggregate aggregate = new NodeMetricAggregate();
        MetricAccumulator accumulator = aggregate.createAccumulator();
        for (int value = 1; value <= 20; value++) {
            DeliveryEvent event = new DeliveryEvent();
            event.ttfbMs = value;
            event.httpStatus = value <= 2 ? 503 : 200;
            event.cacheStatus = value <= 15 ? "HIT" : "MISS";
            event.bytesSent = 100;
            event.originMs = "MISS".equals(event.cacheStatus) ? 25 : 0;
            aggregate.add(event, accumulator);
        }

        NodeMetricSnapshot result = aggregate.getResult(accumulator);
        assertEquals(20, result.requests);
        assertEquals(2, result.errors5xx);
        assertEquals(15, result.cacheHits);
        assertEquals(2000, result.bytesSent);
        assertEquals(125, result.originMsTotal);
        assertEquals(10, result.ttfbP50Ms);
        assertEquals(19, result.ttfbP95Ms);
        assertEquals(20, result.ttfbP99Ms);
    }

    @Test
    void mergedAccumulatorsPreserveSamplesAndCounts() {
        MetricAccumulator left = new MetricAccumulator();
        MetricAccumulator right = new MetricAccumulator();
        DeliveryEvent first = event(10);
        DeliveryEvent second = event(30);
        left.add(first);
        right.add(second);
        left.merge(right);
        assertEquals(2, left.requests);
        assertEquals(10, left.percentile(0.50));
        assertEquals(30, left.percentile(0.95));
    }

    private static DeliveryEvent event(double ttfb) {
        DeliveryEvent event = new DeliveryEvent();
        event.ttfbMs = ttfb;
        event.httpStatus = 200;
        event.cacheStatus = "HIT";
        return event;
    }
}
