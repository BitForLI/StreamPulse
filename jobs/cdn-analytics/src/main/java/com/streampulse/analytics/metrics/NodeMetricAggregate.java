package com.streampulse.analytics.metrics;

import com.streampulse.analytics.schema.DeliveryEvent;
import org.apache.flink.api.common.functions.AggregateFunction;

public class NodeMetricAggregate implements AggregateFunction<DeliveryEvent, MetricAccumulator, NodeMetricSnapshot> {
    @Override public MetricAccumulator createAccumulator() { return new MetricAccumulator(); }
    @Override public MetricAccumulator add(DeliveryEvent event, MetricAccumulator accumulator) {
        accumulator.add(event);
        return accumulator;
    }
    @Override public NodeMetricSnapshot getResult(MetricAccumulator accumulator) {
        NodeMetricSnapshot result = new NodeMetricSnapshot();
        result.requests = accumulator.requests;
        result.errors5xx = accumulator.errors5xx;
        result.cacheHits = accumulator.cacheHits;
        result.bytesSent = accumulator.bytesSent;
        result.originMsTotal = accumulator.originMsTotal;
        result.ttfbP50Ms = accumulator.percentile(0.50);
        result.ttfbP95Ms = accumulator.percentile(0.95);
        result.ttfbP99Ms = accumulator.percentile(0.99);
        return result;
    }
    @Override public MetricAccumulator merge(MetricAccumulator left, MetricAccumulator right) { return left.merge(right); }
}
