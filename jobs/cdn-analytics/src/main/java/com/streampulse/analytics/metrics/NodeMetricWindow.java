package com.streampulse.analytics.metrics;

import java.time.Instant;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class NodeMetricWindow extends ProcessWindowFunction<NodeMetricSnapshot, NodeMetric, NodeDimension, TimeWindow> {
    @Override
    public void process(NodeDimension key, Context context, Iterable<NodeMetricSnapshot> values,
                        Collector<NodeMetric> output) throws Exception {
        NodeMetricSnapshot snapshot = values.iterator().next();
        ValueState<Integer> revisionState = context.windowState().getState(
                new ValueStateDescriptor<>("revision", Integer.class));
        Integer prior = revisionState.value();
        int revision = prior == null ? 0 : prior + 1;
        revisionState.update(revision);

        NodeMetric metric = new NodeMetric();
        metric.windowStart = Instant.ofEpochMilli(context.window().getStart()).toString();
        metric.windowEnd = Instant.ofEpochMilli(context.window().getEnd()).toString();
        metric.revision = revision;
        metric.location = key.location;
        metric.networkId = key.networkId;
        metric.nodeId = key.nodeId;
        metric.requests = snapshot.requests;
        metric.error5xxRate = ratio(snapshot.errors5xx, snapshot.requests);
        metric.cacheHitRatio = ratio(snapshot.cacheHits, snapshot.requests);
        metric.bytesSent = snapshot.bytesSent;
        metric.originMsTotal = snapshot.originMsTotal;
        metric.ttfbP50Ms = snapshot.ttfbP50Ms;
        metric.ttfbP95Ms = snapshot.ttfbP95Ms;
        metric.ttfbP99Ms = snapshot.ttfbP99Ms;
        output.collect(metric);
    }

    private static double ratio(long numerator, long denominator) {
        return denominator == 0 ? 0 : (double) numerator / denominator;
    }
}
