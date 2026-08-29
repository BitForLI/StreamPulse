package com.streampulse.analytics.metrics;

import java.time.Instant;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class NetworkMetricWindow extends ProcessWindowFunction<NodeMetricSnapshot, NetworkMetric, NetworkDimension, TimeWindow> {
    @Override
    public void process(NetworkDimension key, Context context, Iterable<NodeMetricSnapshot> values,
                        Collector<NetworkMetric> output) throws Exception {
        NodeMetricSnapshot snapshot = values.iterator().next();
        NetworkMetric metric = new NetworkMetric();
        metric.windowStart = Instant.ofEpochMilli(context.window().getStart()).toString();
        metric.windowEnd = Instant.ofEpochMilli(context.window().getEnd()).toString();
        metric.revision = WindowRevision.next(context);
        metric.location = key.location;
        metric.networkId = key.networkId;
        metric.requests = snapshot.requests;
        metric.error5xxRate = WindowRevision.ratio(snapshot.errors5xx, snapshot.requests);
        metric.cacheHitRatio = WindowRevision.ratio(snapshot.cacheHits, snapshot.requests);
        metric.bytesSent = snapshot.bytesSent;
        metric.originMsTotal = snapshot.originMsTotal;
        metric.ttfbP50Ms = snapshot.ttfbP50Ms;
        metric.ttfbP95Ms = snapshot.ttfbP95Ms;
        metric.ttfbP99Ms = snapshot.ttfbP99Ms;
        output.collect(metric);
    }
}
