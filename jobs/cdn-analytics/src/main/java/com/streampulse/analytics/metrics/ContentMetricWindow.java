package com.streampulse.analytics.metrics;

import java.time.Instant;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class ContentMetricWindow extends ProcessWindowFunction<NodeMetricSnapshot, ContentMetric, ContentDimension, TimeWindow> {
    @Override
    public void process(ContentDimension key, Context context, Iterable<NodeMetricSnapshot> values,
                        Collector<ContentMetric> output) throws Exception {
        NodeMetricSnapshot snapshot = values.iterator().next();
        ContentMetric metric = new ContentMetric();
        metric.windowStart = Instant.ofEpochMilli(context.window().getStart()).toString();
        metric.windowEnd = Instant.ofEpochMilli(context.window().getEnd()).toString();
        metric.revision = WindowRevision.next(context);
        metric.contentId = key.contentId;
        metric.requests = snapshot.requests;
        metric.cacheHitRatio = WindowRevision.ratio(snapshot.cacheHits, snapshot.requests);
        metric.bytesSent = snapshot.bytesSent;
        metric.originMsTotal = snapshot.originMsTotal;
        output.collect(metric);
    }
}
