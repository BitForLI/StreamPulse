package com.streampulse.analytics.metrics;

import com.streampulse.analytics.schema.DeliveryEvent;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MetricAccumulator implements Serializable {
    public long requests;
    public long errors5xx;
    public long cacheHits;
    public long bytesSent;
    public double originMsTotal;
    public List<Double> ttfbSamples = new ArrayList<>();

    public void add(DeliveryEvent event) {
        requests++;
        if (event.httpStatus >= 500) errors5xx++;
        if ("HIT".equals(event.cacheStatus)) cacheHits++;
        bytesSent += event.bytesSent;
        originMsTotal += event.originMs;
        ttfbSamples.add(event.ttfbMs);
    }

    public MetricAccumulator merge(MetricAccumulator other) {
        requests += other.requests;
        errors5xx += other.errors5xx;
        cacheHits += other.cacheHits;
        bytesSent += other.bytesSent;
        originMsTotal += other.originMsTotal;
        ttfbSamples.addAll(other.ttfbSamples);
        return this;
    }

    public double percentile(double probability) {
        if (ttfbSamples.isEmpty()) return 0;
        List<Double> sorted = new ArrayList<>(ttfbSamples);
        Collections.sort(sorted);
        int index = Math.max(0, (int) Math.ceil(probability * sorted.size()) - 1);
        return sorted.get(index);
    }
}
