package com.streampulse.analytics.metrics;

import java.io.Serializable;

public class NodeMetricSnapshot implements Serializable {
    public long requests;
    public long errors5xx;
    public long cacheHits;
    public long bytesSent;
    public double originMsTotal;
    public double ttfbP50Ms;
    public double ttfbP95Ms;
    public double ttfbP99Ms;
}
