# Metrics and interpretation

## Delivery-derived metrics

- `ttfb_p50_ms`, `ttfb_p95_ms`, `ttfb_p99_ms`: nearest-rank percentiles of
  non-negative DeliveryEvent TTFB in the completed window.
- `error_5xx_rate`: status `500..599` divided by request count.
- `cache_hit_ratio`: normalized `HIT` count divided by request count.
- `bytes_sent`: sum of edge response bytes.
- `origin_ms_total`: sum of origin fetch time; zero on cache hits in synthetic
  data and an observable cost proxy rather than a currency claim.

Node and network views use one-minute tumbling event-time windows. Content
demand/cost uses five-minute windows. Late updates increment `revision` so a
downstream latest-revision view can replace, rather than double-count, results.

## Player QoE boundary

Startup delay, rebuffer ratio, bitrate switches, and playback errors require
PlayerEvent and cannot be inferred honestly from NGINX delivery logs alone.
Those aggregates are not yet implemented, so the current job reports delivery
quality proxies only.
