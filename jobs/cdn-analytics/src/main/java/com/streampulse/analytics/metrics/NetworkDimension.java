package com.streampulse.analytics.metrics;

import java.io.Serializable;
import java.util.Objects;

public class NetworkDimension implements Serializable {
    public String location;
    public String networkId;
    public NetworkDimension() {}
    public NetworkDimension(String location, String networkId) { this.location = location; this.networkId = networkId; }
    @Override public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof NetworkDimension)) return false;
        NetworkDimension that = (NetworkDimension) other;
        return Objects.equals(location, that.location) && Objects.equals(networkId, that.networkId);
    }
    @Override public int hashCode() { return Objects.hash(location, networkId); }
}
