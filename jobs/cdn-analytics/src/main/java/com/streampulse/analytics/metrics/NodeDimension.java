package com.streampulse.analytics.metrics;

import java.io.Serializable;
import java.util.Objects;

public class NodeDimension implements Serializable {
    public String location;
    public String networkId;
    public String nodeId;

    public NodeDimension() {}

    public NodeDimension(String location, String networkId, String nodeId) {
        this.location = location;
        this.networkId = networkId;
        this.nodeId = nodeId;
    }

    @Override public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof NodeDimension)) return false;
        NodeDimension that = (NodeDimension) other;
        return Objects.equals(location, that.location)
                && Objects.equals(networkId, that.networkId)
                && Objects.equals(nodeId, that.nodeId);
    }

    @Override public int hashCode() { return Objects.hash(location, networkId, nodeId); }
}
