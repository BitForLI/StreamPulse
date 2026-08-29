package com.streampulse.analytics.metrics;

import java.io.Serializable;
import java.util.Objects;

public class ContentDimension implements Serializable {
    public String contentId;
    public ContentDimension() {}
    public ContentDimension(String contentId) { this.contentId = contentId; }
    @Override public boolean equals(Object other) {
        return this == other || (other instanceof ContentDimension
                && Objects.equals(contentId, ((ContentDimension) other).contentId));
    }
    @Override public int hashCode() { return Objects.hash(contentId); }
}
