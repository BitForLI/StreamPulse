package com.streampulse.analytics;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class CdnAnalyticsJobTest {
    @Test
    void acceptsSupportedStartingOffsetModes() {
        assertDoesNotThrow(() -> CdnAnalyticsJob.startingOffsets("earliest"));
        assertDoesNotThrow(() -> CdnAnalyticsJob.startingOffsets("LATEST"));
    }

    @Test
    void rejectsAmbiguousStartingOffsetMode() {
        assertThrows(
                IllegalArgumentException.class,
                () -> CdnAnalyticsJob.startingOffsets("committed-or-earliest"));
    }

    @Test
    void requiresPositiveConfiguredParallelism() {
        assertDoesNotThrow(() -> CdnAnalyticsJob.positiveInt("2", "FLINK_PARALLELISM"));
        assertThrows(
                IllegalArgumentException.class,
                () -> CdnAnalyticsJob.positiveInt("0", "FLINK_PARALLELISM"));
    }
}
