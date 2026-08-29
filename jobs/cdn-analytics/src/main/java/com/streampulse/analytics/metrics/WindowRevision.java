package com.streampulse.analytics.metrics;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;

final class WindowRevision {
    private WindowRevision() {}
    static int next(ProcessWindowFunction<?, ?, ?, ?>.Context context) throws Exception {
        ValueState<Integer> state = context.windowState().getState(new ValueStateDescriptor<>("revision", Integer.class));
        Integer prior = state.value();
        int revision = prior == null ? 0 : prior + 1;
        state.update(revision);
        return revision;
    }
    static double ratio(long numerator, long denominator) {
        return denominator == 0 ? 0 : (double) numerator / denominator;
    }
}
