package com.streampulse.analytics.parse;

import com.streampulse.analytics.schema.DeliveryEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class EventIdDeduplicator extends KeyedProcessFunction<String, DeliveryEvent, DeliveryEvent> {
    private transient ValueState<Boolean> seen;

    @Override
    public void open(Configuration parameters) {
        StateTtlConfig ttl = StateTtlConfig.newBuilder(Time.hours(25))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();
        ValueStateDescriptor<Boolean> descriptor = new ValueStateDescriptor<>("seen-event-id", Boolean.class);
        descriptor.enableTimeToLive(ttl);
        seen = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(DeliveryEvent event, Context context, Collector<DeliveryEvent> output) throws Exception {
        if (seen.value() == null) {
            seen.update(Boolean.TRUE);
            output.collect(event);
        }
    }
}
