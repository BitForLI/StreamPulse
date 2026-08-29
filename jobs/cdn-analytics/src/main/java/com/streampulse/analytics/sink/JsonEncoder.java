package com.streampulse.analytics.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

public class JsonEncoder<T> extends RichMapFunction<T, String> {
    private transient ObjectMapper mapper;

    @Override public void open(Configuration parameters) { mapper = new ObjectMapper(); }
    @Override public String map(T value) throws Exception { return mapper.writeValueAsString(value); }
}
