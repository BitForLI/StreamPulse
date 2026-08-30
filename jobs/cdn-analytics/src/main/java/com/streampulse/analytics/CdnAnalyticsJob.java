package com.streampulse.analytics;

import com.streampulse.analytics.metrics.NodeDimension;
import com.streampulse.analytics.metrics.NodeMetric;
import com.streampulse.analytics.metrics.NodeMetricAggregate;
import com.streampulse.analytics.metrics.NodeMetricWindow;
import com.streampulse.analytics.metrics.NetworkDimension;
import com.streampulse.analytics.metrics.NetworkMetric;
import com.streampulse.analytics.metrics.NetworkMetricWindow;
import com.streampulse.analytics.metrics.ContentDimension;
import com.streampulse.analytics.metrics.ContentMetric;
import com.streampulse.analytics.metrics.ContentMetricWindow;
import com.streampulse.analytics.parse.DeliveryEventParser;
import com.streampulse.analytics.parse.EventIdDeduplicator;
import com.streampulse.analytics.parse.LateDeliveryMapper;
import com.streampulse.analytics.schema.DeadLetterEvent;
import com.streampulse.analytics.schema.DeliveryEvent;
import com.streampulse.analytics.sink.JsonEncoder;
import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

public final class CdnAnalyticsJob {
    private static final OutputTag<DeliveryEvent> LATE_DELIVERY =
            new OutputTag<DeliveryEvent>("late-delivery") {};

    private CdnAnalyticsJob() {}

    public static void main(String[] args) throws Exception {
        String brokers = env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");
        String groupId = env("KAFKA_GROUP_ID", "streampulse-cdn-analytics-v1");
        String startingOffsets = env("KAFKA_STARTING_OFFSETS", "earliest");
        String jobName = env("FLINK_JOB_NAME", "StreamPulse CDN Analytics v1");
        String deliveryTopic = env("KAFKA_DELIVERY_TOPIC", "cdn.delivery.v1");
        String deadLetterTopic = env("KAFKA_DEAD_LETTER_TOPIC", "cdn.dead-letter.v1");
        int parallelism = positiveInt(env("FLINK_PARALLELISM", "1"), "FLINK_PARALLELISM");
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(parallelism);
        environment.enableCheckpointing(10_000, CheckpointingMode.EXACTLY_ONCE);
        environment.getCheckpointConfig().setMinPauseBetweenCheckpoints(5_000);
        environment.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        environment.setRestartStrategy(RestartStrategies.failureRateRestart(
                10,
                org.apache.flink.api.common.time.Time.minutes(5),
                org.apache.flink.api.common.time.Time.seconds(5)));

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(deliveryTopic)
                .setGroupId(groupId)
                .setStartingOffsets(startingOffsets(startingOffsets))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        SingleOutputStreamOperator<DeliveryEvent> parsed = environment
                .fromSource(source, WatermarkStrategy.noWatermarks(), "delivery-kafka-source")
                .process(new DeliveryEventParser(deliveryTopic))
                .name("parse-and-validate-delivery");

        DataStream<DeadLetterEvent> deadLetters = parsed.getSideOutput(DeliveryEventParser.DEAD_LETTER);
        DataStream<DeliveryEvent> deduplicated = parsed
                .keyBy(event -> event.eventId)
                .process(new EventIdDeduplicator())
                .name("deduplicate-by-event-id");

        WatermarkStrategy<DeliveryEvent> watermarks = WatermarkStrategy
                .<DeliveryEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner((event, prior) -> event.eventTimestampMillis())
                .withIdleness(Duration.ofSeconds(30));

        DataStream<DeliveryEvent> eventTimeStream = deduplicated.assignTimestampsAndWatermarks(watermarks);

        SingleOutputStreamOperator<NodeMetric> nodeMetrics = eventTimeStream
                .keyBy(event -> new NodeDimension(event.location, event.networkId, event.nodeId))
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .allowedLateness(Time.seconds(5))
                .sideOutputLateData(LATE_DELIVERY)
                .aggregate(new NodeMetricAggregate(), new NodeMetricWindow())
                .name("node-minute-metrics");

        DataStream<NetworkMetric> networkMetrics = eventTimeStream
                .keyBy(event -> new NetworkDimension(event.location, event.networkId))
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .allowedLateness(Time.seconds(5))
                .aggregate(new NodeMetricAggregate(), new NetworkMetricWindow())
                .name("network-minute-metrics");

        DataStream<ContentMetric> contentMetrics = eventTimeStream
                .keyBy(event -> new ContentDimension(event.contentId))
                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                .allowedLateness(Time.seconds(5))
                .aggregate(new NodeMetricAggregate(), new ContentMetricWindow())
                .name("content-five-minute-metrics");

        DataStream<DeadLetterEvent> lateLetters = nodeMetrics.getSideOutput(LATE_DELIVERY)
                .map(new LateDeliveryMapper(deliveryTopic))
                .name("late-event-audit");

        nodeMetrics.map(new JsonEncoder<NodeMetric>())
                .sinkTo(stringSink(brokers, "cdn.metrics.node.1m.v1"))
                .name("node-metrics-kafka-sink");
        networkMetrics.map(new JsonEncoder<NetworkMetric>())
                .sinkTo(stringSink(brokers, "cdn.metrics.network.1m.v1"))
                .name("network-metrics-kafka-sink");
        contentMetrics.map(new JsonEncoder<ContentMetric>())
                .sinkTo(stringSink(brokers, "cdn.metrics.content.5m.v1"))
                .name("content-metrics-kafka-sink");
        deadLetters.union(lateLetters)
                .map(new JsonEncoder<DeadLetterEvent>())
                .sinkTo(stringSink(brokers, deadLetterTopic))
                .name("dead-letter-kafka-sink");

        environment.execute(jobName);
    }

    static KafkaSink<String> stringSink(String brokers, String topic) {
        return KafkaSink.<String>builder()
                .setBootstrapServers(brokers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }

    static OffsetsInitializer startingOffsets(String configuredValue) {
        if ("earliest".equalsIgnoreCase(configuredValue)) {
            return OffsetsInitializer.earliest();
        }
        if ("latest".equalsIgnoreCase(configuredValue)) {
            return OffsetsInitializer.latest();
        }
        throw new IllegalArgumentException(
                "KAFKA_STARTING_OFFSETS must be 'earliest' or 'latest', got: " + configuredValue);
    }

    static int positiveInt(String configuredValue, String name) {
        int parsed = Integer.parseInt(configuredValue);
        if (parsed <= 0) {
            throw new IllegalArgumentException(name + " must be positive, got: " + configuredValue);
        }
        return parsed;
    }
}
