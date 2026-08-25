package com.esyasoft.eventintelligence;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

import java.time.Duration;

public class EventIntelligenceJob {

    private static final String BROKER =
            "kafka-local:29092";

    private static final String INPUT_TOPIC =
            "smart-meter-events";

    private static final String ENRICHED_OUTPUT_TOPIC =
            "flink-java-output-test";

    private static final String PROCESSED_PROJECTION_TOPIC =
            "processed-smart-meter-events";

    private static final String METER_FEATURE_TOPIC =
            "meter-features-15m";

    public static void main(String[] args)
            throws Exception {

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment
                        .getExecutionEnvironment();

        env.setParallelism(1);

        /*
         * Kafka source.
         */
        KafkaSource<String> source =
                KafkaSource.<String>builder()
                        .setBootstrapServers(BROKER)
                        .setTopics(INPUT_TOPIC)
                        .setGroupId(
                                "event-intelligence-java-v1"
                        )
                        .setStartingOffsets(
                                OffsetsInitializer.latest()
                        )
                        .setValueOnlyDeserializer(
                                new SimpleStringSchema()
                        )
                        .build();

        DataStream<String> events =
                env.fromSource(
                        source,
                        WatermarkStrategy.noWatermarks(),
                        "Kafka Smart Meter Event Source"
                );

        /*
         * Validation.
         */
        DataStream<String> validatedEvents =
                events
                        .map(
                                new EventValidationFunction()
                        )
                        .name(
                                "Event Validation"
                        );

        /*
         * Event catalogue + network hierarchy enrichment.
         */
        DataStream<String> enrichedEvents =
                validatedEvents
                        .map(
                                new EventEnrichmentFunction()
                        )
                        .name(
                                "Event Catalogue and Network Enrichment"
                        );

        /*
         * Debug / complete enriched-event branch.
         */
        enrichedEvents
                .sinkTo(
                        createKafkaSink(
                                ENRICHED_OUTPUT_TOPIC
                        )
                )
                .name(
                        "Kafka Enriched Event Sink"
                );

        /*
         * Config-driven use-case plugins.
         *
         * Enabled plugins are loaded from usecases.json.
         *
         * Each plugin receives the common enriched-event
         * stream and writes its result to the Kafka topic
         * specified in configuration.
         *
         * This allows new use cases to be added using:
         *
         * 1. A new UseCasePlugin implementation.
         * 2. A new configuration entry.
         *
         * Core pipeline logic does not need to change.
         */
        UseCasePluginLoader pluginLoader =
                new UseCasePluginLoader();

        for (UseCasePluginLoader.LoadedPlugin loadedPlugin
                : pluginLoader.loadEnabledPlugins()) {

            UseCaseDefinition definition =
                    loadedPlugin.getDefinition();

            UseCasePlugin plugin =
                    loadedPlugin.getPlugin();

            DataStream<String> pluginOutput =
                    plugin.apply(
                            enrichedEvents
                    );

            pluginOutput
                    .sinkTo(
                            createKafkaSink(
                                    definition.getOutputTopic()
                            )
                    )
                    .name(
                            "Kafka "
                                    + definition.getName()
                                    + " Sink"
                    );
        }

        /*
         * ClickHouse-ready processed-event branch.
         */
        DataStream<String> processedEvents =
                enrichedEvents
                        .map(
                                new ProcessedEventProjectionFunction()
                        )
                        .name(
                                "Processed Event Projection"
                        );

        processedEvents
                .sinkTo(
                        createKafkaSink(
                                PROCESSED_PROJECTION_TOPIC
                        )
                )
                .name(
                        "Kafka Processed Event Sink"
                );

        /*
         * 15-minute event-time feature branch.
         *
         * Only valid records with a usable MSN and ts
         * enter this branch.
         */
        DataStream<String> featureEligibleEvents =
                enrichedEvents
                        .filter(
                                new FeatureEligibilityFilter()
                        )
                        .name(
                                "Feature Eligibility Filter"
                        );

        /*
         * Assign source event time from ts and generate
         * bounded-out-of-order watermarks.
         */
        WatermarkStrategy<String> featureWatermarks =
                WatermarkStrategy
                        .<String>forBoundedOutOfOrderness(
                                Duration.ofSeconds(5)
                        )
                        .withTimestampAssigner(
                                new EventTimestampAssigner()
                        );

        DataStream<String> timestampedFeatureEvents =
                featureEligibleEvents
                        .assignTimestampsAndWatermarks(
                                featureWatermarks
                        )
                        .name(
                                "Feature Event-Time and Watermarks"
                        );

        /*
         * Per-meter 15-minute feature aggregation.
         */
        DataStream<String> meterFeatures =
                timestampedFeatureEvents
                        .keyBy(
                                new MeterKeySelector()
                        )
                        .window(
                                TumblingEventTimeWindows.of(
                                        Duration.ofMinutes(15)
                                )
                        )
                        .aggregate(
                                new MeterFeatureAggregateFunction(),
                                new MeterFeatureWindowFunction()
                        )
                        .name(
                                "15-Minute Meter Feature Aggregation"
                        );

        /*
         * Real feature output topic.
         */
        meterFeatures
                .sinkTo(
                        createKafkaSink(
                                METER_FEATURE_TOPIC
                        )
                )
                .name(
                        "Kafka Meter Feature Sink"
                );

        env.execute(
                "Esyasoft Event Intelligence - Plugin-Based Streaming Pipeline"
        );
    }

    /*
     * Shared Kafka sink factory.
     *
     * Plugins and core output branches can use this
     * standardized Kafka sink configuration.
     */
    private static KafkaSink<String> createKafkaSink(
            String topic
    ) {

        return KafkaSink.<String>builder()
                .setBootstrapServers(BROKER)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema
                                .builder()
                                .setTopic(topic)
                                .setValueSerializationSchema(
                                        new SimpleStringSchema()
                                )
                                .build()
                )
                .build();
    }
}