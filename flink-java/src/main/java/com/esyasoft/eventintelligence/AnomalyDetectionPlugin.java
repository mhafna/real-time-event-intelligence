package com.esyasoft.eventintelligence;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

import java.time.Duration;

/**
 * Isolation Forest anomaly-detection use-case plugin.
 *
 * Reuses the existing 15-minute meter feature logic,
 * then scores each completed feature window using the
 * trained portable Isolation Forest model.
 */
public class AnomalyDetectionPlugin
        implements UseCasePlugin {

    @Override
    public String getName() {
        return "anomaly-detection";
    }

    @Override
    public DataStream<String> apply(
            DataStream<String> enrichedEvents
    ) {

        WatermarkStrategy<String> featureWatermarks =
                WatermarkStrategy
                        .<String>forBoundedOutOfOrderness(
                                Duration.ofSeconds(5)
                        )
                        .withTimestampAssigner(
                                new EventTimestampAssigner()
                        );

        DataStream<String> meterFeatures =
                enrichedEvents
                        .filter(
                                new FeatureEligibilityFilter()
                        )
                        .name(
                                "Anomaly Feature Eligibility"
                        )
                        .assignTimestampsAndWatermarks(
                                featureWatermarks
                        )
                        .name(
                                "Anomaly Event-Time and Watermarks"
                        )
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
                                "Anomaly 15-Minute Feature Aggregation"
                        );

        return meterFeatures
                .map(
                        new AnomalyScoringFunction()
                )
                .name(
                        "Isolation Forest Anomaly Scoring"
                );
    }
}