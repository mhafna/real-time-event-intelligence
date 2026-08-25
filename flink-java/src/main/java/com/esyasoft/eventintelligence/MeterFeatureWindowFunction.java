package com.esyasoft.eventintelligence;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class MeterFeatureWindowFunction
        extends ProcessWindowFunction<
                MeterFeatureAccumulator,
                String,
                String,
                TimeWindow> {

    private transient ObjectMapper objectMapper;

    private static final DateTimeFormatter TIME_FORMATTER =
            DateTimeFormatter.ofPattern(
                    "yyyy-MM-dd HH:mm:ss"
            );

    @Override
    public void process(
            String msn,
            Context context,
            Iterable<MeterFeatureAccumulator> elements,
            Collector<String> out
    ) throws Exception {

        if (objectMapper == null) {
            objectMapper =
                    new ObjectMapper();
        }

        MeterFeatureAccumulator accumulator =
                elements.iterator().next();

        ObjectNode feature =
                objectMapper.createObjectNode();

        feature.put(
                "msn",
                msn
        );

        feature.put(
                "window_start",
                formatTime(
                        context.window().getStart()
                )
        );

        feature.put(
                "window_end",
                formatTime(
                        context.window().getEnd()
                )
        );

        feature.put(
                "event_count_15m",
                accumulator.eventCount
        );

        if (accumulator.voltageCount > 0) {

            double averageVoltage =
                    accumulator.voltageSum
                            / accumulator.voltageCount;

            feature.put(
                    "avg_voltage_15m",
                    averageVoltage
            );

        } else {

            feature.putNull(
                    "avg_voltage_15m"
            );
        }

        if (accumulator.minVoltage != null
                && accumulator.maxVoltage != null) {

            double voltageRange =
                    accumulator.maxVoltage
                            - accumulator.minVoltage;

            feature.put(
                    "voltage_range_15m",
                    voltageRange
            );

        } else {

            feature.putNull(
                    "voltage_range_15m"
            );
        }

        feature.put(
                "power_failure_count_15m",
                accumulator.powerFailureCount
        );

        out.collect(
                objectMapper.writeValueAsString(
                        feature
                )
        );
    }

    private String formatTime(
            long epochMillis
    ) {

        return Instant.ofEpochMilli(
                        epochMillis
                )
                .atZone(
                        ZoneOffset.UTC
                )
                .toLocalDateTime()
                .format(
                        TIME_FORMATTER
                );
    }
}
