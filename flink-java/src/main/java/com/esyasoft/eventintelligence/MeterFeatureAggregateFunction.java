package com.esyasoft.eventintelligence;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.api.common.functions.AggregateFunction;

public class MeterFeatureAggregateFunction
        implements AggregateFunction<
                String,
                MeterFeatureAccumulator,
                MeterFeatureAccumulator> {

    private transient ObjectMapper objectMapper;

    @Override
    public MeterFeatureAccumulator createAccumulator() {

        return new MeterFeatureAccumulator();
    }

    @Override
    public MeterFeatureAccumulator add(
            String value,
            MeterFeatureAccumulator accumulator
    ) {

        try {

            if (objectMapper == null) {
                objectMapper = new ObjectMapper();
            }

            JsonNode event =
                    objectMapper.readTree(value);

            accumulator.eventCount++;

            addVoltageIfPresent(
                    event,
                    "v_r",
                    accumulator
            );

            addVoltageIfPresent(
                    event,
                    "v_y",
                    accumulator
            );

            addVoltageIfPresent(
                    event,
                    "v_b",
                    accumulator
            );

            String eventName =
                    textValue(
                            event,
                            "event_name"
                    );

            String eventState =
                    textValue(
                            event,
                            "event_state"
                    );

            if ("Power failure".equalsIgnoreCase(
                        eventName
                    )
                    && "OCCURRENCE".equalsIgnoreCase(
                        eventState
                    )) {

                accumulator.powerFailureCount++;
            }

        } catch (Exception e) {

            throw new RuntimeException(
                    "Failed to aggregate meter feature event",
                    e
            );
        }

        return accumulator;
    }

    @Override
    public MeterFeatureAccumulator getResult(
            MeterFeatureAccumulator accumulator
    ) {

        return accumulator;
    }

    @Override
    public MeterFeatureAccumulator merge(
            MeterFeatureAccumulator a,
            MeterFeatureAccumulator b
    ) {

        MeterFeatureAccumulator merged =
                new MeterFeatureAccumulator();

        merged.eventCount =
                a.eventCount + b.eventCount;

        merged.voltageCount =
                a.voltageCount + b.voltageCount;

        merged.voltageSum =
                a.voltageSum + b.voltageSum;

        merged.powerFailureCount =
                a.powerFailureCount
                        + b.powerFailureCount;

        if (a.minVoltage == null) {
            merged.minVoltage = b.minVoltage;

        } else if (b.minVoltage == null) {
            merged.minVoltage = a.minVoltage;

        } else {
            merged.minVoltage =
                    Math.min(
                            a.minVoltage,
                            b.minVoltage
                    );
        }

        if (a.maxVoltage == null) {
            merged.maxVoltage = b.maxVoltage;

        } else if (b.maxVoltage == null) {
            merged.maxVoltage = a.maxVoltage;

        } else {
            merged.maxVoltage =
                    Math.max(
                            a.maxVoltage,
                            b.maxVoltage
                    );
        }

        return merged;
    }

    private void addVoltageIfPresent(
            JsonNode event,
            String field,
            MeterFeatureAccumulator accumulator
    ) {

        JsonNode node =
                event.get(field);

        if (node == null
                || node.isNull()
                || !node.isNumber()) {

            return;
        }

        accumulator.addVoltage(
                node.asDouble()
        );
    }

    private String textValue(
            JsonNode event,
            String field
    ) {

        JsonNode node =
                event.get(field);

        if (node == null
                || node.isNull()) {

            return null;
        }

        String value =
                node.asText().trim();

        return value.isEmpty()
                ? null
                : value;
    }
}
