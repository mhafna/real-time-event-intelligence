package com.esyasoft.eventintelligence;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;

/**
 * Scores one completed 15-minute meter-feature window
 * using the portable Isolation Forest model.
 */
public class AnomalyScoringFunction
        extends RichMapFunction<String, String> {

    private transient ObjectMapper objectMapper;
    private transient IsolationForestModel model;

    @Override
    public void open(
            OpenContext openContext
    ) throws Exception {

        objectMapper =
                new ObjectMapper();

        model =
                IsolationForestModel.loadFromResource(
                        "models/isolation_forest_model.json"
                );
    }

    @Override
    public String map(
            String value
    ) throws Exception {

        JsonNode feature =
                objectMapper.readTree(value);

        ObjectNode result =
                objectMapper.createObjectNode();

        copyField(feature, result, "msn");
        copyField(feature, result, "window_start");
        copyField(feature, result, "window_end");

        copyField(
                feature,
                result,
                "event_count_15m"
        );

        copyField(
                feature,
                result,
                "avg_voltage_15m"
        );

        copyField(
                feature,
                result,
                "voltage_range_15m"
        );

        copyField(
                feature,
                result,
                "power_failure_count_15m"
        );

        result.put(
                "use_case",
                "anomaly-detection"
        );

        result.put(
                "model_type",
                model.model_type
        );

        result.put(
                "sklearn_version",
                model.sklearn_version
        );

        result.put(
                "model_contamination",
                model.contamination
        );

        JsonNode eventCountNode =
                feature.get(
                        "event_count_15m"
                );

        JsonNode avgVoltageNode =
                feature.get(
                        "avg_voltage_15m"
                );

        JsonNode voltageRangeNode =
                feature.get(
                        "voltage_range_15m"
                );

        /*
         * Current trained model requires these
         * three varying features.
         *
         * power_failure_count_15m remains part
         * of the platform feature schema but was
         * constant zero in this training sample.
         */
        if (!isNumber(eventCountNode)
                || !isNumber(avgVoltageNode)
                || !isNumber(voltageRangeNode)) {

            result.put(
                    "prediction",
                    "UNSCORABLE"
            );

            result.putNull(
                    "confidence"
            );

            result.putNull(
                    "anomaly_score"
            );

            result.put(
                    "severity",
                    "UNKNOWN"
            );

            result.put(
                    "reason",
                    "Required Isolation Forest feature values are missing."
            );

            result.put(
                    "recommendation",
                    "Review feature completeness before anomaly assessment."
            );

            return objectMapper.writeValueAsString(
                    result
            );
        }

        double[] input = {
                eventCountNode.asDouble(),
                avgVoltageNode.asDouble(),
                voltageRangeNode.asDouble()
        };

        double anomalyScore =
                model.anomalyScore(
                        input
                );

        String prediction =
                model.predictLabel(
                        input
                );

        result.put(
                "prediction",
                prediction
        );

        /*
         * Isolation Forest score is not a
         * calibrated probability/confidence.
         */
        result.putNull(
                "confidence"
        );

        result.put(
                "anomaly_score",
                anomalyScore
        );

        if ("ANOMALY".equals(
                prediction
        )) {

            result.put(
                    "severity",
                    "WARNING"
            );

            result.put(
                    "reason",
                    "Unusual combination of 15-minute meter features relative to the learned training distribution."
            );

            result.put(
                    "recommendation",
                    "Review the source events and phase-voltage readings for this meter and time window."
            );

        } else {

            result.put(
                    "severity",
                    "NORMAL"
            );

            result.put(
                    "reason",
                    "15-minute meter feature combination falls within the learned operating distribution."
            );

            result.put(
                    "recommendation",
                    "No anomaly-specific action required."
            );
        }

        return objectMapper.writeValueAsString(
                result
        );
    }

    private boolean isNumber(
            JsonNode node
    ) {

        return node != null
                && !node.isNull()
                && node.isNumber();
    }

    private void copyField(
            JsonNode source,
            ObjectNode target,
            String field
    ) {

        JsonNode node =
                source.get(field);

        if (node == null) {
            target.putNull(field);
        } else {
            target.set(
                    field,
                    node
            );
        }
    }
}