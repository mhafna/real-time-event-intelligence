package com.esyasoft.eventintelligence;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.api.common.functions.FilterFunction;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;

public class FeatureEligibilityFilter
        implements FilterFunction<String> {

    private transient ObjectMapper objectMapper;

    private static final DateTimeFormatter TIME_FORMATTER =
            new DateTimeFormatterBuilder()
                    .appendPattern("yyyy-MM-dd HH:mm:ss")
                    .optionalStart()
                    .appendFraction(
                            ChronoField.NANO_OF_SECOND,
                            0,
                            9,
                            true
                    )
                    .optionalEnd()
                    .toFormatter();

    @Override
    public boolean filter(String value) {

        try {

            if (objectMapper == null) {
                objectMapper = new ObjectMapper();
            }

            JsonNode event =
                    objectMapper.readTree(value);

            if (!event.isObject()) {
                return false;
            }

            JsonNode validationNode =
                    event.get("validation_status");

            if (validationNode == null
                    || !"VALID".equalsIgnoreCase(
                            validationNode.asText()
                    )) {

                return false;
            }

            JsonNode msnNode =
                    event.get("msn");

            if (msnNode == null
                    || msnNode.isNull()
                    || msnNode.asText().trim().isEmpty()) {

                return false;
            }

            JsonNode tsNode =
                    event.get("ts");

            if (tsNode == null
                    || tsNode.isNull()
                    || tsNode.asText().trim().isEmpty()) {

                return false;
            }

            // Confirm timestamp is parseable before it reaches
            // the strict EventTimestampAssigner.
            LocalDateTime.parse(
                    tsNode.asText().trim(),
                    TIME_FORMATTER
            );

            return true;

        } catch (Exception e) {

            // Bad records remain in the normal processed stream,
            // but are excluded from feature calculation.
            return false;
        }
    }
}
