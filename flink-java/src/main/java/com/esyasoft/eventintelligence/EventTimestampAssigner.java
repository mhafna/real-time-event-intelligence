package com.esyasoft.eventintelligence;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;

public class EventTimestampAssigner
        implements SerializableTimestampAssigner<String> {

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
    public long extractTimestamp(
            String element,
            long recordTimestamp
    ) {

        try {

            if (objectMapper == null) {
                objectMapper =
                        new ObjectMapper();
            }

            JsonNode event =
                    objectMapper.readTree(element);

            JsonNode tsNode =
                    event.get("ts");

            if (tsNode == null
                    || tsNode.isNull()
                    || tsNode.asText().trim().isEmpty()) {

                throw new IllegalArgumentException(
                        "Missing event timestamp 'ts'"
                );
            }

            LocalDateTime eventTime =
                    LocalDateTime.parse(
                            tsNode.asText().trim(),
                            TIME_FORMATTER
                    );

            return eventTime
                    .toInstant(ZoneOffset.UTC)
                    .toEpochMilli();

        } catch (Exception e) {

            throw new RuntimeException(
                    "Unable to extract event timestamp from: "
                            + element,
                    e
            );
        }
    }
}
