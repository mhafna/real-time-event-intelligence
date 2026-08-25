package com.esyasoft.eventintelligence;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;

public class OutageProcessFunction
        extends KeyedProcessFunction<String, String, String> {

    private transient ValueState<OutageState> outageState;
    private transient ObjectMapper objectMapper;

    /*
     * Operational requirement:
     * alert if an outage remains unresolved for 5 minutes
     * after the occurrence reaches the streaming pipeline.
     */
    private static final long UNRESOLVED_THRESHOLD_MS =
            Duration.ofMinutes(5).toMillis();

    private static final double UNRESOLVED_THRESHOLD_MINUTES =
            5.0;

    private static final DateTimeFormatter INPUT_TIME_FORMATTER =
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

    private static final DateTimeFormatter OUTPUT_TIME_FORMATTER =
            DateTimeFormatter.ofPattern(
                    "yyyy-MM-dd HH:mm:ss"
            );

    private static final DateTimeFormatter ID_TIME_FORMATTER =
            DateTimeFormatter.ofPattern(
                    "yyyy-MM-dd'T'HH:mm:ss"
            );

    @Override
    public void open(OpenContext openContext)
            throws Exception {

        objectMapper =
                new ObjectMapper();

        ValueStateDescriptor<OutageState> descriptor =
                new ValueStateDescriptor<>(
                        "open-outage-state",
                        OutageState.class
                );

        outageState =
                getRuntimeContext().getState(
                        descriptor
                );
    }

    @Override
    public void processElement(
            String value,
            Context ctx,
            Collector<String> out
    ) throws Exception {

        JsonNode root =
                objectMapper.readTree(value);

        if (!root.isObject()) {
            return;
        }

        ObjectNode event =
                (ObjectNode) root;

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

        String msn =
                textValue(
                        event,
                        "msn"
                );

        String eventTs =
                textValue(
                        event,
                        "ts"
                );

        JsonNode pairNode =
                event.get(
                        "event_tblrefid"
                );

        if (eventName == null
                || eventState == null
                || msn == null
                || eventTs == null
                || pairNode == null
                || pairNode.isNull()) {

            return;
        }

        /*
         * POC outage use case:
         * Power failure only.
         */
        if (!"Power failure".equalsIgnoreCase(
                eventName
        )) {
            return;
        }

        int eventTblRefId =
                pairNode.asInt();

        if ("OCCURRENCE".equalsIgnoreCase(
                eventState
        )) {

            handleOccurrence(
                    event,
                    msn,
                    eventTblRefId,
                    eventName,
                    eventTs,
                    ctx,
                    out
            );

        } else if ("RESTORATION".equalsIgnoreCase(
                eventState
        )) {

            handleRestoration(
                    eventTs,
                    ctx,
                    out
            );
        }
    }

    private void handleOccurrence(
            ObjectNode event,
            String msn,
            int eventTblRefId,
            String eventName,
            String eventTs,
            Context ctx,
            Collector<String> out
    ) throws Exception {

        OutageState existing =
                outageState.value();

        /*
         * Do not reset an already-open outage.
         */
        if (existing != null) {
            return;
        }

        String normalizedStart =
                normalizeTime(
                        eventTs
                );

        /*
         * Processing-time timer:
         *
         * five minutes from the moment this occurrence
         * is processed by Flink.
         */
        long alertTimerTimestamp =
                ctx.timerService()
                        .currentProcessingTime()
                        + UNRESOLVED_THRESHOLD_MS;

        OutageState newState =
                new OutageState(
                        msn,
                        eventTblRefId,
                        eventName,
                        normalizedStart,
                        textValue(
                                event,
                                "dtr_name"
                        ),
                        textValue(
                                event,
                                "dtr_network_code"
                        ),
                        textValue(
                                event,
                                "feeder_name"
                        ),
                        textValue(
                                event,
                                "substation_name"
                        ),
                        alertTimerTimestamp
                );

        outageState.update(
                newState
        );

        /*
         * Register the 5-minute unresolved-outage timer.
         */
        ctx.timerService()
                .registerProcessingTimeTimer(
                        alertTimerTimestamp
                );

        /*
         * Existing behaviour:
         * immediately emit OPEN.
         */
        ObjectNode incident =
                createIncident(
                        newState,
                        "OPEN",
                        null,
                        null
                );

        out.collect(
                objectMapper.writeValueAsString(
                        incident
                )
        );
    }

    private void handleRestoration(
            String eventTs,
            Context ctx,
            Collector<String> out
    ) throws Exception {

        OutageState openOutage =
                outageState.value();

        /*
         * Restoration without a known occurrence.
         */
        if (openOutage == null) {
            return;
        }

        LocalDateTime start =
                parseTime(
                        openOutage.getStartTime()
                );

        LocalDateTime end =
                parseTime(
                        eventTs
                );

        /*
         * Do not close using an invalid or
         * out-of-order restoration earlier
         * than the occurrence.
         */
        if (end.isBefore(start)) {
            return;
        }

        /*
         * If the 5-minute timer has not fired yet,
         * cancel it because restoration arrived.
         */
        Long alertTimerTimestamp =
                openOutage.getAlertTimerTimestamp();

        if (alertTimerTimestamp != null) {

            ctx.timerService()
                    .deleteProcessingTimeTimer(
                            alertTimerTimestamp
                    );
        }

        double durationMinutes =
                Duration.between(
                        start,
                        end
                ).toMillis()
                        / 60000.0;

        String normalizedEnd =
                end.format(
                        OUTPUT_TIME_FORMATTER
                );

        ObjectNode incident =
                createIncident(
                        openOutage,
                        "CLOSED",
                        normalizedEnd,
                        durationMinutes
                );

        out.collect(
                objectMapper.writeValueAsString(
                        incident
                )
        );

        /*
         * Outage is fully restored.
         */
        outageState.clear();
    }

    /*
     * Called when the processing-time timer fires.
     */
    @Override
    public void onTimer(
            long timestamp,
            OnTimerContext ctx,
            Collector<String> out
    ) throws Exception {

        OutageState openOutage =
                outageState.value();

        /*
         * Outage may already have been restored
         * and its state cleared.
         */
        if (openOutage == null) {
            return;
        }

        Long expectedTimerTimestamp =
                openOutage.getAlertTimerTimestamp();

        /*
         * Ignore an old or unrelated timer.
         */
        if (expectedTimerTimestamp == null
                || expectedTimerTimestamp != timestamp) {

            return;
        }

        /*
         * Five processing-time minutes have passed
         * and no restoration has arrived.
         *
         * Emit an operational alert but DO NOT clear
         * the outage state. A later restoration must
         * still be able to close the incident.
         */
        ObjectNode unresolvedIncident =
                createIncident(
                        openOutage,
                        "UNRESOLVED",
                        null,
                        UNRESOLVED_THRESHOLD_MINUTES
                );

        out.collect(
                objectMapper.writeValueAsString(
                        unresolvedIncident
                )
        );

        /*
         * Mark this alert timer as completed so this
         * outage does not produce the same 5-minute
         * alert again.
         */
        openOutage.setAlertTimerTimestamp(
                null
        );

        outageState.update(
                openOutage
        );
    }

    private ObjectNode createIncident(
            OutageState state,
            String status,
            String endTs,
            Double durationMinutes
    ) throws Exception {

        ObjectNode incident =
                objectMapper.createObjectNode();

        LocalDateTime start =
                parseTime(
                        state.getStartTime()
                );

        String incidentId =
                state.getMsn()
                        + ":"
                        + state.getEventTblRefId()
                        + ":"
                        + start.format(
                                ID_TIME_FORMATTER
                        );

        incident.put(
                "incident_id",
                incidentId
        );

        incident.put(
                "msn",
                state.getMsn()
        );

        incident.put(
                "event_name",
                state.getEventName()
        );

        incident.put(
                "event_tblrefid",
                state.getEventTblRefId()
        );

        incident.put(
                "incident_status",
                status
        );

        incident.put(
                "start_ts",
                state.getStartTime()
        );

        if (endTs == null) {

            incident.putNull(
                    "end_ts"
            );

        } else {

            incident.put(
                    "end_ts",
                    endTs
            );
        }

        if (durationMinutes == null) {

            incident.putNull(
                    "duration_minutes"
            );

        } else {

            incident.put(
                    "duration_minutes",
                    durationMinutes
            );
        }

        putNullable(
                incident,
                "dtr_name",
                state.getDtrName()
        );

        putNullable(
                incident,
                "dtr_network_code",
                state.getDtrNetworkCode()
        );

        putNullable(
                incident,
                "feeder_name",
                state.getFeederName()
        );

        putNullable(
                incident,
                "substation_name",
                state.getSubstationName()
        );

        return incident;
    }

    private String normalizeTime(
            String value
    ) {

        return parseTime(
                value
        ).format(
                OUTPUT_TIME_FORMATTER
        );
    }

    private LocalDateTime parseTime(
            String value
    ) {

        return LocalDateTime.parse(
                value.trim(),
                INPUT_TIME_FORMATTER
        );
    }

    private String textValue(
            ObjectNode event,
            String field
    ) {

        JsonNode node =
                event.get(
                        field
                );

        if (node == null
                || node.isNull()) {

            return null;
        }

        String value =
                node.asText()
                        .trim();

        return value.isEmpty()
                ? null
                : value;
    }

    private void putNullable(
            ObjectNode node,
            String field,
            String value
    ) {

        if (value == null) {

            node.putNull(
                    field
            );

        } else {

            node.put(
                    field,
                    value
            );
        }
    }
}
