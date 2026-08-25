package com.esyasoft.eventintelligence;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TopologyInferenceProcessFunction
        extends KeyedProcessFunction<String, String, String> {

    private static final long CO_OCCURRENCE_WINDOW_SECONDS = 5 * 60;

    // POC scoring weights
    private static final double CO_OCCURRENCE_WEIGHT = 0.70;
    private static final double PROXIMITY_WEIGHT = 0.30;

    // At 20 km or more, geographic similarity becomes zero.
    private static final double MAX_PROXIMITY_DISTANCE_KM = 20.0;

    private transient ObjectMapper objectMapper;
    private transient ListState<String> recentKnownOutages;

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

    @Override
    public void open(OpenContext openContext) throws Exception {

        objectMapper = new ObjectMapper();

        ListStateDescriptor<String> descriptor =
                new ListStateDescriptor<>(
                        "recent-known-topology-outages",
                        String.class
                );

        recentKnownOutages =
                getRuntimeContext().getListState(descriptor);
    }

    @Override
    public void processElement(
            String value,
            Context ctx,
            Collector<String> out
    ) throws Exception {

        JsonNode root = objectMapper.readTree(value);

        if (!root.isObject()) {
            return;
        }

        ObjectNode event = (ObjectNode) root;

        String eventName = textValue(event, "event_name");
        String eventState = textValue(event, "event_state");
        String msn = textValue(event, "msn");
        String eventTs = textValue(event, "ts");

        if (eventName == null
                || eventState == null
                || msn == null
                || eventTs == null) {
            return;
        }

        // Topology inference currently uses outage-occurrence
        // behaviour only.
        if (!"Power failure".equalsIgnoreCase(eventName)
                || !"OCCURRENCE".equalsIgnoreCase(eventState)) {
            return;
        }

        LocalDateTime currentEventTime;

        try {
            currentEventTime = parseTime(eventTs);
        } catch (Exception exception) {
            return;
        }

        /*
         * First remove old known-outage observations.
         *
         * The five-minute window is based on source event time,
         * not processing time, because this platform replays
         * historical events as a simulated live stream.
         */
        List<String> retainedOutages =
                retainRecentKnownOutages(currentEventTime);

        recentKnownOutages.update(retainedOutages);

        boolean hierarchyMatch =
                event.path("hierarchy_match").asBoolean(false);

        /*
         * KNOWN METER
         *
         * Store its recent outage occurrence as evidence for
         * topology inference.
         */
        if (hierarchyMatch) {

            storeKnownOutage(
                    event,
                    msn,
                    eventTs,
                    retainedOutages
            );

            return;
        }

        /*
         * UNKNOWN / UNMATCHED METER
         *
         * It needs coordinates so that geographic proximity
         * can contribute to the candidate score.
         */
        Double unknownLatitude =
                doubleValue(event, "latitude");

        Double unknownLongitude =
                doubleValue(event, "longitude");

        if (unknownLatitude == null
                || unknownLongitude == null) {
            return;
        }

        List<CandidateStats> candidates =
                buildCandidates(
                        retainedOutages,
                        currentEventTime,
                        unknownLatitude,
                        unknownLongitude
                );

        if (candidates.isEmpty()) {
            return;
        }

        int maxSupportingMeters =
                candidates.stream()
                        .mapToInt(
                                candidate ->
                                        candidate.supportingMeters.size()
                        )
                        .max()
                        .orElse(1);

        for (CandidateStats candidate : candidates) {

            candidate.coOccurrenceScore =
                    candidate.supportingMeters.size()
                            / (double) maxSupportingMeters;

            double centroidLatitude =
                    candidate.latitudeSum
                            / candidate.coordinateCount;

            double centroidLongitude =
                    candidate.longitudeSum
                            / candidate.coordinateCount;

            candidate.distanceKm =
                    haversineDistanceKm(
                            unknownLatitude,
                            unknownLongitude,
                            centroidLatitude,
                            centroidLongitude
                    );

            candidate.proximityScore =
                    Math.max(
                            0.0,
                            1.0
                                    - (
                                    candidate.distanceKm
                                            / MAX_PROXIMITY_DISTANCE_KM
                            )
                    );

            candidate.topologyScore =
                    (
                            CO_OCCURRENCE_WEIGHT
                                    * candidate.coOccurrenceScore
                    )
                            +
                    (
                            PROXIMITY_WEIGHT
                                    * candidate.proximityScore
                    );
        }

        candidates.sort(
                Comparator
                        .comparingDouble(
                                (CandidateStats candidate) ->
                                        candidate.topologyScore
                        )
                        .reversed()
                        .thenComparingInt(
                                candidate ->
                                        -candidate.supportingMeters.size()
                        )
        );

        int numberToEmit =
                Math.min(3, candidates.size());

        String inferenceId =
                msn
                        + ":"
                        + currentEventTime.toString();

        for (int index = 0;
             index < numberToEmit;
             index++) {

            CandidateStats candidate =
                    candidates.get(index);

            int rank = index + 1;

            ObjectNode result =
                    createResult(
                            inferenceId,
                            msn,
                            eventTs,
                            rank,
                            candidate
                    );

            out.collect(
                    objectMapper.writeValueAsString(result)
            );
        }
    }

    private List<String> retainRecentKnownOutages(
            LocalDateTime currentEventTime
    ) throws Exception {

        List<String> retained = new ArrayList<>();

        for (String storedValue : recentKnownOutages.get()) {

            JsonNode stored =
                    objectMapper.readTree(storedValue);

            String storedTs =
                    textValue(stored, "ts");

            if (storedTs == null) {
                continue;
            }

            LocalDateTime storedTime;

            try {
                storedTime = parseTime(storedTs);
            } catch (Exception exception) {
                continue;
            }

            /*
             * Keep observations no older than five event-time
             * minutes relative to the current event.
             *
             * Future observations are retained here because
             * replay input can occasionally be mildly
             * out-of-order; they are filtered during scoring.
             */
            if (!storedTime.isBefore(
                    currentEventTime.minusSeconds(
                            CO_OCCURRENCE_WINDOW_SECONDS
                    )
            )) {
                retained.add(storedValue);
            }
        }

        return retained;
    }

    private void storeKnownOutage(
            ObjectNode event,
            String msn,
            String eventTs,
            List<String> retainedOutages
    ) throws Exception {

        String feederName =
                textValue(event, "feeder_name");

        String substationName =
                textValue(event, "substation_name");

        Double latitude =
                doubleValue(event, "latitude");

        Double longitude =
                doubleValue(event, "longitude");

        if (feederName == null
                || latitude == null
                || longitude == null) {
            return;
        }

        ObjectNode snapshot =
                objectMapper.createObjectNode();

        snapshot.put("msn", msn);
        snapshot.put("ts", eventTs);
        snapshot.put("feeder_name", feederName);

        if (substationName != null) {
            snapshot.put(
                    "substation_name",
                    substationName
            );
        }

        snapshot.put("latitude", latitude);
        snapshot.put("longitude", longitude);

        retainedOutages.add(
                objectMapper.writeValueAsString(snapshot)
        );

        recentKnownOutages.update(retainedOutages);
    }

    private List<CandidateStats> buildCandidates(
            List<String> retainedOutages,
            LocalDateTime unknownEventTime,
            double unknownLatitude,
            double unknownLongitude
    ) throws Exception {

        List<CandidateStats> candidates =
                new ArrayList<>();

        for (String storedValue : retainedOutages) {

            JsonNode stored =
                    objectMapper.readTree(storedValue);

            String storedMsn =
                    textValue(stored, "msn");

            String storedTs =
                    textValue(stored, "ts");

            String feederName =
                    textValue(stored, "feeder_name");

            String substationName =
                    textValue(stored, "substation_name");

            Double latitude =
                    doubleValue(stored, "latitude");

            Double longitude =
                    doubleValue(stored, "longitude");

            if (storedMsn == null
                    || storedTs == null
                    || feederName == null
                    || latitude == null
                    || longitude == null) {
                continue;
            }

            LocalDateTime knownEventTime;

            try {
                knownEventTime =
                        parseTime(storedTs);
            } catch (Exception exception) {
                continue;
            }

            long secondsDifference =
                    Duration.between(
                            knownEventTime,
                            unknownEventTime
                    ).getSeconds();

            /*
             * Only known outages occurring from 0 to 5 minutes
             * before the unknown outage are evidence.
             */
            if (secondsDifference < 0
                    || secondsDifference
                    > CO_OCCURRENCE_WINDOW_SECONDS) {
                continue;
            }

            CandidateStats candidate =
                    findOrCreateCandidate(
                            candidates,
                            feederName,
                            substationName
                    );

            /*
             * A meter contributes once per candidate feeder,
             * even if duplicate outage events are observed.
             */
            if (candidate.supportingMeters.add(storedMsn)) {

                candidate.latitudeSum += latitude;
                candidate.longitudeSum += longitude;
                candidate.coordinateCount++;
            }
        }

        return candidates;
    }

    private CandidateStats findOrCreateCandidate(
            List<CandidateStats> candidates,
            String feederName,
            String substationName
    ) {

        for (CandidateStats candidate : candidates) {

            if (candidate.feederName.equals(feederName)) {
                return candidate;
            }
        }

        CandidateStats candidate =
                new CandidateStats(
                        feederName,
                        substationName
                );

        candidates.add(candidate);

        return candidate;
    }

    private ObjectNode createResult(
            String inferenceId,
            String unknownMsn,
            String eventTs,
            int rank,
            CandidateStats candidate
    ) {

        ObjectNode result =
                objectMapper.createObjectNode();

        result.put(
                "use_case",
                "topology-inference"
        );

        result.put(
                "inference_id",
                inferenceId
        );

        result.put(
                "unknown_msn",
                unknownMsn
        );

        result.put(
                "event_ts",
                eventTs
        );

        result.put(
                "candidate_rank",
                rank
        );

        result.put(
                "candidate_feeder",
                candidate.feederName
        );

        if (candidate.substationName != null) {
            result.put(
                    "candidate_substation",
                    candidate.substationName
            );
        }

        result.put(
                "supporting_meter_count",
                candidate.supportingMeters.size()
        );

        result.put(
                "cooccurrence_score",
                round(candidate.coOccurrenceScore)
        );

        result.put(
                "distance_km",
                round(candidate.distanceKm)
        );

        result.put(
                "proximity_score",
                round(candidate.proximityScore)
        );

        result.put(
                "topology_score",
                round(candidate.topologyScore)
        );

        /*
         * Standardized plugin-style intelligence fields.
         */
        result.put(
                "prediction",
                candidate.feederName
        );

        result.put(
                "confidence",
                round(candidate.topologyScore)
        );

        result.put(
                "severity",
                "INFO"
        );

        if (rank == 1) {

            result.put(
                    "reason",
                    "Highest combined outage co-occurrence "
                            + "and geographic proximity score."
            );

            result.put(
                    "recommendation",
                    "Review feeder "
                            + candidate.feederName
                            + " as the likely topology assignment."
            );

        } else {

            result.put(
                    "reason",
                    "Alternative candidate based on recent "
                            + "outage and geographic evidence."
            );

            result.put(
                    "recommendation",
                    "Retain as an alternative feeder candidate."
            );
        }

        result.put(
                "generated_at",
                Instant.now().toString()
        );

        return result;
    }

    private LocalDateTime parseTime(String value) {

        return LocalDateTime.parse(
                value,
                INPUT_TIME_FORMATTER
        );
    }

    private String textValue(
            JsonNode node,
            String field
    ) {

        JsonNode value = node.get(field);

        if (value == null
                || value.isNull()) {
            return null;
        }

        String text = value.asText();

        if (text == null
                || text.isBlank()) {
            return null;
        }

        return text;
    }

    private Double doubleValue(
            JsonNode node,
            String field
    ) {

        JsonNode value = node.get(field);

        if (value == null
                || value.isNull()) {
            return null;
        }

        if (value.isNumber()) {
            return value.asDouble();
        }

        try {
            return Double.parseDouble(
                    value.asText()
            );
        } catch (Exception exception) {
            return null;
        }
    }

    private double haversineDistanceKm(
            double lat1,
            double lon1,
            double lat2,
            double lon2
    ) {

        final double earthRadiusKm = 6371.0088;

        double latitudeDifference =
                Math.toRadians(lat2 - lat1);

        double longitudeDifference =
                Math.toRadians(lon2 - lon1);

        double firstLatitude =
                Math.toRadians(lat1);

        double secondLatitude =
                Math.toRadians(lat2);

        double a =
                Math.sin(latitudeDifference / 2)
                        * Math.sin(latitudeDifference / 2)
                        +
                Math.cos(firstLatitude)
                        * Math.cos(secondLatitude)
                        * Math.sin(longitudeDifference / 2)
                        * Math.sin(longitudeDifference / 2);

        double c =
                2 * Math.atan2(
                        Math.sqrt(a),
                        Math.sqrt(1 - a)
                );

        return earthRadiusKm * c;
    }

    private double round(double value) {

        return Math.round(value * 10000.0)
                / 10000.0;
    }

    private static class CandidateStats {

        private final String feederName;
        private final String substationName;

        private final Set<String> supportingMeters =
                new HashSet<>();

        private double latitudeSum;
        private double longitudeSum;
        private int coordinateCount;

        private double coOccurrenceScore;
        private double distanceKm;
        private double proximityScore;
        private double topologyScore;

        private CandidateStats(
                String feederName,
                String substationName
        ) {

            this.feederName = feederName;
            this.substationName = substationName;
        }
    }
}