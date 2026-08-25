package com.esyasoft.eventintelligence;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;

import java.util.Locale;
import java.util.Map;

public class EventEnrichmentFunction
        extends RichMapFunction<String, String> {

    private transient Map<Integer, EventReference> eventCatalogue;
    private transient Map<String, NetworkReference> networkHierarchy;
    private transient ObjectMapper objectMapper;

    @Override
    public void open(OpenContext openContext) throws Exception {

        objectMapper = new ObjectMapper();

        eventCatalogue =
                EventCatalogueLoader.load();

        networkHierarchy =
                NetworkHierarchyLoader.load();

        System.out.println(
                "[FLINK ENRICHMENT] Loaded "
                        + eventCatalogue.size()
                        + " event catalogue records."
        );

        System.out.println(
                "[FLINK ENRICHMENT] Loaded "
                        + networkHierarchy.size()
                        + " network hierarchy records."
        );
    }

    @Override
    public String map(String value) throws Exception {

        JsonNode root =
                objectMapper.readTree(value);

        if (!root.isObject()) {
            return value;
        }

        ObjectNode event =
                (ObjectNode) root;

        enrichEventCatalogue(event);

        enrichNetworkHierarchy(event);

        return objectMapper.writeValueAsString(event);
    }

    private void enrichEventCatalogue(ObjectNode event) {

        JsonNode eventIdNode =
                event.get("evnt_id");

        if (eventIdNode == null
                || eventIdNode.isNull()) {

            addMissingEventCatalogueFields(event);
            return;
        }

        int eventId =
                eventIdNode.asInt();

        EventReference reference =
                eventCatalogue.get(eventId);

        if (reference == null) {

            addMissingEventCatalogueFields(event);
            return;
        }

        event.put(
                "event_catalogue_match",
                true
        );

        event.put(
                "event_tblrefid",
                reference.getEventTblRefId()
        );

        event.put(
                "event_name",
                reference.getEventName()
        );

        event.put(
                "event_state",
                reference.getEventState()
        );

        event.put(
                "isrestoration",
                reference.isRestoration()
        );

        event.put(
                "priorityname",
                reference.getPriorityName()
        );

        event.put(
                "eventclassification_name",
                reference.getClassificationName()
        );
    }

    private void addMissingEventCatalogueFields(
            ObjectNode event
    ) {

        event.put(
                "event_catalogue_match",
                false
        );

        event.putNull("event_tblrefid");
        event.putNull("event_name");
        event.putNull("event_state");
        event.putNull("isrestoration");
        event.putNull("priorityname");
        event.putNull(
                "eventclassification_name"
        );
    }

    private void enrichNetworkHierarchy(
            ObjectNode event
    ) {

        JsonNode msnNode =
                event.get("msn");

        if (msnNode == null
                || msnNode.isNull()) {

            addMissingHierarchyFields(event);
            return;
        }

        String msn =
                msnNode.asText().trim();

        if (msn.isEmpty()) {

            addMissingHierarchyFields(event);
            return;
        }

        String normalizedMsn =
                msn.toUpperCase(Locale.ROOT);

        NetworkReference reference =
                networkHierarchy.get(normalizedMsn);

        if (reference == null) {

            addMissingHierarchyFields(event);
            return;
        }

        event.put(
                "hierarchy_match",
                true
        );

        event.put(
                "msn_normalized",
                normalizedMsn
        );

        event.put(
                "dtr_name",
                reference.getDtrName()
        );

        event.put(
                "dtr_network_code",
                reference.getDtrNetworkCode()
        );

        event.put(
                "feeder_name",
                reference.getFeederName()
        );

        event.put(
                "substation_name",
                reference.getSubstationName()
        );
    }

    private void addMissingHierarchyFields(
            ObjectNode event
    ) {

        event.put(
                "hierarchy_match",
                false
        );

        event.putNull("msn_normalized");
        event.putNull("dtr_name");
        event.putNull("dtr_network_code");
        event.putNull("feeder_name");
        event.putNull("substation_name");
    }
}
