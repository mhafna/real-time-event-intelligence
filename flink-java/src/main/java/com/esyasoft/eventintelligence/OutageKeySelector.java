package com.esyasoft.eventintelligence;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.api.java.functions.KeySelector;

public class OutageKeySelector
        implements KeySelector<String, String> {

    private transient ObjectMapper objectMapper;

    @Override
    public String getKey(String value) throws Exception {

        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
        }

        JsonNode event =
                objectMapper.readTree(value);

        JsonNode msnNode =
                event.get("msn");

        JsonNode pairNode =
                event.get("event_tblrefid");

        String msn =
                msnNode == null || msnNode.isNull()
                        ? "UNKNOWN_MSN"
                        : msnNode.asText().trim();

        String pairId =
                pairNode == null || pairNode.isNull()
                        ? "UNKNOWN_EVENT"
                        : pairNode.asText();

        return msn + "|" + pairId;
    }
}
