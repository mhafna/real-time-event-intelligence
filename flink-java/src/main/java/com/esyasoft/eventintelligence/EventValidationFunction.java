package com.esyasoft.eventintelligence;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;

public class EventValidationFunction
        extends RichMapFunction<String, String> {

    private transient ObjectMapper objectMapper;

    @Override
    public void open(OpenContext openContext) {
        objectMapper = new ObjectMapper();
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

        JsonNode msnNode =
                event.get("msn");

        boolean invalidMsn =
                msnNode == null
                || msnNode.isNull()
                || msnNode.asText().trim().isEmpty();

        if (invalidMsn) {

            event.put(
                    "validation_status",
                    "INVALID"
            );

            event.put(
                    "validation_reason",
                    "MSN is null or blank"
            );

        } else {

            event.put(
                    "validation_status",
                    "VALID"
            );

            event.putNull(
                    "validation_reason"
            );
        }

        return objectMapper.writeValueAsString(
                event
        );
    }
}
