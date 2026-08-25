package com.esyasoft.eventintelligence;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;

public class ProcessedEventProjectionFunction
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

        ObjectNode source =
                (ObjectNode) root;

        ObjectNode output =
                objectMapper.createObjectNode();

        copyNullableText(source, output, "msn");
        copyNullableLong(source, output, "msn_id");
        copyNullableInt(source, output, "evnt_id");

        // Source field "ts" becomes ClickHouse field "event_ts"
        JsonNode tsNode = source.get("ts");

        if (tsNode == null || tsNode.isNull()) {
            output.putNull("event_ts");
        } else {
            output.put(
                    "event_ts",
                    tsNode.asText()
            );
        }

        copyNullableLong(
                source,
                output,
                "log_seq_no"
        );

        copyNullableDouble(source, output, "v_r");
        copyNullableDouble(source, output, "v_y");
        copyNullableDouble(source, output, "v_b");

        copyNullableText(
                source,
                output,
                "validation_status"
        );

        copyNullableText(
                source,
                output,
                "validation_reason"
        );

        copyNullableBoolean(
                source,
                output,
                "event_catalogue_match"
        );

        copyNullableInt(
                source,
                output,
                "event_tblrefid"
        );

        copyNullableText(
                source,
                output,
                "event_name"
        );

        copyNullableText(
                source,
                output,
                "event_state"
        );

        copyNullableBoolean(
                source,
                output,
                "isrestoration"
        );

        copyNullableText(
                source,
                output,
                "priorityname"
        );

        copyNullableText(
                source,
                output,
                "eventclassification_name"
        );

        copyNullableBoolean(
                source,
                output,
                "hierarchy_match"
        );

        copyNullableText(
                source,
                output,
                "msn_normalized"
        );

        copyNullableText(
                source,
                output,
                "dtr_name"
        );

        copyNullableText(
                source,
                output,
                "dtr_network_code"
        );

        copyNullableText(
                source,
                output,
                "feeder_name"
        );

        copyNullableText(
                source,
                output,
                "substation_name"
        );

        return objectMapper.writeValueAsString(
                output
        );
    }

    private void copyNullableText(
            ObjectNode source,
            ObjectNode output,
            String field
    ) {

        JsonNode node = source.get(field);

        if (node == null || node.isNull()) {
            output.putNull(field);
        } else {
            output.put(
                    field,
                    node.asText()
            );
        }
    }

    private void copyNullableInt(
            ObjectNode source,
            ObjectNode output,
            String field
    ) {

        JsonNode node = source.get(field);

        if (node == null || node.isNull()) {
            output.putNull(field);
        } else {
            output.put(
                    field,
                    node.asInt()
            );
        }
    }

    private void copyNullableLong(
            ObjectNode source,
            ObjectNode output,
            String field
    ) {

        JsonNode node = source.get(field);

        if (node == null || node.isNull()) {
            output.putNull(field);
        } else {
            output.put(
                    field,
                    node.asLong()
            );
        }
    }

    private void copyNullableDouble(
            ObjectNode source,
            ObjectNode output,
            String field
    ) {

        JsonNode node = source.get(field);

        if (node == null || node.isNull()) {
            output.putNull(field);
        } else {
            output.put(
                    field,
                    node.asDouble()
            );
        }
    }

    private void copyNullableBoolean(
            ObjectNode source,
            ObjectNode output,
            String field
    ) {

        JsonNode node = source.get(field);

        if (node == null || node.isNull()) {
            output.putNull(field);
        } else {
            output.put(
                    field,
                    node.asBoolean()
            );
        }
    }
}
