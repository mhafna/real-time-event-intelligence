package com.esyasoft.eventintelligence;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.api.java.functions.KeySelector;

public class MeterKeySelector
        implements KeySelector<String, String> {

    private transient ObjectMapper objectMapper;

    @Override
    public String getKey(String value) throws Exception {

        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
        }

        JsonNode event =
                objectMapper.readTree(value);

        return event.get("msn")
                .asText()
                .trim();
    }
}
