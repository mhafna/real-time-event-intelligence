package com.esyasoft.eventintelligence;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class EventCatalogueLoader {

    private static final String RESOURCE_PATH =
            "reference/event_catalogue.csv";

    public static Map<Integer, EventReference> load() throws Exception {

        Map<Integer, EventReference> catalogue = new HashMap<>();

        InputStream inputStream =
                EventCatalogueLoader.class
                        .getClassLoader()
                        .getResourceAsStream(RESOURCE_PATH);

        if (inputStream == null) {
            throw new IllegalStateException(
                    "Could not find resource: " + RESOURCE_PATH
            );
        }

        CSVFormat format =
                CSVFormat.DEFAULT
                        .builder()
                        .setHeader()
                        .setSkipHeaderRecord(true)
                        .get();

        try (
                Reader reader =
                        new InputStreamReader(
                                inputStream,
                                StandardCharsets.UTF_8
                        );

                CSVParser parser =
                        CSVParser.parse(reader, format)
        ) {

            for (CSVRecord record : parser) {

                int eventId =
                        Integer.parseInt(
                                record.get("event_id").trim()
                        );

                int eventTblRefId =
                        Integer.parseInt(
                                record.get("event_tblrefid").trim()
                        );

                String eventName =
                        record.get("event_name").trim();

                String eventState =
                        record.get("event_state").trim();

                boolean restoration =
                        Boolean.parseBoolean(
                                record.get("isrestoration").trim()
                        );

                String priorityName =
                        record.get("priorityname").trim();

                String classificationName =
                        record.get(
                                "eventclassification_name"
                        ).trim();

                EventReference reference =
                        new EventReference(
                                eventId,
                                eventTblRefId,
                                eventName,
                                eventState,
                                restoration,
                                priorityName,
                                classificationName
                        );

                catalogue.put(eventId, reference);
            }
        }

        return catalogue;
    }

    private EventCatalogueLoader() {
        // Utility class
    }
}
