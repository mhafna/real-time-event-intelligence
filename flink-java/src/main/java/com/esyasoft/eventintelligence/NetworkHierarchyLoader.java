package com.esyasoft.eventintelligence;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class NetworkHierarchyLoader {

    private static final String RESOURCE_PATH =
            "reference/network_hierarchy.csv";

    public static Map<String, NetworkReference> load()
            throws Exception {

        Map<String, NetworkReference> hierarchy =
                new HashMap<>();

        InputStream inputStream =
                NetworkHierarchyLoader.class
                        .getClassLoader()
                        .getResourceAsStream(RESOURCE_PATH);

        if (inputStream == null) {
            throw new IllegalStateException(
                    "Could not find resource: "
                            + RESOURCE_PATH
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
                        CSVParser.parse(
                                reader,
                                format
                        )
        ) {

            for (CSVRecord record : parser) {

                String normalizedMsn =
                        record.get("msn_normalized")
                                .trim()
                                .toUpperCase(Locale.ROOT);

                if (normalizedMsn.isEmpty()) {
                    continue;
                }

                NetworkReference reference =
                        new NetworkReference(
                                record.get("substation_name").trim(),
                                record.get("feeder_name").trim(),
                                record.get("dtr_name").trim(),
                                record.get("dtr_network_code").trim(),
                                record.get("msn").trim()
                        );

                hierarchy.put(
                        normalizedMsn,
                        reference
                );
            }
        }

        return hierarchy;
    }

    private NetworkHierarchyLoader() {
        // Utility class
    }
}
