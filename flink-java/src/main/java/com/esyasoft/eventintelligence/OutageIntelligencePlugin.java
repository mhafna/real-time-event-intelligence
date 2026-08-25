package com.esyasoft.eventintelligence;

import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * Use-case plugin for stateful outage intelligence.
 *
 * Reuses the proven outage correlation logic:
 * occurrence -> open incident
 * restoration -> close incident
 */
public class OutageIntelligencePlugin
        implements UseCasePlugin {

    @Override
    public String getName() {
        return "outage-intelligence";
    }

    @Override
    public DataStream<String> apply(
            DataStream<String> enrichedEvents
    ) {

        return enrichedEvents
                .keyBy(
                        new OutageKeySelector()
                )
                .process(
                        new OutageProcessFunction()
                )
                .name(
                        "Outage Intelligence Plugin"
                );
    }
}
