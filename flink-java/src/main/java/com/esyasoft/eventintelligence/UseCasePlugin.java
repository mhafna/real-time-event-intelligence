package com.esyasoft.eventintelligence;

import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * Standard contract for Event Intelligence use-case plugins.
 *
 * A use case receives the common enriched smart-meter event stream
 * and returns its own result stream.
 *
 * New use cases should implement this interface and be enabled
 * through configuration without modifying the core pipeline.
 */
public interface UseCasePlugin {

    /**
     * Unique human-readable name of the use case.
     */
    String getName();

    /**
     * Build the use-case processing logic on top of the
     * common enriched event stream.
     */
    DataStream<String> apply(
            DataStream<String> enrichedEvents
    );
}
