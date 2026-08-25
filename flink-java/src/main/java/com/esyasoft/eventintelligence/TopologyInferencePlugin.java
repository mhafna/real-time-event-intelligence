package com.esyasoft.eventintelligence;

import org.apache.flink.streaming.api.datastream.DataStream;

public class TopologyInferencePlugin implements UseCasePlugin {

    @Override
    public String getName() {
        return "topology-inference";
    }

    @Override
    public DataStream<String> apply(
            DataStream<String> enrichedEvents
    ) {

        return enrichedEvents
                .keyBy(value -> "topology-global-key")
                .process(
                        new TopologyInferenceProcessFunction()
                )
                .name("Topology Candidate Inference");
    }
}