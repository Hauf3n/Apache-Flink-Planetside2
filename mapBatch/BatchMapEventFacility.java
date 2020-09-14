package org.myorg.project.mapBatch;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

public class BatchMapEventFacility implements MapFunction<JsonNode, Tuple5<String, String, Integer, String, Integer>> {
    @Override
    public Tuple5<String, String, Integer, String, Integer> map(JsonNode payload) throws Exception {
        return new Tuple5<>(
                payload.get("event_name").asText(),
                payload.get("facility_id").asText(),
                payload.get("world_id").asInt(),
                payload.get("timestamp").asText(),
                1);
    }
}
