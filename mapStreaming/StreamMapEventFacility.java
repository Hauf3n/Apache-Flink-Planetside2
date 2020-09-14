package org.myorg.project.mapStreaming;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

public class StreamMapEventFacility implements MapFunction<JsonNode, Tuple4<String, String, Integer, Integer>> {
    @Override
    public Tuple4<String, String, Integer, Integer> map(JsonNode payload) throws Exception {
        return new Tuple4<>(
                payload.get("event_name").asText(),
                payload.get("facility_id").asText(),
                payload.get("world_id").asInt(),
                1);
    }
}
