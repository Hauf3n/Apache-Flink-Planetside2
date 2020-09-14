package org.myorg.project.mapStreaming;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

public class StreamMapEventExperience implements MapFunction<JsonNode, Tuple3<String, String, Integer>> {
    @Override
    public Tuple3<String, String, Integer> map(JsonNode payload) throws Exception {
        return new Tuple3<>(
                payload.get("world_id").asText(),
                payload.get("character_id").asText(),
                payload.get("amount").asInt());
    }
}
