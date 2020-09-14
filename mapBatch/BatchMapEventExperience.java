package org.myorg.project.mapBatch;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

public class BatchMapEventExperience implements MapFunction<JsonNode, Tuple4<String, String, String, Integer>> {
    @Override
    public Tuple4<String, String, String, Integer> map(JsonNode payload) throws Exception {
        return new Tuple4<>(
                payload.get("world_id").asText(),
                payload.get("character_id").asText(),
                payload.get("timestamp").asText(),
                payload.get("amount").asInt());
    }
}
