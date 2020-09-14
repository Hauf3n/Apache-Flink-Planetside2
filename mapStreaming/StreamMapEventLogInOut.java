package org.myorg.project.mapStreaming;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

public class StreamMapEventLogInOut implements MapFunction< JsonNode, Tuple3<String, Integer, Integer>> {

    @Override
    public Tuple3<String, Integer, Integer> map(JsonNode payload) throws Exception {

        return new Tuple3<>( payload.get("event_name").asText(),
                payload.get("world_id").asInt(),
                1);

    }
}
