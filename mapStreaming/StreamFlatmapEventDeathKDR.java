package org.myorg.project.mapStreaming;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.util.Collector;

public class StreamFlatmapEventDeathKDR implements FlatMapFunction<JsonNode, Tuple3<String, Integer, Integer>> {
    @Override
    public void flatMap(JsonNode payload, Collector<Tuple3<String, Integer, Integer>> collector) throws Exception {
        collector.collect( new Tuple3<>(
                payload.get("attacker_character_id").asText(),
                1,
                0
        ));
        collector.collect( new Tuple3<>(
                payload.get("character_id").asText(),
                0,
                1
        ));
    }
}
