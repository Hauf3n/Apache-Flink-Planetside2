package org.myorg.project.mapStreaming;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;


public class StreamMapEventHeadshot implements MapFunction<JsonNode, Tuple2<String, Integer>> {

    @Override
    public Tuple2<String, Integer> map(JsonNode payload) throws Exception {
        if (payload.get("is_headshot").asInt() == 1){
            return new Tuple2<>("headshot", 1);
        }
        else{
            return new Tuple2<>("headshot", 0);
        }
    }
}
