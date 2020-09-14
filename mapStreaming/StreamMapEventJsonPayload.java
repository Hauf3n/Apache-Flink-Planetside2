package org.myorg.project.mapStreaming;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

public class StreamMapEventJsonPayload implements MapFunction<String, JsonNode> {

    @Override
    public JsonNode map(String s) throws Exception {
        try{
            ObjectMapper mapper = new ObjectMapper();
            JsonNode msg = mapper.readValue(s, JsonNode.class);
            return mapper.readValue( String.valueOf(msg.get("payload")), JsonNode.class);

        }catch (Exception e){
            e.printStackTrace();
            throw e;
        }
    }
}
