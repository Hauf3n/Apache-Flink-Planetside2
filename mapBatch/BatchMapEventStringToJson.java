package org.myorg.project.mapBatch;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

public class BatchMapEventStringToJson implements MapFunction<String, JsonNode> {
    @Override
    public JsonNode map(String s) throws Exception {
        try{
            ObjectMapper mapper = new ObjectMapper();
            return mapper.readValue(s, JsonNode.class);

        }catch (Exception e){
            e.printStackTrace();
            throw e;
        }
    }
}
