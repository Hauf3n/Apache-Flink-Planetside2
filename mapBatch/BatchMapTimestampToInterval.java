package org.myorg.project.mapBatch;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

public class BatchMapTimestampToInterval implements MapFunction<JsonNode, JsonNode> {

    private int interval;

    public BatchMapTimestampToInterval(int timeGroupIntervalSeconds) {
        interval = timeGroupIntervalSeconds;
    }

    @Override
    public JsonNode map(JsonNode jsonNode) throws Exception {

        long timestamp = jsonNode.get("timestamp").asLong();
        long new_timestamp = (long)(timestamp/(double)interval);
        ((ObjectNode)jsonNode).put("timestamp",String.valueOf(new_timestamp));
        return jsonNode;
    }
}
