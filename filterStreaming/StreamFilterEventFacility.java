package org.myorg.project.filterStreaming;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

public class StreamFilterEventFacility implements FilterFunction<JsonNode> {
    @Override
    public boolean filter(JsonNode payload) throws Exception {
        return String.valueOf(payload.get("event_name")).contains("PlayerFacilityCapture") ||
                String.valueOf(payload.get("event_name")).contains("PlayerFacilityDefend");
    }
}
