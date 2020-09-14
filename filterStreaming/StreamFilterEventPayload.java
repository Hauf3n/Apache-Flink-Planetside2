package org.myorg.project.filterStreaming;

import org.apache.flink.api.common.functions.FilterFunction;

public class StreamFilterEventPayload implements FilterFunction<String> {
    @Override
    public boolean filter(String s) throws Exception {
        return s.contains("\"payload\":");
    }
}
