package org.myorg.project.elasticSearchSinkFunction;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.HashMap;
import java.util.Map;

// https://training.ververica.com/exercises/toElastic.html

public class BasicElasticSearchInserter implements ElasticsearchSinkFunction<Tuple2<String, Double>> {
    @Override
    public void process(Tuple2<String, Double> t, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
        Map<String,String> json = new HashMap<>();

        json.put("eventname",t.f0);
        json.put("value",String.valueOf(t.f1));
        json.put("timestamp", String.valueOf(System.currentTimeMillis()));

        IndexRequest rqst = Requests.indexRequest()
                .index("planetside2")
                .type("metrics")
                .source(json);

        requestIndexer.add(rqst);
    }
}