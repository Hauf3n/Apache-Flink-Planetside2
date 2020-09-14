package org.myorg.project.mapBatch;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class BatchMapEventExperienceXpAvgOfAllXpAvgIntervals implements MapFunction<Tuple3<String, Double, Integer>, Tuple2<String, Double>> {
    @Override
    public Tuple2<String, Double> map(Tuple3<String, Double, Integer> t) throws Exception {
        return new Tuple2<>(
                t.f0,
                t.f1/(double)t.f2
        );
    }
}
