package org.myorg.project.mapBatch;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

public class BatchMapEventExperienceAvgXpPerInterval implements MapFunction<Tuple4<String, Integer, String, Integer>, Tuple3<String, Double, Integer>> {
    @Override
    public Tuple3<String, Double, Integer> map(Tuple4<String, Integer, String, Integer> t) throws Exception {
        return new Tuple3<>(
                t.f0,
                (Integer)t.f3 /(double) t.f1,
                1
        );
    }
}
