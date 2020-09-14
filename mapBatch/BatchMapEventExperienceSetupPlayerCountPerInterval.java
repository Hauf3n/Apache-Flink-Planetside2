package org.myorg.project.mapBatch;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;

public class BatchMapEventExperienceSetupPlayerCountPerInterval implements MapFunction<Tuple4<String, String, String, Integer>, Tuple4<String, Integer, String, Integer>> {
    @Override
    public Tuple4<String, Integer, String, Integer> map(Tuple4<String, String, String, Integer> t) throws Exception {
        return new Tuple4<>(
                t.f0,
                1,
                t.f2,
                t.f3
        );
    }
}
