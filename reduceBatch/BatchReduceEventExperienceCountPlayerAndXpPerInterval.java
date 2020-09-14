package org.myorg.project.reduceBatch;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple4;

public class BatchReduceEventExperienceCountPlayerAndXpPerInterval implements ReduceFunction<Tuple4<String, Integer, String, Integer>> {
    @Override
    public Tuple4<String, Integer, String, Integer> reduce(Tuple4<String, Integer, String, Integer> t2, Tuple4<String, Integer, String, Integer> t1) throws Exception {
        t1.f1 = t1.f1 + t2.f1;
        t1.f3 = t1.f3 + t2.f3;
        return t1;
    }
}
