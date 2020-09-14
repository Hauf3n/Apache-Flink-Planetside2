package org.myorg.project.reduceBatch;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class BatchReduceEventExperienceSumAllAvgXpAndIntervals implements ReduceFunction<Tuple3<String, Double, Integer>> {
    @Override
    public Tuple3<String, Double, Integer> reduce(Tuple3<String, Double, Integer> t2, Tuple3<String, Double, Integer> t1) throws Exception {
        t1.f1 = t1.f1 + t2.f1;
        t1.f2 = t1.f2 + t2.f2;
        return t1;
    }
}
