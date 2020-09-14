package org.myorg.project.reduceStream;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class StreamReduceEventDeathKDR implements ReduceFunction<Tuple3<String, Integer, Integer>> {
    @Override
    public Tuple3<String, Integer, Integer> reduce(Tuple3<String, Integer, Integer> t2, Tuple3<String, Integer, Integer> t1) throws Exception {
        t1.f1 = t1.f1 + t2.f1;
        t1.f2 = t1.f2 + t2.f2;
        return t1;
    }
}
