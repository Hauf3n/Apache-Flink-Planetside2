package org.myorg.project.aggregateStreaming;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.aggregation.AggregationFunction;

public class StreamAggregateEventExperience extends AggregationFunction<Tuple3<String, String, Integer>> implements AggregateFunction<Tuple3<String,String,Integer>,Tuple3<Integer,Integer,Integer>,Tuple3<Integer,Integer,Double>> {

    @Override
    public Tuple3<Integer, Integer, Integer> createAccumulator() {
        return new Tuple3<>(-1,0,0);
    }

    @Override
    public Tuple3<Integer, Integer, Integer> add(Tuple3<String, String, Integer> t1, Tuple3<Integer, Integer, Integer> acc) {

        acc.f0 = Integer.valueOf(t1.f0);
        acc.f1 = acc.f1 + 1;
        acc.f2 = acc.f2 + t1.f2;
        return acc;
    }

    @Override
    public Tuple3<Integer, Integer, Double> getResult(Tuple3<Integer, Integer, Integer> acc) {

        return new Tuple3<>(
                acc.f0,
                acc.f1,
                (double)acc.f2/(double)acc.f1
        );
    }

    @Override
    public Tuple3<Integer, Integer, Integer> merge(Tuple3<Integer, Integer, Integer> acc2, Tuple3<Integer, Integer, Integer> acc1) {
        System.out.println("MERGE");
        acc1.f1 = acc1.f1 + acc2.f1;
        acc1.f2 = acc1.f2 + acc2.f2;
        return acc1;
    }

    @Override
    public Tuple3<String, String, Integer> reduce(Tuple3<String, String, Integer> stringStringIntegerTuple3, Tuple3<String, String, Integer> t1) throws Exception {
        System.err.println("reduce not implemented?");
        return null;
    }
}
