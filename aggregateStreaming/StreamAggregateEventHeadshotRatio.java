package org.myorg.project.aggregateStreaming;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class StreamAggregateEventHeadshotRatio implements AggregateFunction<Tuple2<String, Integer>, Tuple2<Integer, Integer>, Tuple3<Integer,Integer,Double>> {

    @Override
    public Tuple2<Integer, Integer> createAccumulator() {
        return new Tuple2<>(0,0);
    }

    @Override
    public Tuple2<Integer, Integer> add(Tuple2<String, Integer> in, Tuple2<Integer, Integer> acc) {
        if (in.f1 == 1){
            acc.f0 = acc.f0 + 1;
        }else {
            acc.f1 = acc.f1 + 1;
        }
        return  acc;
    }

    @Override
    public Tuple3<Integer,Integer,Double> getResult(Tuple2<Integer, Integer> acc) {
        return new Tuple3<>(acc.f0, acc.f0 + acc.f1, acc.f0 / ((double) acc.f0 + acc.f1));
    }

    @Override
    public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> acc2, Tuple2<Integer, Integer> acc1) {
        acc1.f0 = acc1.f0 + acc2.f0;
        acc1.f1 = acc1.f1 + acc2.f1;
        return acc1;
    }
}





