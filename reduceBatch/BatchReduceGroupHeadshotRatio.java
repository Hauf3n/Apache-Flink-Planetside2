package org.myorg.project.reduceBatch;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

public class BatchReduceGroupHeadshotRatio implements GroupReduceFunction<Tuple2<String, Integer>, Tuple3<Integer, Integer, Double>> {

    @Override
    public void reduce(Iterable<Tuple2<String, Integer>> iterable, Collector<Tuple3<Integer, Integer, Double>> collector) throws Exception {

        int numDeath = 0;
        int numHeadshot = 0;

        for(Tuple2 t : iterable){
            numDeath += 1;
            numHeadshot += (Integer)t.f1;
        }

        collector.collect(new Tuple3<>(
                numHeadshot,
                numDeath,
                numHeadshot/(double)numDeath
        ));
    }
}
