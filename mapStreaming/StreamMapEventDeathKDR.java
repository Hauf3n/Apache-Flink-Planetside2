package org.myorg.project.mapStreaming;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class StreamMapEventDeathKDR implements MapFunction<Tuple3<String, Integer, Integer>, Tuple2<String, Double>> {
    @Override
    public Tuple2<String, Double> map(Tuple3<String, Integer, Integer> t1) throws Exception {
        if(t1.f2 == 0){
            return new Tuple2<>(
                    t1.f0,
                    (double)t1.f1
            );
        }else{
            return new Tuple2<>(
                    t1.f0,
                    t1.f1/(double)t1.f2
            );
        }

    }
}
