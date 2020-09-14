package org.myorg.project.reduceBatch;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

public class BatchReduceGroupAvgLogInOut implements GroupReduceFunction<Tuple4<String, String, Integer, Integer>, Tuple3<String, Integer, Double>> {
    @Override
    public void reduce(Iterable<Tuple4<String, String, Integer, Integer>> iterable, Collector<Tuple3<String, Integer, Double>> collector) throws Exception {
        int timesteps = 0;
        int logInOutSum = 0;
        String event = "";
        int world = -1;

        for (Tuple4 t : iterable){
            timesteps += 1;
            logInOutSum += (Integer)t.f3;
            event = (String)t.f0;
            world = (Integer)t.f2;
        }
        collector.collect(new Tuple3<>(
                event,
                world,
                logInOutSum/(double)timesteps
        ));
    }
}
