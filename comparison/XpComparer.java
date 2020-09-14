package org.myorg.project.comparison;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

// XpComparer - get batch results out of the stream and subtract them to the stream results
// out: subtracted stream results

public class XpComparer extends RichFlatMapFunction<Tuple3<Integer, Integer, Double>, Tuple2<String, Double>> {

    private transient ValueState<Double> batchXp;

    @Override
    public void flatMap(Tuple3<Integer, Integer, Double> t, Collector<Tuple2<String, Double>> collector) throws Exception {

        if(t.f0 == 1){
            batchXp.update(t.f2);
        }
        else{
            Object batch_value = batchXp.value();

            if (batch_value == null){
                return;
            }

            collector.collect(
                    new Tuple2<>(
                            String.valueOf(t.f1),
                            t.f2-(double)batch_value
                    )
            );
        }
    }

    @Override
    public void open(Configuration config) {
        // obtain key-value state for prediction model
        ValueStateDescriptor<Double> descriptor =
                new ValueStateDescriptor<>(
                        // state name
                        "batch XP",
                        // type information of state
                        TypeInformation.of(Double.class));
        batchXp = getRuntimeContext().getState(descriptor);
    }
}
