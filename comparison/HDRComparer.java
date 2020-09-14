package org.myorg.project.comparison;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

// HDRComparer - get batch results out of the stream and subtract them to the stream results
// out: subtracted stream results

public class HDRComparer extends RichFlatMapFunction<Tuple3<String, Integer, Double>, Tuple1<Double>> {

    private transient ValueState<Double> batchHDR;

    @Override
    public void flatMap(Tuple3<String, Integer, Double> t, Collector<Tuple1<Double>> collector) throws Exception {

        if(t.f1 == 1){
            batchHDR.update(t.f2);
        }
        else{
            Object batch_value = batchHDR.value();

            if (batch_value == null){
                return;
            }

            collector.collect(
                    new Tuple1<>(
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
                        "batch HDR",
                        // type information of state
                        TypeInformation.of(Double.class));
        batchHDR = getRuntimeContext().getState(descriptor);
    }
}
