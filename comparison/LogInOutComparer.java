package org.myorg.project.comparison;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

// LogInOutComparer - get batch results out of the stream and subtract them to the stream results
// out: subtracted stream results

public class LogInOutComparer extends RichFlatMapFunction<Tuple4<Integer, String, Integer, Double>, Tuple3<String, Integer, Double>> {

    private transient ValueState<Double> batchLogInOut;

    @Override
    public void flatMap(Tuple4<Integer, String, Integer, Double> t, Collector<Tuple3<String, Integer, Double>> collector) throws Exception {

        if(t.f0 == 1){
            batchLogInOut.update(t.f3);
        }
        else{
            Object batch_value = batchLogInOut.value();

            if (batch_value == null){
                return;
            }

            collector.collect(
                    new Tuple3<>(
                            t.f1,
                            t.f2,
                            t.f3-(double)batch_value
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
                        "batch logInOuts",
                        // type information of state
                        TypeInformation.of(Double.class));
        batchLogInOut = getRuntimeContext().getState(descriptor);
    }

}
