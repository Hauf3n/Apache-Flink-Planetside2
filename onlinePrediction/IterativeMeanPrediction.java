package org.myorg.project.onlinePrediction;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

// ververica
// RichMapFunction to embed iterative mean model into Flink

public class IterativeMeanPrediction extends RichMapFunction<Tuple2<String, Double>, Tuple2<String, Double>> {

    private transient ValueState<IterativeMeanModel> modelState;

    @Override
    public Tuple2<String, Double> map(Tuple2<String, Double> t) throws Exception {

        IterativeMeanModel model = modelState.value();
        if(model == null){
            model = new IterativeMeanModel();
        }

        model.update(t.f1);
        modelState.update(model);

        return new Tuple2<>(t.f0,model.predict());
    }

    @Override
    public void open(Configuration config) {
        // obtain key-value state for prediction model
        ValueStateDescriptor<IterativeMeanModel> descriptor =
                new ValueStateDescriptor<>(
                        // state name
                        "iterative avg model",
                        // type information of state
                        TypeInformation.of(IterativeMeanModel.class));
        modelState = getRuntimeContext().getState(descriptor);
    }


}
