package org.myorg.project.onlinePrediction;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

// ververica
// RichMapFunction to embed consensus trend model into Flink

public class ConsensusTrendPrediction extends RichMapFunction<Tuple2<String, Double>, Tuple2<String, Double>> {

    private transient ValueState<ConsensusTrendModel> modelState;

    @Override
    public Tuple2<String, Double> map(Tuple2<String, Double> t) throws Exception {

        ConsensusTrendModel model = modelState.value();
        if(model == null){
            model = new ConsensusTrendModel(7);
        }

        model.update(t.f1);
        modelState.update(model);

        return new Tuple2<>(t.f0,model.predictTrend());
    }

    @Override
    public void open(Configuration config) {
        // obtain key-value state for prediction model
        ValueStateDescriptor<ConsensusTrendModel> descriptor =
                new ValueStateDescriptor<>(
                        // state name
                        "consensus trend model",
                        // type information of state
                        TypeInformation.of(ConsensusTrendModel.class));
        modelState = getRuntimeContext().getState(descriptor);
    }
}
