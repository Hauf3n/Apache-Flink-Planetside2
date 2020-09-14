package org.myorg.project.onlinePrediction;

// IterativeMeanModel - predict next value based on current mean of given values

public class IterativeMeanModel {

    private int t;
    private double x_t;

    public IterativeMeanModel(){
        t = 0;
        x_t = 0.0;
    }

    public void update(double nextValue){
        t += 1;
        x_t = (t-1)*x_t/t + nextValue/t;
    }

    public double predict(){
        return x_t;
    }
}
