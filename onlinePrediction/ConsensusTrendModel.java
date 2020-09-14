package org.myorg.project.onlinePrediction;


import sun.misc.Queue;
import java.util.Enumeration;

/*

    ConsensusTrendModel - predict future trend,
    if future values will be greater than a certain baseline.

    here: static baseline=0 in order to apply it to the compared stream batch metrics,
    where one just subtracts stream-batch values.

 */

public class ConsensusTrendModel {

    private double baseline;
    private Queue<Double> values;
    private int queueSizeLimit;
    private int queueSize;

    public ConsensusTrendModel(int sizeLimit){

        baseline = 0;
        values = new Queue<>();
        queueSizeLimit = sizeLimit;
        queueSize = 0;
    }

    public void update(double value) throws InterruptedException {

        if(queueSize < queueSizeLimit){
            values.enqueue(value);
            queueSize  += 1;
        }
        else{
            values.dequeue();
            values.enqueue(value);
        }
    }

    public double predictTrend(){

        double trend = 0;

        if(queueSize < queueSizeLimit){
            return 0;
        }

        Enumeration<Double> queue_values = values.elements();
        while(queue_values.hasMoreElements()){
            double element = queue_values.nextElement();
            if(baseline > element){
                trend -= 1;
            }
            else{
                trend += 1;
            }
        }

        if(trend <= 0){
            return trend;
        }
        else{
            return trend;
        }
    }

}
