package hpi.des.flink_tutorial.util;

import org.apache.flink.api.java.tuple.Tuple9;

import java.time.LocalDateTime;

public class TupleExercise4 extends Tuple9<LocalDateTime, LocalDateTime, Integer, Integer, Integer, Double, Double,
        Double, Double> {

    public TupleExercise4(){
        super();
    }

    public TupleExercise4(LocalDateTime tpep_pickup_datetime, LocalDateTime tpep_dropoff_datetime,
                          Integer passenger_count, Integer ratecodeID, Integer payment_type, Double Tip_amount,
                          Double Total_amount, Double tipRatio, Double tripRatioPerPassenger){
        super(tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, ratecodeID, payment_type, Tip_amount,
                Total_amount, tipRatio, tripRatioPerPassenger);
    }

    public TupleExercise4(TupleExercise3 tuple, Double tripRatioPerPassenger){
        super(tuple.tpep_pickup_datetime(), tuple.tpep_dropoff_datetime(), tuple.passenger_count(),
                tuple.ratecodeID(), tuple.payment_type(), tuple.Tip_amount(), tuple.Total_amount(),
                tuple.tipRatio(), tripRatioPerPassenger);
    }

    public LocalDateTime tpep_pickup_datetime() {return this.f0;}
    public LocalDateTime tpep_dropoff_datetime() {return this.f1;}
    public Integer passenger_count() {return this.f2;}
    public Integer ratecodeID() {return this.f3;}
    public Integer payment_type() {return this.f4;}
    public Double Tip_amount() {return this.f5;}
    public Double Total_amount() {return this.f6;}
    public Double tipRatio() {return this.f7;}
    public Double tripRatioPerPassenger() {return this.f8;}
}