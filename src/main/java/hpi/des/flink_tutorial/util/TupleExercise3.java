package hpi.des.flink_tutorial.util;

import org.apache.flink.api.java.tuple.Tuple8;

import java.time.LocalDateTime;

public class TupleExercise3 extends Tuple8<LocalDateTime, LocalDateTime, Integer, Integer, Integer, Double, Double,
        Double> {

    public TupleExercise3(){
        super();
    }

    public TupleExercise3(LocalDateTime tpep_pickup_datetime, LocalDateTime tpep_dropoff_datetime,
                          Integer passenger_count, Integer ratecodeID, Integer payment_type, Double Tip_amount,
                          Double Tolls_amount, Double tipRatio){
        super(tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, ratecodeID, payment_type, Tip_amount,
                Tolls_amount, tipRatio);
    }

    public LocalDateTime tpep_pickup_datetime() {return this.f0;}
    public LocalDateTime tpep_dropoff_datetime() {return this.f1;}
    public Integer passenger_count() {return this.f2;}
    public Integer ratecodeID() {return this.f3;}
    public Integer payment_type() {return this.f4;}
    public Double Tip_amount() {return this.f5;}
    public Double Tolls_amount() {return this.f6;}
    public Double tipRatio() {return this.f7;}
}