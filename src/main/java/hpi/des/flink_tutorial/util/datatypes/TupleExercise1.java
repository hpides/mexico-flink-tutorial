package hpi.des.flink_tutorial.util.datatypes;

import org.apache.flink.api.java.tuple.Tuple7;

public class TupleExercise1 extends Tuple7<Long, Long, Integer, Integer, Integer, Double, Double> {

    public TupleExercise1(){
        super();
    }

    public TupleExercise1(Long tpep_pickup_datetime, Long tpep_dropoff_datetime,
                          Integer passenger_count, Integer ratecodeID, Integer payment_type, Double Tip_amount,
                          Double Total_amount){
        super(tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, ratecodeID, payment_type, Tip_amount,
                Total_amount);
    }

    public Long tpep_pickup_datetime() {return this.f0;}
    public Long tpep_dropoff_datetime() {return this.f1;}
    public Integer passenger_count() {return this.f2;}
    public Integer ratecodeID() {return this.f3;}
    public Integer payment_type() {return this.f4;}
    public Double Tip_amount() {return this.f5;}
    public Double Total_amount() {return this.f6;}
}