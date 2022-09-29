package hpi.des.flink_tutorial.util.datatypes;

import org.apache.flink.api.java.tuple.Tuple5;

public class TupleExercise7 extends Tuple5<Integer, Long, Integer, Long, Double> {

    public TupleExercise7(){
        super();
    }

    public TupleExercise7(Integer PULocationID, Long tpep_pickup_datetime, Integer DOLocationID,
                          Long tpep_dropoff_datetime, Double tipRatioPerPassenger){
        super(PULocationID, tpep_pickup_datetime, DOLocationID, tpep_dropoff_datetime, tipRatioPerPassenger);
    }

    public Integer PULocationID() {return this.f0;}
    public Long tpep_pickup_datetime() {return this.f1;}
    public Integer DOLocationID() {return this.f2;}
    public Long tpep_dropoff_datetime() {return this.f3;}
    public Double tipRatioPerPassenger() {return this.f4;}
}