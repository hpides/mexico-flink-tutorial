package hpi.des.flink_tutorial.util.datatypes;

import org.apache.flink.api.java.tuple.Tuple5;

public class TupleExercise11 extends Tuple5<Long, Integer, String, Float, Float> {

    public TupleExercise11(){
        super();
    }

    public TupleExercise11(Long rideId, Integer passengerCnt, String paymentType, Float totalFare, Float tip){
        super(rideId, passengerCnt, paymentType, totalFare, tip);
    }

    public Long rideId() {return this.f0;}
    public Integer passengerCnt() {return this.f1;}
    public String paymentType() {return this.f2;}
    public Float totalFare() {return this.f3;}
    public Float tip() {return this.f4;}
}