package hpi.des.flink_tutorial.util;

import org.apache.flink.api.java.tuple.Tuple7;

public class TupleExercise12 extends Tuple7<Long, Short, String, Float, Float, Long, Long> {

    public TupleExercise12(){
        super();
    }

    public TupleExercise12(Long rideId, Short passengerCnt, String paymentType, Float totalFare, Float tip,
                           Long ingestionTime, Long emittingTime){
        super(rideId, passengerCnt, paymentType, totalFare, tip, ingestionTime, emittingTime);
    }

    public Long rideId() {return this.f0;}
    public Short passengerCnt() {return this.f1;}
    public String paymentType() {return this.f2;}
    public Float totalFare() {return this.f3;}
    public Float tip() {return this.f4;}
    public Long ingestionTime() {return this.f5;}
    public Long emittingTime() {return this.f6;}
}