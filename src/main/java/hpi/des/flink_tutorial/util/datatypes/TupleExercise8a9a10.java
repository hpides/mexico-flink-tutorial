package hpi.des.flink_tutorial.util.datatypes;

import org.apache.flink.api.java.tuple.Tuple4;

import java.time.LocalDateTime;

public class TupleExercise8a9a10 extends Tuple4<Integer, Double, Double, LocalDateTime> {

    public TupleExercise8a9a10(){
        super();
    }

    public TupleExercise8a9a10(Integer key, Double avgTip, Double sumTip, LocalDateTime windowStart){
        super(key, avgTip, sumTip, windowStart);
    }

    public Integer key() {return this.f0;}
    public Double avgTip() {return this.f1;}
    public Double sumTip() {return this.f2;}
    public LocalDateTime windowStart() {return this.f3;}
}