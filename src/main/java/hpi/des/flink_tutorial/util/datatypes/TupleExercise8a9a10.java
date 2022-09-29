package hpi.des.flink_tutorial.util.datatypes;

import org.apache.flink.api.java.tuple.Tuple4;

public class TupleExercise8a9a10 extends Tuple4<Integer, Double, Double, Long> {

    public TupleExercise8a9a10(){
        super();
    }

    public TupleExercise8a9a10(Integer key, Double avgTip, Double sumTip, Long windowStart){
        super(key, avgTip, sumTip, windowStart);
    }

    public Integer key() {return this.f0;}
    public Double avgTip() {return this.f1;}
    public Double sumTip() {return this.f2;}
    public Long windowStart() {return this.f3;}
}