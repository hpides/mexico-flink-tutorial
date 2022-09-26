package hpi.des.flink_tutorial;

import hpi.des.flink_tutorial.session3.Exercise11EqualToOperator;
import hpi.des.flink_tutorial.session3.Exercise11WhereOperator;
import hpi.des.flink_tutorial.session3.Exercise11WindowJoinOperator;
import hpi.des.flink_tutorial.session3.Exercise11WindowJoinProcessingOperator;
import hpi.des.flink_tutorial.util.datatypes.TaxiFareTuple;
import hpi.des.flink_tutorial.util.datatypes.TaxiRideTuple;
import hpi.des.flink_tutorial.util.datatypes.TupleExercise11;
import junit.framework.TestCase;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;

public class TestSession3 extends TestCase {

    public void testExercise11KeyByTaxiRide() {
        try {
            KeySelector<TaxiRideTuple, Long> selector = (KeySelector<TaxiRideTuple, Long>) new Exercise11WhereOperator();
            TaxiRideTuple ride = new TaxiRideTuple(42, false);
            long result = selector.getKey(ride);
            assertEquals(result, 42);
        }
        catch (Exception e){
            e.printStackTrace();
            fail();
        }
    }

    public void testExercise11KeyByTaxiFare() {
        try {
            KeySelector<TaxiFareTuple, Long> selector = (KeySelector<TaxiFareTuple, Long>) new Exercise11EqualToOperator();
            TaxiFareTuple fare = new TaxiFareTuple(42);
            long result = selector.getKey(fare);
            assertEquals(result, 42);
        }
        catch (Exception e){
            e.printStackTrace();
            fail();
        }
    }

    public void testExercise11WindowJoinOperator() {
        try {
            TumblingProcessingTimeWindows assigner =
                    (TumblingProcessingTimeWindows) Exercise11WindowJoinOperator.getWindow();

            assertEquals(assigner.getSize(), Time.seconds(1).toMilliseconds());
        }
        catch (Exception e){
            e.printStackTrace();
            fail();
        }
    }

    public void testExercise11WindowJoinProcessingOperator() {
        try {
            JoinFunction<TaxiRideTuple, TaxiFareTuple, TupleExercise11> joinFunction =
                    (JoinFunction<TaxiRideTuple, TaxiFareTuple, TupleExercise11>) new Exercise11WindowJoinProcessingOperator();
            TaxiRideTuple ride = new TaxiRideTuple(42, false);
            TaxiFareTuple fare = new TaxiFareTuple(42);

            Tuple5<Long, Integer, String, Float, Float> result = joinFunction.join(ride, fare);

            assertSame(result.f0, 42L);
            assertSame(result.f1, ride.f3);
            assertSame(result.f2, fare.paymentType);
            assertEquals(result.f3, fare.totalFare);
            assertEquals(result.f4, fare.tip);

        }
        catch (Exception e){
            e.printStackTrace();
            fail();
        }
    }

}
