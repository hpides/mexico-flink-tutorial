package hpi.des.flink_tutorial;
import hpi.des.flink_tutorial.session3.Exercise11EqualToOperator;
import hpi.des.flink_tutorial.session3.Exercise11WhereOperator;
import hpi.des.flink_tutorial.session3.Exercise11WindowJoinOperator;
import hpi.des.flink_tutorial.session3.Exercise11WindowJoinProcessingOperator;
import hpi.des.flink_tutorial.session3.generator.datatypes.TaxiFare;
import hpi.des.flink_tutorial.session3.generator.datatypes.TaxiRide;

import junit.framework.TestCase;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;

public class TestSession3 extends TestCase {

    public void testExercise11Where() {
        try {
            KeySelector<TaxiRide, Long> selector = (KeySelector<TaxiRide, Long>) new Exercise11WhereOperator();
            TaxiRide ride = new TaxiRide(42, false);
            long result = selector.getKey(ride);
            assertEquals(42, result);
        }
        catch (Exception e){
            e.printStackTrace();
            fail();
        }
    }

    public void testExercise11EqualTo() {
        try {
            KeySelector<TaxiFare, Long> selector = (KeySelector<TaxiFare, Long>) new Exercise11EqualToOperator();
            TaxiFare fare = new TaxiFare(42);
            long result = selector.getKey(fare);
            assertEquals(42, result);
        }
        catch (Exception e){
            e.printStackTrace();
            fail();
        }
    }

    public void testExercise11WindoJoinOperator() {
        try {
            TumblingProcessingTimeWindows assigner =
                    (TumblingProcessingTimeWindows) Exercise11WindowJoinOperator.getWindow();

            assertEquals(Time.seconds(1).toMilliseconds(), assigner.getSize());
        }
        catch (Exception e){
            e.printStackTrace();
            fail();
        }
    }

    public void testExercise11WindoJoinProcessingOperator() {
        try {
            JoinFunction<TaxiRide, TaxiFare, Tuple5<Long, Short, String, Float, Float>> joinFunction =
                    (JoinFunction<TaxiRide, TaxiFare, Tuple5<Long, Short, String, Float, Float>>) new Exercise11WindowJoinProcessingOperator();
            TaxiRide ride = new TaxiRide(42, false);
            TaxiFare fare = new TaxiFare(42);

            Tuple5<Long, Short, String, Float, Float> result = joinFunction.join(ride, fare);

            assertSame(42L, result.f0);
            assertSame(ride.passengerCnt, result.f1);
            assertSame(fare.paymentType, result.f2);
            assertEquals(fare.totalFare, result.f3);
            assertEquals(fare.tip, result.f4);

        }
        catch (Exception e){
            e.printStackTrace();
            fail();
        }
    }

}
