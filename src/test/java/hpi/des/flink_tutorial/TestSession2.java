package hpi.des.flink_tutorial;
import hpi.des.flink_tutorial.session2.*;
import hpi.des.flink_tutorial.util.DateParser;
import hpi.des.flink_tutorial.util.datatypes.TupleExercise7;
import hpi.des.flink_tutorial.util.datatypes.TupleExercise8a9a10;
import junit.framework.TestCase;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collection;

public class TestSession2 extends TestCase {
    LocalDateTime dateTime1;
    LocalDateTime dateTime2;

    protected void setUp() {
        LocalDate date1 = LocalDate.of(2020, 4, 1);
        LocalTime time1 = LocalTime.of(0, 41, 22);
        this.dateTime1 = LocalDateTime.of(date1, time1);

        LocalDate date2 = LocalDate.of(2020, 4, 1);
        LocalTime time2 = LocalTime.of(1, 1, 53);
        this.dateTime2 = LocalDateTime.of(date2, time2);
    }

    public void testExercise7() {
        try {
            KeySelector<TupleExercise7, Integer> keySelector =
                (KeySelector<TupleExercise7, Integer>) new Exercise7Operator();

            TupleExercise7 event = new TupleExercise7(42, this.dateTime1, 13, this.dateTime2, 0.5);
            int result = keySelector.getKey(event);
            assertEquals(result, 42);

        }
        catch (Exception e){
            e.printStackTrace();
            fail();
        }
    }

    public void testExercise8a() {
        try {
            TumblingEventTimeWindows assigner = (TumblingEventTimeWindows) Exercise8WindowOperator.getWindow();

            WindowAssigner.WindowAssignerContext ctx = new WindowAssigner.WindowAssignerContext() {
                @Override
                public long getCurrentProcessingTime() {
                    return 1;
                }
            };

            Collection<TimeWindow> window1 = assigner.assignWindows(2, 1, ctx);

            assertEquals(window1.size(), 1);
            TimeWindow w = window1.iterator().next();
            assertEquals(w.getStart(), 0);
            assertEquals(w.getEnd(), Time.hours(1).toMilliseconds());

        }
        catch (Exception e){
            e.printStackTrace();
            fail();
        }
    }

    public void testExercise8b() {
        try {
            WindowFunction<TupleExercise7, TupleExercise8a9a10, Integer, TimeWindow> applyFunction =
                    (WindowFunction<TupleExercise7, TupleExercise8a9a10, Integer, TimeWindow>)new Exercise8WindowProcessingOperator();

            TimeWindow window = new TimeWindow(0, 1);

            TupleExercise7 event1 = new TupleExercise7(42, this.dateTime1, 13, this.dateTime2, 1.);
            TupleExercise7 event2 = new TupleExercise7(42, this.dateTime1, 13, this.dateTime2, 4.);

            ArrayList<TupleExercise7> events = new ArrayList<>();
            events.add(event1);
            events.add(event2);

            ArrayList<TupleExercise8a9a10> out = new ArrayList<>();
            ListCollector<TupleExercise8a9a10> collector = new ListCollector<>(out);

            applyFunction.apply(42, window, events, collector) ;
            assertEquals(out.size(), 1);
            assertSame(out.get(0).f0, 42);
            assertEquals(out.get(0).f1, 2.5);
            assertEquals(out.get(0).f2, 5.);
            assertTrue(out.get(0).f3.isEqual(DateParser.millisecondsToLocalDateTime(0)));

        }
        catch (Exception e){
            e.printStackTrace();
            fail();
        }
    }

    public void testExercise9a() {
        try {
            KeySelector<TupleExercise8a9a10, Integer> keySelector =
                    (KeySelector<TupleExercise8a9a10, Integer>) new Exercise9KeyByOperator();

            TupleExercise8a9a10 event = new TupleExercise8a9a10(42, 2.5, 5., dateTime1);
            int result = keySelector.getKey(event);
            assertEquals(result, 42);

        }
        catch (Exception e){
            e.printStackTrace();
            fail();
        }
    }

    public void testExercise9b() {
        try {
            TumblingEventTimeWindows assigner = (TumblingEventTimeWindows) Exercise9WindowOperator.getWindow();

            WindowAssigner.WindowAssignerContext ctx = new WindowAssigner.WindowAssignerContext() {
                @Override
                public long getCurrentProcessingTime() {
                    return 1;
                }
            };

            Collection<TimeWindow> window1 = assigner.assignWindows(2, 1, ctx);

            assertEquals(window1.size(), 1);
            TimeWindow w = window1.iterator().next();
            assertEquals(w.getStart(), 0);
            assertEquals(w.getEnd(), Time.hours(24).toMilliseconds());

        }
        catch (Exception e){
            e.printStackTrace();
            fail();
        }
    }

    public void testExercise9c() {
        try {
            ReduceFunction<TupleExercise8a9a10> reduceFunction =
                    (ReduceFunction<TupleExercise8a9a10>)new Exercise9WindowProcessingOperator();

            TupleExercise8a9a10 event1 = new TupleExercise8a9a10(42, 2.5, 5., this.dateTime1);
            TupleExercise8a9a10 event2 = new TupleExercise8a9a10(42, 3., 10., this.dateTime2);
            TupleExercise8a9a10 event3 = new TupleExercise8a9a10(42, 1.0, 2., this.dateTime2);

            TupleExercise8a9a10 reduce = reduceFunction.reduce(event1, event2); // 42, 3., 15., this.dateTime2
            TupleExercise8a9a10 result = reduceFunction.reduce(reduce, event3); // 42, 3., 17., this.dateTime2

            assertSame(result.f0, 42);
            assertEquals(result.f1, 3.);
            assertEquals(result.f2, 17.);
            assertTrue(result.f3.isEqual(this.dateTime2));

        }
        catch (Exception e){
            e.printStackTrace();
            fail();
        }
    }

    public void testExercise10a() {
        try {
            TumblingEventTimeWindows assigner = (TumblingEventTimeWindows) Exercise10WindowOperator.getWindow();

            WindowAssigner.WindowAssignerContext ctx = new WindowAssigner.WindowAssignerContext() {
                @Override
                public long getCurrentProcessingTime() {
                    return 1;
                }
            };

            Collection<TimeWindow> window1 = assigner.assignWindows(2, 1, ctx);

            assertEquals(window1.size(), 1);
            TimeWindow w = window1.iterator().next();
            assertEquals(w.getStart(), 0);
            assertEquals(w.getEnd(), Time.hours(24).toMilliseconds());

        }
        catch (Exception e){
            e.printStackTrace();
            fail();
        }
    }

    public void testExercise10b() {
        try {
            AggregateFunction<TupleExercise8a9a10, TupleExercise8a9a10, TupleExercise8a9a10> aggregateFunction =
            (AggregateFunction<TupleExercise8a9a10, TupleExercise8a9a10, TupleExercise8a9a10>) new Exercise10WindowProcessingOperator();

            TimeWindow window = new TimeWindow(0, 1);

            TupleExercise8a9a10 event1 = new TupleExercise8a9a10(42, 2.5, 5., this.dateTime2);
            TupleExercise8a9a10 event2 = new TupleExercise8a9a10(42, 3., 10., this.dateTime1);
            TupleExercise8a9a10 event3 = new TupleExercise8a9a10(42, 1.0, 2., this.dateTime2);

            TupleExercise8a9a10 accumulator;
            accumulator = aggregateFunction.createAccumulator();
            accumulator = aggregateFunction.add(event1, accumulator);
            accumulator = aggregateFunction.add(event2, accumulator);
            accumulator = aggregateFunction.add(event3, accumulator);

            TupleExercise8a9a10 result = aggregateFunction.getResult(accumulator);

            assertSame(result.f0, 42);
            assertEquals(result.f1, 3.);
            assertEquals(result.f2, 10.);
            assertTrue(result.f3.isEqual(this.dateTime1));

        }
        catch (Exception e){
            e.printStackTrace();
            fail();
        }
    }
}
