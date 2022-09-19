package hpi.des.flink_tutorial;
import hpi.des.flink_tutorial.session2.*;
import hpi.des.flink_tutorial.util.DateParser;
import junit.framework.TestCase;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
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
            KeySelector<Tuple5<Integer, LocalDateTime, Integer, LocalDateTime, Double>, Integer> keySelector =
                (KeySelector<Tuple5<Integer, LocalDateTime, Integer, LocalDateTime, Double>, Integer>) new Exercise7Operator();

            Tuple5<Integer, LocalDateTime, Integer, LocalDateTime, Double> event = new Tuple5<>(42, this.dateTime1, 13, this.dateTime2, 0.5);
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
            TumblingEventTimeWindows assigner =
                    (TumblingEventTimeWindows) Exercise8WindowOperator.getWindow();

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
            WindowFunction<Tuple5<Integer, LocalDateTime, Integer, LocalDateTime, Double>, Tuple4<Integer,
                    Double, Double, LocalDateTime>, Integer, TimeWindow> applyFunction =
                    (WindowFunction<Tuple5<Integer, LocalDateTime, Integer, LocalDateTime, Double>,
                            Tuple4<Integer, Double, Double, LocalDateTime>, Integer, TimeWindow>)new Exercise8WindowProcessingOperator();

            TimeWindow window = new TimeWindow(0, 1);

            Tuple5<Integer, LocalDateTime, Integer, LocalDateTime, Double> event1 = new Tuple5<>(42, this.dateTime1, 13, this.dateTime2, 1.);
            Tuple5<Integer, LocalDateTime, Integer, LocalDateTime, Double> event2 = new Tuple5<>(42, this.dateTime1, 13, this.dateTime2, 4.);

            ArrayList<Tuple5<Integer, LocalDateTime, Integer, LocalDateTime, Double>> events = new ArrayList<>();
            events.add(event1);
            events.add(event2);

            ArrayList<Tuple4<Integer, Double, Double, LocalDateTime>> out = new ArrayList<>();
            ListCollector<Tuple4<Integer, Double, Double, LocalDateTime>> collector = new ListCollector<>(out);

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
            KeySelector<Tuple4<Integer, Double, Double, LocalDateTime>, Integer> keySelector =
                    (KeySelector<Tuple4<Integer, Double, Double, LocalDateTime>, Integer>) new Exercise9KeyByOperator();

            Tuple4<Integer, Double, Double, LocalDateTime> event = new Tuple4<>(42, 2.5, 5., dateTime1);
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
            TumblingEventTimeWindows assigner =
                    (TumblingEventTimeWindows) Exercise9WindowOperator.getWindow();

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
            ReduceFunction<Tuple4<Integer, Double, Double, LocalDateTime>> reduceFunction =
                    (ReduceFunction<Tuple4<Integer, Double, Double, LocalDateTime>>)new Exercise9WindowProcessingOperator();

            Tuple4<Integer, Double, Double, LocalDateTime> event1 = new Tuple4<>(42, 2.5, 5., this.dateTime1);
            Tuple4<Integer, Double, Double, LocalDateTime> event2 = new Tuple4<>(42, 3., 10., this.dateTime2);
            Tuple4<Integer, Double, Double, LocalDateTime> event3 = new Tuple4<>(42, 1.0, 2., this.dateTime2);

            Tuple4<Integer, Double, Double, LocalDateTime> reduce = reduceFunction.reduce(event1, event2); // 42, 3., 15., this.dateTime2
            Tuple4<Integer, Double, Double, LocalDateTime> result = reduceFunction.reduce(reduce, event3); // 42, 3., 17., this.dateTime2

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
            TumblingEventTimeWindows assigner =
                    (TumblingEventTimeWindows) Exercise10WindowOperator.getWindow();

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
            AggregateFunction<Tuple4<Integer, Double, Double, LocalDateTime>, Tuple4<Integer, Double, Double, LocalDateTime>,
                    Tuple4<Integer, Double, Double, LocalDateTime>> aggregateFunction =
            (AggregateFunction<Tuple4<Integer, Double, Double, LocalDateTime>,
                    Tuple4<Integer, Double, Double, LocalDateTime>,
                    Tuple4<Integer, Double, Double, LocalDateTime>>) new Exercise10WindowProcessingOperator();

            TimeWindow window = new TimeWindow(0, 1);

            Tuple4<Integer, Double, Double, LocalDateTime> event1 = new Tuple4<>(42, 2.5, 5., this.dateTime2);
            Tuple4<Integer, Double, Double, LocalDateTime> event2 = new Tuple4<>(42, 3., 10., this.dateTime1);
            Tuple4<Integer, Double, Double, LocalDateTime> event3 = new Tuple4<>(42, 1.0, 2., this.dateTime2);

            Tuple4<Integer, Double, Double, LocalDateTime> accumulator;
            accumulator = aggregateFunction.createAccumulator();
            accumulator = aggregateFunction.add(event1, accumulator);
            accumulator = aggregateFunction.add(event2, accumulator);
            accumulator = aggregateFunction.add(event3, accumulator);

            Tuple4<Integer, Double, Double, LocalDateTime> result = aggregateFunction.getResult(accumulator);

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
