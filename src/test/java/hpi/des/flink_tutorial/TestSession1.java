package hpi.des.flink_tutorial;

import hpi.des.flink_tutorial.session1.*;
import hpi.des.flink_tutorial.util.TaxiRideTuple;
import hpi.des.flink_tutorial.util.TupleExercise1;
import hpi.des.flink_tutorial.util.TupleExercise3;
import hpi.des.flink_tutorial.util.TupleExercise4;
import junit.framework.TestCase;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;

public class TestSession1 extends TestCase {
    LocalDateTime dateTime1;
    LocalDateTime dateTime2;

    protected void setUp(){
        LocalDate date1 = LocalDate.of(2020, 4, 1);
        LocalTime time1 = LocalTime.of(0, 41, 22);
        this.dateTime1 = LocalDateTime.of(date1, time1);

        LocalDate date2 = LocalDate.of(2020, 4, 1);
        LocalTime time2 = LocalTime.of(1, 1, 53);
        this.dateTime2 = LocalDateTime.of(date2, time2);

    }

    public void testExercise1(){
        try {
            MapFunction<TaxiRideTuple, TupleExercise1> operator;
            operator = (MapFunction<TaxiRideTuple, TupleExercise1>) new Exercise1Operator();
            TaxiRideTuple ride1 = new TaxiRideTuple("1", this.dateTime1, this.dateTime2, 1, 1.20, 3, "N", 41, 24, 2, 5.5, 0.5, 0.5, 0.0, 0.0, 0.3, 6.8, 0.0);


            TupleExercise1 result = operator.map(ride1);
            assertEquals(result.f0, ride1.f1);
            assertEquals(result.f1, ride1.f2);
            assertEquals(result.f2, ride1.f3);
            assertEquals(result.f3, ride1.f5);
            assertEquals(result.f4, ride1.f9);
            assertEquals(result.f5, ride1.f13);
            assertEquals(result.f6, ride1.f16);
        }
        catch (Exception e){
            e.printStackTrace();
            fail();
        }
    }

    public void testExercise2() {
        try {
            FilterFunction<TupleExercise1> operator;
            operator = (FilterFunction<TupleExercise1>) new Exercise2Operator();
            TupleExercise1 ride1 = new TupleExercise1(this.dateTime1, this.dateTime2, 1, null, 2, 0.0, 6.8);
            TupleExercise1 ride2 = new TupleExercise1(this.dateTime1, this.dateTime2, 1, 2, 1, 0.0, 6.8);
            TupleExercise1 ride3 = new TupleExercise1(this.dateTime1, this.dateTime2, 1, 1, 1, 0.0, 6.8);
            TupleExercise1 ride4 = new TupleExercise1(this.dateTime1, null, null, 1, 1, 0.0, 6.8);

            assertFalse(operator.filter(ride1));
            assertFalse(operator.filter(ride2));
            assertTrue(operator.filter(ride3));
            assertTrue(operator.filter(ride4));
        }
        catch(Exception e){
            e.printStackTrace();
            fail();
        }
    }

    public void testExercise3(){
        try{
            FlatMapFunction<TupleExercise1, TupleExercise3>
            operator = (FlatMapFunction<TupleExercise1, TupleExercise3>) new Exercise3Operator();

            ArrayList<TupleExercise3> out = new ArrayList<>();
            ListCollector<TupleExercise3> collector = new ListCollector<>(out);

            TupleExercise1 ride1 = new TupleExercise1(this.dateTime1, this.dateTime2, 1, 1, 2, null, 6.8);
            TupleExercise1 ride2 = new TupleExercise1(this.dateTime1, this.dateTime2, 1, 2, 1, -1.0, 6.8);
            TupleExercise1 ride3 = new TupleExercise1(this.dateTime1, this.dateTime2, 1, 1, 1, 1.0, null);
            TupleExercise1 ride4 = new TupleExercise1(this.dateTime1, this.dateTime2, 1, 1, 1, 1.0, -1.);
            TupleExercise1 ride5 = new TupleExercise1(this.dateTime1, this.dateTime2, 1, 2, 1, 1.0, 0.0);
            TupleExercise1 ride6 = new TupleExercise1(this.dateTime1, this.dateTime2, 1, 1, 1, 1.0, 2.0);

            operator.flatMap(ride1, collector);
            operator.flatMap(ride2, collector);
            operator.flatMap(ride3, collector);
            operator.flatMap(ride4, collector);
            operator.flatMap(ride5, collector);
            operator.flatMap(ride6, collector);

            assertEquals(out.size(), 1);

            for(int i = 0; i < 7; i++){
                assertSame(out.get(0).getField(i), ride6.getField(i));
            }
            assertEquals(out.get(0).f7, 0.5);
        }
        catch(Exception e){
            e.printStackTrace();
            fail();
        }
    }

    public void testExercise4(){
        try{
            FlatMapFunction<TupleExercise3,
                    TupleExercise4>
                    operator = (FlatMapFunction<TupleExercise3, TupleExercise4>) new Exercise4Operator();

            ArrayList<TupleExercise4> out = new ArrayList<>();
            ListCollector<TupleExercise4> collector = new ListCollector<>(out);

            TupleExercise3 ride1 = new TupleExercise3(this.dateTime1, this.dateTime2, null, 1, 1, 1.0, 2.0, 0.5);
            TupleExercise3 ride2 = new TupleExercise3(this.dateTime1, this.dateTime2, 0, 1, 1, 1.0, 2.0, 0.5);
            TupleExercise3 ride3 = new TupleExercise3(this.dateTime1, this.dateTime2, 1, 1, 1, 1.0, 2.0, 0.01);
            TupleExercise3 ride4 = new TupleExercise3(this.dateTime1, this.dateTime2, 1, 1, 1, 1.0, 2.0, 0.5);

            operator.flatMap(ride1, collector);
            operator.flatMap(ride2, collector);
            operator.flatMap(ride3, collector);
            operator.flatMap(ride4, collector);

            assertEquals(out.size(), 1);

            for(int i = 0; i < 8; i++){
                assertSame(out.get(0).getField(i), ride4.getField(i));
            }
            assertEquals(out.get(0).f8, 0.5);
        }
        catch(Exception e){
            e.printStackTrace();
            fail();
        }
    }

    public void testExercise5a() {
        try {
            SlidingProcessingTimeWindows windowAssigner = (SlidingProcessingTimeWindows)Exercise5WindowOperator.getWindow();

            assertFalse(windowAssigner.isEventTime());
            assertEquals(windowAssigner.getSize(), Time.seconds(1).toMilliseconds());
            assertEquals(windowAssigner.getSlide(), Time.milliseconds(200).toMilliseconds());
        }
        catch(Exception e){
            e.printStackTrace();
            fail();
        }
    }

    public void testExercise5b() {
        try {
            AllWindowFunction<TupleExercise4,
                    Double, TimeWindow> operator = (AllWindowFunction<TupleExercise4, Double, TimeWindow>) new Exercise5WindowProcessingOperator();

            SlidingProcessingTimeWindows assigner = SlidingProcessingTimeWindows.of(Time.seconds(1), Time.milliseconds(200));

            TupleExercise4 ride1 =
                    new TupleExercise4(this.dateTime1, this.dateTime2, 1, 1, 1, 1.0, 2.0, 0.5, 0.5);
            TupleExercise4 ride2 =
                    new TupleExercise4(this.dateTime1, this.dateTime2, 1, 1, 1, 1.0, 2.0, 3., 3.);
            TupleExercise4 ride3 =
                    new TupleExercise4(this.dateTime1, this.dateTime2, 1, 1, 1, 1.0, 2.0, 4., 4.);

            ArrayList<TupleExercise4> rides =
                    new ArrayList<>();

            rides.add(ride1);
            rides.add(ride2);
            rides.add(ride3);

            ArrayList<Double> out = new ArrayList<>();
            ListCollector<Double> collector = new ListCollector<>(out);

            TimeWindow window = new TimeWindow(0, 2);
            operator.apply(window, rides, collector);
            assertEquals(out.size(), 1);
            assertEquals(out.get(0), 4.);
        }
        catch(Exception e){
            e.printStackTrace();
            fail();
        }
    }
}
