package hpi.des.flink_tutorial.session1;

import hpi.des.flink_tutorial.util.datatypes.TaxiRideTuple;
import hpi.des.flink_tutorial.util.datatypes.TupleExercise1;
import hpi.des.flink_tutorial.util.datatypes.TupleExercise3;
import hpi.des.flink_tutorial.util.datatypes.TupleExercise4;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.File;
import java.net.URL;
import java.nio.file.Paths;

public class TipInvestigation {
    public static String getInputFilePath(String filename) throws Exception {
        URL res = TipInvestigation.class.getClassLoader().getResource(filename);

        if(res == null){
            System.err.println("Could not find file "+filename+" in the resources folder under src/main");
            throw new Exception();
        }

        File file = Paths.get(res.toURI()).toFile();
        return file.getAbsolutePath();
    }

/*
Session 1: tip investigation

In this session, our objective is to analyze the tips given to taxi drivers in New York City in April 2020.
o The code relative to this session is located at hpi.des.flink_tutorial.session1.TipInvestigation
o Use the data dictionary located at hpi.des.flink_tutorial.resources.data_dict.txt to have an overview of the input
  data
o Also use the tuples that we prepared for you in hpi.des.flink_tutorial.util.datatypes
o You can use the method print() to debug your application
  (https://ci.apache.org/projects/flink/flink-docs-stable/dev/datastream_api.html#data-sinks)
o In this session, we are using processing time
  (https://ci.apache.org/projects/flink/flink-docs-release-1.12/learn-flink/streaming_analytics.html#event-time-and-watermarks).
*/

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> sourceStream =
                env.readTextFile(getInputFilePath("yellow_tripdata_2020-04.csv"));

        sourceStream.flatMap(new FlatMapFunction<String, TaxiRideTuple>() {
                    @Override
                    public void flatMap(String s, Collector<TaxiRideTuple> collector) throws Exception {
                        String[] fields = s.split(",");
                        try {
                            collector.collect(new TaxiRideTuple(fields));
                        } catch (Exception ignored) {}
                    }
                })

/*
Exercise 1) Analyze the class TaxiRideTuple which defines the data type parsed from the input data. As we do not need
all the 18 fields defined in TaxiRideTuple, write an operator that will transform a TaxiRide to a TupleExercise1
containing only the fields “tpep_pickup_date_time”, “tpep_dropoff_datetime”, “passenger_count”, “ratecodeID”,
“payment_type”, “Tip_amount” and “Total_amount”.

Tip 1) The "MapFunction" operator is one of the Flink operators capable of transforming a stream
(https://nightlies.apache.org/flink/flink-docs-release-1.12/dev/stream/operators/#map). Use the data dictionary located
at hpi.des.flink_tutorial.resources.data_dict.txt to have an overview of the input data. With ".map()" you can add the
operator to the pipeline.
 */
                /*
                .map(new MapFunction<TaxiRideTuple, TupleExercise1>() {
                         @Override
                         public TupleExercise1 map(TaxiRideTuple taxiRideTuple) throws Exception {
                             return // Your code
                                     ;
                         }
                     }
                )
                 */

/*
Exercise 2) The value of the field “Tip_amount” applies only to fares paid with a credit card. Furthermore, we are only
interested in standard fares (you need to check the “ratecodeID”). Write an operator capable of filtering fares that
were not paid (you need to check the “payment_type”) with a credit card (not 1) or which are not standard fares (not 1)
and remove tuples with null as “ratecodeID”.

Tip 2) The "FilterFunction" operator is capable of filtering events from a stream
(https://nightlies.apache.org/flink/flink-docs-release-1.12/dev/stream/operators/#filter). With ".filter()" you can add
the operator to the pipeline.
 */
                /* Uncomment!
                .filter(new FilterFunction<TupleExercise1>() {
                            @Override
                            public boolean filter(TupleExercise1 tupleExercise1) throws Exception {
                                return // Your code
                                        ;
                            }
                        }
                )
                 */

/*
Exercise 3) In order to analyze the tips, we opt to use its tip ratio, i.e., its proportion to the total value of the
fare. For example, a tip of US$ 2 for a US$ 20 taxi ride has a tip ratio of <= 0.1. Write an operator that will add to
the end of the tuple at the 8th position the tip ratio of the taxi ride. If the tip has an invalid value
(null, tip < 0) or if the total value has an invalid value (null, total <= 0) you must discard the event.

Tip 3) The "MapFunction" operator is capable of transforming a stream and outputting zero or many results from a single
event (https://nightlies.apache.org/flink/flink-docs-release-1.12/dev/stream/operators/#flatmap). With ".flatMap()" you
can add the operator to the pipeline.
 */
                /* Uncomment
                .flatMap(new FlatMapFunction<TupleExercise1, TupleExercise3>() {
                    @Override
                    public void flatMap(TupleExercise1 tupleExercise1, Collector<TupleExercise3> collector) throws Exception {
                        // Your code
                    }
                })
                 */

/*
Exercise 4) We would like to improve our tip ratio metric by also calculating the tip ratio per passenger. Moreover, we
want to discard events having an invalid value (null or <= 0) for the number of passengers or containing a tip ratio per
passenger of <= 0.01. Add the ratio at the last position of the tuple.

Tip 4) The flatMap operator is capable of transforming a stream and outputting zero or many results from a single event
(https://nightlies.apache.org/flink/flink-docs-release-1.12/dev/stream/operators/#flatmap).
 */
                /* Uncomment
                .flatMap(
                    // Your code
                )
                 */

/*
Exercise 5) Now that we have the tip ratio per passenger metric, we can start comparing the different events. Write an
operator that will output the largest tip ratio per passenger observed in the last 200 millisecond of processing and updated
every 10 milliseconds.

Tip 5) The ".windowAll()" operator performs window processing on non-partitioned data
(https://nightlies.apache.org/flink/flink-docs-release-1.12/dev/stream/operators/#windowall). The apply operator process
data from a window (https://nightlies.apache.org/flink/flink-docs-release-1.12/dev/stream/operators/#window-apply). A
more specific option for processing data from a window, instead of the apply operator, are aggregation operators
(https://nightlies.apache.org/flink/flink-docs-release-1.12/dev/stream/operators/#aggregations-on-windows). Use the
“SlidingProcessingTimeWindows.of()” window operator function and the “Time” class to bound the data stream. Define a
"AllWindowFunction" that computes the maximum "tripRatioPerPassenger" & ".apply()" it to the Sliding Window.
 */

                /* Uncomment!
                .windowAll(
                    // Your code
                )
                .apply(new AllWindowFunction<TupleExercise4, Double, TimeWindow>() {
                    @Override
                    public void apply(
                            TimeWindow timeWindow,
                            Iterable<TupleExercise4> iterable,
                            Collector<Double> collector
                    ) throws Exception {
                        // Your code
                    }
                })*/

                .print();

        env.execute("Exercise Session 1");
    }
}
