package hpi.des.flink_tutorial.session2;

import hpi.des.flink_tutorial.util.datatypes.TaxiRideTuple;
import hpi.des.flink_tutorial.util.datatypes.TupleExercise4;
import hpi.des.flink_tutorial.util.datatypes.TupleExercise7;
import hpi.des.flink_tutorial.util.datatypes.TupleExercise8a9a10;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.File;
import java.net.URL;
import java.nio.file.Paths;
import java.time.Duration;

public class EventTimeTipInvestigation {
    public static String getInputFilePath(String filename) throws Exception {
        URL res = EventTimeTipInvestigation.class.getClassLoader().getResource(filename);

        if(res == null){
            System.err.println("Could not find file "+filename+" in the resources folder under src/main");
            throw new Exception();
        }

        File file = Paths.get(res.toURI()).toFile();
        return file.getAbsolutePath();
    }

/*
Session 2: Event time tip investigation

In this section we are going to continue analyzing tips, however, now we want to use the notion of event-time.
o The code relative to this session is located at hpi.des.flink_tutorial.session2.EventTimeTipInvestigation
o This session starts from a similar point where we stopped in the previous session
    o The starting stream is composed of a Tuple5 containing “PULocationID”, “tpep_pickup_datetime”, “DOLocationID”,
      “tpep_dropoff_datetime”, “tipRatioPerPassenger”.
*/

    public static void main(String[] args) throws Exception {

/*
Exercise 6) When dealing with event-time stream processing in Flink, it is essential to define how to extract the
timestamp of each event and how to define watermarks. In Flink you need to define the maximum lateness or
out-of-orderness of events in a stream and which field of an event contains the event timestamp. Consider the following:
* The maximum out-of-orderness of events are 10 minutes of (event time).
* The “tpep_pickup_datetime” field defines the timestamp of an event.

Tip 6) Have a look on how Flink deals with event time stream processing
(https://nightlies.apache.org/flink/flink-docs-release-1.12/dev/event_time.html). Watermark strategy objects are
responsible for defining how to extract timestamps and the frequency that watermarks are emitted
(https://nightlies.apache.org/flink/flink-docs-release-1.12/dev/event_timestamps_watermarks.html#introduction-to-watermark-strategies).
Use "Duration" to define the out-of-orderness time and use the "SerializableTimestampAssigner" to extract the correct
timestamp from the events.
 */
        WatermarkStrategy<TaxiRideTuple> watermarkStrategy =
                WatermarkStrategy.<TaxiRideTuple>forBoundedOutOfOrderness(
                                Duration.ofMinutes(10)
                        )
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<TaxiRideTuple>() {
                                    @Override
                                    public long extractTimestamp(TaxiRideTuple taxiRideTuple, long l) {
                                        return taxiRideTuple.tpep_pickup_datetime();
                                    }
                                }
                        );

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<TaxiRideTuple> taxiRideStream =
                env.readTextFile(getInputFilePath("yellow_tripdata_2020-04.csv"))
                .flatMap(new FlatMapFunction<String, TaxiRideTuple>() {
                    @Override
                    public void flatMap(String s, Collector<TaxiRideTuple> collector) throws Exception {
                        try {
                            collector.collect(new TaxiRideTuple(s.split(",")));
                        } catch (Exception ignored) {}
                    }
                })
                .assignTimestampsAndWatermarks(watermarkStrategy);

        taxiRideStream
                .flatMap(new FlatMapFunction<TaxiRideTuple, TupleExercise7>() {
                    @Override
                    public void flatMap(TaxiRideTuple taxiRideTuple, Collector<TupleExercise7> collector) throws Exception {
                        double tipRatio;
                        double tipRatioPerPassenger;

                        // keep only standard fares payed with credit card
                        if(taxiRideTuple.ratecodeID() != null && taxiRideTuple.ratecodeID() == 1 &&
                                taxiRideTuple.payment_type() != null && taxiRideTuple.payment_type() == 1){

                            // check whether tip and total fare are valid and calculate ratio tip/fare
                            if(taxiRideTuple.Tip_amount() != null && taxiRideTuple.Tip_amount() >= 0 &&
                                    taxiRideTuple.Total_amount() != null && taxiRideTuple.Total_amount() > 0){
                                tipRatio = taxiRideTuple.Tip_amount()/taxiRideTuple.Total_amount();

                                // check whether number of passengers is valid and calculate tip ratio per passenger
                                if(taxiRideTuple.passenger_count() != null && taxiRideTuple.passenger_count() > 0){
                                    tipRatioPerPassenger = tipRatio/taxiRideTuple.passenger_count();

                                    // filter out ratios <= 1%
                                    if(tipRatioPerPassenger > 0.01){
                                        collector.collect(new TupleExercise7(
                                                taxiRideTuple.PULocationID(), taxiRideTuple.tpep_pickup_datetime(),
                                                taxiRideTuple.DOLocationID(), taxiRideTuple.tpep_dropoff_datetime(),
                                                tipRatioPerPassenger)
                                        );
                                    }
                                }
                            }
                        }
                    }
        }) //.keyBy(...).window(...).apply(...) etc.

/*
Exercise 7) In this session we also want to take advantage of Flink’s parallelism. In order to do that, Flink partitions
the data and distributes them among its Task Managers. Flink will try to do that by itself, depending on the operator,
but the user can also define how to partition the data. Write an operator that will partition the data based
on the value of the "PULocationID".

Tip 7) The method "keyBy()" is capable of partitioning a stream based on a key
(https://nightlies.apache.org/flink/flink-docs-release-1.12/dev/stream/operators/#keyby and
https://nightlies.apache.org/flink/flink-docs-release-1.12/concepts/stateful-stream-processing.html#keyed-state). In
Flink you use the “KeySelector” operator to partition a data stream.
 */
                /* Uncomment!
                .keyBy(new KeySelector<TupleExercise7, Integer>() {
                    @Override
                    public Integer getKey(TupleExercise7 tupleExercise7) throws Exception {
                        return // Your code
                                ;
                    }
                })
                 */

/*
Exercise 8) Now that we have our stream partitioned by pick-up location, we want to get more insights about the tips
from rides started at each pick-up location. Write an operator capable of calculating the sum and average of tip ratios
per passenger in a given region at every hour (event time). You must also add to your output tuple the starting time
used to calculate the sum and maximum. For example, if the sum and average of the tip ratio per passenger are 100 and
1.2 and they were calculated between 01/04/2020 12:00:00 and 01/04/2020 13:00:00 in the pickup region with id 13, then
your operator should output “13, 1.2, 100, 01/04/2020 12:00:00”.

Tip 8) The operator window is capable of windowing data streams and the operator apply is capable of processing data
stored in windows. (https://nightlies.apache.org/flink/flink-docs-release-1.12/dev/stream/operators/windows.html and
https://nightlies.apache.org/flink/flink-docs-release-1.12/dev/stream/operators/#window-apply). Window times are aligned
with the epoch, hence,
“hourly tumbling windows [...] will get windows such as 1:00:00.000 - 1:59:59.999, 2:00:00.000 - 2:59:59.999 and so on”.
Use the “TumblingEventTimeWindows.of()” window operator function and the “Time” class to bound the data stream and
“WindowFunction” to aggregate the events. Also use the “getStart()” function of the “TimeWindow” class to get the window
start time. Remember that the first parameter of the apply function is the partition key when using “WindowFunction”.
 */
                /* Uncomment!
                .window(
                        // Your code
                )
                .apply(new WindowFunction<TupleExercise7, TupleExercise8a9a10, Integer, TimeWindow>() {
                    @Override
                    public void apply(
                            Integer integer,
                            TimeWindow timeWindow,
                            Iterable<TupleExercise7> iterable,
                            Collector<TupleExercise8a9a10> collector
                    ) throws Exception {
                        // Your code
                    }
                })
                 */

/*
Exercise 9) Add another operator to calculate the best average and total sum of tip ratios per passenger during a
day (24h) per pick-up location. You should keep the time of the best average in the output. Notice that the result of
“apply” is a DataStream, hence you will have to partition your data again before defining your window operator.

Tip 9) The operator reduce can be an efficient alternative to using apply to process incremental windows
(https://nightlies.apache.org/flink/flink-docs-release-1.12/dev/stream/operators/#reduce). The method keyBy is capable
of partitioning a stream based on a key
(https://nightlies.apache.org/flink/flink-docs-release-1.12/dev/stream/operators/#keyby). Use the
“TumblingEventTimeWindows.of()” window operator function and the “Time” class to bound the data stream Use the
“ReduceFunction” to pick the best of to events.
 */
                /*
                .keyBy(new KeySelector<TupleExercise8a9a10, Integer>() {
                    @Override
                    public Integer getKey(TupleExercise8a9a10 tupleExercise8a9a10) throws Exception {
                        return // Your code
                                ;
                    }
                })
                .window(
                        // Your code
                )
                .reduce(new ReduceFunction<TupleExercise8a9a10>(){
                    @Override
                    public TupleExercise8a9a10 reduce(TupleExercise8a9a10 tupleExercise8a9a10, TupleExercise8a9a10 t1) throws Exception {
                        return // Your code
                                ;
                    }
                })
                 */

/*
Exercise 10) Write an operator capable of calculating the best pick-up location among all locations every 24 hours. The
best pick-up location is the location with the largest sum of tip ratios during a day. You must add the pick up location
id to your result.

Tip 10) The operator windowAll
(https://nightlies.apache.org/flink/flink-docs-release-1.12/dev/stream/operators/#windowall) is capable of windowing
data streams from all partitions. The unit tests expect you to use an aggregation function instead of the apply function
that we have been using so far
(https://nightlies.apache.org/flink/flink-docs-release-1.12/dev/stream/operators/#aggregations-on-windows). Use the
“TumblingEventTimeWindows.of()” window operator function and the “Time” class to bound the data stream and the
“AggregateFunction” operator to aggregate the results.
 */
                /*
                .windowAll(
                        // Your code
                )
                .aggregate(new AggregateFunction<TupleExercise8a9a10, TupleExercise8a9a10, TupleExercise8a9a10>() {
                    @Override
                    public TupleExercise8a9a10 createAccumulator() {
                        return null;
                    }

                    @Override
                    public TupleExercise8a9a10 add(
                            TupleExercise8a9a10 tupleExercise8a9a10,
                            TupleExercise8a9a10 tupleExercise8a9a102) {
                        return // Your code
                                ;
                    }

                    @Override
                    public TupleExercise8a9a10 getResult(TupleExercise8a9a10 tupleExercise8a9a10) {
                        return // Your code
                                ;
                    }

                    @Override
                    public TupleExercise8a9a10 merge(TupleExercise8a9a10 tupleExercise8a9a10, TupleExercise8a9a10 acc1) {
                        return // Your code
                                ;
                    }
                })
                 */
                .print();

        env.execute("Exercise Session 2");
    }

}
