package hpi.des.flink_tutorial.session3;

import hpi.des.flink_tutorial.util.datatypes.TaxiFareTuple;
import hpi.des.flink_tutorial.util.datatypes.TaxiRideTuple;
import hpi.des.flink_tutorial.session3.generator.sources.TaxiFareGeneratorProcTime;
import hpi.des.flink_tutorial.session3.generator.sources.TaxiRideGeneratorProcTime;
import hpi.des.flink_tutorial.util.datatypes.TupleExercise11;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.concurrent.TimeUnit;

/*
Session 3: Joins and analysis of a Flink job performance

o The code relative to this session is located at hpi.des.flink_tutorial.session3
    o StreamJoin is the class for Exercise 11
    o StreamJoinPerformance is the class for Exercise 12
o In this session we are using processing time
o There are two data generators located at hpi.des.flink_tutorial.session3.generators.sources which are able to generate
  taxi ride related events:
    o The TaxiRide generator generates data related specifically to the taxi rides (e.g., number of passengers, and
      pick-up and drop-off locations and time).
    o The TaxiFare generator generates data related to the fares (e.g., fare value, tip, and payment type)
    o Both TaxiRide and TaxiFare types have an attribute rideId that can be used for joining.
 */

public class StreamJoin {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /* Uncomment these lines!
        final StreamingFileSink<TupleExercise11> sink = StreamingFileSink.forRowFormat(
                        new Path("/tmp/output_session_3a"),
                        new SimpleStringEncoder<TupleExercise11>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(1))
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(1))
                                .withMaxPartSize(1024 * 100)
                                .build())
                .build();
         */

        env.setParallelism(4);

        DataStream<TaxiRideTuple> rideStream = env.addSource(new TaxiRideGeneratorProcTime());
        DataStream<TaxiFareTuple> fareStream = env.addSource(new TaxiFareGeneratorProcTime());

/*
Exercise 11) Write an operator capable of joining the TaxiRide and TaxiFare streams using a 1 second tumbling processing
time window. The resulting event must contain ride id, passenger count, payment type, total fare, and tip. The print
method is very useful for quickly debugging and checking the results of your application using an IDE. Another option to
get stream output is to write the results to disk using a file sink. Use the StreamFileSinkFactory class available at
hpi.des.flink_tutorial.session3.util to create a sink object that you can add to your job. You will have to modify the
type of your sink depending on the events data type.

Tip 11) The operator join allows joining two streams using different strategies to aggregate the tuples
(https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/dev/datastream/operators/overview/#window-join,
https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/dev/datastream/operators/joining/). The addSink operator
adds a sink to the job
(https://nightlies.apache.org/flink/flink-docs-release-1.12/dev/connectors/streamfile_sink.html). Use the
“TumblingEventTimeWindows.of()” window operator function and the “Time” class to bound the data stream. Use the
“JoinFunction” operator to join streams.
 */

        //rideStream.print();
        /* Uncomment these lines!
        rideStream
                // Exercise 11: joining TaxiRide and TaxiFare streams
                .join(fareStream)
                .where(new KeySelector<TaxiRideTuple, Long>() {
                    @Override
                    public Long getKey(TaxiRideTuple taxiRideTuple) throws Exception {
                        return // Your code
                                ;
                    }
                })
                .equalTo(new KeySelector<TaxiFareTuple, Long>() {
                    @Override
                    public Long getKey(TaxiFareTuple taxiFareTuple) throws Exception {
                        return // Your code
                                ;
                    }
                })
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                .apply(new JoinFunction<TaxiRideTuple, TaxiFareTuple, TupleExercise11>() {
                    @Override
                    public TupleExercise11 join(TaxiRideTuple taxiRideTuple, TaxiFareTuple taxiFareTuple) throws Exception {
                        return // Your code
                                ;
                    }
                })

                .addSink(sink);

        env.execute("Exercise Session 3a");
        */
    }
}