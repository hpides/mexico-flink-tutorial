package hpi.des.flink_tutorial.session3;

import hpi.des.flink_tutorial.util.datatypes.TaxiFareTuple;
import hpi.des.flink_tutorial.util.datatypes.TaxiRideTuple;
import hpi.des.flink_tutorial.session3.generator.sources.TaxiFareGeneratorProcTime;
import hpi.des.flink_tutorial.session3.generator.sources.TaxiRideGeneratorProcTime;
import hpi.des.flink_tutorial.util.datatypes.TupleExercise11;
import hpi.des.flink_tutorial.util.datatypes.TupleExercise12;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.concurrent.TimeUnit;

public class Exercise12StreamJoinPerformance {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamingFileSink<TupleExercise12> sink = StreamingFileSink.forRowFormat(
                        new Path("/tmp/output_session_3b"),
                        new SimpleStringEncoder<TupleExercise12>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(1))
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(1))
                                .withMaxPartSize(1024 * 100)
                                .build())
                .build();

/*
Exercise 12) Copy your solution from StreamJoin to StreamJoinPerformance as a starting point. Modify your Flink job so
we can measure its performance in terms of throughput and latency. How would you measure the resource consumption of
your job (RAM and CPU)? There are no automated tests for this exercise.


Tip 12.a) You can extend a tuple on your own by wrapping a tuple in a Tuple2. For example if you would like to add
value to the TaxiFare tuple you can use a mapper function like this.
new MapFunction<TaxiFare, Tuple2<TaxiFare, Long>>() {
// Your code here
}

Tip 12.b) Adding timestamps marks to the event as they move down the pipeline is a common technique to keep track of latency. You can measure throughput on the sources and sinks. There are many *nix resource monitoring tools such as dstat, perf, ps, and top. In this exercise you can use System.currentTimeMillis() to measure the current time in your code.

Tip 12.c) You can also define operators inplace like in the example below.
.where(new KeySelector<TaxiRideTuple, Long>() {
// Your code here
})
*/

        /* Uncomment these lines!
        DataStream<Tuple2<TaxiFareTuple, Long>> fareStream = env.addSource(new TaxiFareGeneratorProcTime())
                .map(new MapFunction<TaxiFareTuple, Tuple2<TaxiFareTuple, Long>>() {
                    @Override
                    public Tuple2<TaxiFareTuple, Long> map(TaxiFareTuple value) throws Exception {
                        return // Your code
                                ;
                    }
                });

        DataStream<Tuple2<TaxiRideTuple, Long>> rideStream = env.addSource(new TaxiRideGeneratorProcTime())
                .map(new MapFunction<TaxiRideTuple, Tuple2<TaxiRideTuple, Long>>() {
                    @Override
                    public Tuple2<TaxiRideTuple, Long> map(TaxiRideTuple value) throws Exception {
                        return // Your code
                                ;
                    }
                });

        rideStream
                .join(fareStream)
                .where(new KeySelector<Tuple2<TaxiRideTuple, Long>, Long>() {
                    @Override
                    public Long getKey(Tuple2<TaxiRideTuple, Long> value) throws Exception {
                        return // Your code
                                ;
                    }
                })
                .equalTo(new KeySelector<Tuple2<TaxiFareTuple, Long>, Long>() {
                    @Override
                    public Long getKey(Tuple2<TaxiFareTuple, Long> value) throws Exception {
                        return // Your code
                                ;
                    }
                })
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                .apply(new JoinFunction<Tuple2<TaxiRideTuple, Long>, Tuple2<TaxiFareTuple, Long>, TupleExercise12>() {
                    @Override
                    public TupleExercise12 join(Tuple2<TaxiRideTuple, Long> first, Tuple2<TaxiFareTuple, Long> second) throws Exception {
                        return // Your code
                                ;
                    }
                })
                // rideStream.print();
                .addSink(sink);

        env.execute("Exercise Session 3b");
        */
    }

}
