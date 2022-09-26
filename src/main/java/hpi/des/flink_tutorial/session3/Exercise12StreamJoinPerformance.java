package hpi.des.flink_tutorial.session3;

import hpi.des.flink_tutorial.util.datatypes.TaxiFareTuple;
import hpi.des.flink_tutorial.util.datatypes.TaxiRideTuple;
import hpi.des.flink_tutorial.session3.generator.sources.TaxiFareGeneratorProcTime;
import hpi.des.flink_tutorial.session3.generator.sources.TaxiRideGeneratorProcTime;
import hpi.des.flink_tutorial.util.StreamingFileSinkFactory;
import hpi.des.flink_tutorial.util.datatypes.TupleExercise11;
import hpi.des.flink_tutorial.util.datatypes.TupleExercise12;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

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

public class Exercise12StreamJoinPerformance {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamingFileSink<TupleExercise12> sink = StreamingFileSinkFactory.newSink("/tmp/output_session_3b");

        /* Uncomment these lines!
        DataStream<Tuple2<TaxiFareTuple, Long>> fareStream = env.addSource(new TaxiFareGeneratorProcTime());

        DataStream<Tuple2<TaxiRideTuple, Long>> rideStream = env.addSource(new TaxiRideGeneratorProcTime());

        rideStream
                .join(fareStream)
                .where()
                .equalTo()
                .window()
                .apply()
                // rideStream.print();
                .addSink(sink);*/

        env.execute("Exercise Session 3b");
    }

}
