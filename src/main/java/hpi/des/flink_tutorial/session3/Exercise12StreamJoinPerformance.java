package hpi.des.flink_tutorial.session3;

import hpi.des.flink_tutorial.session3.generator.datatypes.TaxiFare;
import hpi.des.flink_tutorial.session3.generator.datatypes.TaxiRide;
import hpi.des.flink_tutorial.session3.generator.sources.TaxiFareGeneratorProcTime;
import hpi.des.flink_tutorial.session3.generator.sources.TaxiRideGeneratorProcTime;
import hpi.des.flink_tutorial.util.StreamingFileSinkFactory;
import hpi.des.flink_tutorial.util.TupleExercise11;
import hpi.des.flink_tutorial.util.TupleExercise12;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/*
Exercise 12) Modify your Flink job so we can measure its performance in terms of throughput and latency. Use the class
StreamJoinPerformance as a starting point. How would you measure the resource consumption of your job (RAM and CPU)?
Run your job on your IDE and in standalone mode. There are no automated tests for this exercise.

Tip 12) Adding timestamps marks to the event as they move down the pipeline is a common technique to keep track of
latency. You can measure throughput on the sources and sinks. There are many *nix resource monitoring tools such as
dstat, perf, ps, and top. In standalone mode, you can use the Flink Web Dashboard, by default available at
localhost:8081 (https://nightlies.apache.org/flink/flink-docs-release-1.12/ops/rest_api.html). Check your flink-conf.yml
for more details
(https://nightlies.apache.org/flink/flink-docs-release-1.12/deployment/resource-providers/standalone/#example-standalone-ha-cluster-with-2-jobmanagers).
*/

public class Exercise12StreamJoinPerformance {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamingFileSink<TupleExercise11> sink = StreamingFileSinkFactory.newSink("/tmp/output_session_3b");

        DataStream<TaxiFare> fareStream = env.addSource(new TaxiFareGeneratorProcTime());
        DataStream<TaxiRide> rideStream = env.addSource(new TaxiRideGeneratorProcTime());

        rideStream
                .join(fareStream)
                .where(new KeySelector<TaxiRide, Long>() {
                    @Override
                    public Long getKey(TaxiRide value) throws Exception {
                        return value.rideId;
                    }
                })
                .equalTo(new KeySelector<TaxiFare, Long>() {
                    @Override
                    public Long getKey(TaxiFare value) throws Exception {
                        return value.rideId;
                    }
                })
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                .apply(new JoinFunction<TaxiRide, TaxiFare, TupleExercise12>() {
                    @Override
                    public TupleExercise12 join(Tuple2<TaxiRide, Long> first, Tuple2<TaxiFare, Long> second) throws Exception {
                        TupleExercise12 result = new TupleExercise12(
                                first.f0.rideId,
                                first.f0.passengerCnt,
                                second.f0.paymentType,
                                second.f0.totalFare, second.f0.tip,
                                0, // you need to modify this ...
                                0 // ... and you need to modify this
                        );

                        return result;
                    }
                })
                // rideStream.print();
                .addSink(sink);

        env.execute("Exercise Session 3b");
    }

}
