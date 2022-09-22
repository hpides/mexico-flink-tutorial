package hpi.des.flink_tutorial.session3;

import hpi.des.flink_tutorial.session3.generator.datatypes.TaxiFare;
import hpi.des.flink_tutorial.session3.generator.datatypes.TaxiRide;
import hpi.des.flink_tutorial.session3.generator.sources.TaxiFareGeneratorProcTime;
import hpi.des.flink_tutorial.session3.generator.sources.TaxiRideGeneratorProcTime;
import hpi.des.flink_tutorial.util.StreamingFileSinkFactory;
import hpi.des.flink_tutorial.util.TupleExercise11;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;

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

        final StreamingFileSink<TupleExercise11> sink = StreamingFileSinkFactory.newSink("/tmp/output_session_3a");

        env.setParallelism(4);

        DataStream<TaxiRide> rideStream = env.addSource(new TaxiRideGeneratorProcTime());
        DataStream<TaxiFare> fareStream = env.addSource(new TaxiFareGeneratorProcTime());

        // rideStream.print();
        rideStream.addSink(sink);

        env.execute("Exercise Session 3a");
    }
}