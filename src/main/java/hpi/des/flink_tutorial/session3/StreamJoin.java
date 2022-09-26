package hpi.des.flink_tutorial.session3;

import hpi.des.flink_tutorial.util.datatypes.TaxiFareTuple;
import hpi.des.flink_tutorial.util.datatypes.TaxiRideTuple;
import hpi.des.flink_tutorial.session3.generator.sources.TaxiFareGeneratorProcTime;
import hpi.des.flink_tutorial.session3.generator.sources.TaxiRideGeneratorProcTime;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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

        //final StreamingFileSink<TupleExercise11> sink = StreamingFileSinkFactory.newSink("/tmp/output_session_3a"); // Uncomment!

        env.setParallelism(4);

        DataStream<TaxiRideTuple> rideStream = env.addSource(new TaxiRideGeneratorProcTime());
        DataStream<TaxiFareTuple> fareStream = env.addSource(new TaxiFareGeneratorProcTime());

        //rideStream.print();
        /* Uncomment these lines!
        rideStream
                // Exercise 11: joining TaxiRide and TaxiFare streams
                .join(fareStream)
                .where(new Exercise11WhereOperator())
                .equalTo(new Exercise11EqualToOperator())
                .window(Exercise11WindowJoinOperator.getWindow())
                .apply(new Exercise11WindowJoinProcessingOperator())

                .addSink(sink); */

        env.execute("Exercise Session 3a");
    }
}