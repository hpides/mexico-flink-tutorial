package hpi.des.flink_tutorial.session3;

import hpi.des.flink_tutorial.session3.generator.datatypes.TaxiFare;
import hpi.des.flink_tutorial.session3.generator.datatypes.TaxiRide;
import hpi.des.flink_tutorial.session3.generator.sources.TaxiFareGeneratorProcTime;
import hpi.des.flink_tutorial.session3.generator.sources.TaxiRideGeneratorProcTime;
import hpi.des.flink_tutorial.util.StreamingFileSinkFactory;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;

public class StreamJoin {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final StreamingFileSink<TaxiRide> sink = StreamingFileSinkFactory.newSink("/tmp/output_session_3a");

        env.setParallelism(4);

        DataStream<TaxiRide> rideStream = env.addSource(new TaxiRideGeneratorProcTime());
        DataStream<TaxiFare> fareStream = env.addSource(new TaxiFareGeneratorProcTime());

        // rideStream.print();
        rideStream.addSink(sink);

        env.execute("Exercise Session 3a");
    }
}