package hpi.des.flink_tutorial.session3;

import hpi.des.flink_tutorial.session3.generator.datatypes.TaxiFare;
import hpi.des.flink_tutorial.session3.generator.datatypes.TaxiRide;
import hpi.des.flink_tutorial.session3.generator.sources.TaxiFareGeneratorProcTime;
import hpi.des.flink_tutorial.session3.generator.sources.TaxiRideGeneratorProcTime;
import hpi.des.flink_tutorial.util.StreamingFileSinkFactory;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class StreamJoinPerformance {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamingFileSink<Tuple5<Long, Short, String, Float, Float>> sink = StreamingFileSinkFactory.newSink("/tmp/output_session_3b");

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
                .apply(new JoinFunction<TaxiRide, TaxiFare, Tuple5<Long, Short, String, Float, Float>>() {
                    @Override
                    public Tuple5<Long, Short, String, Float, Float> join(TaxiRide first, TaxiFare second) throws Exception {
                        Tuple5<Long, Short, String, Float, Float> result = new Tuple5<>();
                        result.f0 = first.rideId;
                        result.f1 = first.passengerCnt;
                        result.f2 = second.paymentType;
                        result.f3 = second.totalFare;
                        result.f4 = second.tip;
                        return result;
                    }
                })
                // rideStream.print();
                .addSink(sink);

        env.execute("Exercise Session 3b");
    }

}
