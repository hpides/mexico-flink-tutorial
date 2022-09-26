package hpi.des.flink_tutorial.session2;

import hpi.des.flink_tutorial.session1.TransformSourceStreamOperator;
import hpi.des.flink_tutorial.util.InputFile;
import hpi.des.flink_tutorial.util.datatypes.TaxiRideTuple;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/*
Session 2: Event time tip investigation

In this section we are going to continue analyzing tips, however, now we want to use the notion of event-time.
o The code relative to this session is located at hpi.des.flink_tutorial.session2.EventTimeTipInvestigation
o This session starts from a similar point where we stopped in the previous session
    o The starting stream is composed of a Tuple5 containing “PULocationID”, “tpep_pickup_datetime”, “DOLocationID”,
      “tpep_dropoff_datetime”, “tipRatioPerPassenger”.
*/

public class EventTimeTipInvestigation {

/*
Exercise 6) When dealing with event-time stream processing in Flink, it is essential to define how to extract the
timestamp of each event and how to define watermarks. Consider the following:
* The “tpep_pickup_datetime” field defines the timestamp of an event.
* A watermark must be emitted every 10 minutes (event time).
* Timestamps must be defined in milliseconds from epoch (use the method localDateTimeToMilliseconds available in
  hpi.des.flink_tutorial.util.DateParser to convert from LocalDateTime to milliseconds).

Find and complete the code of the methods “exercise6WatermarkInterval” and “exercise6GetTimestampFromTaxiRideTuple”.

Tip 6) Have a look on how Flink deals with event time stream processing
(https://nightlies.apache.org/flink/flink-docs-release-1.12/dev/event_time.html). Watermark strategy objects are
responsible for defining how to extract timestamps and the frequency that watermarks are emitted
(https://nightlies.apache.org/flink/flink-docs-release-1.12/dev/event_timestamps_watermarks.html#introduction-to-watermark-strategies).
Use the method localDateTimeToMilliseconds available in hpi.des.flink_tutorial.util.DateParser to convert from
LocalDateTime to milliseconds. Use the “millisecondsToLocalDateTime” method available at
hpi.des.flink_tutorial.util.DateParser to transform a “LocalDateTime” to a timestamp in “long” format. Also have a
look at the output types of the functions.
 */

    public static Duration exercise6WatermarkInterval(long durationInMinutes){
        // your code here
        return null;
    }

    public static SerializableTimestampAssigner<TaxiRideTuple> exercise6GetTimestampFromTaxiRideTuple(){
        // your code here
        return null;
    }

    public static void main(String[] args) throws Exception {
        // exercise 6 - implement methods exercise6WatermarkInterval and exercise6GetTimestampFromTaxiRideTuple used in the following call
        WatermarkStrategy<TaxiRideTuple> watermarkStrategy = WatermarkStrategy.<TaxiRideTuple>forBoundedOutOfOrderness(exercise6WatermarkInterval(1))
                .withTimestampAssigner(exercise6GetTimestampFromTaxiRideTuple());

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<TaxiRideTuple> taxiRideStream = env.readTextFile(InputFile.getInputFilePath())
                .flatMap(new TransformSourceStreamOperator())
                .assignTimestampsAndWatermarks(watermarkStrategy);

        taxiRideStream
                .flatMap(new PreprocessStream()) //.keyBy(...).window(...).apply(...) etc.
                // exercise 7
                // exercise 8
                // exercise 9
                // exercise 10
                .print();

        env.execute("Exercise Session 2");
    }

}
