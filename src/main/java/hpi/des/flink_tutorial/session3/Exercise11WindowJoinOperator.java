package hpi.des.flink_tutorial.session3;

import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

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
(https://nightlies.apache.org/flink/flink-docs-release-1.12/dev/connectors/streamfile_sink.html).
 */

// implement here the class of your operator. For window operators, you must modify the method getWindow and use it
// in your stream processing job.
public class Exercise11WindowJoinOperator {
    public static WindowAssigner<Object, TimeWindow> getWindow(){
        // Add your code here
        return null;
    }
}
