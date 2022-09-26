package hpi.des.flink_tutorial.session1;

/*
Exercise 5) Now that we have the tip ratio per passenger metric, we can start comparing the different events. Write an
operator that will output the largest tip ratio per passenger observed in the last 1 second of processing and updated
every 200 milliseconds.

Tip 5) The windowAll operator performs window processing on non partitioned data
(https://nightlies.apache.org/flink/flink-docs-release-1.12/dev/stream/operators/#windowall). The apply operator process
data from a window (https://nightlies.apache.org/flink/flink-docs-release-1.12/dev/stream/operators/#window-apply). A
more specific option for processing data from a window, instead of the apply operator, are aggregation operators
(https://nightlies.apache.org/flink/flink-docs-release-1.12/dev/stream/operators/#aggregations-on-windows). Use the
“SlidingProcessingTimeWindows.of()” window operator function and the “Time” class to bound the data stream.
 */

// implement here the class of your operator. Check TransformSourceStreamOperator.java for an example. Don't change the name
// of the class because it is used in the testing.
public class Exercise5WindowProcessingOperator {
}
