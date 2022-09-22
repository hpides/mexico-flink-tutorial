package hpi.des.flink_tutorial.session1;

import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/*
Exercise 5) Now that we have the tip ratio per passenger metric, we can start comparing the different events. Write an
operator that will output the largest tip ratio per passenger observed in the last 1 second of processing and updated
every 200 milliseconds.

Tip 5) The windowAll operator performs window processing on non partitioned data
(https://nightlies.apache.org/flink/flink-docs-release-1.12/dev/stream/operators/#windowall). The apply operator process
data from a window (https://nightlies.apache.org/flink/flink-docs-release-1.12/dev/stream/operators/#window-apply). A
more specific option for processing data from a window, instead of the apply operator, are aggregation operators
(https://nightlies.apache.org/flink/flink-docs-release-1.12/dev/stream/operators/#aggregations-on-windows).
 */

// implement here the class of your operator. For window operators, you must modify the method getWindow and use it
// in your stream processing job.
public class Exercise5WindowOperator {

    public static WindowAssigner<Object, TimeWindow> getWindow(){
        // Add your code here
        return null;
    }
}
