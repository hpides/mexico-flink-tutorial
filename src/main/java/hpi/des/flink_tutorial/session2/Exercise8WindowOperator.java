package hpi.des.flink_tutorial.session2;

import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/*
Exercise 8) Now that we have our stream partitioned by pick-up location, we want to get more insights about the tips
from rides started at each pick-up location. Write an operator capable of calculating the sum and average of tip ratios
per passenger in a given region at every hour (event time). You must also add to your output tuple the starting time
used to calculate the sum and maximum. For example, if the sum and average of the tip ratio per passenger are 100 and
1.2 and they were calculated between 01/04/2020 12:00:00 and 01/04/2020 13:00:00 in the pick up region with id 13, then
your operator should output “13, 1.2, 100, 01/04/2020 12:00:00”. Use the “millisecondsToLocalDateTime” method available
at hpi.des.flink_tutorial.util.DateParser.

Tip 8) The operator window is capable of windowing data streams and the operator apply is capable of processing data
stored in windows. (https://nightlies.apache.org/flink/flink-docs-release-1.12/dev/stream/operators/windows.html and
https://nightlies.apache.org/flink/flink-docs-release-1.12/dev/stream/operators/#window-apply). Window times are aligned
with the epoch, hence,
“hourly tumbling windows [...] will get windows such as 1:00:00.000 - 1:59:59.999, 2:00:00.000 - 2:59:59.999 and so on”.
Use the “TumblingEventTimeWindows.of()” window operator function and the “Time” class to bound the data stream and
“WindowFunction” to aggregate the events. Also use the “getStart()” function of the “TimeWindow” class to get the window
start time. Remember that the first parameter of the apply function is the partition key when using “WindowFunction”.
 */

// implement here the class of your operator. For window operators, you must modify the method getWindow and use it
// in your stream processing job.
public class Exercise8WindowOperator {
    public static WindowAssigner<Object, TimeWindow> getWindow(){
        // Add your code here
        return null;
    }
}
