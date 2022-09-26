package hpi.des.flink_tutorial.session2;

/*
Exercise 10) Write an operator capable of calculating the best pick-up location (location with largest tip sum) among all locations every 24 hours. The
best pick-up location is the location with the largest sum of tip ratios during a day. You must add the pick up location
id to your result.

Tip 10) The operator windowAll
(https://nightlies.apache.org/flink/flink-docs-release-1.12/dev/stream/operators/#windowall) is capable of windowing
data streams from all partitions. The unit tests expect you to use an aggregation function instead of the apply function
that we have been using so far
(https://nightlies.apache.org/flink/flink-docs-release-1.12/dev/stream/operators/#aggregations-on-windows). Use the
“TumblingEventTimeWindows.of()” window operator function and the “Time” class to bound the data stream and the
“AggregateFunction” operator to aggregate the results.
 */

public class Exercise10WindowProcessingOperator {
}
