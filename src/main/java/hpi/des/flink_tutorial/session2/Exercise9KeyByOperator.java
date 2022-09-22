package hpi.des.flink_tutorial.session2;

/*
Exercise 9) Add another operator to calculate the best average and total sum of tip ratios per passenger during a
day (24h) per pick-up location. You should keep the time of the best average in the output. Notice that the result of
“apply” is a DataStream, hence you will have to partition your data again before defining your window operator.

Tip 9) The operator reduce can be an efficient alternative to using apply to process incremental windows
(https://nightlies.apache.org/flink/flink-docs-release-1.12/dev/stream/operators/#reduce). The method keyBy is capable
of partitioning a stream based on a key
(https://nightlies.apache.org/flink/flink-docs-release-1.12/dev/stream/operators/#keyby)
 */

public class Exercise9KeyByOperator {
}
