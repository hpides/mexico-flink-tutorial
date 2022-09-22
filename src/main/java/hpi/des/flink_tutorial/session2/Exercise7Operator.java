package hpi.des.flink_tutorial.session2;

/*
Exercise 7) In this session we also want to take advantage of Flink’s parallelism. In order to do that, Flink partitions
the data and distributes them among its Task Managers. Flink will try to do that by itself, depending on the operator,
but the user can also define how she wants to partition the data. Write an operator that will partition the data based
on the value of the pick-up location id.

Tip 7) The method keyBy is capable of partitioning a stream based on a key
(https://nightlies.apache.org/flink/flink-docs-release-1.12/dev/stream/operators/#keyby and
https://nightlies.apache.org/flink/flink-docs-release-1.12/concepts/stateful-stream-processing.html#keyed-state).
 */

public class Exercise7Operator {
}
