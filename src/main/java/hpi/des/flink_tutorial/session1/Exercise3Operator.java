package hpi.des.flink_tutorial.session1;

/*
Exercise 3) In order to analyze the tips, we opt to use its tip ratio, i.e., its proportion to the total value of the
fare. For example, a tip of US$ 2 for a US$ 20 taxi ride has a tip ratio of <= 0.1. Write an operator that will add to
the end of the tuple at the 8th position the tip ratio of the taxi ride. If the tip has an invalid value (null, tip < 0)
or if the total value has an invalid value (null, total <= 0) you must discard the event.

Tip 3) The flatMap operator is capable of transforming a stream and outputting zero or many results from a single event
(https://nightlies.apache.org/flink/flink-docs-release-1.12/dev/stream/operators/#flatmap).
 */

// implement here the class of your operator. Check TransformSourceStreamOperator.java for an example. Don't change the name
// of the class because it is used in the testing.
public class Exercise3Operator {
}
